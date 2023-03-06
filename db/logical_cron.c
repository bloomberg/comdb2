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

#include "sql.h"
#include "views.h"
#include "locks.h"
#include "logical_cron.h"
#include "thread_malloc.h"

typedef struct logical_state {
    long long clock;
} logical_state_t;

/* this should be called under sched->mtx */
static int logical_is_exec_time(sched_if_t *impl, cron_event_t *event)
{
    logical_state_t *state = (logical_state_t *)impl->state;
    int rc = 0;
    assert(event);
    assert(state);

    rc = event->epoch <= state->clock;

    return rc;
}

static int logical_wait_next_event(sched_if_t *impl, cron_event_t *event)
{
    logical_state_t *state = (logical_state_t *)impl->state;
    struct errstat err = {0};
    cron_sched_t *sched = impl->sched;
    struct timespec ts;
    int rc;

    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += impl->default_sleep_idle;
    ts.tv_nsec = 0;

    rc = cron_timedwait(sched, &ts);

    /* refresh the counter here */
    state->clock = logical_cron_read_persistent(impl->name, &err);
    if (errstat_get_rc(&err) != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s error rc %d \"%s\"\n", __func__,
               errstat_get_rc(&err), errstat_get_str(&err));
    }

    return rc;
}

/* assert: read lock on crons.rwlock */
void logical_cron_incr(sched_if_t *impl)
{
    cron_sched_t *sched = impl->sched;
    logical_state_t *state = (logical_state_t *)impl->state;
    cron_lock(sched);
    state->clock++;
    cron_unlock(sched);
}

/* assert: read lock on crons.rwlock */
void logical_cron_set(sched_if_t *impl, unsigned long long val)
{
    cron_sched_t *sched = impl->sched;
    logical_state_t *state = (logical_state_t *)impl->state;
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

static char *logical_cron_describe(sched_if_t *impl)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Logical cron %s clock %lld", impl->name,
             ((logical_state_t *)impl->state)->clock);
    return strdup(msg);
}

#define LOGICAL_CRON_THREAD_MEMORY 1048576

static void *logical_cron_kickoff(struct cron_event *event, struct errstat *err)
{
    sched_if_t *schedif = event->schedif;
    logical_state_t *state = schedif->state;
    struct sql_thread *thd;
    const char *tablename = LOGICAL_CRON_SYSTABLE;

    logmsg(LOGMSG_INFO, "Starting logical cron %s\n", (char *)event->arg1);

    sql_mem_init(NULL);
    thread_memcreate(LOGICAL_CRON_THREAD_MEMORY);

    /* logical cron runs sql against cron_logical_cron */
    thd = start_sql_thread();
    if (!thd) {
        return NULL;
    }

    /* construct rootpage cache that includes only logical_cron_systable */
    get_copy_rootpages_selectfire(thd, 1, &tablename, NULL, NULL, 1);

    /* check to see if there is a persistent value for this scheduler */
    state->clock = logical_cron_read_persistent(schedif->name, err);
    if (errstat_get_rc(err) != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s error rc %d \"%s\"\n", __func__,
               errstat_get_rc(err), errstat_get_str(err));
    }

    return NULL;
}

/**
 * Restart a logical scheduler
 *
 */
int logical_cron_init(const char *sched_name, struct errstat *err)
{
    cron_sched_t *sched;
    char *name;
    sched_if_t intf = {0};
    uuid_t source_id;

    sched = cron_sched_byname(sched_name);
    if (sched) {
        /* re-initializing a logical cron need to clear existing events */
        cron_clear_queue(sched);
        return VIEW_NOERR;
    }

    if (logical_cron_create(
            &intf, logical_cron_describe,
            timepart_event_describe /*reusing old event sequence */)) {
        logmsg(LOGMSG_ERROR, "%s Malloc error!\n", __func__);
        return VIEW_ERR_GENERIC;
    }
    intf.name = strdup(sched_name);

    comdb2uuid(source_id);
    name = strdup(sched_name);

    /* create a logical schedule */
    sched = cron_add_event(NULL, sched_name, INT_MIN, logical_cron_kickoff,
                           name, NULL, NULL, NULL, &source_id, err, &intf);
    if (!sched) {
        logmsg(LOGMSG_USER, "failed to create logical scheduler %s!\n",
               sched_name);
        free(intf.name);
        free(name);
        return VIEW_ERR_GENERIC;
    }

    return VIEW_NOERR;
}

#define LOGICAL_CRON_SYSTABLE_SCHEMA                                           \
    "create table comdb2_logical_cron (name cstring(128) primary key, value "  \
    "int)"

unsigned long long logical_cron_read_persistent(const char *name,
                                                struct errstat *err)
{
    struct dbtable *db;
    long long counter = 0LL;
    char *query;
    struct sql_thread *thd = pthread_getspecific(query_info_key);

    if (!thd)
        return counter;

    db = get_dbtable_by_name(LOGICAL_CRON_SYSTABLE);
    /* if table doesn't exist, return */
    if (!db) {
        errstat_set_rcstrf(err, VIEW_ERR_GENERIC, "Table missing \"%s\"",
                           LOGICAL_CRON_SYSTABLE);
        logmsg(LOGMSG_ERROR, "Table missing \"%s\"\n", LOGICAL_CRON_SYSTABLE);
        logmsg(LOGMSG_ERROR, "Create it using \"%s\"\n",
               LOGICAL_CRON_SYSTABLE_SCHEMA);
        goto done;
    }

    query = sqlite3_mprintf("sElEct value frOm \"%w\" where name='%q'",
                            LOGICAL_CRON_SYSTABLE, name);
    if (!query) {
        errstat_set_rcstrf(err, VIEW_ERR_MALLOC, "%s malloc error\n", __func__);
        logmsg(LOGMSG_ERROR, "%s malloc error\n", __func__);
        goto done;
    }

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDONLY);
    counter = run_sql_thd_return_ll(query, thd, err);
    if (counter == LLONG_MIN)
        counter = 0;
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDONLY);

    sqlite3_free(query);

done:
    return counter;
}

char *logical_cron_update_sql(const char *name, long long value, int increment)
{
    char *query;

    if (increment)
        query = sqlite3_mprintf(
            "INSERT INTO \"%w\" (name, value) values ('%q', %lld) ON CONFLICT "
            "(name) DO "
            "UPDATE SET value="
            "coalesce((select value from \"%w\" where name='%q'), 0)"
            "+1 where name = '%q'",
            LOGICAL_CRON_SYSTABLE, name, value, LOGICAL_CRON_SYSTABLE, name,
            name);
    else
        query =
            sqlite3_mprintf("INSERT INTO \"%w\" (name, value) values ('%q', %lld) "
                            "ON CONFLICT (name) DO "
                            "UPDATE SET value=%lld where name = '%q'",
                            LOGICAL_CRON_SYSTABLE, name, value, value, name);
    return query;
}

int logical_cron_create_backend(void)
{
    return -1;
}
