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
#include "logical_cron.h"

typedef struct logical_state {
    unsigned long long clock;
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
    cron_sched_t *sched = impl->sched;
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += impl->default_sleep_idle;
    ts.tv_nsec = 0;

    return cron_timedwait(sched, &ts);
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

static char* logical_cron_describe(sched_if_t *impl)
{
    char msg[256];
    snprintf(msg, sizeof(msg), "Logical cron %s", impl->name);
    return strdup(msg);
}

static void *logical_cron_kickoff(uuid_t source_id, void *arg1, void *arg2, 
                                   void *arg3, struct errstat *err)
{
    logmsg(LOGMSG_INFO, "Starting logical cron %s\n", (char*)arg1);
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
        return VIEW_NOERR;
    }

    if (logical_cron_create(&intf, logical_cron_describe,
                timepart_event_describe /*reusing old event sequence */)) {
        logmsg(LOGMSG_ERROR, "%s Malloc error!\n", __func__);
        return VIEW_ERR_GENERIC;
    }
    intf.name = strdup(sched_name);

    comdb2uuid(source_id);

    /* create a logical schedule */
    name = strdup(sched_name);
    sched = cron_add_event(NULL, sched_name, 0, logical_cron_kickoff, 
                name, NULL,  NULL, &source_id, err, &intf);
    if (!sched) {
        logmsg(LOGMSG_USER, "failed to create logical scheduler %s!\n", sched_name);
        free(intf.name);
        free(name);
        return VIEW_ERR_GENERIC;
    }

    return VIEW_NOERR;
}

#define LOGICAL_CRON_SYSTABLE "comdb2_logical_cron"
#define LOGICAL_CRON_SYSTABLE_SCHEMA  \
    "create table comdb2_logical_cron (name cstring(128) primary key, value int, pad cstring(376) null) "

static int _check_systable(tran_type *tran, struct errstat *err)
{
    struct dbtable *db = get_dbtable_by_name_locked(tran, LOGICAL_CRON_SYSTABLE);
    if (!db) { 
        errstat_set_rcstrf(err, VIEW_ERR_GENERIC, "Table missing \"%s\"",
                LOGICAL_CRON_SYSTABLE);
        logmsg(LOGMSG_ERROR, "Table missing \"%s\"\n", LOGICAL_CRON_SYSTABLE);
        logmsg(LOGMSG_ERROR, "Create it using \"%s\"\n", LOGICAL_CRON_SYSTABLE_SCHEMA);
        return VIEW_ERR_GENERIC;
    }

    return VIEW_NOERR;
}


/**
 * Set a persistent logical counter 
 * If the counter "name" doesn't exist, it is created first
 *
 */
int logical_cron_bend_set(tran_type *tran, const char *name, 
        unsigned long long value, struct errstat *err)
{

    int rc = VIEW_NOERR;

    if((rc = _check_systable(tran, err)))
        return rc;

    return 0;
}

/**
 * Increment a persistent logical counter;
 * If the counter doesn't exists, it is created and set to 0
 *
 */
int logical_cron_bend_incr(tran_type *tran, const char *name,
        struct errstat *err)
{
    int rc = VIEW_NOERR;

    if((rc = _check_systable(tran, err)))
        return rc;

    return 0;
}

int _logical_cron_sql(struct sqlclntstate *clnt, const char *name,
        struct errstat *err, long long value, bool increment)
{
    int rc;
    char *query;

    if (increment)
        query = sqlite3_mprintf("UPDATE %s SET vAlUE = vAlUE+1 where name = %s",
                LOGICAL_CRON_SYSTABLE, name);
    else
        query = sqlite3_mprintf("UPDATE %s SET vAlUE = %lld  where name = %s",
                LOGICAL_CRON_SYSTABLE, value, name);

    clnt->dbtran.mode = TRANLEVEL_SERIAL;

    rc = run_internal_sql_clnt(clnt, query);
    if (rc) {
        errstat_set_rcstrf(err, rc, "failed to update the counter");
        rc = VIEW_ERR_GENERIC;
    } else {
        rc = VIEW_NOERR;
    }
    return rc;
}

int logical_cron_sql_incr(struct sqlclntstate *clnt, const char *name,
        struct errstat *err)
{
    return _logical_cron_sql(clnt, name, err, 0, 1);
}

int logical_cron_sql_set(struct sqlclntstate *clnt, const char *name,
    long long value, struct errstat *err)
{
    return _logical_cron_sql(clnt, name, err, value, 0);
}
