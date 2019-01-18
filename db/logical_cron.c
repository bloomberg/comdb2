
typedef struct logical_state_impl {
    cron_if_t itf;
    unsigned long long clock;
} logical_state_impl_t;

static int logical_is_exec_time(cron_sched_t *sched, cron_event_t *event)
{
    assert(event);

    return event->epoch <= ((logical_state_impl_t*)sched->ifs).clock;
}

static int logical_wait_next_event(cron_sched_t *sched, cron_event_t *event)
{
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += sched->ifs->default_sleep_idle;
    ts.tv_nsec = 0;

    return pthread_cond_timedwait(&sched->cond, &sched->mtx, &ts);
}


pthread_mutex_t _crons_mtx = PTHREAD_MUTEX_INITIALIZER;


struct logical_cron_if {
    cron_if_t itf;
    unsigned long long clock;
};
/* assert: read lock on crons.rwlock */
void logical_cron_incr(cron_sched_t *sched)
{
    Pthread_mutex_lock(&sched->mtx);
    sched->logical++;
    Pthread_mutex_unlock(&sched->mtx);
}

/* assert: read lock on crons.rwlock */
void logical_cron_set(cron_sched_t *sched, unsigned long long val)
{
    Pthread_mutex_lock(&sched->mtx);
    sched->logical = val;
    Pthread_mutex_unlock(&sched->mtx);
}


static void* _logical_cron_test_callback(uuid_t source_id, void *arg1, void *arg2, void *arg3, 
                           struct errstat *err)
{
    cron_sched_t *sched = *(cron_sched_t**)arg3;
    uuidstr_t us;
    int rc;
    int *nrolls = (int*)arg2;
    

        /*
        sched->default_sleep_idle = bdb_attr_get(thedb->bdb_attr, 
                BDB_ATTR_CRON_LOGICAL_IDLE_SECS);
                */

    Pthread_mutex_lock(&_crons_mtx);
    logmsg(LOGMSG_USER, "Test callback %s %s %s %d\n", sched->name,
           comdb2uuidstr(source_id, us), (char*)arg1, *(int*)arg2);

    (*nrolls)++;
    rc = (cron_add_event(sched, NULL, *nrolls, _logical_cron_test_callback,
                arg1, arg2, arg3, (uuid_t*)&source_id, err, ) == NULL)?
        err->errval:VIEW_NOERR;
    Pthread_mutex_unlock(&_crons_mtx);
    if (rc) {
        logmsg(LOGMSG_USER, "Logical test unit failed iteration %d rc %d\n", *nrolls, rc);
    }
    
    return NULL;
}

int logical_cron_unit_test(FILE *out, const char *name)
{
    cron_sched_t *sched;
    uuid_t source_id;
    struct errstat err = {0};
    int nrolls = 1;
    logical_state_impl_t state = {0};

    state.type = CRON_LOGICAL;
    state.default_sleep_idle = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_CRON_LOGICAL_IDLE_SECS);
    state.is_exec_time = logical_is_exec_time;
    state.wait_next_event = logical_wait_next_event;
    state.copy = logical_state_copy;
    state.column = logical_column;


    comdb2uuid(source_id);

    Pthread_rwlock_rdlock(&crons.rwlock);
    /* create a logical schedule */
    sched = cron_add_event(NULL, name, 1, _logical_cron_test_callback, "testing",
                           &nrolls, &sched, &source_id, &err, (cron_ifs_t*)&state);
    Pthread_rwlock_unlock(&crons.rwlock);
    if(!sched) {
        logmsg(LOGMSG_USER, "failed to create logical scheduler!\n");
        return -1;
    }

    /* push output and rollout, a few times */
    

    logical_cron_set(sched);

    
    return 0;
}

