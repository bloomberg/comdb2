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

/********* SYSTABLE INTERFACE IMPLEMENTATION HERE *******************/

int timepart_systable_timepartitions_collect(void **data, int *nrecords)
{
    timepart_views_t *views = thedb->timepart_views;
    timepart_view_t *view;
    systable_timepartition_t *arr = NULL;
    int narr = 0;
    int rc = 0;
    uuidstr_t us;

    Pthread_rwlock_rdlock(&views_lk);
    arr = calloc(views->nviews, sizeof(systable_timepartition_t));
    if (!arr) {
        logmsg(LOGMSG_ERROR, "%s OOM %zu!\n", __func__,
               sizeof(systable_timepartition_t) * views->nviews);
        rc = -1;
        goto done;
    }
    for (narr = 0; narr < views->nviews; narr++) {
        view = views->views[narr];
        arr[narr].name = strdup(view->name);
        arr[narr].period = strdup(period_to_name(view->period));
        arr[narr].retention = view->retention;
        arr[narr].nshards = view->nshards;
        arr[narr].version =
            get_dbtable_by_name(view->shards[0].tblname)->tableversion;
        arr[narr].shard0name = strdup(view->shard0name);
        arr[narr].starttime = view->starttime;
        arr[narr].sourceid = strdup(comdb2uuidstr(view->source_id, us));
        if (!arr[narr].name || !arr[narr].period || !arr[narr].shard0name ||
            !arr[narr].sourceid) {
            logmsg(LOGMSG_ERROR, "%s OOM!\n", __func__);
            timepart_systable_timepartitions_free(arr, narr);
            narr = 0;
            arr = NULL;
            rc = -1;
            goto done;
        }
    }
done:
    Pthread_rwlock_unlock(&views_lk);
    *data = arr;
    *nrecords = narr;
    return rc;
}

void timepart_systable_timepartitions_free(void *arr, int nrecords)
{
    systable_timepartition_t *parr = (systable_timepartition_t *)arr;
    int i;

    for (i = 0; i < nrecords; i++) {
        if (parr[i].name)
            free(parr[i].name);
        if (parr[i].period)
            free(parr[i].period);
        if (parr[i].shard0name)
            free(parr[i].shard0name);
        if (parr[i].sourceid)
            free(parr[i].sourceid);
    }
    free(arr);
}

int timepart_systable_timepartshards_collect(void **data, int *nrecords)
{
    timepart_views_t *views = thedb->timepart_views;
    timepart_view_t *view;
    systable_timepartshard_t *arr = NULL;
    int nshard;
    int narr = 0;
    int nview;
    int rc = 0;

    Pthread_rwlock_rdlock(&views_lk);
    narr = 0;
    for (nview = 0; nview < views->nviews; nview++) {
        narr += views->views[nview]->nshards;
    }
    arr = calloc(narr, sizeof(systable_timepartshard_t));
    if (!arr) {
        logmsg(LOGMSG_ERROR, "%s OOM %zu!\n", __func__,
               sizeof(systable_timepartshard_t) * narr);
        narr = 0;
        rc = -1;
        goto done;
    }
    narr = 0;
    for (nview = 0; nview < views->nviews; nview++) {
        view = views->views[nview];

        for (nshard = 0; nshard < view->nshards; nshard++) {
            arr[narr].name = strdup(view->name);
            arr[narr].shardname = strdup(view->shards[nshard].tblname);
            arr[narr].low = view->shards[nshard].low;
            arr[narr].high = view->shards[nshard].high;
            if (!arr[narr].name || !arr[narr].shardname) {
                logmsg(LOGMSG_ERROR, "%s OOM\n", __func__);
                timepart_systable_timepartshards_free(arr, narr);
                narr = 0;
                arr = NULL;
                rc = -1;
            }
            narr++;
        }
    }
done:
    Pthread_rwlock_unlock(&views_lk);
    *data = arr;
    *nrecords = narr;
    return rc;
}

void timepart_systable_timepartshards_free(void *arr, int nrecords)
{
    systable_timepartshard_t *parr = (systable_timepartshard_t *)arr;
    int i;

    for (i = 0; i < nrecords; i++) {
        if (parr[i].name)
            free(parr[i].name);
        if (parr[i].shardname)
            free(parr[i].shardname);
    }
    free(arr);
}

int timepart_systable_timepartevents_collect(void **arr, int *nrecords)
{
    int allocsize = 0;
    return cron_systable_sched_events_collect(
        timepart_sched, (systable_cron_events_t **)arr, nrecords, &allocsize);
}

void timepart_systable_timepartevents_free(void *arr, int nrecords)
{
    cron_systable_events_free(arr, nrecords);
}
