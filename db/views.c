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

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <string.h>
#include <pthread.h>
#include <memory_sync.h>

#include "comdb2.h"
#include "crc32c.h"
#include "sql.h"
#include "views.h"
#include "locks.h"
#include "schemachange.h"
#include "bdb_schemachange.h"
#include "str0.h"
#include "logmsg.h"
#include "cron.h"
#include "cron_systable.h"
#include "timepart_systable.h"
#include "logical_cron.h"
#include "sc_util.h"
#include "bdb_int.h"

#define VIEWS_MAX_RETENTION 1000

extern int gbl_is_physical_replicant;
int gbl_partitioned_table_enabled = 1;
int gbl_merge_table_enabled = 1;

struct timepart_shard {
    char *tblname; /* name of the table covering the shard, can be an alias */
    int low;  /* lowest value in this shard, including limit  [low, high) */
    int high; /* excluding limit, values up to this can be stored in this shard
                 [low, high) */
};
typedef struct timepart_shard timepart_shard_t;

struct timepart_view {
    char *name;                       /* name of the view, visible in sql */
    enum view_partition_period period; /* when do we rotate to a new shard */
    int retention;                    /* how many shards are preserved */
    int nshards;                      /* how many shards */
    timepart_shard_t *shards;         /* array of shard pointers */
    int version;      /* in-memory versioning, allowing single view refreshes */
    char *shard0name; /* original first table name length, used to generate
                         names */
    int starttime;    /* info about the beginning of the rollout */
    int roll_time;    /* cached time for next rollout */
    uuid_t source_id; /* identifier for view, unique as compared to name */
    enum TIMEPART_ROLLOUT_TYPE rolltype; /* add/drop shard, or truncate */
    int current_shard; /* where to insert; always 0 for add/drop rollout */
};

struct timepart_views {
    int nviews;              /* how many views we have */
    timepart_view_t **views; /* array of views */
    int preemptive_rolltime; /* seconds ahead of rollout when
                                I need to create the new shard */
    int rollout_delete_lag;  /* how long to wait after rolling
                                a new shard in before the oldest
                                one is removed, if past retention */
};

pthread_rwlock_t views_lk;

/*
 Cron scheduler
 */
cron_sched_t *timepart_sched;

static timepart_view_t *_get_view(timepart_views_t *views, const char *name);
static timepart_view_t *_get_view_index(timepart_views_t *views,
                                        const char *name, int *idx);
static int _generate_new_shard_name(const char *oldname, char *newname,
                                    int newnamelen, int nextnum, int maxshards,
                                    int testing, struct errstat *err);
static int _extract_shardname_index(const char *tblName,
                                    const char *originalName, int *span,
                                    int testing);
static int _views_do_op(timepart_views_t *views, const char *name,
                        int (*op)(timepart_views_t *, timepart_view_t *,
                                  struct errstat *),
                        struct errstat *err);
static char *_describe_row(const char *tblname, const char *prefix,
                           enum views_trigger_op op_type, struct errstat *err);
static void *timepart_cron_kickoff(struct cron_event *_, struct errstat *err);
static int _next_shard_exists(timepart_view_t *view, char *newShardName,
                              int newShardNameLen);
static void _remove_view_entry(timepart_views_t *views, int i);

void *_view_cron_phase1(struct cron_event *event, struct errstat *err);
void *_view_cron_phase2(struct cron_event *event, struct errstat *err);
void *_view_cron_phase3(struct cron_event *event, struct errstat *err);
void *_view_cron_new_rollout(struct cron_event *event, struct errstat *err);
static int _views_rollout_phase1(timepart_view_t *view, char **newShardName,
                                 struct errstat *err);
static int _views_rollout_phase2(timepart_view_t *view,
                                 const char *newShardName, int *timeNextRollout,
                                 char **removeShardName, struct errstat *err);
static int _views_rollout_phase3(const char *oldShardName, struct errstat *err);

static int _view_restart(timepart_view_t *view, struct errstat *err);
static int _view_restart_new_rollout(timepart_view_t *view,
                                     struct errstat *err);
int views_cron_restart(timepart_views_t *views);

static int _view_get_next_rollout(enum view_partition_period period,
                                  int retention, int startTime, int crtTime,
                                  int nshards, int back_in_time);
static int _generate_evicted_shard_name(timepart_view_t *view,
                                        int checked_number,
                                        char *evictedShardName,
                                        int evictedShardNameLen,
                                        int *rolloutTime, int retention);
static int _validate_view_id(timepart_view_t *view, uuid_t source_id, 
                             const char *str, struct errstat *err);
static void _handle_view_event_error(timepart_view_t *view, uuid_t source_id,
                                     struct errstat *err);
static void print_dbg_verbose(const char *name, uuid_t *source_id, const char *prefix, 
                              const char *fmt, ...);
static int _view_update_table_version(timepart_view_t *view, tran_type *tran);
static cron_sched_t *_get_sched_byname(enum view_partition_period period,
                                       const char *sched_name);
static void _view_unregister_shards_lkless(timepart_views_t *views,
                                           timepart_view_t *view);

static int _create_inmem_view(timepart_views_t *views, timepart_view_t *view,
                              struct errstat *err);
static void _view_find_current_shard(timepart_view_t *view);

enum _check_flags {
   _CHECK_ONLY_INITIAL_SHARD, _CHECK_ALL_SHARDS, _CHECK_ONLY_CURRENT_SHARDS
};

static timepart_view_t *_check_shard_collision(timepart_views_t *views,
                                               const char *tblname, int *indx,
                                               enum _check_flags flag);

int timepart_copy_access(bdb_state_type *bdb_state, void *tran, char *dst,
                         char *src, int acquire_schema_lk);

int timepart_copy_access(bdb_state_type *bdb_state, void *tran, char *dst,
                         char *src, int acquire_schema_lk);

char *timepart_describe(sched_if_t *_)
{
    return strdup("Time partition scheduler");
}

char *timepart_event_describe(sched_if_t *_, cron_event_t *event)
{
    const char *name;
    if (event->func == _view_cron_phase1)
        name = "AddShard";
    else if (event->func == _view_cron_phase2)
        name = "RollShards";
    else if (event->func == _view_cron_phase3)
        name = "DropShard";
    else if (event->func == _view_cron_new_rollout)
        name = "Truncate";
    else
        name = "Unknown";

    return strdup(name);
}

/**
 * Initialize the views
 *
 */
timepart_views_t *timepart_views_init(struct dbenv *dbenv)
{
    sched_if_t tpt_cron = {0};
    struct errstat xerr = {0};
    const char *name = "timepart_cron";

    time_cron_create(&tpt_cron, timepart_describe, timepart_event_describe);

    Pthread_rwlock_init(&views_lk, NULL);

    /* create the timepart scheduler */
    timepart_sched = cron_add_event(NULL, name, INT_MIN, timepart_cron_kickoff,
                                    NULL, NULL, NULL, NULL, NULL, &xerr,
                                    &tpt_cron);

    if (timepart_sched == NULL)
        return NULL;

    /* read the llmeta views, if any */
    dbenv->timepart_views = views_create_all_views();

    if (!dbenv->timepart_views) {
        return NULL;
    }

    return dbenv->timepart_views;
}

static int _get_preemptive_rolltime(timepart_view_t *view)
{
    if (IS_TIMEPARTITION(view->period)) {
        if (unlikely(view->period == VIEW_PARTITION_TEST2MIN))
            return 30;
        return thedb->timepart_views->preemptive_rolltime;
    }
    return 0;
}

static int _get_delete_lag(timepart_view_t *view)
{
    if (IS_TIMEPARTITION(view->period)) {
        if (unlikely(view->period == VIEW_PARTITION_TEST2MIN))
            return 5;
        return thedb->timepart_views->rollout_delete_lag;
    }
    return 0;
}

/**
 * Add timepart view, this is a schema change driven event
 *
 */
int timepart_add_view(void *tran, timepart_views_t *views,
                      timepart_view_t *view, struct errstat *err)
{
    char next_existing_shard[MAXTABLELEN + 1];
    int preemptive_rolltime = _get_preemptive_rolltime(view);
    int published = 0;
    char *tmp_str;
    int rc;
    int tm;

    if (view->period == VIEW_PARTITION_MANUAL) {
        if ((rc = logical_cron_init(view->name, err)) != 0) {
            errstat_set_strf(err, "Failed to create logical scheduler for %s",
                             view->name);
            errstat_set_rc(err, rc = VIEW_ERR_GENERIC);
            goto done;
        }
    }

    /* publish in llmeta and signal sqlite about it; we do have the schema_lk
       for the whole function call */
    rc = partition_llmeta_write(tran, view, 0, err);
    if (rc != VIEW_NOERR) {
        return rc;
    }
    published = 1;

    Pthread_rwlock_wrlock(&views_lk);

    /* create initial rollout */
    tm = _view_get_next_rollout(view->period, view->retention, view->starttime,
                                view->shards[0].low, view->nshards, 0);
    if (tm == INT_MAX) {
        errstat_set_strf(err, "Failed to compute next rollout time");
        errstat_set_rc(err, rc = VIEW_ERR_BUG);
        goto done;
    }

    rc = _next_shard_exists(view, next_existing_shard,
                            sizeof(next_existing_shard));
    if (rc == VIEW_ERR_EXIST) {
        errstat_set_strf(err, "Next shard %s overlaps existing table for %s",
                         next_existing_shard, view->name);
        errstat_set_rc(err, rc);
        goto done;
    } else if (rc != VIEW_NOERR) {
        errstat_set_strf(err, "Failed to check or generate the next shard");
        errstat_set_rc(err, rc);
        goto done;
    }

    get_dbtable_by_name(view->shard0name)->timepartition_name = view->name;

    view->roll_time = tm;

    if (!bdb_attr_get(thedb->bdb_attr, BDB_ATTR_TIMEPART_NO_ROLLOUT)) {
        print_dbg_verbose(view->name, &view->source_id, "III",
                          "Adding phase 1 at %d\n",
                          view->roll_time - preemptive_rolltime);

        rc = (cron_add_event(_get_sched_byname(view->period, view->name), NULL,
                             view->roll_time - preemptive_rolltime,
                             _view_cron_phase1, tmp_str = strdup(view->name),
                             NULL, NULL, NULL, &view->source_id, err, NULL) == NULL)
                 ? err->errval
                 : VIEW_NOERR;
        if (rc != VIEW_NOERR) {
            if (tmp_str)
                free(tmp_str);
            goto done;
        }
    } else {
        logmsg(LOGMSG_WARN,
               "Time partitions rollouts are stopped; no rollouts!\n");
    }

    rc = _create_inmem_view(views, view, err);
    if (rc != VIEW_NOERR)
        goto done;

    /* At this point, we have updated the llmeta (which gets replicated)
       and updated in-memory structure;  we are deep inside schema change,
       which will trigger updates for existing sqlite engine upon return;
       new sqlite engines will pick up new configuration just fine.

       NOTE:
       As it is now, changing partitions doesn't update any global counters:
          - bdb_bump_dbopen_gen()
          - gbl_analyze_gen
          - gbl_views_gen (at least this one should get updated)

       Even if it is too early, we signal here the schema has change.  If the
       transaction
       changing the partitions fails, we would generate a NOP refresh with no
       impact!
     */
    gbl_views_gen++;

done:
    if (rc) {
        if (published) {
            int irc = views_write_view(NULL, view->name, NULL, 1 /*unused*/);
            if (irc) {
                logmsg(
                    LOGMSG_ERROR,
                    "Failed %d to remove tpt %s llmeta during failed create\n",
                    irc, view->name);
            }
        }
    }
    Pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Change the number of shards retained
 *
 */
int timepart_view_set_retention(timepart_views_t *views, const char *name,
                                int retention)
{
    timepart_view_t *view;
    int rc;

    Pthread_rwlock_wrlock(&views_lk);

    view = _get_view(views, name);
    if (!view) {
        rc = VIEW_ERR_EXIST;
        goto done;
    }

    view->retention = retention;

    /* TODO: trigger a cleanup */

    rc = VIEW_NOERR;
done:
    Pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Change the future partition schedule
 *
 */
int timepart_view_set_period(timepart_views_t *views, const char *name,
                             enum view_partition_period period)
{
    timepart_view_t *view;
    int rc;

    Pthread_rwlock_wrlock(&views_lk);

    view = _get_view(views, name);
    if (!view) {
        rc = VIEW_ERR_EXIST;
        goto done;
    }

    view->period = period;

    /* TODO: trigger a rollout thread update */

    rc = VIEW_NOERR;
done:
    Pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Free a partime view; struct in invalid upon return
 *
 */
void timepart_free_view(timepart_view_t *view)
{
    int i;

    if (view->shards) {
        for (i = 0; i < view->nshards; i++) {
            free(view->shards[i].tblname);
        }
        free(view->shards);
    }
    free(view->name);
    free(view->shard0name);

    memset(view, 0xFF, sizeof(*view));

    free(view);
}

/**
 * Delete partime view
 *
 * NOTE: the tables are not affected, but the sqlite engines will
 * be updated and the access to data has to be direct, if any
 *
 */
int timepart_del_view(void *tran, timepart_views_t *views, const char *name)
{
    timepart_view_t *view;
    int i;
    int rc = VIEW_NOERR;

    Pthread_rwlock_wrlock(&views_lk);

    view = NULL;
    for (i = 0; i < views->nviews; i++) {
        if (strcasecmp(views->views[i]->name, name) == 0) {
            view = views->views[i];
            break;
        }
    }

    if (view) {

        _view_unregister_shards_lkless(views, view);

        timepart_free_view(view);

        _remove_view_entry(views, i);

        rc = views_write_view(tran, name, NULL, 1 /*unused*/);

        /* inform sqlite engines to wipe the view out */
        gbl_views_gen++;

        if (rc != VIEW_NOERR) {
            logmsg(LOGMSG_ERROR, "failed to delete view from llmeta\n");
            goto done;
        }
    } else {
        rc = VIEW_ERR_EXIST;
    }

done:
    Pthread_rwlock_unlock(&views_lk);

    return rc;
}

/* unlocked !*/
static void timepart_free_views_unlocked(timepart_views_t *views)
{
    int i;

    if (views->views) {
        for (i = 0; i < views->nviews; i++) {
            timepart_free_view(views->views[i]);
        }

        free(views->views);
    }
    bzero(views, sizeof(*views));
}

/**
 * Free all the views
 *
 */
static void timepart_free_views(timepart_views_t *views)
{
    Pthread_rwlock_wrlock(&views_lk);
    timepart_free_views_unlocked(views);
    Pthread_rwlock_unlock(&views_lk);
}

/**
 * Delete the oldest shard
 *
 */
int timepart_del_oldest_shard(timepart_view_t *view)
{
    return VIEW_ERR_UNIMPLEMENTED;
}

/*
 * UNLOCKED
 */
static timepart_view_t *_get_view_index(timepart_views_t *views,
                                        const char *name, int *idx)
{
    int i;

    for (i = 0; i < views->nviews; i++) {
        if (strcasecmp(views->views[i]->name, name) == 0) {
            if (idx)
                *idx = i;
            return views->views[i];
        }
    }
    return NULL;
}
static timepart_view_t *_get_view(timepart_views_t *views, const char *name)
{
    return _get_view_index(views, name, NULL);
}

static struct dbtable *_table_alias_fallback(timepart_view_t *view)
{
    struct dbtable *db = NULL;
    if (view->rolltype == TIMEPART_ROLLOUT_TRUNCATE) {
        db = get_dbtable_by_name(view->name);
        if (db) {
            /* create alias alias */
            timepart_alias_table(view, db);
        }
    }
    return db;
}

/**
 * Handle views change on replicants
 * - tran has the replication gbl_rep_lockid lockerid.
 *
 * NOTE: this is done from scdone_callback, so we have WRLOCK(schema_lk)
 */
int views_handle_replicant_reload(void *tran, const char *name)
{
    timepart_views_t *_views = thedb->timepart_views;
    timepart_view_t *view = NULL;
    struct dbtable *db;
    int rc = VIEW_NOERR;
    char *str = NULL;
    struct errstat xerr = {0};
    int i;

    Pthread_rwlock_wrlock(&views_lk);

    logmsg(LOGMSG_INFO, "Replicant updating views counter=%d\n", gbl_views_gen);

    /* the change to llmeta was propagated already */
    rc = views_read_view(tran, name, &str);
    if (rc == VIEW_ERR_EXIST) {
        /* this is a partition drop */
        view = NULL;
        goto alter_struct;
    } else if (rc != VIEW_NOERR || !str) {
        logmsg(LOGMSG_ERROR, "%s: failure to read views metadata!\n", __func__);
        goto done;
    }

    /* create the new in-memory view object */
    view = timepart_deserialize_view(str, &xerr);
    if (!view) {
        logmsg(LOGMSG_ERROR, "%s: view recreate error %d %s\n", __func__,
                xerr.errval, xerr.errstr);
        goto done;
    }

    /* double-checks... make sure all the tables exist */
    for (i = 0; i < view->nshards; i++) {
        db = get_dbtable_by_name(view->shards[i].tblname);
        if (!db) {
            struct dbtable *db2 = _table_alias_fallback(view);
            if (!db2) {
                logmsg(LOGMSG_ERROR,
                       "%s: unable to locate shard %s for view %s\n", __func__,
                       view->shards[i].tblname, view->name);
                timepart_free_view(view);
                view = NULL;
                goto done;
            }
            db = db2;
            /* we alias a table, we need to bump dbopen */
            create_sqlmaster_records(tran);
            create_sqlite_master(); /* create sql statements */

            BDB_BUMP_DBOPEN_GEN(views, "alias table");
        }
        db->tableversion = table_version_select(db, tran);
        db->timepartition_name = view->name;

        if (view->rolltype == TIMEPART_ROLLOUT_TRUNCATE) {
            /* the order of the shards does not dictate current shard,
             * determine the current shard here
             */
            _view_find_current_shard(view);
        }
    }

alter_struct:
    /* we need to destroy existing view, if any */
    /* locate the impacted view */
    for (i = 0; i < _views->nviews; i++) {
        if (strcasecmp(_views->views[i]->name, name) == 0) {

            if (!view) {
                /* this is drop view, unregister shards */
                _view_unregister_shards_lkless(_views, _views->views[i]);
            }
            timepart_free_view(_views->views[i]);

            if (view) {
                _views->views[i] = view;
            } else {
                /* drop view */
                _remove_view_entry(_views, i);
            }

            break;
        }
    }
    if (i >= _views->nviews && view) {
        /* this is really a new view */
        /* adding the view to the list */
        _views->views = (timepart_view_t **)realloc(
            _views->views, sizeof(timepart_view_t *) * (_views->nviews + 1));
        if (!_views->views) {
            logmsg(LOGMSG_ERROR, "%s Malloc OOM", __func__);
            _views->nviews = 0;
            goto done;
        }

        _views->views[_views->nviews] = view;
        _views->nviews++;
    }

    /* NOTE: this has to be done under schema change lock */
    /* NOTE2: if the above fails, this will be skipped, so existing sqlite
       engines
       will survive for awhile with consistent but stale views data */
    ++gbl_views_gen;

    /* mark the view version */
    if (view)
        view->version = gbl_views_gen;

    /* At this point, the in-memory matches llmeta update, and existing sqlite
       engines
       will be notified to check view versioning and update the mismatching ones
       */

    rc = VIEW_NOERR;

done:
    if (str)
        free(str);

    Pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Delete the oldest shard, if is beyond retention limit
 *
 * NOTE: this is under mutex!
 *
 * NOTE: see also views_rollout.  the lazy purger will become NOP.
 */
int views_purge(timepart_views_t *views, timepart_view_t *view,
                struct errstat *err)
{
    return _views_rollout_phase3(view->shards[view->nshards - 1].tblname, err);
}

/**
 * Manual partition roll
 *
 */
int views_do_rollout(timepart_views_t *views, const char *name)
{
    logmsg(LOGMSG_ERROR, "%s NOT IMPLEMENTED", __func__);
    return 0;
}

/**
 * Manual partition purge, if any
 *
 */
int views_do_purge(timepart_views_t *views, const char *name)
{
    struct errstat xerr = {0};
    int rc;

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);

    rdlock_schema_lk();

    rc = _views_do_op(views, name, views_purge, &xerr);

    unlock_schema_lk();

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

    return rc;
}

static int _shard_suffix_str_len(int maxshards)
{
    /* we need desired #shards + 1, to facilitate decoupled
       rollout (add and delete) */
    maxshards++;

    /* no sharding basically */
    if (maxshards < 10)
        return 1;
    else if (maxshards < 100)
        return 2;
    else if (maxshards < 1000)
        return 3;
    else
        abort(); /* infinite */
}

static int _extract_shardname_index(const char *tblName,
                                    const char *originalName, int *span,
                                    int testing)
{
    int nextNum;

    if (!strcasecmp(tblName, originalName)) {
        return 0; /* initial shard */
    }

    if (likely(!testing)) {
        nextNum = atoi(tblName + 1); /* skip $ */

        if (span) {
            char *_ = strchr(tblName, '_');
            if (_) {
                *span = _ - tblName - 1;
            }
        }
    } else {
        nextNum = atoi(&tblName[strlen(originalName)]);
        if (span) {
            *span = strlen(tblName) - strlen(originalName);
        }
    }

    return nextNum;
}

/** dummy version for now */
static int _generate_new_shard_name(const char *oldname, char *newname,
                                    int newnamelen, int nextnum, int maxshards,
                                    int testing, struct errstat *err)
{
    int suffix_len = 0;
    int len = 0;

    /* NOTE: in the testing mode, the name format is primitive, intended to
       generated
       predictable names that a testcase can check against it
       In non-testing mode, the name is a $num_csc2[shard0name+num]
       */
    if (unlikely(testing)) {
        /* get the length of the number sufix */
        suffix_len = _shard_suffix_str_len(maxshards);

        snprintf(newname, newnamelen, "%s%.*d", oldname, suffix_len, nextnum);
    } else {
        char hash[128];
        len = snprintf(hash, sizeof(hash), "%u%s", nextnum, oldname);
        len = crc32c((uint8_t *)hash, len);
        snprintf(newname, newnamelen, "$%u_%X", nextnum, len);
    }
    newname[newnamelen - 1] = '\0';

    return VIEW_NOERR;
}

static int _view_check_sharding(timepart_view_t *view, struct errstat *err)
{
    /* make sure retention is reasonable */
    if (view->retention >= VIEWS_MAX_RETENTION) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Retention too high for \"%s\"", view->name);
        return err->errval = VIEW_ERR_PARAM;
    }

    if (strlen(view->shard0name) + _shard_suffix_str_len(view->retention) >
        MAXTABLELEN) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Table %s name too long, no space for sharding",
                 view->shard0name);
        return err->errval = VIEW_ERR_PARAM;
    }

    return VIEW_NOERR;
}

static int _views_do_op(timepart_views_t *views, const char *name,
                        int (*op)(timepart_views_t *, timepart_view_t *,
                                  struct errstat *),
                        struct errstat *err)
{
    timepart_view_t *view;
    char *view_str;
    int view_str_len;

    int rc = VIEW_NOERR;

    Pthread_rwlock_wrlock(&views_lk);

    view = _get_view(views, name);

    if (!view) {
        errstat_set_strf(err, "%s: unable to find view %s\n", __func__, name);
        errstat_set_rc(err, rc = VIEW_ERR_EXIST);
        goto done;
    }

    rc = (*op)(views, view, err);
    if (rc != VIEW_NOERR) {
        goto done;
    }

    /* serialize existing view */
    view_str = NULL;
    view_str_len = 0;
    rc = timepart_serialize_view(view, &view_str_len, &view_str, 0);
    if (rc != VIEW_NOERR) {
        errstat_set_strf(err, "Failed to reserialize view %s", view->name);
        errstat_set_rc(err, rc = VIEW_ERR_BUG);
        goto done;
    }

    /* save the view */
    rc = views_write_view(NULL, view->name, view_str, 1);
    if (rc != VIEW_NOERR) {
        errstat_set_strf(err, "Failed to llmeta save view %s", view->name);
        errstat_set_rc(err, rc = VIEW_ERR_LLMETA);
        goto done;
    }

    /*all done, lets tell the world*/

    /* NOTE: we already have the lock */
    ++gbl_views_gen;
    view->version = gbl_views_gen;

done:

    Pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Phase 1 of the rollout, create the next table
 *
 */
void *_view_cron_phase1(struct cron_event *event, struct errstat *err)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    timepart_view_t *view;
    char *arg1 = (char *)event->arg1;
    char *arg2 = (char *)event->arg2;
    char *arg3 = (char *)event->arg3;
    char *name = arg1;
    char *pShardName = NULL;
    int rc = 0;
    char *tmp_str;
    int run;
    int shardChangeTime = 0;

    if (!name) {
        errstat_set_rc(err, VIEW_ERR_BUG);
        errstat_set_strf(err, "%s no name?", __func__);
        run = 0;
        goto done;
    }

    assert(arg2 == NULL);
    assert(arg3 == NULL);

    run = (!gbl_exit);
    if (run && (thedb->master != gbl_myhostname || gbl_is_physical_replicant))
        run = 0;

    if (run) {
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);

        BDB_READLOCK(__func__);
        wrlock_schema_lk();
        Pthread_rwlock_wrlock(&views_lk);

        view = _get_view(thedb->timepart_views, name);
        if (!view) {
            errstat_set_strf(err, "View %s missing", name);
            errstat_set_rc(err, rc = VIEW_ERR_BUG);
            goto done;
        }

        print_dbg_verbose(view->name, &view->source_id, "TTT",
                          "Running phase1 at %u arg1=%p (name=\"%s\") arg2=%p "
                          "arg3=%p\n",
                          comdb2_time_epoch(), arg1, (char *)arg1, arg2, arg3);

        /* this is a safeguard! we take effort to schedule cleanup of 
        a dropped partition ahead of everything, but jic ! */
        if (unlikely(
                _validate_view_id(view, event->source_id, "phase 1", err))) {
            rc = VIEW_ERR_GENERIC; /* silently dropped obsolete event, don't
                                      leak wr sc */
            goto done;
        }

        if (view->nshards > view->retention) {
            errstat_set_strf(err, "view %s already rolled, missing purge?",
                             view->name);
            errstat_set_rc(err, rc = VIEW_ERR_BUG);
            goto done;
        }

        rc = _views_rollout_phase1(view, &pShardName, err);
        shardChangeTime = view->roll_time;

        /* do NOT override rc at this point! */
    }

done:
    if (run) {
        Pthread_rwlock_unlock(&views_lk);
        /* commit_adaptive unlocks the schema-lk */
        if (rc != VIEW_NOERR)
            unlock_schema_lk();
        csc2_free_all();
        BDB_RELLOCK();
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

        /* queue the next event, done with the mutex released to avoid
           racing against scheduler callback runs */
        if (rc == VIEW_NOERR) {
            print_dbg_verbose(view->name, &view->source_id, "LLL",
                              "Adding phase 2 at %d for %s\n",
                              shardChangeTime, pShardName);

            if (cron_add_event(_get_sched_byname(view->period, view->name),
                               NULL, shardChangeTime, _view_cron_phase2,
                               tmp_str = strdup(name), pShardName, NULL,
                               NULL, &view->source_id, err, NULL) == NULL) {
                logmsg(LOGMSG_ERROR, "%s: failed rc=%d errstr=%s\n", __func__,
                        err->errval, err->errstr);
                if (tmp_str)
                    free(tmp_str);
                if (pShardName)
                    free(pShardName);
            }
        } else {
            _handle_view_event_error(view, event->source_id, err);
            if (pShardName)
                free(pShardName);
        }
    }

    return NULL;
}

static int
_view_cron_schedule_next_rollout(timepart_view_t *view, int timeCrtRollout,
                                 int timeNextRollout, char *removeShardName,
                                 const char *name, struct errstat *err)
{
    int delete_lag = _get_delete_lag(view);
    int preemptive_rolltime = _get_preemptive_rolltime(view);
    char *tmp_str;
    int tm;

    if (removeShardName) {
        /* we need to schedule a purge */
        tm = timeCrtRollout + delete_lag;

        print_dbg_verbose(view->name, &view->source_id, "LLL",
                          "Adding phase 3 at %d for %s\n", 
                          tm, removeShardName);

        if (cron_add_event(_get_sched_byname(view->period, view->name), NULL,
                           tm, _view_cron_phase3, removeShardName, NULL, NULL,
                           NULL, &view->source_id, err, NULL) == NULL) {
            logmsg(LOGMSG_ERROR, "%s: failed rc=%d errstr=%s\n", __func__,
                    err->errval, err->errstr);
            free(removeShardName);
            return FDB_ERR_GENERIC;
        }
    }

    /* schedule the next rollout as well */
    tm = timeNextRollout - preemptive_rolltime;
    print_dbg_verbose(view->name, &view->source_id, "LLL",
                      "Adding phase 1 at %d\n", 
                      tm);

    if (cron_add_event(_get_sched_byname(view->period, view->name), NULL, tm,
                       _view_cron_phase1, tmp_str = strdup(name), NULL, NULL,
                       NULL, &view->source_id, err, NULL) == NULL) {
        if (tmp_str) {
            free(tmp_str);
        }
        return FDB_ERR_GENERIC;
    }

    return FDB_NOERR;
}

/**
 * Phase 2 of the rollout, add the table to the view
 *
 */
void *_view_cron_phase2(struct cron_event *event, struct errstat *err)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    timepart_view_t *view;
    char *arg1 = (char *)event->arg1;
    char *arg2 = (char *)event->arg2;
    char *arg3 = (char *)event->arg3;
    char *name = arg1;
    char *pShardName = (char *)arg2;
    int run = 0;
    int timeNextRollout = 0;
    int timeCrtRollout = 0;
    char *removeShardName = NULL;
    int rc = 0;
    int bdberr;

    if (!name || !pShardName) {
        errstat_set_rc(err, VIEW_ERR_BUG);
        errstat_set_strf(err, "%s no name or shardname?", __func__);
        run = 0;
        goto done;
    }

    assert(arg3 == NULL);

    run = (!gbl_exit);
    if (run && (thedb->master != gbl_myhostname || gbl_is_physical_replicant))
        run = 0;

    if (run) {
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
        rdlock_schema_lk();
        Pthread_rwlock_wrlock(&views_lk);

        view = _get_view(thedb->timepart_views, name);
        if (!view) {
            errstat_set_rc(err, rc = VIEW_ERR_BUG);
            errstat_set_strf(err, "View %s missing", name);
            goto done;
        }

        print_dbg_verbose(view->name, &view->source_id, "TTT",
                          "Running phase2 at %u arg1=%p (name=\"%s\") arg2=%p "
                          "(shard=\"%s\") arg3=%p\n",
                          comdb2_time_epoch(), arg1,
                          (arg1) ? (char *)arg1 : "NULL", arg2,
                          (arg2) ? (char *)arg2 : "NULL", arg3);

        /* this is a safeguard! we take effort to schedule cleanup of 
        a dropped partition ahead of everything, but jic ! */
        if (unlikely(
                _validate_view_id(view, event->source_id, "phase 2", err))) {
            rc = VIEW_ERR_BUG;
            goto done;
        }


        if (view->nshards > view->retention) {
            errstat_set_rc(err, rc = VIEW_ERR_BUG);
            errstat_set_strf(err, "view %s already rolled, missing purge?",
                             view->name);
            goto done;
        }

        /* lets save this */
        timeCrtRollout = view->roll_time;

        BDB_READLOCK(__func__);

        rc = _views_rollout_phase2(view, pShardName, &timeNextRollout,
                                   &removeShardName, err);

        if (rc == VIEW_NOERR) {
            /* send signal to replicants that partition configuration changed */
            rc = bdb_llog_views(
                thedb->bdb_env, view->name,
                0 /*not sure it is safe here to wait, so don't*/, &bdberr);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, 
                        "%s -- bdb_llog_views view %s rc:%d bdberr:%d\n",
                        __func__, view->name, rc, bdberr);
            }
        }

        BDB_RELLOCK();

        /* tell the world */
        gbl_views_gen++;
        view->version = gbl_views_gen;

        /* do NOT override rc at this point! */
    }

done:
    if (run) {
        Pthread_rwlock_unlock(&views_lk);
        unlock_schema_lk();
        csc2_free_all();
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

        /*  schedule next */
        if (rc == VIEW_NOERR) {
            rc = _view_cron_schedule_next_rollout(view, timeCrtRollout,
                                                  timeNextRollout,
                                                  removeShardName, name, err);
            return NULL;
        } else {
            _handle_view_event_error(view, event->source_id, err);
        }
    }

    return NULL;
}

/**
 * Phase 3 of the rollout, add the table to the view
 *
 */
void *_view_cron_phase3(struct cron_event *event, struct errstat *err)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    char *arg1 = (char *)event->arg1;
    char *arg2 = (char *)event->arg2;
    char *arg3 = (char *)event->arg3;
    char *pShardName = arg1;
    int run = 0;
    int rc;

    print_dbg_verbose(NULL, NULL, "TTT",
                      "Running phase3 at %u arg1=%p arg2=%p arg3=%p\n",
                      comdb2_time_epoch(), arg1, arg2, arg3);

    if (!pShardName) {
        errstat_set_rc(err, VIEW_ERR_BUG);
        errstat_set_strf(err, "%s no shardname?", __func__);
        run = 0;
        goto done;
    }

    run = (!gbl_exit);
    if (run && (thedb->master != gbl_myhostname || gbl_is_physical_replicant))
        run = 0;

    if (run) {
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
        BDB_READLOCK(__func__);
        wrlock_schema_lk();
        Pthread_rwlock_wrlock(&views_lk); /* I might decide to not lock this */

        rc = _views_rollout_phase3(pShardName, err);
        if (rc != VIEW_NOERR) {
            logmsg(LOGMSG_ERROR, "%s: phase 3 failed rc=%d errstr=%s\n", __func__,
                    err->errval, err->errstr);
        }

        Pthread_rwlock_unlock(&views_lk);
        if (rc != VIEW_NOERR)
            unlock_schema_lk();
        csc2_free_all();
        BDB_RELLOCK();
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);
    }
done:
    return NULL;
}

static char* comdb2_partition_info_locked(const char *partition_name, 
                                          const char *option)
{
    timepart_views_t *views = thedb->timepart_views;
    timepart_view_t *view;
    char *ret_str = NULL;
    int i;
    int ret_len;
    int crt_len;
    int is_check = 0;
    struct dbtable *db;
    char *check_rep="";


    view = _get_view(views, partition_name);
    if (!view)
        goto done;

    if(!strcmp(option, "tables") || (is_check=!strcmp(option, "check"))) {
        ret_len = 0;
        for (i = 0; i < view->nshards; i++) {
            ret_len += strlen(view->shards[i].tblname);
            ret_len += 1; /* either a separator ; or terminal 0 */
            if(is_check)
                ret_len += 128; /* optional qualifier per shard */
        }

        ret_str = (char *)malloc(ret_len);
        if (!ret_str)
            goto done;

        crt_len = 0;
        for (i = 0; i < view->nshards; i++) {
            if(is_check) {
                db = get_dbtable_by_name(view->shards[i].tblname);
                if(!db) {
                    check_rep = " [MISSING!]";
                } else {
                    check_rep = "";
                }
            }

            snprintf(ret_str + crt_len, ret_len - crt_len, 
                     "%s%s%s", view->shards[i].tblname, check_rep, 
                     (i < view->nshards) ? ";" : "");
            crt_len += strlen(view->shards[i].tblname);
            crt_len+=strlen(check_rep);
            crt_len += 1;
        }
        assert(crt_len <= ret_len);
        ret_str[crt_len-1]='\0';
    } else {
        /* cson*/

        ret_len = 0;
        ret_str = NULL;
        if (timepart_serialize_view(view, &ret_len, &ret_str, 1) !=
            VIEW_NOERR) {
            if (ret_str)
                free(ret_str);
            ret_str = NULL;
        }
    }

done:
    return ret_str;
}

/* needs lock on views_lk */
static timepart_view_t* _check_shard_collision(timepart_views_t *views, 
                                               const char *tblname, int *indx,
                                               enum _check_flags flag)
{
    timepart_view_t *view;
    int i,j;

    for(i=0; i<views->nviews; i++) {
        view = views->views[i];
        if(flag == _CHECK_ALL_SHARDS ||
                flag == _CHECK_ONLY_INITIAL_SHARD) {
            if(!strcasecmp(tblname, view->shard0name)) {
                *indx = -1;
                return view;
            }
        }

        if(flag == _CHECK_ALL_SHARDS ||
           flag == _CHECK_ONLY_CURRENT_SHARDS) {
            for(j=0;j<view->nshards;j++) {
                if(!strcasecmp(tblname, view->shards[j].tblname))
                {
                    *indx = j;
                    return view;
                }
            }
        }
    }

    return NULL;
}

/**
 * Check if the name already exists as a table or as a shard!
 *
 */
int comdb2_partition_check_name_reuse(const char *tblname, char **partname, int *indx)
{
    timepart_views_t *views = thedb->timepart_views;
    timepart_view_t *view;
    int rc = VIEW_NOERR;

    Pthread_rwlock_rdlock(&views_lk);

    view = _check_shard_collision(views, tblname, indx, _CHECK_ALL_SHARDS);
    if(view) {
        if(*indx==-1)
            *partname = strdup(view->shard0name);
        else
            *partname = strdup(view->shards[*indx].tblname);

        rc = VIEW_ERR_EXIST;
    }

    Pthread_rwlock_unlock(&views_lk);
    return rc;
}

/**
 * List all partitions currently configured 
 *
 */
void comdb2_partition_info_all(const char *option)
{
    timepart_views_t *views = thedb->timepart_views;
    timepart_view_t *view;
    int i;
    char *info;

    rdlock_schema_lk(); /* prevent race with partitions sc */
    Pthread_rwlock_rdlock(&views_lk);

    for(i=0; i<views->nviews; i++) {
        view = views->views[i];

        info = comdb2_partition_info_locked(view->name, option);

        if(!info) {
            logmsg(LOGMSG_ERROR, "Partition \"%s\" has problems!\n", view->name);
        } else {
            logmsg(LOGMSG_USER, "Partition \"%s\":\n%s\n", view->name, info);
        }
    }

    Pthread_rwlock_unlock(&views_lk);
    unlock_schema_lk();
}

/**
 * Returns various information about a partition, based on option
 * Things like CSON representation for the partition, list of tables ...
 *
 * Return, if any, is malloc-ed!
 */
char* comdb2_partition_info(const char *partition_name, const char *option)
{
    char *ret_str = NULL;

    Pthread_rwlock_rdlock(&views_lk);

    ret_str = comdb2_partition_info_locked(partition_name, option);

    Pthread_rwlock_unlock(&views_lk);

    return ret_str;
}

/**
 * Dump the timepartition json configuration
 * Used for schema copy only
 * Returns: 0 - no tps; 1 - has tps
 *
 */
int timepart_dump_timepartitions(FILE *dest)
{
    timepart_views_t *views = thedb->timepart_views;
    char *info;
    int has_tp = 0;

    Pthread_rwlock_rdlock(&views_lk);

    has_tp = views->nviews > 0;

    if (has_tp) {
        info = views_read_all_views();
        if (info) {
            fprintf(dest, "%s", info);
            free(info);
        } else {
            logmsg(
                LOGMSG_ERROR,
                "Cannot get timepartition configuration string from llmeta\n");
            has_tp = -1;
        }
    }

    Pthread_rwlock_unlock(&views_lk);

    return has_tp;
}

static char *_describe_row(const char *tblname, const char *prefix,
                           enum views_trigger_op op_type, struct errstat *err)
{
    struct dbtable *gdb;
    char *cols_str;
    char *tmp_str;
    int i;
    char *in_default;

    assert(op_type == VIEWS_TRIGGER_QUERY || op_type == VIEWS_TRIGGER_INSERT ||
           op_type == VIEWS_TRIGGER_UPDATE);

    gdb = get_dbtable_by_name(tblname);
    if (!gdb) {
        err->errval = VIEW_ERR_BUG;
        snprintf(err->errstr, sizeof(err->errstr), "Missing shard %s???\n",
                 tblname);
        return NULL;
    }

    cols_str = sqlite3_mprintf("%s", (prefix) ? prefix : "");
    for (i = 0; i < gdb->schema->nmembers; i++) {
        /* take care of default fields */
        if (!(op_type == VIEWS_TRIGGER_INSERT &&
              gdb->schema->member[i].in_default))

        {
            tmp_str = sqlite3_mprintf(
                "%s%s\"%w\"%s%w%s%s", cols_str,
                (op_type == VIEWS_TRIGGER_INSERT) ? "new." : "",
                gdb->schema->member[i].name,
                (op_type == VIEWS_TRIGGER_UPDATE) ? "=new.\"" : "",
                (op_type == VIEWS_TRIGGER_UPDATE) ? gdb->schema->member[i].name
                                                  : "",
                (op_type == VIEWS_TRIGGER_UPDATE) ? "\"" : "",
                (i < (gdb->schema->nmembers - 1)) ? ", " : "");
        } else {
            in_default = sql_field_default_trans(&gdb->schema->member[i], 0);
            if (!in_default)
                goto malloc;

            tmp_str =
                sqlite3_mprintf("%scoalesce(new.\"%w\", %s)%s", cols_str,
                                gdb->schema->member[i].name, in_default,
                                (i < (gdb->schema->nmembers - 1)) ? ", " : "");
            sqlite3_free(in_default);
        }

        sqlite3_free(cols_str);
        if (!tmp_str) {
            goto malloc;
        }
        cols_str = tmp_str;
    }

    errstat_set_rc(err, VIEW_NOERR);

    return cols_str;

malloc:
    err->errval = VIEW_ERR_MALLOC;
    snprintf(err->errstr, sizeof(err->errstr), "Out of memory\n");
    return NULL;
}

static void *timepart_cron_kickoff(struct cron_event *_, struct errstat *err)
{
    logmsg(LOGMSG_INFO, "Starting views cron job\n");
    return NULL;
}

static int _views_rollout_phase1(timepart_view_t *view, char **pShardName,
                                 struct errstat *err)
{
    char newShardName[MAXTABLELEN + 1];
    int rc;

    *pShardName = NULL;

    /* make sure the stale oldest shard was rollout */
    if (view->nshards == view->retention + 1) {
        errstat_set_strf(err,
                         "too fast rollout, please purge old shards for \"%s\"",
                         view->name);
        errstat_set_rc(err, rc = VIEW_ERR_PURGE);
        return rc;
    }

    rc = _next_shard_exists(view, newShardName, sizeof(newShardName));
    if (rc != VIEW_NOERR) {
        errstat_set_rc(err, rc);
        if (rc == VIEW_ERR_EXIST) {
            errstat_set_strf(err, "Cannot rollout, next shard exists");
            logmsg(LOGMSG_ERROR, "%s Cannot rollout, next shard %s exists",
                   view->name, newShardName);
        } else {
            errstat_set_strf(err, "Cannot rollout, failed to get next name");
        }

        return rc;
    }

    /* add the table, using the same configuration as the older tables */
    rc = sc_timepart_add_table(view->shards[0].tblname, newShardName, err);
    if (rc != SC_VIEW_NOERR) {
        return err->errval;
    }

    *pShardName = strdup(newShardName);

    return VIEW_NOERR;
}

/**
 * Insert a shard in the view structure, based on an existing new table
 *"newShardName",
 *
 */
static int _views_rollout_phase2(timepart_view_t *view,
                                 const char *newShardName, int *timeNextRollout,
                                 char **removeShardName, struct errstat *err)
{
    tran_type *tran;
    int bdberr = 0;
    int rc;

    /* are we at stable regime (full retention)? */
    if (view->nshards == view->retention) {
        /* we will queue oldest shard for removal */
        *removeShardName = strdup(view->shards[view->nshards - 1].tblname);
        if (!(*removeShardName)) {
            goto oom;
        }
        memmove(&view->shards[1], &view->shards[0],
                sizeof(view->shards[0]) * (view->nshards - 1));
    } else {
        /* make space for newest shard */
        view->shards = (timepart_shard_t *)realloc(
            view->shards, (view->nshards + 1) * sizeof(timepart_shard_t));
        if (!view->shards) {
            goto oom;
        }
        *removeShardName = NULL;
        memmove(&view->shards[1], &view->shards[0],
                sizeof(view->shards[0]) * view->nshards);
        view->nshards++;
    }

    /* set in the newest shard */
    view->shards[0].tblname = strdup(newShardName);
    if (!view->shards[0].tblname) {
        goto oom;
    }
    view->shards[0].low = view->roll_time;
    if (view->retention > 1)
        view->shards[1].high = view->roll_time;
    else
        view->shards[0].high = INT_MAX;
    /* we we purge oldest shard, we need to adjust the min of current oldest */
    view->shards[view->nshards - 1].low = INT_MIN;

    /* the only way to move 1 shard partitions forward is to continuously update
       the start time */
    if (view->retention == 1)
        view->starttime = view->roll_time;

    /* we need to schedule the next rollout */
    *timeNextRollout =
        _view_get_next_rollout(view->period, view->retention, view->starttime,
                               view->shards[0].low, view->nshards, 0);
    if ((*timeNextRollout) == INT_MAX) {
        errstat_set_strf(err, "Failed to compute next rollout time");
        return err->errval = VIEW_ERR_BUG;
    }
    view->roll_time = *timeNextRollout;

    tran = bdb_tran_begin(thedb->bdb_env, NULL, &bdberr);
    if (!tran || bdberr) {
        goto oom;
    }
    /* update the version of the table */
    rc = _view_update_table_version(view, tran);
    if (rc != VIEW_NOERR) {
        bdb_tran_abort(thedb->bdb_env, tran, &bdberr);
        return err->errval = rc;
    }

    /* time to make this known to the world */
    rc = partition_llmeta_write(tran, view, 1, err);
    if (rc != VIEW_NOERR) {
        bdb_tran_abort(thedb->bdb_env, tran, &bdberr);
        return err->errval;
    }

    rc = bdb_tran_commit(thedb->bdb_env, tran, &bdberr);
    if (rc || bdberr) {
        return err->errval = VIEW_ERR_LLMETA;
    }

    return err->errval = VIEW_NOERR;

oom:
    errstat_set_strf(err, "OOM Malloc");
    return err->errval = VIEW_ERR_MALLOC;
}

static int _views_rollout_phase3(const char *oldestShardName,
                                 struct errstat *err)
{
    int rc;

    /* do schema change to drop table */
    rc = sc_timepart_drop_table(oldestShardName, err);
    if (rc != SC_VIEW_NOERR) {
        return err->errval;
    }

    return err->errval = VIEW_NOERR;
}

static int _schedule_drop_shard(timepart_view_t *view,
                                const char *evicted_shard, int evict_time,
                                struct errstat *err)
{
    char *tmp_str1;
    int rc;

    print_dbg_verbose(view->name, &view->source_id, "RRR",
                      "Adding phase 3 at %d for %s\n", evict_time,
                      evicted_shard);

    /* we missed phase 3, queue it */
    rc = (cron_add_event(_get_sched_byname(view->period, view->name), NULL,
                         evict_time, _view_cron_phase3,
                         tmp_str1 = strdup(evicted_shard), NULL, NULL, NULL,
                         NULL, err, NULL) == NULL)
             ? err->errval
             : VIEW_NOERR;

    if (rc != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed rc=%d errstr=%s\n", __func__,
               err->errval, err->errstr);
        free(tmp_str1);
        return rc;
    }

    return 0;
}

static int _get_biggest_shard_number(timepart_view_t *view, int *oldest,
                                     int *newest)
{
    int max = 0;
    int crt;
    int i;
    int span = 0;

    for (i = 0; i < view->nshards; i++) {
        crt = _extract_shardname_index(view->shards[i].tblname,
                                       view->shard0name, &span,
                                       view->period == VIEW_PARTITION_TEST2MIN);
        if (i == 0 && newest)
            *newest = crt;
        if (i == view->nshards - 1 && oldest)
            *oldest = crt;

        if (span != 0) {
            /* mild optimization to recover large initial retentions reduced to
               much smaller retentions */
            int larger = 1;
            while (--span)
                larger *= 10;
            if (crt < 9) {
                if (larger == 100)
                    crt = 99;
                else if (larger == 1000)
                    crt = 999;
                else if (larger >= 10000)
                    crt = VIEWS_MAX_RETENTION;
            } else if (crt < 99) {
                if (larger == 1000)
                    crt = 999;
                else if (larger >= 10000)
                    crt = VIEWS_MAX_RETENTION;
            }
        }
        if (crt > max)
            max = crt;
    }

    return max;
}

/* done under mutex */
static int _view_restart(timepart_view_t *view, struct errstat *err)
{
    int preemptive_rolltime = _get_preemptive_rolltime(view);
    char next_existing_shard[MAXTABLELEN + 1];
    char evicted_shard[MAXTABLELEN + 1];
    struct dbtable *evicted_db;
    int evicted_time = 0;
    int tm;
    int rc;
    char *tmp_str1;
    char *tmp_str2;
    int evicted_shard0;
    int i;

    if (view->period == VIEW_PARTITION_MANUAL) {
        logical_cron_init(view->name, err);
    }

    evicted_shard0 = 0;
    /* if we are NOT in filling stage, recover phase 3 first */
    if ((view->nshards == view->retention) &&
        /* if the oldest shard is the initial table, nothing to do ! */
        (strcasecmp(view->shard0name,
                    view->shards[view->retention - 1].tblname))) {
        /* get the maximum index from the existing rows; use that number to
           assert the presumptive previous shard range; check from the newest to
           the oldest, modulo presumptive range */

        int oldest = 0, newest = 0, crt;
        int pres_range = _get_biggest_shard_number(view, &oldest, &newest) + 1;

        i = 1;
        while ((crt = (newest + i) % (pres_range + 1)) != oldest) {
            /* check if the previously evicted shard still exists */
            rc = _generate_evicted_shard_name(view, crt, evicted_shard,
                                              sizeof(evicted_shard),
                                              &evicted_time, view->nshards);
            if (rc != VIEW_NOERR) {
                errstat_set_strf(err, "Failed to generate evicted shard name");
                return err->errval = VIEW_ERR_BUG;
            }

            evicted_db = get_dbtable_by_name(evicted_shard);
            if (!evicted_db) {
                /* there is one exception here; during the initial reach of
                   "retention" shards, the phase 3 that can be lost is for
                   shard0name, not "$0_..." If "$0_..." is missing, check also
                   the initial shard, so we don't leak it */
                if (strncasecmp(evicted_shard, "$0_", 3) == 0) {
                    evicted_db = get_dbtable_by_name(view->shard0name);
                    if (evicted_db) {
                        strncpy(evicted_shard, view->shard0name,
                                sizeof(evicted_shard));
                        evicted_shard0 = 1;
                    }
                }
            }

            if (evicted_db) {
                /* we cannot jump into chron scheduler keeping locks that chron
                   functions
                   might acquire */
                Pthread_rwlock_unlock(&views_lk);

                rc = _schedule_drop_shard(view, evicted_shard,
                                          evicted_time - preemptive_rolltime,
                                          err);

                /* get back views global lock */
                Pthread_rwlock_wrlock(&views_lk);
            }

            i++;
        }
   }

   /* recover phase 2 */
   tm = _view_get_next_rollout(view->period, view->retention, view->starttime,
                               view->shards[0].low, view->nshards, 0);
   if (tm == INT_MAX) {
       errstat_set_strf(err, "Failed to compute next rollout time");
       return err->errval = VIEW_ERR_BUG;
    }

   /**
    * Currently, if we have found an existing "evicted" shard, this can either be the oldest
    * shard that was not yet removed due to a master swing, or the next shard which was not 
    * yet integrated (there is an exception, see below).
    *
    * We could try to check what is the most possible event out of these too cases, but there are
    * corner cases when the db is down while rollouts are required, making datetime comparison 
    * tricky.
    * Instead, we let the evicted shard be deleted and we properly recreate it.
    *  
    * There is an exception to the above: if the evicted shard is the initial table, the next shard
    * will have a new name; if both initial table and next shard exist - case might be hard to hit
    * with anything but test2min partition mode, btw-, it is ok to delete the evicted shard and reuse
    * the existing next shard.
    *
    **/
    rc = _next_shard_exists(view, next_existing_shard,
                            sizeof(next_existing_shard));
    if (rc == VIEW_ERR_EXIST) {
        if (evicted_shard0 || (view->nshards < view->retention) ||
            (strcasecmp(view->shard0name,
                        view->shards[view->retention - 1].tblname) == 0)) {
            /* In this unique case, the evicted shard and the next shard have different names!
               Recovering the existing next shard is ok */
            logmsg(LOGMSG_WARN, "Found existing next shard %s for view %s, recovering\n", 
                    next_existing_shard, view->name);
        } else {
            logmsg(LOGMSG_WARN, "Found existing next shard %s for view %s, will recreate\n", 
                    next_existing_shard, view->name);
            rc = VIEW_NOERR;
        }
    } else if (rc != VIEW_NOERR) {
        errstat_set_rc(err, rc);
        errstat_set_strf(err, "Failed to check or generate the next shard");
        return rc;
    }

    view->roll_time = tm;
    if (rc == VIEW_NOERR) {
        print_dbg_verbose(view->name, &view->source_id, "RRR",
                          "Adding phase 1 at %d for %s\n",
                          view->roll_time - preemptive_rolltime, 
                          next_existing_shard);

        rc = (cron_add_event(_get_sched_byname(view->period, view->name), NULL,
                             view->roll_time - preemptive_rolltime,
                             _view_cron_phase1, tmp_str1 = strdup(view->name),
                             NULL, NULL, NULL, &view->source_id, err, NULL) == NULL)
                 ? err->errval
                 : VIEW_NOERR;
        tmp_str2 = NULL;
    } else {
        assert(rc == VIEW_ERR_EXIST);
        print_dbg_verbose(view->name, &view->source_id, "RRR",
                          "Adding phase 2 at %d for %s\n",
                          view->roll_time, next_existing_shard);

        rc = (cron_add_event(_get_sched_byname(view->period, view->name), NULL,
                             view->roll_time, _view_cron_phase2,
                             tmp_str1 = strdup(view->name),
                             tmp_str2 = strdup(next_existing_shard), NULL,
                             NULL, &view->source_id, err, NULL) == NULL)
                 ? err->errval
                 : VIEW_NOERR;
    }
    if (rc != VIEW_NOERR) {
        if (tmp_str1)
            free(tmp_str1);
        if (tmp_str2)
            free(tmp_str2);

        return rc;
    }

    errstat_set_rc(err, rc = VIEW_NOERR);

    return rc;
}

/**
 * Queue up the necessary events to rollout time partitions 
 * Done during restart and master swing 
 *
 */
int views_cron_restart(timepart_views_t *views)
{
    bdb_state_type    *bdb_state = thedb->bdb_env;
    timepart_view_t *view;
    int i;
    int rc;
    struct errstat xerr = {0};

    /* in case of regular master swing, clear pre-existing views event,
       we will requeue them */
    cron_clear_queue(timepart_sched);

    /* corner case: master started and schema change for time partition
       submitted before watchdog thread has time to restart it, will deadlock
       if this is the case, abort the schema change */
    rc = pthread_rwlock_trywrlock(&views_lk);
    if (rc == EBUSY) {
        if (get_schema_change_in_progress(__func__, __LINE__)) {
            logmsg(LOGMSG_ERROR, "Schema change started too early for time "
                                 "partition: aborting\n");
            gbl_sc_abort = 1;
            MEMORY_SYNC;
        }
        Pthread_rwlock_wrlock(&views_lk);
    } else if (rc) {
        abort();
    }

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
    BDB_READLOCK(__func__);

    if (thedb->master == gbl_myhostname && !gbl_is_physical_replicant) {
        /* queue all the events required for this */
        for(i=0;i<views->nviews; i++)
        {

            /* are the rollouts stopped? */
            if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_TIMEPART_NO_ROLLOUT)) {
                logmsg(LOGMSG_WARN, "Time partitions rollouts are stopped; "
                                    "will not start rollouts!\n");
                goto done;
            }

            view = views->views[i];

            if (view->rolltype == TIMEPART_ROLLOUT_TRUNCATE) {
                rc = _view_restart_new_rollout(view, &xerr);
            } else {
                rc = _view_restart(view, &xerr);
            }
            if(rc!=VIEW_NOERR)
            {
                goto done;
            }
        }
    }
    rc = VIEW_NOERR;

done:
    BDB_RELLOCK();

    Pthread_rwlock_unlock(&views_lk);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);
    return rc;
}

static int _generate_new_shard_name_wrapper(timepart_view_t *view,
                                            char *newShardName,
                                            int newShardNameLen, int retention)
{
    int nextNum;
    struct errstat xerr = {0};
    int rc;

    /* normally, for a partition with retention 4 (for example), 
     * which is not yet full following the creation, the shards are 
     * in like:
     * "original_table", or
     * "shard1, original_table", or
     * "shard2, shard1, original_table". 
     * (order is from newest to oldest shard).
     *
     * So the next shard to be rolled in is always view->nshards (3 here)
     *
     * For a full partition, shards are like
     * "shard3, shard2, shard1, original_table", or
     * "shard4, shard3, shard2, shard1", or
     * "shard0, shard4, shard3, shard2", or
     * "shard1, shard0, shard4, shard3", or
     * "shard2, shard1, shard0, shard4", or
     * "shard3, shard2, shard1, shard0"
     *
     * The next shard for this is always (newest shard # + 1) % retention
     *
     * BUT! Corner case: if we increase the retention of a full partition, the 
     * next shards depends on the configuration.
     *
     * We know that we only allow rollout in configurations:
     * "shard3, shard2, shard1, original_table", or
     * "shard4, shard3, shard2, shard1", or
     * "shard3, shard2, shard1, shard0"
     *
     * The next shard depends of the case:
     * "shard3, shard2, shard1, original_table" -> view->nshards!
     * "shard4, shard3, shard2, shard1", -> (newest shard # + 1) % retention
     * "shard3, shard2, shard1, shard0" -> (newest shard # + 1) % retention
     *
     * So to get the newest shard, we can check of the oldest shard is the
     * original table (i.e. it doesn't start with '$'), and if it is next 
     * shard is view->nshards.  Otherwise is (newest shard # + 1) % retention
     *
     * NOTE: test2min is special, it will not support increasing at this point
     * so the old condition to check if partition has all shards works 
     * (view->retention == view->nshards)
     */

    /* extract the next id to be used */
    if (view->shards[view->nshards-1].tblname[0] == '$' || 
        unlikely(view->period == VIEW_PARTITION_TEST2MIN &&
                 view->retention == view->nshards)) {
        nextNum = _extract_shardname_index(
            view->shards[0].tblname, view->shard0name, NULL,
            view->period == VIEW_PARTITION_TEST2MIN);
        if (nextNum > retention) {
            nextNum = 0; /* go back to 0, the partition was shrinked */
        } else {
            nextNum = (nextNum + 1) % (retention + 1);
        }
    } else {
        nextNum = view->nshards;
    }

    /* generate new filename */
    rc = _generate_new_shard_name(
        view->shard0name, newShardName, newShardNameLen, nextNum, retention,
        view->period == VIEW_PARTITION_TEST2MIN, &xerr);
    if (rc != VIEW_NOERR) {
        return xerr.errval;
    }

    return VIEW_NOERR;
}

static int _generate_evicted_shard_name(timepart_view_t *view,
                                        int checked_number,
                                        char *evictedShardName,
                                        int evictedShardNameLen,
                                        int *rolloutTime, int retention)
{
    struct errstat xerr = {0};
    int rc;

    if (view->nshards < retention) {
        /* no eviction yet */
        return VIEW_ERR_EXIST;
    }

    /* generate new filename */
    rc = _generate_new_shard_name(
        view->shard0name, evictedShardName, evictedShardNameLen, checked_number,
        retention, view->period == VIEW_PARTITION_TEST2MIN, &xerr);
    if (rc != VIEW_NOERR) {
        return xerr.errval;
    }

    *rolloutTime =
        _view_get_next_rollout(view->period, view->retention, view->starttime,
                               view->shards[0].low, view->nshards, 1);
    if ((*rolloutTime) == INT_MAX) {
        logmsg(LOGMSG_ERROR, "%s: failed rc=%d errstr=%s\n", __func__, xerr.errval,
                xerr.errstr);
        return VIEW_ERR_BUG;
    }

    return VIEW_NOERR;
}

static int _next_shard_exists(timepart_view_t *view, char *newShardName,
                              int newShardNameLen)
{
    struct dbtable *db;
    int rc;

    rc = _generate_new_shard_name_wrapper(view, newShardName, newShardNameLen,
                                          view->retention);
    if (rc != VIEW_NOERR)
        return rc;

    /* does this table exists ?*/
    db = get_dbtable_by_name(newShardName);
    if (db) {
        return VIEW_ERR_EXIST;
    }

    return VIEW_NOERR;
}

/**
 * Create partition llmeta entry
 *
 */
int partition_llmeta_write(void *tran, timepart_view_t *view, int override,
                           struct errstat *err)
{
    char *view_str;
    int view_str_len;
    int rc;

    /* reserialize the view */
    view_str = NULL;
    view_str_len = 0;
    rc = timepart_serialize_view(view, &view_str_len, &view_str, 0);
    if (rc != VIEW_NOERR) {
        errstat_set_strf(err, "Failed to reserialize view %s", view->name);
        errstat_set_rc(err, rc = VIEW_ERR_BUG);
        goto done;
    }

    /* save the view */
    rc = views_write_view(tran, view->name, view_str, override);
    if (rc != VIEW_NOERR) {
        if (!override && rc == VIEW_ERR_EXIST)
            errstat_set_rcstrf(err, VIEW_ERR_EXIST,
                               "Partition %s already exists", view->name);
        else
            errstat_set_rcstrf(err, VIEW_ERR_LLMETA,
                               "Failed to llmeta save view %s", view->name);
        goto done;
    }

done:
    if (view_str)
        free(view_str);
    return rc;
}

/**
 * Delete partition llmeta entry
 *
 */
int partition_llmeta_delete(void *tran, const char *name, struct errstat *err)
{
    int rc = views_write_view(tran, name, NULL, 1 /*unused*/);
    if (rc != VIEW_NOERR) {
        errstat_set_rcstrf(err, VIEW_ERR_LLMETA,
                           "Unable to remove partition %s", name);
    }
    return rc;
}

/**
 * this function takes into account various time anomolies and
 * differences from day to day, month to month, year to year
 * to establish the next rollout time
 *
 */
static int _view_get_next_rollout_epoch(enum view_partition_period period,
                                        int retention, int startTime,
                                        int crtTime, int nshards,
                                        int back_in_time)
{
    int timeNextRollout = INT_MAX;
    char query[1024];
    char *fmt_forward =
        "select cast((cast(%d as datetime) + cast(%d as %s)) as int) as val";
    char *fmt_backward =
        "select cast((cast(%d as datetime) - cast(%d as %s)) as int) as val";
    char *fmt;
    char *cast_str = NULL;
    int cast_val = 0;
    struct errstat err = {0};

    fmt = (back_in_time) ? fmt_backward : fmt_forward;

    /* time reference */
    if (crtTime == INT_MIN && retention > 1) {
        /* if this is the first shard incompassing all records, starttime tells
           us where we shard this in two */
        assert(nshards == 1);

        return startTime;
    } else if (retention == 1) {
        /* for retention 1, the startTime moves forward, since there is no other
        persistent info about rollouts */
        crtTime = startTime;
    }

    /* if there are at least 2 hards, low was the original split, and now we
       generate a new split based on newest split + period */
    switch (period) {
    case VIEW_PARTITION_DAILY:
        /* 24 hours */
        cast_str = "hours";
        cast_val = 24;
        break;
    case VIEW_PARTITION_WEEKLY:
        /* 7 days */
        cast_str = "days";
        cast_val = 7;
        break;
    case VIEW_PARTITION_MONTHLY:
        /* 1 month */
        cast_str = "months";
        cast_val = 1;
        break;
    case VIEW_PARTITION_YEARLY:
        /* 1 year */
        cast_str = "years";
        cast_val = 1;
        break;
    case VIEW_PARTITION_TEST2MIN:
        /* test 2 mins */
        cast_str = "minutes";
        cast_val = 2;
        break;
    case VIEW_PARTITION_INVALID:
        logmsg(LOGMSG_ERROR, "%s bug!\n", __func__);
        return INT_MAX;
    }

    snprintf(query, sizeof(query), fmt, crtTime, cast_val, cast_str);

    /* note: this is run when a new rollout is decided.  It doesn't have
    to be fast, or highly optimized (like running directly datetime functions */
    timeNextRollout = run_sql_return_ll(query, &err);
    if (errstat_get_rc(&err) != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s error rc %d \"%s\"\n", __func__,
               errstat_get_rc(&err), errstat_get_str(&err));
    }

    return timeNextRollout;
}

static int _view_get_next_rollout(enum view_partition_period period,
                                  int retention, int startTime, int crtTime,
                                  int nshards, int back_in_time)
{
    if (IS_TIMEPARTITION(period))
        return _view_get_next_rollout_epoch(period, retention, startTime,
                                            crtTime, nshards, back_in_time);

    if (period == VIEW_PARTITION_MANUAL)
        return (crtTime == INT_MIN) ? startTime : (crtTime + 1);

    abort();
}

/**
 * Signal looping workers of views of db event like exiting
 *
 */
void views_signal(timepart_views_t *views)
{
    Pthread_rwlock_rdlock(&views_lk);

    if (views) {
        cron_signal_all();
    }

    Pthread_rwlock_unlock(&views_lk);
}

static void _remove_view_entry(timepart_views_t *views, int i)
{
    if (i < views->nviews - 1) {
        memmove(&views->views[i], &views->views[i + 1],
                (views->nviews - i - 1) * sizeof(views->views[0]));
    } else {
        assert(i == (views->nviews - 1));
    }
    views->nviews--;
    views->views[views->nviews] = NULL;
}

/* Set timepartition_name for the next shard, if exist; set to NULL to
 * unregister */
static void _shard_timepartition_name_set(timepart_view_t *view,
                                          const char *name)
{
    struct dbtable *db;
    char next_shard[MAXTABLELEN + 1];
    int rc;

    if (view->rolltype == TIMEPART_ROLLOUT_TRUNCATE)
        return;

    rc = _generate_new_shard_name_wrapper(view, next_shard, sizeof(next_shard),
                                          view->retention);
    if (rc == VIEW_NOERR) {
        /* does this table exists ?*/
        db = get_dbtable_by_name(next_shard);
        if (db) {
            db->timepartition_name = name;
        }
    }
}

/* Register timepartition name for all its table shards */
static int _view_register_shards(timepart_views_t *views,
                                 timepart_view_t *view,
                                 struct errstat *err)
{
    timepart_view_t *chk_view;
    struct dbtable *db;
    int rc;
    int i;
    int indx;

    rc = VIEW_NOERR;

    Pthread_rwlock_rdlock(&views_lk);

    /* check partition name collision */
    chk_view = _check_shard_collision(views, view->name, &indx, 
                                      _CHECK_ALL_SHARDS);
    if(chk_view) {
        if(indx==-1)
            errstat_set_rcstrf(err, rc = VIEW_ERR_EXIST,
                               "Name %s matches seed shard partition \"%s\"",
                               view->name, chk_view->name);
        else
            errstat_set_rcstrf(err, rc = VIEW_ERR_EXIST,
                               "Name %s matches shard %d partition \"%s\"",
                               view->name, indx, chk_view->name);
        goto done;
    }

    for (i = 0; i < view->nshards; i++) {
        db = get_dbtable_by_name(view->shards[i].tblname);
        if (!db) {
            /* if this is a truncate partition, it is possible the partition
             * was created based on an existing table; check if there is
             * a table having the partition name, and if it is, alias it
             */
            struct dbtable *db2 = _table_alias_fallback(view);
            if (!db2) {
                errstat_set_rcstrf(err, rc = VIEW_ERR_EXIST,
                                   "Partition %s shard %s doesn't exist!",
                                   view->name, view->shards[i].tblname);
                goto done;
            }
            db = db2;
        } else {
            /* _check_shard_collision prevents this, but just
               in case that code changes, make sure we did not
               register shard with another time partition already */
            if (unlikely(db->timepartition_name)) {
                errstat_set_rcstrf(err, rc = VIEW_ERR_EXIST,
                                   "Partition %s shard %s reused!", view->name,
                                   view->shards[i].tblname);
                goto done;
            }
            db->timepartition_name = view->name;
        }
    }

    /* also mark the next shard */
    _shard_timepartition_name_set(view, view->name);

done:
    Pthread_rwlock_unlock(&views_lk);

    return rc;
}

/* Unregister timepartition name for all its table shards */
static void _view_unregister_shards_lkless(timepart_views_t *views,
                                           timepart_view_t *view)
{
    struct dbtable *db;
    int i;

    for (i = 0; i < view->nshards; i++) {
        db = get_dbtable_by_name(view->shards[i].tblname);
        if (db) {
            db->timepartition_name = NULL;
        }
    }

    /* also reset the next shard */
    _shard_timepartition_name_set(view, NULL);
}

/**
 * Run "func" for each shard of a partition
 * If partition is visible to the rest of the world, it is locked by caller.
 * Otherwise, this can run unlocked (temp object generated by sc).
 *
 */
int timepart_foreach_shard_lockless(timepart_view_t *view,
                                    int func(const char *, timepart_sc_arg_t *),
                                    timepart_sc_arg_t *arg)
{
    int rc = 0;
    int i;
    arg->nshards = view->nshards;
    for (i = arg->indx; i < view->nshards; i++) {
        if (arg)
            arg->indx = i;
        logmsg(LOGMSG_INFO, "%s Applying %p to %s (existing shard)\n", __func__,
               func, view->shards[i].tblname);
        rc = func(view->shards[i].tblname, arg);
        if (rc) {
            break;
        }
    }
    return rc;
}

/**
 * Run "func" for each shard, starting with "first_shard".
 * Callback receives the name of the shard and argument struct
 * NOTE: first_shard == -1 means include the next shard if
 * already created
 *
 */
int timepart_foreach_shard(const char *view_name,
                           int func(const char *, timepart_sc_arg_t *),
                           timepart_sc_arg_t *arg, int first_shard)
{
    timepart_views_t *views;
    timepart_view_t *view;
    char next_shard[MAXTABLELEN + 1];
    int rc = 0;

    Pthread_rwlock_rdlock(&views_lk);

    views = thedb->timepart_views;

    view = _get_view(views, view_name);
    if (!view) {
        rc = VIEW_ERR_EXIST;
        goto done;
    }
    if (arg) {
        arg->view_name = view_name;
        arg->nshards = view->nshards;
        if (arg->s) {
            /* we use pointer to view->name instead of strdup */
            arg->s->timepartition_name = view->name;
        }
    }

    if (first_shard == -1) {
        rc = _next_shard_exists(view, next_shard, sizeof(next_shard));
        if (rc == VIEW_ERR_EXIST) {
            logmsg(LOGMSG_INFO, "%s Applying %p to %s (next shard)\n", __func__,
                   func, next_shard);
            rc = func(next_shard, arg);
        }
    }

    rc = timepart_foreach_shard_lockless(view, func, arg);

done:
    Pthread_rwlock_unlock(&views_lk);

    return rc;
}

static int _validate_view_id(timepart_view_t *view, uuid_t source_id, 
                             const char *str, struct errstat *err)
{
    int rc;

    rc = !comdb2uuid_is_zero(source_id) &&
            comdb2uuidcmp(view->source_id, source_id);
    if(rc && err) {
        logmsg(LOGMSG_WARN, "View %s was dropped and added back, "
                "recovering %s\n", view->name, str);
        errstat_set_strf(err, "View %s was dropped and added back, "
                "recovering %s", view->name, str);
        errstat_set_rc(err, rc = VIEW_ERR_BUG);
    }

    return rc;
}

static void _handle_view_event_error(timepart_view_t *view, uuid_t source_id,
                                     struct errstat *err)
{
    /* update the error */
    struct errstat newerr = {0};
    if(view)
        errstat_set_strf(&newerr, "%s: %s", view->name, err->errstr);
    else {
        uuidstr_t us;
        errstat_set_strf(&newerr, "id %s: %s", 
                comdb2uuidstr(source_id, us),
                err->errstr);
    }
    *err = newerr;
}

static void print_dbg_verbose(const char *name, uuid_t *source_id, 
                              const char *prefix, const char *fmt, ...)
{
    va_list va;
    uuidstr_t us;

    if(!bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DEBUG_TIMEPART_CRON))
        return;

    if(name)
        logmsg(LOGMSG_USER, "%s %s%s%s: ", prefix, name,
                (source_id)?" ":"",
                (source_id)?comdb2uuidstr((unsigned char*)source_id, us):"");
    else
        logmsg(LOGMSG_USER, "%s%s%s: ", prefix,
                (source_id)?" ":"",
                (source_id)?comdb2uuidstr((unsigned char*)source_id, us):"");


    va_start(va, fmt);
    
    logmsgv(LOGMSG_USER, fmt, va);

    va_end(va);
}

/**
 * Update the retention of the existing partition
 * NOTE: this is called from bpfunc on master, and we already
 * have the views lock
 */
int timepart_update_retention(void *tran, const char *name, int retention, struct errstat *err)
{
   timepart_views_t *views = thedb->timepart_views;
   timepart_view_t *view;
   int rc = VIEW_NOERR;

   /* make sure we are unique */
   view = _get_view(views, name);
   if (!view)
   {
      errstat_set_strf(err, "Partition %s doesn't exists!", name);
      errstat_set_rc(err, rc = VIEW_ERR_EXIST);
      goto done;
   }

   if(view->retention < retention) 
   {
       /* increasing the retention only works if the shard configuration is in a
          special ordering so that recovering still works;
          examples where increasing is possible (retention  =  4):
          2, 1, orig_table
          3, 2, 1, orig_table
          3, 2, 1, 0
          4, 3, 2, 1
          examples where increasing it is not possible
          0, 3, 2, 1
          1, 0, 3, 2
          2, 1, 0, 3
          While in theory it would be possible to increase the retention in the
          last set of examples, this would violate the assumption that the shard
          numbers are rotating in order and that is used to proper recover when
          master switches/crashes during a rollout sequence
        */
       if (view->nshards == view->retention &&
           strcasecmp(view->shard0name,
                      view->shards[view->nshards - 1].tblname) != 0 &&
           view->shards[view->nshards-1].tblname[0] == '$' &&
           view->shards[view->nshards-1].tblname[1] != '0' &&
           view->shards[view->nshards-1].tblname[1] != '1') {
           errstat_set_strf(
               err, "Partition %s retention cannot be increased at this time!",
               name);
           errstat_set_rc(err, rc = VIEW_ERR_EXIST);
           goto done;
       }

       view->retention = retention;

       rc = partition_llmeta_write(tran, view, 1, err);
       if (rc != VIEW_NOERR) {
           goto done;
       }
   } else if (view->retention > retention) {
       int i;
       char **extra_shards = NULL;
       int n_extra_shards = view->nshards - retention;
       int old_retention = view->retention;
       int old_low = view->shards[view->nshards - 1].low;

       if (n_extra_shards > 0) {
           extra_shards = alloca(n_extra_shards * sizeof(char *));

           /* check if the new retention is less than existing shards,
              and if so, free the oldest max(0, view->nshards-view->retention)
              shards */
           for (i = 0; i < n_extra_shards; i++) {
               extra_shards[i] = view->shards[i + retention].tblname;
           }

           view->nshards = retention;
           /* we need to update the LOW of the new oldest shard */
           old_low = view->shards[view->nshards - 1].low;
           view->shards[view->nshards - 1].low = INT_MIN;
       }
       view->retention = retention;

       rc = partition_llmeta_write(tran, view, 1, err);
       if (rc != VIEW_NOERR) {
           view->shards[view->nshards - 1].low = old_low;
           view->retention = old_retention;
           view->nshards += n_extra_shards;
           goto done;
       } else {
           /* safe now to remove in-memory structure */
           view->shards = realloc(view->shards,
                                  sizeof(timepart_shard_t) * view->retention);
       }

       if (extra_shards) {
           int irc = 0;

           for (i = 0; i < n_extra_shards; i++) {
               irc =
                   (cron_add_event(_get_sched_byname(view->period, view->name),
                                   NULL, 0, _view_cron_phase3, extra_shards[i],
                                   NULL, NULL, NULL, NULL, err, NULL) == NULL)
                       ? err->errval
                       : VIEW_NOERR;
               if (irc != VIEW_NOERR) {
                   logmsg(LOGMSG_ERROR, "%s: failed rc=%d errstr=%s\n",
                          __func__, err->errval, err->errstr);
                   free(extra_shards[i]);
               }
           }
       }
   }

done:
    return rc;
}

/**
 * Locking the views subsystem, needed for ordering locks with schema
 *
 */
void views_lock(void)
{
    Pthread_rwlock_rdlock(&views_lk);
}
void views_unlock(void)
{
    Pthread_rwlock_unlock(&views_lk);
}

static int _view_update_table_version(timepart_view_t *view, tran_type *tran)
{
    unsigned long long version;
    int rc = VIEW_NOERR;

    /* get existing versioning, if any */
    if (strcmp(view->shards[0].tblname, view->shard0name) &&
        view->nshards > 1) {

        version = comdb2_table_version(view->shards[1].tblname);

        rc = table_version_set(tran, view->shards[0].tblname, version);
        if (rc)
            rc = VIEW_ERR_LLMETA;
    }

    return rc;
}


/**
 * Get number of shards
 *
 */
int timepart_get_num_shards(const char *view_name)
{
    timepart_views_t *views;
    timepart_view_t *view;
    int nshards;

    Pthread_rwlock_rdlock(&views_lk);

    views = thedb->timepart_views;

    view = _get_view(views, view_name);
    if (view)
        nshards = view->nshards;
    else
        nshards = -1;

    Pthread_rwlock_unlock(&views_lk);

    return nshards;
}


void check_columns_null_and_dbstore(const char *name, struct dbtable *tbl)
{
    /* if a column has both a default and a null value, NULL cannot be
    explicitely
    be inserted as value */

    struct schema *sc = tbl->schema;
    int i;

    for (i = 0; i < sc->nmembers; i++) {
        if (sc->member[i].in_default && !(sc->member[i].flags & NO_NULL)) {
            logmsg(LOGMSG_WARN,
                   "WARNING: Partition %s schema field %s that "
                   "has dbstore but cannot be set NULL\n",
                   name, sc->member[i].name);
        }
    }
}

static cron_sched_t *_get_sched_byname(enum view_partition_period period,
                                       const char *sched_name)
{
    if (IS_TIMEPARTITION(period))
        return timepart_sched;
    if (period == VIEW_PARTITION_MANUAL)
        return cron_sched_byname(sched_name);
    abort();
}

/**
 * Check if a table name is the next shard for a time partition
 * and if so, returns the pointer to the partition name
 * NOTE: this is expensive, so only use it in schema resume
 * or recovery operations that lack information about the underlying
 * table (for regular operations, pass down this information from caller
 * instead of calling this function). Recovery includes sc_callbacks
 * NOTE2: it grabs views repository
 *
 */
const char *timepart_is_next_shard(const char *shardname,
                                   unsigned long long *version)
{
    timepart_views_t *views;
    timepart_view_t *view;
    int i;
    char next_shard[MAXTABLELEN + 1];
    const char *ret_name = NULL;
    int rc;

    Pthread_rwlock_rdlock(&views_lk);

    views = thedb->timepart_views;

    for (i = 0; i < views->nviews; i++) {
        view = views->views[i];

        if (view->rolltype == TIMEPART_ROLLOUT_TRUNCATE)
            continue;

        rc = _generate_new_shard_name_wrapper(
            view, next_shard, sizeof(next_shard), view->retention);
        if (rc != VIEW_NOERR) {
            logmsg(LOGMSG_ERROR,
                   "%s fail to generate next shard name view %s\n", __func__,
                   view->name);
            continue;
        }

        if (strncasecmp(next_shard, shardname, strlen(next_shard) + 1) == 0) {
            logmsg(LOGMSG_INFO,
                   "%s table %s is the next shard for partition %s\n", __func__,
                   shardname, view->name);
            ret_name = view->name;
            if (version) {
                struct dbtable *dbt =
                    get_dbtable_by_name(view->shards[0].tblname);
                if (dbt)
                    *version = dbt->tableversion;
                else {
                    logmsg(LOGMSG_ERROR,
                           "%s: Unable to find shard 0 %s for partition %s\n",
                           __func__, view->shards[0].tblname, view->name);
                    *version = 0;
                }
            }
            break;
        }
    }

    Pthread_rwlock_unlock(&views_lk);

    return ret_name;
}

static void _failed_new_view(timepart_view_t **view, const char *errs, int rc,
                             struct errstat *err, const char *func, int line)
{
    errstat_set_rcstrf(err, rc, "Error %s at %s %d", errs, func, line);

    if (*view) {
        timepart_free_view(*view);
        *view = NULL;
    }
}

timepart_view_t *timepart_new_partition(const char *name, int period,
                                        int retention, long long start,
                                        uuid_t *uuid,
                                        enum TIMEPART_ROLLOUT_TYPE rolltype,
                                        const char **partition_name,
                                        struct errstat *err)
{
    timepart_view_t *view;

    view = (timepart_view_t *)calloc(1, sizeof(timepart_view_t));
    if (!view) {
        _failed_new_view(&view, "malloc oom", VIEW_ERR_MALLOC, err, __func__,
                         __LINE__);
        goto done;
    }

    view->name = strdup(name);
    if (!view->name) {
        _failed_new_view(&view, "malloc oom", VIEW_ERR_MALLOC, err, __func__,
                         __LINE__);
        goto done;
    }

    if (partition_name)
        *partition_name = view->name;

    assert(period != VIEW_PARTITION_INVALID);
    view->period = period;
    assert(retention > 0);
    view->retention = retention;
    view->starttime = start;
    view->rolltype = rolltype;

    if (!uuid) {
        /* assign a uuid */
        comdb2uuid(view->source_id);
    } else {
        comdb2uuidcpy(view->source_id, *uuid);
    }
done:
    return view;
}

static int _get_starttime_to_future(timepart_view_t *view)
{
    if (view->period == VIEW_PARTITION_MANUAL)
        return view->starttime;

    int current_time = comdb2_time_epoch();
    int current_starttime = view->starttime;
    while (current_starttime < current_time) {
        current_starttime =
            _view_get_next_rollout(view->period, view->retention,
                                   current_starttime, current_starttime, 1, 0);

        current_time = comdb2_time_epoch();
    }
    return current_starttime;
}

int timepart_populate_shards(timepart_view_t *view, struct errstat *err)
{
    int rc = VIEW_NOERR;
    int i;
    char new_name[MAXTABLELEN + 1];

    assert(!view->shards && view->nshards >= 0);

    view->shards =
        (timepart_shard_t *)calloc(view->retention, sizeof(timepart_shard_t));
    if (!view->shards) {
        errstat_set_rcstrf(err, rc = VIEW_ERR_MALLOC, "malloc %s %d", __func__,
                           __LINE__);
        goto error;
    }

    /* since we do not need to do fake rollouts, lets bring starttime to present
     */
    int next_rollout = _get_starttime_to_future(view);

    /* generate shard names */
    char *old_name = view->name;
    for (i = view->nshards; i < view->retention; i++) {
        rc = _generate_new_shard_name(
            old_name, new_name, sizeof(new_name), i, view->retention,
            view->period == VIEW_PARTITION_TEST2MIN, err);
        if (rc) {
            errstat_set_rcstrf(err, rc = VIEW_ERR_GENERIC,
                               "Failed to generate shard %i name", i);
            goto error;
        }
        view->shards[i].tblname = strdup(new_name);
        if (!view->shards[i].tblname) {
            errstat_set_rcstrf(err, rc = VIEW_ERR_GENERIC,
                               "malloc shard %i name", i);
            goto error;
        }
        old_name = new_name;
    }
    /* rollout times */
    view->shards[0].low = INT_MIN;
    view->shards[0].high = next_rollout;
    for (i = 1; i < view->retention; i++) {
        view->shards[i].low = view->shards[i].high = INT_MAX;
    }
    view->nshards = view->retention;

    /* there is no shard0name here, just use "<none>" */
    view->shard0name = strdup("<none>");

error:

    return rc;
}

static int _view_new_rollout_lkless(char *name, int period, int roll_time,
                                    uuid_t *source_id, struct errstat *err)
{
    int rc;

    print_dbg_verbose(name, source_id, "TTT", "Truncate rollout %d\n",
                      roll_time);

    rc = (cron_add_event(_get_sched_byname(period, name), NULL, roll_time,
                         _view_cron_new_rollout, name, NULL, NULL, NULL,
                         source_id, err, NULL) == NULL)
             ? err->errval
             : VIEW_NOERR;

    if (rc != VIEW_NOERR)
        free(name);

    return rc;
}

static void _view_find_current_shard(timepart_view_t *view)
{
    int now = comdb2_time_epoch();
    int newest_earlier_shard = -1;
    int i;

    /* Try to find a shard with time interval matching the current time;
     * Corner case: it is possible for the db to be down and miss a few
     * rollouts; at that point, no shard time interval will match; in this
     * case, use the last shard to be used as current.
     */
    for (i = 0; i < view->nshards; i++) {
        if (view->shards[i].low <= now) {
            if (now < view->shards[i].high) {
                print_dbg_verbose(view->name, &view->source_id, "SSS",
                                  "Found current shard %d\n", i);
                view->current_shard = i;
                return;
            }
        }
        if (newest_earlier_shard < 0 || (view->shards[i].low != INT_MAX && 
            view->shards[newest_earlier_shard].low < view->shards[i].low))
            newest_earlier_shard = i;
    }
    assert(newest_earlier_shard >= 0 && newest_earlier_shard < view->nshards);

    print_dbg_verbose(view->name, &view->source_id, "SSS",
                      "Stale rollout, use last shard%d\n",
                      newest_earlier_shard);
    view->current_shard = newest_earlier_shard;
}

static int _view_restart_new_rollout(timepart_view_t *view, struct errstat *err)
{
    int rc = 0;

    assert(view->rolltype == TIMEPART_ROLLOUT_TRUNCATE);
    if (view->period == VIEW_PARTITION_MANUAL) {
        rc = logical_cron_init(view->name, err);
        if (rc)
            return rc;
    }

    print_dbg_verbose(view->name, &view->source_id, "III",
                      "Restart new rollout mode\n");

    _view_find_current_shard(view);

    view->roll_time = view->shards[view->current_shard].high;
    rc = _view_new_rollout_lkless(strdup(view->name), view->period,
                                  view->roll_time, &view->source_id, err);
    if (rc != VIEW_NOERR)
        goto error;

    bzero(err, sizeof(*err));

    return 0;

error:
    logmsg(LOGMSG_ERROR, "Failed to restart partition %s rc %d \"%s\"\n",
           view->name, rc, err->errstr);
    return rc;
}

static void _extract_info(timepart_view_t *view, char **name_dup, int *period,
                          int *rolltime, uuid_t *source_id)
{
    *name_dup = strdup(view->name);
    *period = view->period;
    *rolltime = view->shards[view->current_shard].high;
    comdb2uuidcpy(*source_id, view->source_id);
}

/* called for a truncate rollout before finalize commits the tran */
int partition_truncate_callback(tran_type *tran, struct schema_change_type *s)
{
    struct errstat err = {0};
    int rc;
    int bdberr = 0;

    rc = partition_llmeta_write(tran, s->newpartition, 1, &err);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s Failed to write partition %s llmeta %d %s\n",
               __func__, s->timepartition_name, err.errval, err.errstr);
        return rc;
    }

    rc = bdb_llog_partition(thedb->bdb_env, tran, s->newpartition->name,
                            &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "Failed to signal replicants for %s rc %d bdberr %d\n",
               s->newpartition->name, rc, bdberr);
    }
    return rc;
}

/* truncate rollout */
void *_view_cron_new_rollout(struct cron_event *event, struct errstat *err)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    timepart_view_t *view;
    int rc = VIEW_NOERR;
    char *name_dup;
    int period;
    int rolltime;
    uuid_t source_id;

    char *name = (char *)event->arg1;

    if (!name) {
        errstat_set_rcstrf(err, VIEW_ERR_BUG, "%s no name?", __func__);
        return NULL;
    }

    if (!gbl_exit &&
        !(thedb->master != gbl_myhostname || gbl_is_physical_replicant)) {

        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
        BDB_READLOCK(__func__);
        wrlock_schema_lk();
        Pthread_rwlock_wrlock(&views_lk);

        view = _get_view(thedb->timepart_views, name);
        if (view) {
            /* update the current shard */
            int next_shard = (view->current_shard + 1) % view->retention;
            view->shards[next_shard].low =
                view->shards[view->current_shard].high;
            view->shards[next_shard].high = rolltime = _view_get_next_rollout(
                view->period, view->retention, view->shards[next_shard].low,
                view->shards[next_shard].low, view->nshards, 0);
            view->current_shard = next_shard;

            _extract_info(view, &name_dup, &period, &rolltime, &source_id);

            /* here we truncate the current shard */
            print_dbg_verbose(name, &view->source_id, "CCC",
                              "Truncate rollout shard %d at %d\n",
                              view->current_shard, rolltime);
            rc = sc_timepart_truncate_table(
                view->shards[view->current_shard].tblname, err, view);
            if (rc != VIEW_NOERR) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to truncate current shard %d %s\n", __func__,
                       err->errval, err->errstr);
                goto done;
            }

            view->version = gbl_views_gen;
            rc = VIEW_NOERR;
            goto done;
        } else {
            errstat_set_rcstrf(err, rc = VIEW_ERR_BUG, "View %s missing", name);
        }

    done:
        Pthread_rwlock_unlock(&views_lk);
        if (rc != VIEW_NOERR)
            unlock_schema_lk();
        BDB_RELLOCK();
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

        if (rc == VIEW_NOERR) {
            rc = _view_new_rollout_lkless(name_dup, period, rolltime,
                                          &source_id, err);
            if (rc != VIEW_NOERR) {
                logmsg(LOGMSG_ERROR, "Failed to add new rollout event %s\n",
                       name);
            }
        }
    }

    return NULL;
}

static int _create_inmem_view(timepart_views_t *views, timepart_view_t *view,
                              struct errstat *err)
{
    /* adding the view to the list */
    views->views = (timepart_view_t **)realloc(
        views->views, sizeof(timepart_view_t *) * (views->nviews + 1));
    if (!views->views) {
        errstat_set_rcstrf(err, VIEW_ERR_MALLOC, "%s Malloc OOM", __func__);
        return err->errval;
    }

    views->views[views->nviews] = view;
    views->nviews++;
    return VIEW_NOERR;
}

int timepart_create_inmem_view(timepart_view_t *view)
{
    char *name_dup;
    int period;
    int rolltime;
    uuid_t source_id;
    struct errstat err = {0};
    int rc;

    Pthread_rwlock_wrlock(&views_lk);
    rc = _create_inmem_view(thedb->timepart_views, view, &err);

    if (rc == VIEW_NOERR)
        _extract_info(view, &name_dup, &period, &rolltime, &source_id);

    Pthread_rwlock_unlock(&views_lk);

    if (rc != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "Unable to add view %s rc %d \"%s\"\n", view->name,
               err.errval, err.errstr);
    } else {
        if (period == VIEW_PARTITION_MANUAL) {
            /* dedicated logical cron schedulers */
            rc = logical_cron_init(name_dup, &err);
            if (rc) {
                logmsg(LOGMSG_ERROR, "Failed to initialize logical cron %s\n",
                       name_dup);
                free(name_dup);
                return rc;
            }
        }

        /* we need to add the first scheduler event */
        rc = _view_new_rollout_lkless(name_dup, period, rolltime, &source_id,
                                      &err);
        if (rc != VIEW_NOERR) {
            logmsg(LOGMSG_ERROR, "Failed to add new rollout event %s\n",
                   view->name);
        }
    }
    return rc;
}

int timepart_destroy_inmem_view(const char *name)
{
    timepart_view_t *view;
    int idx;

    Pthread_rwlock_wrlock(&views_lk);
    view = _get_view_index(thedb->timepart_views, name, &idx);
    if (!view) {
        logmsg(LOGMSG_ERROR, "%s: unable to find partition %s\n", __func__,
               name);
        Pthread_rwlock_unlock(&views_lk);
        return VIEW_ERR_EXIST;
    }

    _remove_view_entry(thedb->timepart_views, idx);

    Pthread_rwlock_unlock(&views_lk);

    uuid_t source_id;
    comdb2uuidcpy(source_id, view->source_id);

    timepart_free_view(view);

    cron_clear_queue_for_sourceid(timepart_sched, &source_id);

    return VIEW_NOERR;
}

int timepart_num_views(void)
{
    return thedb->timepart_views ? thedb->timepart_views->nviews : 0;
}

const char *timepart_name(int i)
{
    const char *name = NULL;
    Pthread_rwlock_rdlock(&views_lk);

    if (i >= 0 && i < thedb->timepart_views->nviews)
        name = thedb->timepart_views->views[i]->name;

    Pthread_rwlock_unlock(&views_lk);

    return name;
}

void timepart_alias_table(timepart_view_t *view, struct dbtable *db)
{
    assert(!db->sqlaliasname);
    db->sqlaliasname = strdup(view->shards[0].tblname);
    hash_sqlalias_db(db, db->sqlaliasname);
    db->timepartition_name = view->name;
}

int timepart_is_partition(const char *name)
{
    timepart_view_t *view;
    int ret = 0;

    Pthread_rwlock_rdlock(&views_lk);
    view = _get_view(thedb->timepart_views, name);
    if (view)
        ret = 1;
    Pthread_rwlock_unlock(&views_lk);

    return ret;
}

int timepart_allow_drop(const char *zPartitionName)
{
    timepart_view_t *view;
    int ret = -1;

    Pthread_rwlock_rdlock(&views_lk);
    view = _get_view(thedb->timepart_views, zPartitionName);
    if (view && view->rolltype == TIMEPART_ROLLOUT_TRUNCATE)
        ret = 0;
    Pthread_rwlock_unlock(&views_lk);

    return ret;
}

int timepart_clone_access_version(tran_type *tran,
                                  const char *timepartition_name,
                                  const char *tablename,
                                  unsigned long long version)
{
    int rc;
    rc = timepart_copy_access(thedb->bdb_env, tran, (char *)tablename,
                              (char *)timepartition_name, 0);
    if (rc)
        return rc;

    rc = table_version_set(tran, tablename, version);

    return rc;
}

char *timepart_shard_name(const char *p, int i, int aliased,
                          unsigned long long *version)
{
    char *ret = NULL;
    timepart_view_t *view;

    if (version)
        *version = 0ULL;

    if (i < 0)
        return NULL;

    Pthread_rwlock_rdlock(&views_lk);
    view = _get_view(thedb->timepart_views, p);
    if (view && i < view->nshards) {
        struct dbtable *db = get_dbtable_by_name(view->shards[i].tblname);
        if (aliased && db->sqlaliasname)
            ret = strdup(db->sqlaliasname);
        else
            ret = strdup(db->tablename);
        if (version) {
            *version = db->tableversion;
            fprintf(stderr, "Got %s version %llu\n", ret, *version);
        }
    }
    Pthread_rwlock_unlock(&views_lk);

    return ret;
}

int partition_publish(tran_type *tran, struct schema_change_type *sc)
{
    char *partition_name = NULL;
    int rc = VIEW_NOERR;

    if (sc->partition.type != PARTITION_NONE) {
        switch (sc->partition.type) {
        case PARTITION_ADD_TIMED:
        case PARTITION_ADD_MANUAL: {
            assert(sc->newpartition != NULL);
            timepart_create_inmem_view(sc->newpartition);
            break;
        }
        case PARTITION_REMOVE:
        case PARTITION_MERGE: {
            /* preserve name to signal replicants */
            partition_name = strdup(sc->timepartition_name);
            rc = timepart_destroy_inmem_view(sc->timepartition_name);
            if (rc)
                abort(); /* restart will fix this*/
            break;
        }
        } /*switch */
        int bdberr = 0;
        rc = bdb_llog_partition(thedb->bdb_env, tran,
                                partition_name ? partition_name
                                               : (char *)sc->timepartition_name,
                                &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "%s: Failed to log scdone for partition %s\n",
                   __func__, partition_name);
        }
        free(partition_name);
    }
    return rc;
}

/* revert partition publish */
void partition_unpublish(struct schema_change_type *sc)
{
   if (sc->partition.type != PARTITION_NONE) {
        switch (sc->partition.type) {
        case PARTITION_ADD_TIMED:
        case PARTITION_ADD_MANUAL: {
            assert(sc->newpartition != NULL);
            timepart_destroy_inmem_view(sc->timepartition_name);
            break;
        }
        case PARTITION_REMOVE: {
            assert(sc->newpartition != NULL);
            int rc = timepart_create_inmem_view(sc->newpartition);
            if (rc)
                abort(); /* restart will fix this*/
            break;
        }
        }
    }
}


/* return the default value for a manual partition */
int logical_partition_next_rollout(const char *name)
{
    int ret = -1;
    Pthread_rwlock_rdlock(&views_lk);
    timepart_view_t *view = _get_view(thedb->timepart_views, name);
    if (!view) {
        logmsg(LOGMSG_ERROR, "Partition %s does not exist!\n", name);
        ret = 0;
        goto done;
    }
    ret = view->shards[view->current_shard].high;
    /* legacy manual impementation has high set to INT_MAX */
    if (ret == INT_MAX) {   
        ret = view->shards[view->current_shard].low;
        if (ret == INT_MIN) {
            ret = 0;
        } else {
            ret += 1;
        }
    }
done:
    Pthread_rwlock_unlock(&views_lk);
    return ret;
}

#include "views_systable.c"

#include "views_serial.c"

#include "views_sqlite.c"

