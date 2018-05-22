#include <stdlib.h>
#include <pthread.h>
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

#define VIEWS_MAX_RETENTION 1000
#define VIEWS_DFT_PREEMPT_ROLL_SECS 1800
#define VIEWS_DFT_ROLL_DELETE_LAG_SECS 5


struct timepart_shard {
    char *tblname; /* name of the table covering the shard, can be an alias */
    int low;  /* lowest value in this shard, including limit  [low, high) */
    int high; /* excluding limit, values up to this can be stored in this shard
                 [low, high) */
};
typedef struct timepart_shard timepart_shard_t;

struct timepart_view {
    char *name;                       /* name of the view, visible in sql */
    enum view_timepart_period period; /* when do we rotate to a new shard */
    int retention;                    /* how many shard are preserves */
    int nshards;                      /* how many shards */
    timepart_shard_t *shards;         /* array of shard pointers */
    int version;      /* in-memory versioning, allowing single view refreshes */
    char *shard0name; /* original first table name length, used to generate
                         names */
    int starttime;    /* info about the beginning of the rollout */
    int purge_time;   /* set if there is a purger thread assigned */
    int roll_time;    /* cached time for next rollout */
    uuid_t source_id; /* identifier for view, unique as compare to name */
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

/*
   NOTE: for now, since views access is done only when sqlite engines are
   created,
   when shards are rolled, or when db starts, we can use a mutex
*/
pthread_rwlock_t views_lk;

/*
 Cron scheduler
 */
cron_sched_t *timepart_sched;

static int _start_views_cron(void);
static timepart_view_t *_get_view(timepart_views_t *views, const char *name);
static int _generate_new_shard_name(const char *oldname, char *newname,
                                    int newnamelen, int nextnum, int maxshards,
                                    int testing, struct errstat *err);
static int _extract_shardname_index(const char *tblName, const char *originalName, int maxShards);
static int _convert_time(char *sql);
static int _views_do_op(timepart_views_t *views, const char *name,
                        int (*op)(timepart_views_t *, timepart_view_t *,
                                  struct errstat *),
                        struct errstat *err);
static char *_describe_row(const char *tblname, const char *prefix,
                           enum views_trigger_op op_type, struct errstat *err);
static void *timepart_cron_kickoff(uuid_t source_id, void *arg1, void *arg2, 
                                   void *arg3, struct errstat *err);
static int _next_shard_exists(timepart_view_t *view, char *newShardName,
                              int newShardNameLen);
static void _remove_view_entry(timepart_views_t *views, int i);

void *_view_cron_phase1(uuid_t source_id, void *arg1, void *arg2, void *arg3, 
                        struct errstat *err);
void *_view_cron_phase2(uuid_t source_id, void *arg1, void *arg2, void *arg3,
                        struct errstat *err);
void *_view_cron_phase3(uuid_t source_id, void *arg1, void *arg2, void *arg3,
                        struct errstat *err);
static int _views_rollout_phase1(timepart_view_t *view, char **newShardName,
                                 struct errstat *err);
static int _views_rollout_phase2(timepart_view_t *view,
                                 const char *newShardName, int *timeNextRollout,
                                 char **removeShardName, struct errstat *err);
static int _views_rollout_phase3(const char *oldShardName, struct errstat *err);

static int _view_restart(timepart_view_t *view, struct errstat *err);
int views_cron_restart(timepart_views_t *views);

static int _view_rollout_publish(void *tran, timepart_view_t *view,
                                 struct errstat *err);
static int _view_get_next_rollout(enum view_timepart_period period,
                                  int startTime, int crtTime, int nshards,
                                  int back_in_time);
static int _generate_evicted_shard_name(timepart_view_t *view,
                                        char *evictedShardName,
                                        int evictedShardNameLen,
                                        int *rolloutTime);
static int _validate_view_id(timepart_view_t *view, uuid_t source_id, 
                             const char *str, struct errstat *err);
static void _handle_view_event_error(timepart_view_t *view, uuid_t source_id,
                                     struct errstat *err);
static void print_dbg_verbose(const char *name, uuid_t *source_id, const char *prefix, 
                              const char *fmt, ...);
static int _view_update_table_version(timepart_view_t *view, tran_type *tran);

enum _check_flags {
   _CHECK_ONLY_INITIAL_SHARD, _CHECK_ALL_SHARDS, _CHECK_ONLY_CURRENT_SHARDS
};

static timepart_view_t* _check_shard_collision(timepart_views_t *views, const char *tblname, 
      int *indx, enum _check_flags flag);

/**
 * Initialize the views
 *
 */
timepart_views_t *timepart_views_init(struct dbenv *dbenv)
{
    int rc;

    pthread_rwlock_init(&views_lk, NULL);

    /* hack for now to force natural types */
    if (_start_views_cron())
        return NULL;

    /* read the llmeta views, if any */
    dbenv->timepart_views = views_create_all_views();

    if (!dbenv->timepart_views) {
        return NULL;
    }

#if 0
   NOTE: this is done in new_master_callback, upon master
            election 
    rc = views_cron_restart(dbenv->timepart_views);
    if (rc != VIEW_NOERR) {
        /* this is an abort, keep the object */
        return NULL;
    }
#endif

    return dbenv->timepart_views;
}

/** 
 * Check if a name is a shard 
 *
 */
int timepart_is_shard(const char *name, int lock)
{
   timepart_views_t  *views = thedb->timepart_views;
   timepart_view_t   *view;
   int               rc;
   int               indx;

   rc = 0;

   if(lock)
       pthread_rwlock_rdlock(&views_lk);

   view = _check_shard_collision(views, name, &indx, _CHECK_ALL_SHARDS);

   if(view)
   {  
      rc = 1;
   }

   if(lock)
       pthread_rwlock_unlock(&views_lk);

   return rc;
}

/** 
 * Check if a name is a timepart
 *
 */
int timepart_is_timepart(const char *name, int lock)
{
   timepart_views_t  *views = thedb->timepart_views;
   int               i;
   int               rc;

   rc = 0;

   if (!views)
       return 0;

   if(lock)
       pthread_rwlock_rdlock(&views_lk);

   for(i=0; i<views->nviews; i++)
   {
      if(!strcasecmp(name, views->views[i]->name))
      {
         rc = 1;
         break;
      }
   }

   if(lock)
       pthread_rwlock_unlock(&views_lk);

   return rc;
}

/**
 * Add timepart view, this is a schema change driven event
 *
 */
int timepart_add_view(void *tran, timepart_views_t *views,
                      timepart_view_t *view, struct errstat *err)
{
    char next_existing_shard[MAXTABLELEN + 1];
    int preemptive_rolltime = thedb->timepart_views->preemptive_rolltime;
    timepart_view_t *oldview;
    timepart_shard_t *shard;
    char *tmp_str;
    int rc;
    int tm;

    if (unlikely(view->period == VIEW_TIMEPART_TEST2MIN)) {
        preemptive_rolltime = 30; /* 30 seconds in advance we add a new table */
    }

    pthread_rwlock_wrlock(&views_lk);

    /* make sure we are unique */
    oldview = _get_view(views, view->name);
    if (oldview) {
        errstat_set_strf(err, "Partition %s exists!", view->name);
        errstat_set_rc(err, rc = VIEW_ERR_EXIST);
        goto done;
    }

    /* publish in llmeta and signal sqlite about it; we do have the schema_lk
       for the whole function call */
    rc = _view_rollout_publish(tran, view, err);
    if (rc != VIEW_NOERR) {
        goto done;
    }

    /* create initial rollout */
    tm = _view_get_next_rollout(view->period, view->starttime,
                                view->shards[0].low, view->nshards, 0);
    if (tm == INT_MAX) {
        errstat_set_strf(err, "Failed to compute next rollout time");
        errstat_set_rc(err, rc = VIEW_ERR_BUG);
        goto done;
    }

    rc = _next_shard_exists(view, next_existing_shard,
                            sizeof(next_existing_shard));
    if (rc == VIEW_ERR_EXIST) {
        errstat_set_strf(err, "Next shard %s verlaps existing table for %s",
                         next_existing_shard, view->name);
        errstat_set_rc(err, rc);
        goto done;
    } else if (rc != VIEW_NOERR) {
        errstat_set_strf(err, "Failed to check or generate the next shard");
        errstat_set_rc(err, rc);
        goto done;
    }

    view->roll_time = tm;

    if (!bdb_attr_get(thedb->bdb_attr, BDB_ATTR_TIMEPART_NO_ROLLOUT)) {
        print_dbg_verbose(view->name, &view->source_id, "III",
                          "Adding phase 1 at %d\n",
                          view->roll_time - preemptive_rolltime);

        rc = (cron_add_event(timepart_sched, NULL,
                             view->roll_time - preemptive_rolltime,
                             _view_cron_phase1, tmp_str = strdup(view->name),
                             NULL, NULL, &view->source_id, err) == NULL)
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

    /* adding the view to the list */
    views->views = (timepart_view_t **)realloc(
        views->views, sizeof(timepart_view_t *) * (views->nviews + 1));
    if (!views->views) {
        errstat_set_strf(err, "%s Malloc OOM", __func__);
        errstat_set_rc(err, rc = VIEW_ERR_MALLOC);
        goto done;
    }

    views->views[views->nviews] = view;
    views->nviews++;

    rc = VIEW_NOERR;

    /* At this point, we have updated the llmeta (which gets replicated)
       and updated in-memory structure;  we are deep inside schema change,
       which will trigger updates for existing sqlite engine upon return;
       new sqlite engines will pick up new configuration just fine.

       NOTE:
       As it is now, changing partitions doesn't update any global counters:
          - gbl_dbopen_gen
          - gbl_analyze_gen
          - gbl_views_gen (at least this one should get updated)

       Even if it is too early, we signal here the schema has change.  If the
       transaction
       changing the partitions fails, we would generate a NOP refresh with no
       impact!
     */
    gbl_views_gen++;

done:
    pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Add one (existing) table to an existing view
 *
 * Used to populate initially the in-memory struct
 * based on persistent older views.
 *
 */
int timepart_view_add_newest_shard(timepart_views_t *views, const char *name,
                                   const char *tblname, int limit)
{
    timepart_view_t *view;
    timepart_shard_t *shard;
    int rc;

    pthread_rwlock_wrlock(&views_lk);

    view = _get_view(views, name);

    if (!view) {
        rc = VIEW_ERR_EXIST;
        goto done;
    }

    if (!view->nshards) {
        rc = VIEW_ERR_BUG;
        goto done;
    }

    /* check limit against low of existing last shard; this is invalid */
    shard = &view->shards[view->nshards - 1];
    if (shard->low >= limit) {
        logmsg(LOGMSG_ERROR, "%s: new shard has lower limit < than existing lower "
                        "limit for last shard!\n",
                __func__);
        return VIEW_ERR_PARAM;
    }

    /* add initial shard */
    view->shards = (timepart_shard_t *)realloc(
        view->shards, (view->nshards + 1) * sizeof(timepart_shard_t));
    if (!view->shards) {
        rc = VIEW_ERR_MALLOC;
        goto done;
    }

    /* adjust old limit */
    shard = &view->shards[view->nshards - 1];
    shard->high = limit; /* TODO: check against races? */

    /* insert newest shard */
    view->nshards++;
    shard = &view->shards[view->nshards - 1];
    shard->tblname = strdup(tblname);
    shard->low = limit;
    shard->high = INT_MAX;

    /* TODO: trigger a schema change event to refresh sqlite engines */
    rc = VIEW_NOERR;
done:
    pthread_rwlock_unlock(&views_lk);

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
    int i;
    int rc;

    pthread_rwlock_wrlock(&views_lk);

    view = _get_view(views, name);
    if (!view) {
        rc = VIEW_ERR_EXIST;
        goto done;
    }

    view->retention = retention;

    /* TODO: trigger a cleanup */

    rc = VIEW_NOERR;
done:
    pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Change the future partition schedule
 *
 */
int timepart_view_set_period(timepart_views_t *views, const char *name,
                             enum view_timepart_period period)
{
    timepart_view_t *view;
    int i;
    int rc;

    pthread_rwlock_wrlock(&views_lk);

    view = _get_view(views, name);
    if (!view) {
        rc = VIEW_ERR_EXIST;
        goto done;
    }

    view->period = period;

    /* TODO: trigger a rollout thread update */

    rc = VIEW_NOERR;
done:
    pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Free a partime view; struct in invalid upon return
 *
 */
int timepart_free_view(timepart_view_t *view)
{
    int i;

    if (view->shards) {
        for (i = 0; i < view->nshards; i++) {
            if (view->shards[i].tblname)
                free(view->shards[i].tblname);
        }
        free(view->shards);
    }
    if (view->name)
        free(view->name);
    if (view->shard0name)
        free(view->shard0name);

    memset(view, 0xFF, sizeof(*view));

    free(view);

    return VIEW_NOERR;
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
    int irc;

    pthread_rwlock_wrlock(&views_lk);

    view = NULL;
    for (i = 0; i < views->nviews; i++) {
        if (strcasecmp(views->views[i]->name, name) == 0) {
            view = views->views[i];
            break;
        }
    }

    if (view) {
        rc = timepart_free_view(view);

        _remove_view_entry(views, i);

        rc = views_write_view(tran, name, NULL);

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
    pthread_rwlock_unlock(&views_lk);

    return rc;
}

/* unlocked !*/
int timepart_free_views_unlocked(timepart_views_t *views)
{
    int i;
    int rc = VIEW_NOERR;
    int irc;

    if (views->views) {
        for (i = 0; i < views->nviews; i++) {
            irc = timepart_free_view(views->views[i]);
            if (irc != VIEW_NOERR) {
                if (rc == VIEW_NOERR)
                    rc = irc;
                logmsg(LOGMSG_ERROR, "%s error freeing view %d\n", __func__, i);
            }
        }

        free(views->views);
    }
    bzero(views, sizeof(*views));

    return rc;
}

/**
 * Free all the views
 *
 */
int timepart_free_views(timepart_views_t *views)
{
    timepart_view_t *view;
    int rc = VIEW_NOERR;

    pthread_rwlock_wrlock(&views_lk);

    rc = timepart_free_views_unlocked(views);

    pthread_rwlock_unlock(&views_lk);

    return rc;
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
static timepart_view_t *_get_view(timepart_views_t *views, const char *name)
{
    int i;

    for (i = 0; i < views->nviews; i++) {
        if (strcasecmp(views->views[i]->name, name) == 0) {
            return views->views[i];
        }
    }
    return NULL;
}

/**
 * Handle views change on replicants
 *
 * NOTE: this is done from scdone_callback, which gives us a big
 *WRLOCK(schema_lk)
 */
int views_handle_replicant_reload(const char *name)
{
    timepart_views_t *views = thedb->timepart_views;
    timepart_view_t *view = NULL;
    timepart_views_t *oldview = NULL;
    struct dbtable *db;
    int rc = VIEW_NOERR;
    char *str = NULL;
    struct errstat xerr = {0};
    int i;

    pthread_rwlock_wrlock(&views_lk);

    logmsg(LOGMSG_INFO, "Replicant updating views counter=%d\n", gbl_views_gen);

    /* the change to llmeta was propagated already */
    rc = views_read_view(name, &str);
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
            logmsg(LOGMSG_ERROR, "%s: unable to locate shard %s for view %s\n",
                    __func__, view->shards[i].tblname, view->name);
            timepart_free_view(view);
            view = NULL;
            goto done;
        }
        db->tableversion = table_version_select(db, NULL);
    }

alter_struct:
    /* we need to destroy existing view, if any */
    /* locate the impacted view */
    for (i = 0; i < views->nviews; i++) {
        if (strcasecmp(views->views[i]->name, name) == 0) {
            timepart_free_view(views->views[i]);

            if (view) {
                /* rollout */
                views->views[i] = view;
            } else {
                /* drop view */
                _remove_view_entry(views, i);
            }

            break;
        }
    }
    if (i >= views->nviews && view) {
        /* this is really a new view */
        /* adding the view to the list */
        views->views = (timepart_view_t **)realloc(
            views->views, sizeof(timepart_view_t *) * (views->nviews + 1));
        if (!views->views) {
            logmsg(LOGMSG_ERROR, "%s Malloc OOM", __func__);
            views->nviews = 0;
            goto done;
        }

        views->views[views->nviews] = view;
        views->nviews++;
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

    pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Create a new shard;
 * If too many shards, we trim the oldest one
 *
 * NOTE: this is under mutex!
 *
 * NOTE:
 * the partitions always have space for retention+1 shards, to decouple
 * new shard creation from oldest shard removal
 */
int views_rollout(timepart_views_t *views, timepart_view_t *view,
                  struct errstat *err)
{
    char *pShardName;
    char *removeShardName;
    int timeNextRollout;
    int rc;

    rc = _views_rollout_phase1(view, &pShardName, err);
    if (rc != VIEW_NOERR) {
        return rc;
    }
    rc = _views_rollout_phase2(view, pShardName, &timeNextRollout,
                               &removeShardName, err);
    if (rc != VIEW_NOERR) {
        return rc;
    }
    if (view->nshards == view->retention + 1) {
        assert(removeShardName);
        rc = _views_rollout_phase3(removeShardName, err);
    }

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
    struct errstat xerr = {0};
    int rc;

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);

    rdlock_schema_lk();

    rc = _views_do_op(views, name, views_rollout, &xerr);

    unlock_schema_lk();

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

    return rc;
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

static int _extract_shardname_index(const char *tblName, const char *originalName, int maxShards)
{
   int indexLen;
   int nextNum;

   if(!strcasecmp(tblName, originalName))
   {
      return 0;   /* initial shard */
   }

   /* we have all the shards, what is next one ?*/
   indexLen = _shard_suffix_str_len(maxShards);

   nextNum = atoi(tblName+1); /* skip $ */

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
        newname[newnamelen - 1] = '\0';
    } else {
        char hash[128];
        len = snprintf(hash, sizeof(hash), "%u%s", nextnum, oldname);
        len = crc32c((uint8_t *)hash, len);
        snprintf(newname, newnamelen, "$%u_%X", nextnum, len);
    }

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

int _some_callback(void *theresult, int ncols, char **vals, char **cols)
{
    *(int *)theresult = atoi(vals[0]);

    return SQLITE_OK;
}

/* some ripoff from sqlanalyze.c */
static int _convert_time(char *sql)
{
    sqlite3 *sqldb;
    int rc;
    int crc;
    int got_curtran = 0;
    char *msg;
    struct sql_thread *thd;
    int ret = INT_MAX;
    ;

    struct sqlclntstate client;
    reset_clnt(&client, NULL, 1);
    strncpy(client.tzname, "UTC", sizeof(client.tzname));
    sql_set_sqlengine_state(&client, __FILE__, __LINE__, SQLENG_NORMAL_PROCESS);
    client.dbtran.mode = TRANLEVEL_SOSQL;
    client.sql = sql;

    start_sql_thread();

    thd = pthread_getspecific(query_info_key);
    sql_get_query_id(thd);
    client.debug_sqlclntstate = pthread_self();
    thd->clnt = &client;

    if ((rc = get_curtran(thedb->bdb_env, &client)) != 0) {
        logmsg(LOGMSG_ERROR, "%s: failed to open a new curtran, rc=%d\n", __func__, rc);
        goto cleanup;
    }
    got_curtran = 1;

    if ((rc = sqlite3_open_serial("db", &sqldb, NULL)) != 0) {
        logmsg(LOGMSG_ERROR, "%s:sqlite3_open rc %d\n", __func__, rc);
        goto cleanup;
    }

    if ((rc = sqlite3_exec(sqldb, sql, _some_callback, &ret, &msg)) != 0) {
        logmsg(LOGMSG_ERROR, "query:%s failed rc %d: %s\n", sql, rc,
               msg ? msg : "<unknown error>");
        goto cleanup;
    }
    thd->clnt = NULL;

    if ((crc = sqlite3_close(sqldb)) != 0)
        logmsg(LOGMSG_ERROR, "close rc %d\n", crc);

cleanup:
    if (got_curtran) {
        crc = put_curtran(thedb->bdb_env, &client);
        if (crc)
            logmsg(LOGMSG_ERROR, "%s: failed to close curtran\n", __func__);
    }

    thd->clnt = NULL;
    done_sql_thread();
    return ret;
}

/* get view and check */
static timepart_view_t *_get_view_check(timepart_views_t *views,
                                        const char *name)
{
    timepart_view_t *view;

    view = _get_view(views, name);
    if (!view) {
        logmsg(LOGMSG_ERROR, "%s: view \"%s\" removed?\n", __func__, name);
        return NULL;
    }

    if (view->nshards != view->retention + 1) {
        logmsg(LOGMSG_ERROR, "%s: nshards wrong??? retention=%d, nshards=%d\n",
                __func__, view->retention, view->period);
        return NULL;
    }

    return view;
}

#if 0
/* a sleeping thread waiting for cleanup */
static void* _view_cleanup_thd(void *voidarg)
{
   timepart_views_t  *views = thedb->timepart_views;
   timepart_view_t   *view;
   char              *name = (char*)voidarg;
   char              *tblname;
   int               timetodelete;
   int               now;
   struct errstat    xerr;

   if(!name)
   {
      fprintf(stderr, "%s: oom\n", __func__);
      return NULL;
   }

   pthread_mutex_lock(&views_mtx);

   view = _get_view_check(views, name);
   if(!view)
      goto error;

   if(view->nshards <= view->retention)
   {
      /* already purged, maybe manual */
      fprintf(stderr, "%s: partition %s already purged\n", __func__, view->name);
      goto error;
   }

   timetodelete = view->shards[0].low;

   /* set this so that I don't duplicate deleters */
   view->purge_time = timetodelete;



   /* sleep until the moment */
   now = comdb2_time_epoch();

   if(now<timetodelete)
   {
      view = NULL;

      fprintf(stderr, "Sleeping %d before deleting oldest shard for view \"%s\"\n",
            timetodelete-now, name); 

      pthread_mutex_unlock(&views_mtx);

      sleep(timetodelete-now);

      pthread_mutex_lock(&views_mtx);


      view = _get_view_check(views, name);
      if(!view)
         goto error;

      if(view->nshards <= view->retention)
      {
         /* already purged, maybe manual */
         fprintf(stderr, "%s: woke up, but partition %s already purged\n", __func__, view->name);
         goto error;
      }
   }
  
   /* we have the oldest shard here in view->shard[view->retention] */
   view->shards[view->retention-1].low = INT_MIN;
   tblname = strdup(view->shards[view->retention].tblname);
   bzero(&view->shards[view->retention], sizeof(view->shards[0]));
  
   /* we have removed the table from visible view, all we have to do is 
   notify the sqlite engines of the change, and do a lazy table drop */

   rdlock_schema_lk();
   ++gbl_views_gen;
   view->version = gbl_views_gen;
   unlock_schema_lk();


   /* we are done with critical path */
   pthread_mutex_unlock(&views_mtx);

   sc_timepart_drop_table(tblname, &xerr);

   free(tblname);
   free(name);

   view->purge_time = 0;
   
   return NULL;

error:

   view->purge_time = 0;

   pthread_mutex_lock(&views_mtx);
   free(name);
   return NULL;
}
#endif

static int _views_do_op(timepart_views_t *views, const char *name,
                        int (*op)(timepart_views_t *, timepart_view_t *,
                                  struct errstat *),
                        struct errstat *err)
{
    timepart_view_t *view;
    char *view_str;
    int view_str_len;

    int rc = VIEW_NOERR;

    pthread_rwlock_wrlock(&views_lk);

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
    rc = views_write_view(NULL, view->name, view_str);
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

    pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Phase 1 of the rollout, create the next table
 *
 */
void *_view_cron_phase1(uuid_t source_id, void *arg1, void *arg2, void *arg3,
                        struct errstat *err)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    timepart_view_t *view;
    char *name = (char *)arg1;
    char *pShardName = NULL;
    int rc;
    char *tmp_str;
    int run;
    int shardChangeTime;

    if (!name) {
        errstat_set_rc(err, VIEW_ERR_BUG);
        errstat_set_strf(err, "%s no name?", __func__);
        run = 0;
        goto done;
    }

    assert(arg2 == NULL);
    assert(arg3 == NULL);

    run = (!gbl_exit);
    if(run && thedb->master != gbl_mynode)
        run = 0;

    if (run) {
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
        pthread_rwlock_wrlock(&views_lk);

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
        if(unlikely(_validate_view_id(view, source_id, "phase 1", err))) {
            /*TODO*/
            goto done;
        }

        if (view->nshards > view->retention) {
            errstat_set_strf(err, "view %s already rolled, missing purge?",
                             view->name);
            errstat_set_rc(err, rc = VIEW_ERR_BUG);
            goto done;
        }

        BDB_READLOCK(__func__);

        rc = _views_rollout_phase1(view, &pShardName, err);
        shardChangeTime = view->roll_time;

        BDB_RELLOCK();

        /* do NOT override rc at this point! */
    }

done:
    if (run) {
        pthread_rwlock_unlock(&views_lk);
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

        /* queue the next event, done with the mutex released to avoid
           racing against scheduler callback runs */
        if (rc == VIEW_NOERR) {
            print_dbg_verbose(view->name, &view->source_id, "LLL",
                              "Adding phase 2 at %d for %s\n",
                              shardChangeTime, pShardName);

            if (cron_add_event(timepart_sched, NULL, shardChangeTime,
                               _view_cron_phase2, tmp_str = strdup(name),
                               pShardName, NULL, &view->source_id, err) == NULL) {
                logmsg(LOGMSG_ERROR, "%s: failed rc=%d errstr=%s\n", __func__,
                        err->errval, err->errstr);
                if (tmp_str)
                    free(tmp_str);
                if (pShardName)
                    free(pShardName);
            }
        } else {
            _handle_view_event_error(view, source_id, err);
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
    int delete_lag = thedb->timepart_views->rollout_delete_lag;
    int preemptive_rolltime = thedb->timepart_views->preemptive_rolltime;
    char *tmp_str;
    int tm;
    int rc = FDB_NOERR;

    if (unlikely(view->period == VIEW_TIMEPART_TEST2MIN)) {
        preemptive_rolltime = 30; /* 30 seconds in advance we add a new table */
        delete_lag = 5;
    }

    if (removeShardName) {
        /* we need to schedule a purge */
        tm = timeCrtRollout + delete_lag;

        print_dbg_verbose(view->name, &view->source_id, "LLL",
                          "Adding phase 3 at %d for %s\n", 
                          tm, removeShardName);

        if (cron_add_event(timepart_sched, NULL, tm, _view_cron_phase3,
                           removeShardName, NULL, NULL, &view->source_id, 
                           err) == NULL) {
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

    if (cron_add_event(timepart_sched, NULL, tm, _view_cron_phase1,
                       tmp_str = strdup(name), NULL, NULL, &view->source_id,
                       err) == NULL) {
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
void *_view_cron_phase2(uuid_t source_id, void *arg1, void *arg2, void *arg3, 
                        struct errstat *err)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    timepart_view_t *view;
    char *name = (char *)arg1;
    char *pShardName = (char *)arg2;
    int run = 0;
    int timeNextRollout;
    int timeCrtRollout;
    char *removeShardName;
    int rc;
    int bdberr;

    if (!name || !pShardName) {
        errstat_set_rc(err, VIEW_ERR_BUG);
        errstat_set_strf(err, "%s no name or shardname?", __func__);
        run = 0;
        goto done;
    }

    assert(arg3 == NULL);

    run = (!gbl_exit);
    if(run && thedb->master != gbl_mynode)
        run = 0;

    if (run) {
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
        rdlock_schema_lk();
        pthread_rwlock_wrlock(&views_lk);

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
        if(unlikely(_validate_view_id(view, source_id, "phase 2", err))) {
            /*TODO*/
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
        pthread_rwlock_unlock(&views_lk);
        unlock_schema_lk();
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

        /*  schedule next */
        if (rc == VIEW_NOERR) {
            rc = _view_cron_schedule_next_rollout(view, timeCrtRollout,
                                                  timeNextRollout,
                                                  removeShardName, name, err);
            return NULL;
        } else {
            _handle_view_event_error(view, source_id, err);
        }
    }

    return NULL;
}

/**
 * Phase 3 of the rollout, add the table to the view
 *
 */
void *_view_cron_phase3(uuid_t source_id, void *arg1, void *arg2, void *arg3,
                        struct errstat *err)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    char *pShardName = (char *)arg1;
    int run = 0;
    int tm;
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
    if(run && thedb->master != gbl_mynode)
        run = 0;

    if (run) {
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
        pthread_rwlock_wrlock(&views_lk); /* I might decide to not lock this */

        BDB_READLOCK(__func__);

        rc = _views_rollout_phase3(pShardName, err);

        BDB_RELLOCK();

        if (rc != VIEW_NOERR) {
            logmsg(LOGMSG_ERROR, "%s: phase 3 failed rc=%d errstr=%s\n", __func__,
                    err->errval, err->errstr);
        }
    }

done:
    if (run) {
        pthread_rwlock_unlock(&views_lk);
        bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

#if 0
        if(rc!=VIEW_NOERR)
        {
            /* update the error */
            struct errstat newerr;
            errstat_set_strf(&newerr, "%s: %s", view->name, err->errstr);
            *err = newerr;
        }
#endif

    }

    return NULL;
}

/**
 * Start a views cron thread
 *
 */
static int _start_views_cron(void)
{
    struct errstat xerr = {0};

    if (timepart_sched)
        abort();

    timepart_sched =
        cron_add_event(NULL, "timepart_cron", INT_MIN, timepart_cron_kickoff,
                       NULL, NULL, NULL, NULL, &xerr);

    return (!timepart_sched) ? xerr.errval : VIEW_NOERR;
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

    pthread_rwlock_rdlock(&views_lk);

    view = _check_shard_collision(views, tblname, indx, _CHECK_ALL_SHARDS);
    if(view) {
        if(*indx==-1)
            *partname = strdup(view->shard0name);
        else
            *partname = strdup(view->shards[*indx].tblname);

        rc = VIEW_ERR_EXIST;
    }

    pthread_rwlock_unlock(&views_lk);
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

    pthread_rwlock_rdlock(&views_lk);

    for(i=0; i<views->nviews; i++) {
        view = views->views[i];

        info = comdb2_partition_info_locked(view->name, option);

        if(!info) {
            logmsg(LOGMSG_ERROR, "Partition \"%s\" has problems!\n", view->name);
        } else {
            logmsg(LOGMSG_USER, "Partition \"%s\":\n%s\n", view->name, info);
        }
    }

    pthread_rwlock_unlock(&views_lk);
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

    pthread_rwlock_rdlock(&views_lk);

    ret_str = comdb2_partition_info_locked(partition_name, option);

    pthread_rwlock_unlock(&views_lk);

    return ret_str;
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
                "%s%s\"%s\"%s%s%s%s", cols_str,
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
                sqlite3_mprintf("%scoalesce(new.\"%s\", %s)%s", cols_str,
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

static void *timepart_cron_kickoff(uuid_t source_id, void *arg1, void *arg2, 
                                   void *arg3, struct errstat *err)
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
    view->shards[0].low = view->shards[1].high = view->roll_time;
    view->shards[0].high = INT_MAX;
    /* we we purge oldest shard, we need to adjust the min of current oldest */
    view->shards[view->nshards - 1].low = INT_MIN;

    /* we need to schedule the next rollout */
    *timeNextRollout = _view_get_next_rollout(
        view->period, view->starttime, view->shards[0].low, view->nshards, 0);
    if ((*timeNextRollout) == INT_MAX) {
        errstat_set_strf(err, "Failed to compute next rollout time");
        return err->errval = VIEW_ERR_BUG;
    }
    view->roll_time = *timeNextRollout;

    tran = bdb_tran_begin_set_retries(thedb->bdb_env, NULL, 0, &bdberr);
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
    rc = _view_rollout_publish(tran, view, err);
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

/* done under mutex */
static int _view_restart(timepart_view_t *view, struct errstat *err)
{
    int delete_lag = thedb->timepart_views->rollout_delete_lag;
    int preemptive_rolltime = thedb->timepart_views->preemptive_rolltime;
    char next_existing_shard[MAXTABLELEN + 1];
    char evicted_shard[MAXTABLELEN + 1];
    struct dbtable *evicted_db;
    int evicted_time;
    int tm;
    int rc;
    char *tmp_str1;
    char *tmp_str2;
    int evicted_shard0;


    if (unlikely(view->period == VIEW_TIMEPART_TEST2MIN)) {
        preemptive_rolltime = 30; /* 30 seconds in advance we add a new table */
        delete_lag = 5;
    }

    evicted_shard0 = 0;
    /* if we are NOT in filling stage, recover phase 3 first */
   if((view->nshards == view->retention) &&
         /* if the oldest shard is the initial table, nothing to do ! */
         (strcasecmp(view->shard0name, view->shards[view->retention-1].tblname
         ))) {
        /* check if the previously evicted stage still exists */
        rc = _generate_evicted_shard_name(view, evicted_shard,
                                          sizeof(evicted_shard), &evicted_time);
        if (rc != VIEW_NOERR) {
            errstat_set_strf(err, "Failed to generate evicted shard name");
            return err->errval = VIEW_ERR_BUG;
        }


        evicted_db = get_dbtable_by_name(evicted_shard);
        if(!evicted_db) {
            /* there is one exception here; during the initial reach of "retention"
               shards, the phase 3 that can be lost is for shard0name, not "$0_..."
               If "$0_..." is missing, check also the initial shard, so we don't leak it */
            if(strncasecmp(evicted_shard, "$0_", 3) == 0) {
                evicted_db = get_dbtable_by_name(view->shard0name);
                if(evicted_db) {
                    strncpy(evicted_shard, view->shard0name, sizeof(evicted_shard));
                    evicted_shard0 = 1;
                }
            }
        }

        if (evicted_db) {
            /* we cannot jump into chron scheduler keeping locks that chron
               functions
               might acquire */
            pthread_rwlock_unlock(&views_lk);

            print_dbg_verbose(view->name, &view->source_id, "RRR",
                              "Adding phase 3 at %d for %s\n",
                              evicted_time - preemptive_rolltime,
                              evicted_shard);

            /* we missed phase 3, queue it */
            rc = (cron_add_event(
                      timepart_sched, NULL, evicted_time - preemptive_rolltime,
                      _view_cron_phase3, tmp_str1 = strdup(evicted_shard), NULL,
                      NULL, &view->source_id, err) == NULL)
                     ? err->errval
                     : VIEW_NOERR;

            /* get back views global lock */
            pthread_rwlock_wrlock(&views_lk);

            if (rc != VIEW_NOERR) {
                logmsg(LOGMSG_ERROR, "%s: failed rc=%d errstr=%s\n", __func__,
                        err->errval, err->errstr);
                free(tmp_str1);
                return rc;
            }
        }
    }

    /* recover phase 2 */
    tm = _view_get_next_rollout(view->period, view->starttime,
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
        if (evicted_shard0 || (view->nshards < view->retention)) {
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

        rc = (cron_add_event(timepart_sched, NULL,
                             view->roll_time - preemptive_rolltime,
                             _view_cron_phase1, tmp_str1 = strdup(view->name),
                             NULL, NULL, &view->source_id, err) == NULL)
                 ? err->errval
                 : VIEW_NOERR;
        tmp_str2 = NULL;
    } else {
        assert(rc == VIEW_ERR_EXIST);
        print_dbg_verbose(view->name, &view->source_id, "RRR",
                          "Adding phase 2 at %d for %s\n",
                          view->roll_time, next_existing_shard);

        rc = (cron_add_event(timepart_sched, NULL, view->roll_time,
                             _view_cron_phase2, tmp_str1 = strdup(view->name),
                             tmp_str2 = strdup(next_existing_shard), NULL,
                             &view->source_id, err) == NULL)
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
        if (gbl_schema_change_in_progress) {
            logmsg(LOGMSG_ERROR, "Schema change started too early for time "
                                 "partition: aborting\n");
            gbl_sc_abort = 1;
            MEMORY_SYNC;
        }
        pthread_rwlock_wrlock(&views_lk);
    } else if (rc) {
        abort();
    }

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
    BDB_READLOCK(__func__);

    if(thedb->master == gbl_mynode) {
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

            rc = _view_restart(view, &xerr);
            if(rc!=VIEW_NOERR)
            {
                goto done;
            }
        }
    }
    rc = VIEW_NOERR;

done:
    BDB_RELLOCK();

    pthread_rwlock_unlock(&views_lk);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);
    return rc;
}

static int _generate_new_shard_name_wrapper(timepart_view_t *view,
                                            char *newShardName,
                                            int newShardNameLen)
{
    int nextNum;
    struct errstat xerr = {0};
    int rc;

    /* extract the next id to be used */
    if (view->nshards == view->retention) {
        nextNum = _extract_shardname_index(view->shards[0].tblname, 
                                           view->shard0name, view->retention);
        nextNum = (nextNum + 1) % (view->retention + 1);
    } else {
        nextNum = view->nshards;
    }

    /* generate new filename */
    rc = _generate_new_shard_name(
        view->shard0name, newShardName, newShardNameLen, nextNum,
        view->retention, view->period == VIEW_TIMEPART_TEST2MIN, &xerr);
    if (rc != VIEW_NOERR) {
        return xerr.errval;
    }

    return VIEW_NOERR;
}

static int _generate_evicted_shard_name(timepart_view_t *view,
                                        char *evictedShardName,
                                        int evictedShardNameLen,
                                        int *rolloutTime)
{
    int nextNum;
    struct errstat xerr = {0};
    int rc;

    if (view->nshards < view->retention) {
        /* no eviction yet */
        return VIEW_ERR_EXIST;
    }

    nextNum = _extract_shardname_index(view->shards[0].tblname, 
                                       view->shard0name, view->retention);
    /*
     * whena adding t0, evicting t1
     * +t1 => -t2
     * ...
     * +t(N-1) => -tN
     * +tN => t0
     */
    nextNum = (nextNum+1) % (view->retention+1);

    /* generate new filename */
    rc = _generate_new_shard_name(
        view->shard0name, evictedShardName, evictedShardNameLen, nextNum,
        view->retention, view->period == VIEW_TIMEPART_TEST2MIN, &xerr);
    if (rc != VIEW_NOERR) {
        return xerr.errval;
    }

    *rolloutTime = _view_get_next_rollout(
        view->period, view->starttime, view->shards[0].low, view->nshards, 1);
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

    rc = _generate_new_shard_name_wrapper(view, newShardName, newShardNameLen);
    if (rc != VIEW_NOERR)
        return rc;

    /* does this table exists ?*/
    db = get_dbtable_by_name(newShardName);
    if (db) {
        return VIEW_ERR_EXIST;
    }

    return VIEW_NOERR;
}

/* NOTE: this is done under views_lk MUTEX ! */
static int _view_rollout_publish(void *tran, timepart_view_t *view,
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
    rc = views_write_view(tran, view->name, view_str);
    if (rc != VIEW_NOERR) {
        errstat_set_strf(err, "Failed to llmeta save view %s", view->name);
        errstat_set_rc(err, err->errval = VIEW_ERR_LLMETA);
        goto done;
    }

done:
    if (view_str)
        free(view_str);
    return rc;
}

/**
 * this function takes into account various time anomolies and
 * differences from day to day, month to month, year to year
 * to establish the next rollout time
 *
 */
static int _view_get_next_rollout(enum view_timepart_period period,
                                  int startTime, int crtTime, int nshards,
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

    fmt = (back_in_time) ? fmt_backward : fmt_forward;

    /* time reference */
    if (crtTime == INT_MIN) {
        /* if this is the first shard incompassing all records, starttime tells
           us
           where we shard this in two */
        assert(nshards == 1);

        return startTime;
    }

    /* if there are at least 2 hards, low was the original split, and now we
       generate a new split based on newest split + period */
    switch (period) {
    case VIEW_TIMEPART_DAILY:
        /* 24 hours */
        cast_str = "hours";
        cast_val = 24;
        break;
    case VIEW_TIMEPART_WEEKLY:
        /* 7 days */
        cast_str = "days";
        cast_val = 7;
        break;
    case VIEW_TIMEPART_MONTHLY:
        /* 1 month */
        cast_str = "months";
        cast_val = 1;
        break;
    case VIEW_TIMEPART_YEARLY:
        /* 1 year */
        cast_str = "years";
        cast_val = 1;
        break;
    case VIEW_TIMEPART_TEST2MIN:
        /* test 2 mins */
        cast_str = "minutes";
        cast_val = 2;
        break;
    case VIEW_TIMEPART_INVALID:
        logmsg(LOGMSG_ERROR, "%s bug!\n", __func__);
        return INT_MAX;
    }

    snprintf(query, sizeof(query), fmt, crtTime, cast_val, cast_str);

    /* note: this is run when a new rollout is decided.  It doesn't have
    to be fast, or highly optimized (like running directly datetime functions */
    timeNextRollout = _convert_time(query);

    return timeNextRollout;
}

/**
 * Signal looping workers of views of db event like exiting
 *
 */
void views_signal(timepart_views_t *views)
{
    pthread_rwlock_rdlock(&views_lk);

    if (views && timepart_sched) {
        cron_signal_worker(timepart_sched);
    }

done:
    pthread_rwlock_unlock(&views_lk);
}

static void _remove_view_entry(timepart_views_t *views, int i)
{
    /* we don't deallocate, not a bug; realloc will turn NOP */
    if (i < views->nviews - 1) {
        memmove(&views->views[i], &views->views[i + 1],
                (views->nviews - i - 1) * sizeof(views->views[0]));
    } else {
        assert(i == (views->nviews - 1));
        views->views[i] = NULL;
    }
    views->nviews--;
}

/**
 * Best effort to validate a view;  are the table present?
 * Is there another partition colliding with it?
 *
 */
int views_validate_view(timepart_views_t *views, timepart_view_t *view, 
                        struct errstat *err)
{
    timepart_view_t *chk_view;
    struct dbtable *db;
    int rc;
    int i;
    int indx;

    rc = VIEW_NOERR;

    pthread_rwlock_rdlock(&views_lk);

    /* check partition name collision */
    chk_view = _check_shard_collision(views, view->name, &indx, 
                                      _CHECK_ALL_SHARDS);
    if(chk_view) {
        if(indx==-1)
            errstat_set_strf(err, "Name %s matches seed shard partition \"%s\"",
                    view->name, chk_view->name);
        else
            errstat_set_strf(err, "Name %s matches shard %d partition \"%s\"",
                    view->name, indx, chk_view->name);
        errstat_set_rc(err, rc = VIEW_ERR_EXIST);
        goto done;
    }

    if(bdb_attr_get(thedb->bdb_attr, BDB_ATTR_TIMEPART_CHECK_SHARD_EXISTENCE)) {
        for(i=0;i<view->nshards;i++) {  
            /* do all the shards exist? */
            db = get_dbtable_by_name(view->shards[i].tblname);
            if(!db) {
                errstat_set_strf(err, "Partition %s shard %s doesn't exist!",
                        view->name, view->shards[i].tblname);
                errstat_set_rc(err, rc = VIEW_ERR_EXIST);
                goto done;
            }
        }
    }


done:
    pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Run "func" for each shard, starting with "first_shard".
 * Callback receives the name of the shard and argument struct
 *
 */
int timepart_foreach_shard(const char *view_name,
                           int func(const char *, timepart_sc_arg_t *),
                           timepart_sc_arg_t *arg, int first_shard)
{
    struct schema_change_type *s = arg->s;
    const char *original_name = s->table;
    timepart_views_t *views;
    timepart_view_t *view;
    int rc;
    int i;

    pthread_rwlock_rdlock(&views_lk);

    views = thedb->timepart_views;

    view = _get_view(views, view_name);
    if (!view) {
        rc = VIEW_ERR_EXIST;
        goto done;
    }
    if (arg) {
        arg->view_name = view_name;
        arg->nshards = view->nshards;
    }
    for (i = first_shard; i < view->nshards; i++) {
        if (arg)
            arg->indx = i;
        rc = func(view->shards[i].tblname, arg);
        if (rc) {
            break;
        }
    }

done:
    pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * Under views lock, call a function for each shard
 * 
 */
int timepart_for_each_shard(const char *name,
      int (*func)(const char *shardname))
{
   timepart_views_t  *views = thedb->timepart_views;
   timepart_view_t   *view;
   int               rc = VIEW_NOERR, irc;
   int               i;

   pthread_rwlock_rdlock(&views_lk);

   view = _get_view(views, name);
   if(!view)
   {
      rc = VIEW_ERR_EXIST;
      goto done;
   }


   for(i=0;i<view->nshards; i++)
   {
      irc = func(view->shards[i].tblname);
      if(irc && rc == VIEW_NOERR)
      {
         rc = VIEW_ERR_SC;
      }
   }

done:
    pthread_rwlock_unlock(&views_lk);

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
 *
 */
int timepart_update_retention(void *tran, const char *name, int retention, struct errstat *err)
{
   timepart_views_t *views = thedb->timepart_views;
   timepart_view_t *view;
   int rc = VIEW_NOERR;

   pthread_rwlock_wrlock(&views_lk);

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
      view->retention = retention;

      rc = _view_rollout_publish(tran, view, err);
      if(rc!=VIEW_NOERR)
      {
         goto done;
      }
   }

done:
    pthread_rwlock_unlock(&views_lk);
    return rc;
}

/**
 * Locking the views subsystem, needed for ordering locks with schema
 *
 */
void views_lock(void)
{
    pthread_rwlock_rdlock(&views_lk);
}
void views_unlock(void)
{
    pthread_rwlock_unlock(&views_lk);
}

/**
 * Get the name of the newest shard
 *
 */
char *timepart_newest_shard(const char *view_name, unsigned long long *version)
{
    timepart_views_t *views = thedb->timepart_views;
    timepart_view_t *view;
    char *ret = NULL;

    pthread_rwlock_rdlock(&views_lk);

    view = _get_view(views, view_name);
    if (view) {
        struct dbtable *db;

        ret = strdup(view->shards[0].tblname);

        if (version && (db = get_dbtable_by_name(ret)))
            *version = db->tableversion;
    }

    pthread_rwlock_unlock(&views_lk);

    return ret;
}

/**
 * Time partition schema change resume
 *
 */
int timepart_resume_schemachange(int check_llmeta(const char *))
{
    timepart_views_t *views;
    timepart_view_t *view;
    int i;
    int rc = 0;

    pthread_rwlock_wrlock(&views_lk);

    views = thedb->timepart_views;
    for (i = 0; i < views->nviews; i++) {
        view = views->views[i];
        rc = check_llmeta(view->name);
        if (rc)
            break;
    }

    pthread_rwlock_unlock(&views_lk);

    return rc;
}

/**
 * During resume, we need to check at if the interrupted alter made any
 * progress, and continue with that shard
 *
 */
int timepart_schemachange_get_shard_in_progress(const char *view_name,
                                                int check_llmeta(const char *))
{
    timepart_views_t *views;
    timepart_view_t *view;
    int rc;
    int i;

    pthread_rwlock_wrlock(&views_lk);

    views = thedb->timepart_views;

    view = _get_view(views, view_name);
    if (view) {
        for (i = 0; i < view->nshards; i++) {
            rc = check_llmeta(view->shards[i].tblname);
            if (rc)
                break;
        }
    }
    if (rc == 1)
        rc = i;

    pthread_rwlock_unlock(&views_lk);

    return rc;
}

static int _view_update_table_version(timepart_view_t *view, tran_type *tran)
{
    struct dbtable *db;
    unsigned long long version;
    int rc = VIEW_NOERR;

    /* get existing versioning, if any */
    if (strcmp(view->shards[0].tblname, view->shard0name)) {
        assert(view->nshards > 1);

        version = comdb2_table_version(view->shards[1].tblname);

        rc = table_version_set(tran, view->shards[0].tblname, version);
        if (rc)
            rc = VIEW_ERR_LLMETA;
    }

    return rc;
}

/**
 * Returned a malloced string for the "iRowid"-th timepartition, column iCol
 * NOTE: this is called with a read lock in views structure
 */
void timepart_systable_column(sqlite3_context *ctx, int iRowid,
                              enum systable_columns iCol)
{
    timepart_views_t *views = thedb->timepart_views;
    timepart_view_t *view;
    uuidstr_t us;

    if (iRowid < 0 || iRowid >= views->nviews || iCol >= VIEWS_MAXCOLUMN) {
        sqlite3_result_null(ctx);
    }

    view = views->views[iRowid];

    switch (iCol) {
    case VIEWS_NAME:
        sqlite3_result_text(ctx, view->name, -1, NULL);
        break;
    case VIEWS_PERIOD:
        sqlite3_result_text(ctx, period_to_name(view->period), -1, NULL);
        break;
    case VIEWS_RETENTION:
        sqlite3_result_int(ctx, view->retention);
        break;
    case VIEWS_NSHARDS:
        sqlite3_result_int(ctx, view->nshards);
        break;
    case VIEWS_VERSION: {
        struct dbtable *table = get_dbtable_by_name(view->shards[0].tblname);
        int version = 0;
        assert(table);

        sqlite3_result_int(ctx, table->tableversion);
    } break;
    case VIEWS_SHARD0NAME:
        sqlite3_result_text(ctx, view->shard0name, -1, NULL);
        break;
    case VIEWS_STARTTIME:
        sqlite3_result_int(ctx, view->starttime);
        break;
    case VIEWS_SOURCEID:
        sqlite3_result_text(ctx, comdb2uuidstr(view->source_id, us), -1, NULL);
        break;
    }
}

/**
 * Returned a malloced string for the "iRowid"-th shard, column iCol of
 * timepart iTimepartId
 * NOTE: this is called with a read lock in views structure
 */
void timepart_systable_shard_column(sqlite3_context *ctx, int iTimepartId,
                                    int iRowid,
                                    enum systable_shard_columns iCol)
{
    timepart_views_t *views = thedb->timepart_views;
    timepart_view_t *view;
    timepart_shard_t *shard;
    uuidstr_t us;

    if (iTimepartId < 0 || iTimepartId >= views->nviews ||
        iCol >= VIEWS_SHARD_MAXCOLUMN) {
        sqlite3_result_null(ctx);
        return;
    }

    view = views->views[iTimepartId];

    if (iRowid >= view->nshards) {
        sqlite3_result_null(ctx);
        return;
    }
    shard = &view->shards[iRowid];

    switch (iCol) {
    case VIEWS_SHARD_VIEWNAME:
        sqlite3_result_text(ctx, view->name, -1, NULL);
        break;
    case VIEWS_SHARD_NAME:
        sqlite3_result_text(ctx, shard->tblname, -1, NULL);
        break;
    case VIEWS_SHARD_START:
        sqlite3_result_int(ctx, shard->low);
        break;
    case VIEWS_SHARD_END:
        sqlite3_result_int(ctx, shard->high);
        break;
    }
}

/**
 * Get number of views
 *
 */
int timepart_get_num_views(void)
{
    return thedb->timepart_views->nviews;
}

/**
 *  Move iRowid to point to the next shard, switching shards in the process
 *  NOTE: this is called with a read lock in views structure
 */
void timepart_systable_next_shard(int *piTimepartId, int *piRowid)
{
    timepart_views_t *views = thedb->timepart_views;
    timepart_view_t *view;

    if (*piTimepartId >= views->nviews)
        return;
    view = views->views[*piTimepartId];
    (*piRowid)++;
    if (*piRowid >= view->nshards) {
        *piRowid = 0;
        (*piTimepartId)++;
    }
}

/**
 * Open/close the event queue
 */
int timepart_events_open(int *num)
{
    cron_lock(timepart_sched);

    *num = cron_num_events(timepart_sched);

    return VIEW_NOERR;
}

int timepart_events_close(void)
{
    cron_unlock(timepart_sched);

    return VIEW_NOERR;
}

static const char *_events_name(FCRON func)
{
    const char *name;
    if (func == _view_cron_phase1)
        name = "AddShard";
    else if (func == _view_cron_phase2)
        name = "RollShards";
    else if (func == _view_cron_phase3)
        name = "DropShard";
    else
        name = "Unknown";

    return name;
}

/**
 * Get event data
 */
void timepart_events_column(sqlite3_context *ctx, int iRowid, int iCol)
{
    const char *name;
    FCRON func;
    int epoch;
    uuid_t *sid;
    void *arg1;
    void *arg2;
    void *arg3;
    uuidstr_t us;

    if (iRowid < 0 || iCol >= VIEWS_SHARD_MAXCOLUMN) {
        sqlite3_result_null(ctx);
        return;
    }

    if (cron_event_details(timepart_sched, iRowid, &func, &epoch, &arg1, &arg2,
                           &arg3, sid) != 1) {
        sqlite3_result_null(ctx);
        return;
    }

    switch (iCol) {
    case VIEWS_EVENT_NAME:
        name = _events_name(func);
        sqlite3_result_text(ctx, name, -1, NULL);
        break;
    case VIEWS_EVENT_WHEN:
        sqlite3_result_int(ctx, epoch);
        break;
    case VIEWS_EVENT_SOURCEID:
        sqlite3_result_text(ctx, comdb2uuidstr(*sid, us), -1, NULL);
        break;
    case VIEWS_EVENT_ARG1:
        sqlite3_result_text(ctx, (char *)arg1, -1, NULL);
        break;
    case VIEWS_EVENT_ARG2:
        if (arg2)
            sqlite3_result_text(ctx, (char *)arg2, -1, NULL);
        else
            sqlite3_result_null(ctx);
        break;
    case VIEWS_EVENT_ARG3:
        sqlite3_result_null(ctx);
        break;
    }
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

#include "views_serial.c"

#include "views_sqlite.c"

