/*
   Copyright 2020 Bloomberg Finance L.P.

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

#include <comdb2.h>
#include <sql.h>
#include <metrics.h>
#include <logmsg.h>
#include <util.h>
#include "comdb2_query_preparer.h"

extern void rcache_init(size_t, size_t);
extern void rcache_destroy(void);

typedef struct pool_foreach_data {
    thdpool_foreach_fn callback; /* in: foreach_all_sql_pools */
    void *user;  /* in: foreach_all_sql_pools */
    SBUF2 *sb;   /* in: list_all_sql_pools */
    int64_t sum; /* out: list_all_sql_pools, get_all_sql_pool_timeouts */
} pool_foreach_data_t;

static struct thdpool *sqlengine_pool = NULL;
static hash_t *sqlengine_pool_hash = NULL;
static pthread_mutex_t sqlengine_pool_mutex = PTHREAD_MUTEX_INITIALIZER;

void sqlengine_thd_start(struct thdpool *pool, struct sqlthdstate *thd,
                         enum thrtype type)
{
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    sql_mem_init(NULL);

    thd->thr_self = thrman_register(type);
    thd->logger = thrman_get_reqlogger(thd->thr_self);
    thd->sqldb = NULL;
    thd->sqldbx = NULL;
    thd->stmt_cache = NULL;
    thd->have_lastuser = 0;
    thd->query_preparer_running = 0;

    start_sql_thread();

    thd->sqlthd = pthread_getspecific(query_info_key);
    rcache_init(bdb_attr_get(thedb->bdb_attr, BDB_ATTR_RCACHE_COUNT),
                bdb_attr_get(thedb->bdb_attr, BDB_ATTR_RCACHE_PGSZ));
}

void sqlengine_thd_end(struct thdpool *pool, struct sqlthdstate *thd)
{
    rcache_destroy();
    struct sql_thread *sqlthd;
    if ((sqlthd = pthread_getspecific(query_info_key)) != NULL) {
        /* sqlclntstate shouldn't be set: sqlclntstate is memory on another
         * thread's stack that will not be valid at this point. */

        if (sqlthd->clnt) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d sqlthd->clnt set in thd-teardown\n", __FILE__,
                   __LINE__);
            if (gbl_abort_invalid_query_info_key) {
                abort();
            }
            sqlthd->clnt = NULL;
        }
    }

    if (thd->stmt_cache)
        stmt_cache_delete(thd->stmt_cache);
    sqlite3_close_serial(&thd->sqldb);

    if (gbl_old_column_names && query_preparer_plugin &&
        query_preparer_plugin->do_cleanup_thd) {
        query_preparer_plugin->do_cleanup_thd(thd);
    }

    /* AZ moved after the close which uses thd for rollbackall */
    done_sql_thread();

    sql_mem_shutdown(NULL);

    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
}

static void thdpool_sqlengine_start(struct thdpool *pool, void *thd)
{
    sqlengine_thd_start(pool, (struct sqlthdstate *) thd,
                        THRTYPE_SQLENGINEPOOL);
}

static void thdpool_sqlengine_end(struct thdpool *pool, void *thd)
{
    sqlengine_thd_end(pool, (struct sqlthdstate *) thd);
}

static void thdpool_sqlengine_dque(struct thdpool *pool, struct workitem *item,
                                   int timeout)
{
    time_metric_add(thedb->sql_queue_time,
                    comdb2_time_epochms() - item->queue_time_ms);
}

static void clnt_queued_event(void *p)
{
    struct sqlclntstate *clnt = (struct sqlclntstate *)p;
    clnt_change_state(clnt, CONNECTION_QUEUED);
}

static struct thdpool *create_sql_pool(const char *zName, int nThreads)
{
    struct thdpool *pool = thdpool_create(
        (zName != NULL) ? zName : SQL_POOL_LEGACY_NAME,
        sizeof(struct sqlthdstate));

    if (pool == NULL) return NULL;

    if (!gbl_exit_on_pthread_create_fail)
        thdpool_unset_exit(pool);

    thdpool_set_stack_size(pool, SQL_POOL_STACK_SIZE);
    thdpool_set_init_fn(pool, thdpool_sqlengine_start);
    thdpool_set_delt_fn(pool, thdpool_sqlengine_end);
    thdpool_set_dque_fn(pool, thdpool_sqlengine_dque);
    thdpool_set_queued_callback(pool, clnt_queued_event);

    if (zName != NULL) {
        /* TODO: *TUNING* Defaults for non-default pools. */
        thdpool_set_maxthds(pool, nThreads);
        thdpool_set_maxqueueoverride(pool, SQL_POOL_NAMED_MAXQ_OVERRIDE);
    } else {
        thdpool_set_minthds(pool, SQL_POOL_DEFLT_MIN_THREADS);
        thdpool_set_maxthds(pool, SQL_POOL_DEFLT_MAX_THREADS);
        thdpool_set_linger(pool, SQL_POOL_LINGER_SECS);
        thdpool_set_maxqueueoverride(pool, SQL_POOL_DEFLT_MAXQ_OVERRIDE);
        thdpool_set_maxqueueagems(pool, SQL_POOL_MAXQ_AGE_MS);
        thdpool_set_dump_on_full(pool, 1);
    }

    return pool;
}

/*
** WARNING: The "try_add_sql_pool" function assumes the hash lock is already
**          held.
*/
static int try_add_sql_pool(const char *zName, int nThreads,
                            struct thdpool *pool)
{
    if (pool == NULL) return -1; /* cannot add, invalid pool */
    if (sqlengine_pool_hash == NULL)
    {
        sqlengine_pool_hash = hash_init_strptr(offsetof(pool_entry_t, zName));
        if (sqlengine_pool_hash == NULL) return -2; /* OOM (?) */
    }
    const char *zEntryName = (zName != NULL) ?
        thdpool_get_name(pool) : SQL_POOL_DEFLT_NAME;
    pool_entry_t *entry = hash_find(sqlengine_pool_hash, &zEntryName);
    if (entry != NULL) return -3; /* cannot add, already present */
    entry = calloc(1, sizeof(pool_entry_t));
    if (entry == NULL) return -4; /* OOM */
    entry->zName = zEntryName;
    entry->nThreads = nThreads; /* as specified */
    entry->pPool = pool;
    if (hash_add(sqlengine_pool_hash, entry) != 0) {
        free(entry);
        return -5;
    }
    return 0;
}

int get_default_sql_pool_max_threads(void)
{
    struct thdpool *pool = get_default_sql_pool(0);
    return (pool != NULL) ? thdpool_get_maxthds(pool) : SQL_POOL_DEFLT_MAX_THREADS;
}

struct thdpool *get_default_sql_pool(int bCreate)
{
    struct thdpool *pool = sqlengine_pool;
    if (bCreate && (pool == NULL)) {
        Pthread_mutex_lock(&sqlengine_pool_mutex);
        pool = sqlengine_pool; /* NOTE: Double-checked lock. */
        if (pool == NULL) {
            pool = create_sql_pool(NULL, 0);
            if (pool != NULL) {
                try_add_sql_pool(NULL, 0, pool);
                sqlengine_pool = pool;
            }
        }
        Pthread_mutex_unlock(&sqlengine_pool_mutex);
    }
    if (pool == NULL) abort(); /* NOTE: Default pool MUST exist. */
    return pool;
}

struct thdpool *get_sql_pool(struct sqlclntstate *clnt)
{
    if ((clnt != NULL) && (clnt->pPool != NULL)) return clnt->pPool;
    return get_default_sql_pool(0);
}

struct thdpool *get_named_sql_pool(const char *zName, int bCreate, int nThreads)
{
    if ((zName != NULL) && strcasecmp(zName, SQL_POOL_DEFLT_NAME)) {
        struct thdpool *pool = NULL;
        Pthread_mutex_lock(&sqlengine_pool_mutex);
        if (sqlengine_pool_hash != NULL) {
            pool_entry_t *entry = hash_find(sqlengine_pool_hash, &zName);
            if (entry != NULL) {
                pool = entry->pPool;
            } else if (bCreate) {
                pool = create_sql_pool(zName, nThreads);
                if (pool != NULL) {
                    if (try_add_sql_pool(zName, nThreads, pool) != 0) {
                        thdpool_destroy(&pool, SQL_POOL_STOP_TIMEOUT_US);
                        pool = NULL;
                    }
                }
            }
        }
        Pthread_mutex_unlock(&sqlengine_pool_mutex);
        return pool;
    } else {
        return get_default_sql_pool(bCreate);
    }
}

static int get_timeout_sql_pool_func(void *obj, void *arg)
{
    pool_entry_t *entry = (pool_entry_t *)obj;
    if ((entry == NULL) || (entry->pPool == NULL)) return 0;
    pool_foreach_data_t *data = (pool_foreach_data_t *)arg;
    if (data == NULL) return 0;
    data->sum += thdpool_get_timeouts(entry->pPool);
    return 0;
}

int64_t get_all_sql_pool_timeouts(void)
{
    pool_foreach_data_t data = {0};
    Pthread_mutex_lock(&sqlengine_pool_mutex);
    if (sqlengine_pool_hash != NULL) {
        hash_for(sqlengine_pool_hash, get_timeout_sql_pool_func, &data);
    }
    Pthread_mutex_unlock(&sqlengine_pool_mutex);
    return data.sum;
}

static int list_all_sql_pools_func(void *obj, void *arg)
{
    pool_entry_t *entry = (pool_entry_t *)obj;
    if (entry == NULL) return 0;
    struct thdpool *pool = entry->pPool;
    pool_foreach_data_t *data = (pool_foreach_data_t *)arg;
    SBUF2 *sb = (data != NULL) ? data->sb : NULL;
    if (sb != NULL) {
        /* NOTE: Being called from comdb2_save_ruleset(), use SBUF. */
        /* NOTE: Also, skip emitting the "default" SQL engine pool. */
        if ((entry->zName != NULL) &&
            (pool != get_default_sql_pool(0))) {
            sbuf2printf(sb, "pool %s", entry->zName);
            if (entry->nThreads != 0) {
                sbuf2printf(sb, " threads %lld", entry->nThreads);
            }
            sbuf2printf(sb, "\n");
            data->sum++;
        }
    } else {
        /* NOTE: Being called from process_command(), use logmsg. */
        logmsg(LOGMSG_USER,
               "%s: (name {%s}, threads %lld) ==> (name {%s}, threads %d)\n",
               __func__, (entry->zName != NULL) ? entry->zName : "<null>",
               entry->nThreads,
               (pool != NULL) ? thdpool_get_name(pool) : "<null>",
               (pool != NULL) ? thdpool_get_maxthds(pool) : -1);
        data->sum++;
    }
    return 0;
}

int list_all_sql_pools(SBUF2 *sb)
{
    pool_foreach_data_t data = {0};
    Pthread_mutex_lock(&sqlengine_pool_mutex);
    if (sqlengine_pool_hash != NULL) {
        data.sb = sb;
        hash_for(sqlengine_pool_hash, list_all_sql_pools_func, &data);
    }
    Pthread_mutex_unlock(&sqlengine_pool_mutex);
    return data.sum;
}

static int print_sql_pool_func(void *obj, void *arg)
{
    pool_entry_t *entry = (pool_entry_t *)obj;
    if ((entry == NULL) || (entry->pPool == NULL)) return 0;
    thdpool_print_stats(stdout, entry->pPool);
    return 0;
}

void print_all_sql_pool_stats(FILE *hFile)
{
    Pthread_mutex_lock(&sqlengine_pool_mutex);
    if (sqlengine_pool_hash != NULL) {
        hash_for(sqlengine_pool_hash, print_sql_pool_func, NULL);
    }
    Pthread_mutex_unlock(&sqlengine_pool_mutex);
}

static int foreach_sql_pool_func(void *obj, void *arg)
{
    pool_entry_t *entry = (pool_entry_t *)obj;
    if (entry == NULL) return 0;
    struct thdpool *pool = entry->pPool;
    if (pool == NULL) return 0;
    pool_foreach_data_t *data = (pool_foreach_data_t *)arg;
    if (data == NULL) return 0;
    Pthread_mutex_unlock(&sqlengine_pool_mutex);
    thdpool_foreach(pool, data->callback, data->user);
    Pthread_mutex_lock(&sqlengine_pool_mutex);
    return 0;
}

void foreach_all_sql_pools(thdpool_foreach_fn foreach_fn, void *user)
{
    Pthread_mutex_lock(&sqlengine_pool_mutex);
    if (sqlengine_pool_hash != NULL) {
        pool_foreach_data_t data = {0};
        data.callback = foreach_fn;
        data.user = user;
        hash_for(sqlengine_pool_hash, foreach_sql_pool_func, &data);
    }
    Pthread_mutex_unlock(&sqlengine_pool_mutex);
}

static int stop_sql_pool_func(void *obj, void *arg)
{
    pool_entry_t *entry = (pool_entry_t *)obj;
    if ((entry == NULL) || (entry->pPool == NULL)) return 0;
    thdpool_stop(entry->pPool);
    return 0;
}

void stop_all_sql_pools(void)
{
    Pthread_mutex_lock(&sqlengine_pool_mutex);
    if (sqlengine_pool_hash != NULL) {
        hash_for(sqlengine_pool_hash, stop_sql_pool_func, NULL);
    }
    Pthread_mutex_unlock(&sqlengine_pool_mutex);
}

static int resume_sql_pool_func(void *obj, void *arg)
{
    pool_entry_t *entry = (pool_entry_t *)obj;
    if ((entry == NULL) || (entry->pPool == NULL)) return 0;
    thdpool_resume(entry->pPool);
    return 0;
}

void resume_all_sql_pools(void)
{
    Pthread_mutex_lock(&sqlengine_pool_mutex);
    if (sqlengine_pool_hash != NULL) {
        hash_for(sqlengine_pool_hash, resume_sql_pool_func, NULL);
    }
    Pthread_mutex_unlock(&sqlengine_pool_mutex);
}

static int destroy_sql_pool_func(void *obj, void *arg)
{
    pool_entry_t *entry = (pool_entry_t *)obj;
    if (entry == NULL) return 0;
    /* NOTE: The "default" SQL engine pool cannot be destroyed here. */
    if ((entry->pPool != NULL) && (entry->pPool != sqlengine_pool)) {
        thdpool_destroy(&entry->pPool, SQL_POOL_STOP_TIMEOUT_US);
    }
    entry->zName = NULL; /* NOT OWNED, DO NOT FREE */
    free(entry);
    return 0;
}

int destroy_sql_pool(const char *zName, int coopWaitUs)
{
    int rc = 0;
    Pthread_mutex_lock(&sqlengine_pool_mutex);
    if (sqlengine_pool_hash != NULL) {
        pool_entry_t *entry = hash_find(sqlengine_pool_hash, &zName);
        if (entry != NULL) {
            if (hash_del(sqlengine_pool_hash, entry) == 0) {
                rc++;
            }
            /* NOTE: The "default" SQL engine pool cannot be destroyed here. */
            if ((entry->pPool != NULL) &&
                (entry->pPool != sqlengine_pool) &&
                (thdpool_destroy(&entry->pPool, coopWaitUs) == 0)) {
                rc += 2;
            }
            entry->zName = NULL; /* NOT OWNED, DO NOT FREE */
            free(entry);
        }
    }
    Pthread_mutex_unlock(&sqlengine_pool_mutex);
    return rc;
}

void destroy_all_sql_pools(void)
{
    Pthread_mutex_lock(&sqlengine_pool_mutex);
    if (sqlengine_pool_hash != NULL) {
        hash_for(sqlengine_pool_hash, destroy_sql_pool_func, NULL);
    }
    Pthread_mutex_unlock(&sqlengine_pool_mutex);
}
