/**
 * Handle to creation of views in sqlite
 *
 */
#include <sqlite3.h>
#include <sqliteInt.h>

static char *_views_create_view_query(timepart_view_t *view, sqlite3 *db,
                                      struct errstat *err);
static char *_views_destroy_view_query(const char *view_name, sqlite3 *db,
                                       struct errstat *err);
static int _views_run_sql(sqlite3 *db, const char *stmt_str,
                          struct errstat *err);
static int _view_delete_if_missing(const char *name, sqlite3 *db, void *arg);

//#ifdef COMDB2_UPDATEABLE_VIEWS
static int _views_create_triggers(timepart_view_t *view, sqlite3 *db,
                                  struct errstat *err);
static int _views_destroy_triggers(const char *view_name, sqlite3 *db,
                                   struct errstat *err);

static void dbg_verbose_sqlite(const char *fmt, ...);
#include "views_updates.c"
#include "logmsg.h"

//#endif

/**
 * Populate an sqlite db with views
 *
 */
int views_sqlite_update(timepart_views_t *views, sqlite3 *db,
                        struct errstat *err)
{
    timepart_view_t *view;
    Table *tab;
    int rc;
    int i;

    pthread_mutex_lock(&views_mtx);

    /* look at the in-memory views and check sqlite views */
    for (i = 0; i < views->nviews; i++) {
        view = views->views[i];

        /* check if this exists?*/
        tab = sqlite3FindTableCheckOnly(db, view->name, NULL);
        if (tab) {
            /* paranoia */
            if (tab->pSelect == NULL) {
                abort();
            }

            /* found view, is it the same version ? */
            if (view->version != tab->version) {
                /* older version, destroy current view */
                rc = views_sqlite_del_view(view, db, err);
                if (rc != VIEW_NOERR) {
                    logmsg(LOGMSG_ERROR, "%s: failed to remove old view\n",
                            __func__);
                    goto done;
                }
            } else {
                /* up to date, nothing to do */
                continue;
            }
        }

        /* add the view */
        rc = views_sqlite_add_view(views->views[i], db, err);
        if (rc != VIEW_NOERR) {
            goto done;
        }
    }

    /* at this point we covered view inserts and updates;
       check sqlite and make sure all the views still exist
     */
    rc = sqlite3PredicatedClearViews(db, _view_delete_if_missing, views);
    if (rc != SQLITE_OK) {
        rc = VIEW_ERR_SQLITE;
        goto done;
    }

    rc = VIEW_NOERR;

done:
    pthread_mutex_unlock(&views_mtx);

    return rc;
}

/**
 * Create a sqlite view
 *
 *
 */
int views_sqlite_add_view(timepart_view_t *view, sqlite3 *db,
                          struct errstat *err)
{
    char *stmt_str;
    int rc;

    /* create the statement */
    stmt_str = _views_create_view_query(view, db, err);
    if (!stmt_str) {
        return err->errval;
    }

    rc = _views_run_sql(db, stmt_str, err);

    /* free the statement */
    sqlite3DbFree(db, stmt_str);

    if (rc != VIEW_NOERR) {
        return err->errval;
    }

#ifdef COMDB2_UPDATEABLE_VIEWS
    rc = _views_create_triggers(view, db, err);
#endif

    return rc;
}

/* internal view delete function, callable from sqlite callback */
int _views_sqlite_del_view(const char *view_name, sqlite3 *db,
                           struct errstat *err)
{
    char *stmt_str;
    int rc;

#ifdef COMDB2_UPDATEABLE_VIEWS
    rc = _views_destroy_triggers(view_name, db, err);
#endif

    /* create the statement */
    stmt_str = _views_destroy_view_query(view_name, db, err);
    if (!stmt_str) {
        return err->errval;
    }

    rc = _views_run_sql(db, stmt_str, err);

    /* free the statement */
    sqlite3DbFree(db, stmt_str);

    return rc;
}

/**
 * Delete a sqlite view
 *
 *
 */
int views_sqlite_del_view(timepart_view_t *view, sqlite3 *db,
                          struct errstat *err)
{
    return _views_sqlite_del_view(view->name, db, err);
}

static char *_views_create_view_query(timepart_view_t *view, sqlite3 *db,
                                      struct errstat *err)
{
    char *select_str = NULL;
    char *cols_str = NULL;
    char *tmp_str = NULL;
    char *ret_str = NULL;
    const char *table0name;
    Table *pTbl;
    int i;

    if (view->nshards == 0) {
        err->errval = VIEW_ERR_BUG;
        snprintf(err->errstr, sizeof(err->errstr), "View %s has no shards???\n",
                 view->name);
        return NULL;
    }
    table0name = view->shards[0].tblname;

#if 0
   This is not ready yet, let us use the struct dbtable instead 
   /* extract schema from first table */
   pTbl = sqlite3FindTableCheckOnly(db, table0name, NULL);
   if(!pTbl)
   {
      err->errval = VIEW_ERR_BUG;
      snprintf(err->errstr, sizeof(err->errstr), "View %s has missing shard %s???\n",
            view->name, table0name);
      return NULL;
   }
 
   /* generate the column list */
   for(i=0;i<pTbl->nCol; i++)
   {
      cols_str = sqlite3_mprintf("\"%s\"%s",
         pTbl->aCol[i].zName,
         (i<(pTbl->nCol-1))?", ":"");
      if(!cols_str)
      {
         goto malloc;
      }
   }
#endif

    cols_str = sqlite3_mprintf("rowid as __hidden__rowid, ");
    if (!cols_str) {
        goto malloc;
    }
    cols_str = _describe_row(table0name, cols_str, VIEWS_TRIGGER_QUERY, err);
    if (!cols_str) {

        /* preserve error, if any */
        if (err->errval != VIEW_NOERR)
            return NULL;
        goto malloc;
    }

    /* generate the select union for shards */
    /* TODO: put conditions for shards */
    select_str = sqlite3_mprintf("");
    for (i = 0; i < view->nshards; i++) {
        tmp_str = sqlite3_mprintf("%s%sSELECT %s FROM \"%s\"", select_str,
                                  (i > 0) ? " UNION ALL " : "", cols_str,
                                  view->shards[i].tblname);
        sqlite3DbFree(db, select_str);
        if (!select_str) {
            sqlite3DbFree(db, cols_str);
            goto malloc;
        }
        select_str = tmp_str;
    }

/* generate create view statement */
#if 0
   ONLY IN sqlite ver 3.9.0     

   ret_str = sqlite3_mprintf("CREATE VIEW %s (%s) AS %s",
      view->name, cols_str, select_str);
#endif
    ret_str = sqlite3_mprintf("CREATE VIEW %s AS %s", view->name, select_str);
    if (!ret_str) {
        sqlite3DbFree(db, select_str);
        sqlite3DbFree(db, cols_str);
        goto malloc;
    }

    sqlite3DbFree(db, select_str);
    sqlite3DbFree(db, cols_str);

    dbg_verbose_sqlite("Generated:\n\"%s\"\n", ret_str);

    return ret_str;

malloc:
    err->errval = VIEW_ERR_MALLOC;
    snprintf(err->errstr, sizeof(err->errstr), "View %s out of memory\n",
             view->name);
    return NULL;
}

static char *_views_destroy_view_query(const char *view_name, sqlite3 *db,
                                       struct errstat *err)
{
    char *ret_str = NULL;

    ret_str = sqlite3_mprintf("DROP VIEW %s", view_name);
    if (!ret_str) {
        goto malloc;
    }

    return ret_str;

malloc:
    err->errval = VIEW_ERR_MALLOC;
    snprintf(err->errstr, sizeof(err->errstr), "View %s out of memory\n",
             view_name);
    return NULL;
}

static int _views_run_sql(sqlite3 *db, const char *stmt_str,
                          struct errstat *err)
{
    char *errstr = NULL;
    int rc;

    /* create the view */
    rc = sqlite3_exec(db, stmt_str, NULL, NULL, &errstr);
    if (rc != SQLITE_OK) {
        err->errval = VIEW_ERR_BUG;
        snprintf(err->errstr, sizeof(err->errstr), "Sqlite error \"%s\"",
                 errstr);
        /* can't control sqlite errors */
        err->errstr[sizeof(err->errstr) - 1] = '\0';

        logmsg(LOGMSG_ERROR, "%s: sqlite error \"%s\"\n", __func__, err->errstr);

        if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_TIMEPART_ABORT_ON_PREPERROR)) {
            abort();
        }

        if (errstr)
            sqlite3DbFree(db, errstr);
        return err->errval;
    }

    /* use sqlite to add the view */
    return VIEW_NOERR;
}

static int _view_delete_if_missing(const char *name, sqlite3 *db, void *arg)
{
    timepart_views_t *views = (timepart_views_t *)arg;
    int i;
    int rc;
    struct errstat err = {0};

    for (i = 0; i < views->nviews; i++) {
        if (strcasecmp(views->views[i]->name, name) == 0) {
            break;
        }
    }

    /* if the view doesn't exist anymore, delete it */
    if (i >= views->nviews) {
        rc = _views_sqlite_del_view(name, db, &err);
        if (rc != VIEW_NOERR) {
            logmsg(LOGMSG_ERROR, "%s: failed to clear old view %s rc=%d str=%s\n",
                    __func__, name, rc, err.errstr);
        }
        return 1;
    }

    return 0;
}

#ifdef COMDB2_UPDATEABLE_VIEWS

static int _views_create_triggers(timepart_view_t *view, sqlite3 *db,
                                  struct errstat *err)
{
    typedef char *(*PFUNC)(timepart_view_t *, struct errstat *);

    PFUNC funcs[3] = {(PFUNC)&_views_create_delete_trigger_query,
                      (PFUNC)&_views_create_update_trigger_query,
                      (PFUNC)&_views_create_insert_trigger_query};
    PFUNC func;
    char *stmt_str;
    int i;
    int rc;

    for (i = 0; i < sizeof(funcs) / sizeof(funcs[0]); i++) {
        func = funcs[i];

        /* there is a populated side row, get rid if it so trigger
           can use that too */
        clearClientSideRow(NULL);

        /* create delete trigger of the view */
        stmt_str = func(view, err);
        if (!stmt_str) {
            return err->errval;
        }

        rc = _views_run_sql(db, stmt_str, err);

        sqlite3_free(stmt_str);
        if (rc) {
            return err->errval;
        }
    }
    /* clear last side row, might break a racing sql otherwise */
    clearClientSideRow(NULL);
    return VIEW_NOERR;
}

static int _views_destroy_triggers(const char *view_name, sqlite3 *db,
                                   struct errstat *err)
{
    typedef char *(*PFUNC)(const char *, struct errstat *);

    PFUNC funcs[3] = {(PFUNC)&_views_destroy_delete_trigger_query,
                      (PFUNC)&_views_destroy_update_trigger_query,
                      (PFUNC)&_views_destroy_insert_trigger_query};
    PFUNC func;
    char *stmt_str;
    int i;
    int rc;

    for (i = 0; i < sizeof(funcs) / sizeof(funcs[0]); i++) {
        func = funcs[i];

        /* create delete trigger of the view */
        stmt_str = func(view_name, err);
        if (!stmt_str) {
            return err->errval;
        }

        rc = _views_run_sql(db, stmt_str, err);

        sqlite3_free(stmt_str);
        if (rc) {
            return err->errval;
        }

        /* there is a populated side row, get rid if it so trigger
           can use that too */
        clearClientSideRow(NULL);
    }
    return VIEW_NOERR;
}

static void dbg_verbose_sqlite(const char *fmt, ...)
{
    va_list va;

    if(!bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DEBUG_TIMEPART_SQLITE))
        return;

    va_start(va, fmt);
    vfprintf(stderr, fmt, va);
    va_end(va);
}
#endif
