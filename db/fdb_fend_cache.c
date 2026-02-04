/*
   Copyright 2015 Bloomberg Finance L.P.

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

#include <stdio.h>
#include <pthread.h>

#include <comdb2.h>
#include <sql.h>
#include <bdb_api.h>
#include <util.h>

#include "fdb_fend.h"
#include "fdb_fend_cache.h"

/**
 * Cache implemented as a decorator pattern
 *
 */

struct fdb_sqlstat_table {
    char *name; /* sqlite_statN */

    pthread_mutex_t mtx;    /* mutex for accessing this cache */
    struct temp_table *tbl; /* implementation, how about a temp table? */

    int nrows; /* how many rows, there is only one updater */
};

struct fdb_sqlstat_cache {
    fdb_t *fdb;          /* which foreign db this belong to */
    const char *fdbname; /* pointer to fdb name, not owned */
    int nalloc;          /* allocated array */
    int nused;           /* number of cached sqlite stats, usually 1 or 2 */
    fdb_sqlstat_table_t *arr; /* array of cached sqlite stat data */
    pthread_mutex_t arr_lock; /* array lock */
};

struct fdb_sqlstat_cursor {
    struct temp_cursor *cur; /* cursor on unlderlying temp table */

    char *name; /* name of the underlying cache */

    fdb_cursor_if_t *intf; /* pointer to interface */
    fdb_t *fdb;            /* which foreign db */
};

static char *fdb_sqlstat_cursor_id(BtCursor *pCur);
static char *fdb_sqlstat_cursor_get_data(BtCursor *pCur);
static int fdb_sqlstat_cursor_get_datalen(BtCursor *pCur);
static unsigned long long fdb_sqlstat_cursor_get_genid(BtCursor *pCur);
static void fdb_sqlstat_cursor_get_found_data(BtCursor *pCur,
                                              unsigned long long *genid,
                                              int *datalen, char **data);
static int fdb_sqlstat_cursor_move(BtCursor *pCur, int how);
static int fdb_sqlstat_cursor_close(BtCursor *pCur);
static int fdb_sqlstat_cursor_find(BtCursor *pCur, Mem *key, int nfields,
                                   int bias);
static int fdb_sqlstat_cursor_find_last(BtCursor *pCur, Mem *key, int nfields,
                                        int bias);
static int fdb_sqlstat_cursor_set_hint(BtCursor *pCur, void *hint);
static void *fdb_sqlstat_cursor_get_hint(BtCursor *pCur);
static int fdb_sqlstat_cursor_set_sql(BtCursor *pCur, const char *sql);
static char *fdb_sqlstat_cursor_name(BtCursor *pCur);
static int fdb_sqlstat_cursor_has_partidx(BtCursor *pCur);
static int fdb_sqlstat_cursor_has_expridx(BtCursor *pCur);
static char *fdb_sqlstat_cursor_dbname(BtCursor *pCur);
static int fdb_sqlstat_cursor_access(BtCursor *pCur, int how);

static int fdb_sqlstat_cursor_insert(BtCursor *pCur, struct sqlclntstate *clnt,
                                     fdb_tran_t *trans,
                                     unsigned long long genid, int datalen,
                                     char *data);
static int fdb_sqlstat_cursor_delete(BtCursor *pCur, struct sqlclntstate *clnt,
                                     fdb_tran_t *trans,
                                     unsigned long long genid);
static int fdb_sqlstat_cursor_update(BtCursor *pCur, struct sqlclntstate *clnt,
                                     fdb_tran_t *trans,
                                     unsigned long long oldgenid,
                                     unsigned long long genid, int datalen,
                                     char *data);


static int __fdb_sqlstat_table_init(fdb_sqlstat_table_t *tbl, const char *name)
{
    int bdberr = 0;
    tbl->tbl = bdb_temp_table_create(thedb->bdb_env, &bdberr);
    if (!tbl->tbl) {
        logmsg(LOGMSG_ERROR, "%s: failed to create temp table bdberr=%d\n",
               __func__, bdberr);
        return -1;
    }
    tbl->name = strdup(name);
    tbl->nrows = 0;
    Pthread_mutex_init(&tbl->mtx, NULL);
    return 0;
}

/**
 * Create the local cache, we are under a mutex
 *
 */
int fdb_sqlstat_cache_create(struct sqlclntstate *clnt, Vdbe *vdbe, fdb_t *fdb,
                             const char *fdbname, fdb_sqlstat_cache_t **pcache)
{
    fdb_sqlstat_cache_t *cache;
    int rc;

    cache = (fdb_sqlstat_cache_t *)calloc(1, sizeof(fdb_sqlstat_cache_t));
    if (!cache) {
        logmsg(LOGMSG_ERROR, "%s: malloc!\n", __func__);
        rc = -1;
        goto done;
    }

    cache->fdb = fdb;
    cache->fdbname = fdbname;
    cache->nalloc = 2;
    cache->arr = (fdb_sqlstat_table_t *)calloc(cache->nalloc,
                                               sizeof(fdb_sqlstat_table_t));
    if (!cache->arr) {
        free(cache);
        logmsg(LOGMSG_ERROR, "%s: malloc!\n", __func__);
        cache = NULL;
        rc = -1;
        goto done;
    }

    Pthread_mutex_init(&cache->arr_lock, NULL);
    if (__fdb_sqlstat_table_init(&cache->arr[0], "sqlite_stat1")) {
        fdb_sqlstat_cache_destroy(&cache);
        rc = -2;
        goto done;
    }
    if (__fdb_sqlstat_table_init(&cache->arr[1], "sqlite_stat4")) {
        fdb_sqlstat_cache_destroy(&cache);
        rc = -3;
        goto done;
    }

    rc = fdb_sqlstat_cache_populate(clnt, vdbe, fdb, cache->arr[0].tbl, cache->arr[1].tbl,
                                    &cache->arr[0].nrows, &cache->arr[1].nrows);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed to populate sqlite_stat tables, rc=%d\n", __func__,
               rc);
        fdb_sqlstat_cache_destroy(&cache);
        rc = -3;
        goto done;
    }

    rc = 0;

done:
    *pcache = cache;
    return rc;
}


static int __sqlstat_table_destroy(fdb_sqlstat_table_t *tbl)
{
    int bdberr = 0;
    int rc = 0;

    if (tbl->tbl) {
        rc = bdb_temp_table_close(thedb->bdb_env, tbl->tbl, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to create temp table bdberr=%d\n",
                    __func__, bdberr);
        }

        free(tbl->name);
        Pthread_mutex_destroy(&tbl->mtx);
    }
    bzero(tbl, sizeof(*tbl));

    return rc;
}

/**
 * Destroy the local cache
 *
 */
void fdb_sqlstat_cache_destroy(fdb_sqlstat_cache_t **pcache)
{
    fdb_sqlstat_cache_t *cache;
    int rc;

    cache = *pcache;

    if (!cache)
        return;

    assert(cache->nalloc == 2);

    /* retrieve records */
    rc = __sqlstat_table_destroy(&cache->arr[0]);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to depopulate sqlite_stat1 rc=%d\n",
               __func__, rc);
    }

    rc = __sqlstat_table_destroy(&cache->arr[1]);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to depopulate sqlite_stat4 rc=%d\n",
               __func__, rc);
    }

    free(cache->arr);
    Pthread_mutex_destroy(&cache->arr_lock);
    free(cache);

    *pcache = NULL;
}

/**
 * Open a cursor to the sqlite_stat cache
 *
 */
/* NOTE: It locks access to sqlstat (for now) until closed */
fdb_cursor_if_t *fdb_sqlstat_cache_cursor_open(struct sqlclntstate *clnt,
                                               Vdbe *vdbe,  /* gives us fdb_tbl locals */
                                               fdb_t *fdb, const char *name,
                                               fdb_sqlstat_cache_t *cache)
{
    fdb_sqlstat_table_t *tbl;
    fdb_sqlstat_cursor_t *fdbc;
    fdb_cursor_if_t *fdbc_if;
    int bdberr = 0;

    if (is_stat1(name)) {
        tbl = &cache->arr[0];
    } else if (is_stat4(name)) {
        tbl = &cache->arr[1];
    } else {
        abort();
    }

    int len = sizeof(fdb_cursor_if_t) + sizeof(fdb_sqlstat_cursor_t);
    fdbc_if = (fdb_cursor_if_t *)calloc(1, len);
    if (!fdbc_if) {
        fdb_sqlstats_put(fdb);
        return NULL;
    }

    fdbc_if->impl = (fdb_cursor_t *)((char *)fdbc_if + sizeof(fdb_cursor_if_t));
    fdbc = (fdb_sqlstat_cursor_t *)fdbc_if->impl;

    fdbc->intf = fdbc_if;
    fdbc->fdb = fdb;
    fdbc->name = strdup(name);
    fdbc->cur = bdb_temp_table_cursor(thedb->bdb_env, tbl->tbl, NULL, &bdberr);
    if (!fdbc->cur) {
        logmsg(LOGMSG_ERROR,
               "%s: creating temp table cursor failed bdberr=%d\n", __func__,
               bdberr);
        free(fdbc->name);
        free(fdbc_if);
        fdb_sqlstats_put(fdb);
        return NULL;
    }

    fdbc_if->close = fdb_sqlstat_cursor_close;
    fdbc_if->id = fdb_sqlstat_cursor_id;
    fdbc_if->data = fdb_sqlstat_cursor_get_data;
    fdbc_if->datalen = fdb_sqlstat_cursor_get_datalen;
    fdbc_if->genid = fdb_sqlstat_cursor_get_genid;
    fdbc_if->get_found_data = fdb_sqlstat_cursor_get_found_data;
    fdbc_if->set_hint = fdb_sqlstat_cursor_set_hint;
    fdbc_if->get_hint = fdb_sqlstat_cursor_get_hint;
    fdbc_if->set_sql = fdb_sqlstat_cursor_set_sql;
    fdbc_if->name = fdb_sqlstat_cursor_name;
    fdbc_if->tblname = fdb_sqlstat_cursor_name;
    fdbc_if->tbl_has_partidx = fdb_sqlstat_cursor_has_partidx;
    fdbc_if->tbl_has_expridx = fdb_sqlstat_cursor_has_expridx;
    fdbc_if->dbname = fdb_sqlstat_cursor_dbname;
    fdbc_if->access = fdb_sqlstat_cursor_access;
    fdbc_if->move = fdb_sqlstat_cursor_move;
    fdbc_if->find = fdb_sqlstat_cursor_find;
    fdbc_if->find_last = fdb_sqlstat_cursor_find_last;

    fdbc_if->insert = fdb_sqlstat_cursor_insert;
    fdbc_if->delete = fdb_sqlstat_cursor_delete;
    fdbc_if->update = fdb_sqlstat_cursor_update;

    return fdbc_if;
}

/**
 * Close a cursor
 *
 */
/* NOTE: it releases lock to sqlstat */
static int fdb_sqlstat_cursor_close(BtCursor *cur)
{
    fdb_cursor_if_t *fdbc_if;
    fdb_t *fdb;
    fdb_sqlstat_cursor_t *fdbc;
    int rc;
    int bdberr;

    fdbc_if = cur->fdbc;
    fdbc = (fdb_sqlstat_cursor_t *)fdbc_if->impl;
    fdb = fdbc->fdb;

    bdberr = 0;
    rc = bdb_temp_table_close_cursor(thedb->bdb_env, fdbc->cur, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: failed closing temp table cursor rc=%d bdberr=%d\n",
               __func__, rc, bdberr);
    }

    free(fdbc->name);
    free(fdbc_if);

    fdb_sqlstats_put(fdb);

    return rc;
}

static char *fdb_sqlstat_cursor_id(BtCursor *pCur)
{
    return NULL;
}

static char *fdb_sqlstat_cursor_get_data(BtCursor *pCur)
{
    fdb_cursor_if_t *fdbc_if = pCur->fdbc;
    fdb_sqlstat_cursor_t *fdbc = (fdb_sqlstat_cursor_t *)fdbc_if->impl;

    return bdb_temp_table_data(fdbc->cur);
}

static int fdb_sqlstat_cursor_get_datalen(BtCursor *pCur)
{
    fdb_cursor_if_t *fdbc_if = pCur->fdbc;
    fdb_sqlstat_cursor_t *fdbc = (fdb_sqlstat_cursor_t *)fdbc_if->impl;

    return bdb_temp_table_datasize(fdbc->cur);
}

static unsigned long long fdb_sqlstat_cursor_get_genid(BtCursor *pCur)
{
    return -1ULL;
}

static void fdb_sqlstat_cursor_get_found_data(BtCursor *pCur,
                                              unsigned long long *genid,
                                              int *datalen, char **data)
{
    fdb_cursor_if_t *fdbc_if = pCur->fdbc;
    fdb_sqlstat_cursor_t *fdbc = (fdb_sqlstat_cursor_t *)fdbc_if->impl;

    *genid = -1ULL;
    *datalen = bdb_temp_table_datasize(fdbc->cur);
    *data = bdb_temp_table_data(fdbc->cur);
}

static int fdb_sqlstat_cursor_move(BtCursor *pCur, int how)
{
    fdb_cursor_if_t *fdbc_if = pCur->fdbc;
    fdb_sqlstat_cursor_t *fdbc = (fdb_sqlstat_cursor_t *)fdbc_if->impl;
    int rc = 0;
    int bdberr = 0;

    switch (how) {
    case CFIRST:
        rc = bdb_temp_table_first(thedb->bdb_env, fdbc->cur, &bdberr);
        break;

    case CLAST:
        rc = bdb_temp_table_last(thedb->bdb_env, fdbc->cur, &bdberr);
        break;

    case CNEXT:
        rc = bdb_temp_table_next(thedb->bdb_env, fdbc->cur, &bdberr);
        break;

    case CPREV:
        rc = bdb_temp_table_prev(thedb->bdb_env, fdbc->cur, &bdberr);
        break;
    }

    if (rc == IX_PASTEOF)
        rc = IX_EMPTY;

    if (rc && rc != IX_EMPTY) {
        logmsg(LOGMSG_ERROR,
               "%s: error moving sql stat cursor how=%d rc=%d bdberr=%d\n",
               __func__, how, rc, bdberr);
    }

    return rc;
}

static int fdb_sqlstat_cursor_find(BtCursor *pCur, Mem *key, int nfields,
                                   int bias)
{
    abort(); /* this should not happen */
}

static int fdb_sqlstat_cursor_find_last(BtCursor *pCur, Mem *key, int nfields,
                                        int bias)
{
    abort(); /* this should not happen */
}

static int fdb_sqlstat_cursor_set_hint(BtCursor *pCur, void *hint)
{
    return -1;
}

static void *fdb_sqlstat_cursor_get_hint(BtCursor *pCur) { return NULL; }

static int fdb_sqlstat_cursor_set_sql(BtCursor *pCur, const char *sql)
{
    abort();
}

static char *fdb_sqlstat_cursor_name(BtCursor *pCur)
{

    fdb_sqlstat_cursor_t *fdbc = (fdb_sqlstat_cursor_t *)pCur->fdbc->impl;

    return fdbc->name;
}

static int fdb_sqlstat_cursor_has_partidx(BtCursor *pCur) { return 0; }

static int fdb_sqlstat_cursor_has_expridx(BtCursor *pCur) { return 0; }

static char *fdb_sqlstat_cursor_dbname(BtCursor *pCur)
{

    fdb_sqlstat_cursor_t *fdbc = (fdb_sqlstat_cursor_t *)pCur->fdbc->impl;

    return (char *)fdb_dbname_name(fdbc->fdb);
}

static int fdb_sqlstat_cursor_access(BtCursor *pCur, int how) { return 0; }

static int fdb_sqlstat_cursor_insert(BtCursor *pCur, struct sqlclntstate *clnt,
                                     fdb_tran_t *trans,
                                     unsigned long long genid, int datalen,
                                     char *data)
{
    abort();
}

static int fdb_sqlstat_cursor_delete(BtCursor *pCur, struct sqlclntstate *clnt,
                                     fdb_tran_t *trans,
                                     unsigned long long genid)
{
    abort();
}

static int fdb_sqlstat_cursor_update(BtCursor *pCur, struct sqlclntstate *clnt,
                                     fdb_tran_t *trans,
                                     unsigned long long oldgenid,
                                     unsigned long long genid, int datalen,
                                     char *data)
{
    abort();
}
