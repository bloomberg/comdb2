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

/**
 * Connection to remote databases: locally caching master records
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <pthread.h>
#include <assert.h>
#include <alloca.h>
#include <poll.h>
#include <time.h>

#include <rtcpu.h>
#include <list.h>
#include <sbuf2.h>
#include <ctrace.h>

#include <gettimeofday_ms.h>

#include "comdb2.h"
#include "sql.h"
#include "sqlite3.h"
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "fdb_fend.h"
#include "fdb_boots.h"
#include "fdb_comm.h"
#include "fdb_fend_cache.h"
#include "fdb_access.h"
#include "fdb_bend.h"
#include "fdb_systable.h"
#include "osqlsession.h"
#include "util.h"
#include "logmsg.h"

#include "ssl_support.h"
#include "ssl_io.h"
#include "ssl_bend.h"
#include "comdb2_query_preparer.h"

extern int gbl_fdb_resolve_local;
extern int gbl_fdb_allow_cross_classes;
extern int gbl_partial_indexes;
extern int gbl_expressions_indexes;

int gbl_fdb_track = 0;
int gbl_fdb_track_times = 0;
int gbl_test_io_errors = 0;
int gbl_fdb_push_remote = 0;
int gbl_fdb_incoherence_percentage = 0;
int gbl_fdb_io_error_retries = 16;
int gbl_fdb_io_error_retries_phase_1 = 6;
int gbl_fdb_io_error_retries_phase_2_poll = 100;

struct fdb_tbl;
struct fdb;
struct fdb_cursor;
struct fdb_access;

static int _test_trap_dlock1 = 0;

/* fdb_tbl_ent stores one entry per data or index for a foreign table */
struct fdb_tbl_ent {
    int rootpage;        /* local rootpage in the specific sqlite3 engine */
    int source_rootpage; /* rootpage of the table in the source fdb */
    int ixnum;           /* index number, -1 is data */
    unsigned long long _version; /* cached version, use tbl->version in most
                                    cases; it is used here for sqlite_stats */

    void *ent;  /* sqlite_master row pointer */
    int entlen; /* sqlite_master row len */

    /* explain support */
    char *name;          /* name of the table/index */
    struct fdb_tbl *tbl; /* let me go rootpage->table(name) */

    LINKC_T(
        struct fdb_tbl_ent) lnk; /* link for entries list (data and indexes) */
};

/* foreign db table structure, caches the sql master rows */
struct fdb_tbl {
    char *name;
    int name_len; /* no zero */

    unsigned long long version; /* version of the tbl/index when cached */

    /* explain support */
    struct fdb *fdb; /* tbl->fdb(name) */

    int nix;        /* number of indexes */
    int ix_partial; /* is there partial index */
    int ix_expr;    /* is there expressions index */

    LISTC_T(struct fdb_tbl_ent) ents;
    pthread_mutex_t ents_mtx; /* entries add/rm lock */ /*TODO: review this
                                                           mutex, we need
                                                           something else */

    int need_version; /* a remote op detected that local is stale, and this
                         hints to the new version */
};

/* foreign db structure, caches the used tables for the remote db */
struct fdb {
    char *dbname;
    int dbname_len; /* excluding terminal 0 */
    enum mach_class class
        ;      /* what class is the cluster CLASS_PROD, CLASS_TEST, ... */
    int class_override; /* set if class is part of table name at creation */
    int local; /* was this added by a LOCAL access ?*/
    int dbnum; /* cache dbnum for db, needed by current dbt_handl_alloc* */

    int users; /* how many clients this db has, sql engines and cursors */
    pthread_mutex_t users_mtx;

    hash_t *
        h_ents_rootp;    /* FDB_TBL_ENT_T data and index entries, by rootpage */
    hash_t *h_ents_name; /* FDB_TBL_ENT_T data and index entries, by name */
    hash_t *h_tbls_name; /* FDB_TBL_T entries */
    pthread_rwlock_t h_rwlock; /* hash lock */

    fdb_location_t *loc; /* where is the db located? */
    SBUF2 *dbcon;        /* cached db connection */
    pthread_mutex_t dbcon_mtx;

    Schema *schema; /* shared schema for fdb tables */

    fdb_sqlstat_cache_t *sqlstats; /* cache of sqlite stats, per foreign db */
    pthread_mutex_t sqlstats_mtx;  /* mutex for stats */

    int has_sqlstat1; /* if sqlstat1 was found */
    int has_sqlstat4; /* if sqlstat4 was found */

    int server_version; /* save the server_version */
    ssl_mode ssl; /* does this server needs ssl */
};

/* cache of foreign dbs */
struct fdb_cache {
    int nalloc;                /* allocated array */
    int nused;                 /* number of foreign dbs */
    fdb_t **arr;               /* the array of foreign_db objects */
    pthread_rwlock_t arr_lock; /* nalloc, nused and arr lock */

    hash_t *h_curs;               /* list of cursors */
    pthread_rwlock_t h_curs_lock; /* cursors lock, receive side */
};

typedef struct fcon_sock {
    SBUF2 *sb;
} fcon_sock_t;

typedef struct fcon_tag {
    int blah;
} fcon_tag_t;

enum fdb_cur_stream_state {
    FDB_CUR_IDLE = 0,
    FDB_CUR_STREAMING = 1,
    FDB_CUR_ERROR = 2
};

struct fdb_cursor {
    char *cid;             /* identity of cursor id */
    char *tid;             /* transaction id owning cursor */
    fdb_cursor_if_t *intf; /* pointer to interface */
    fdb_tbl_ent_t *ent;    /* pointer to Btree entry or NULL */
    int flags;             /* type of cursor, supporting various protocols */

    fdb_tran_t *trans; /* which subtransaction this is part of */

    int type; /* to allow future multiple connectors */
    union {
        fcon_sock_t sock;
        fcon_tag_t tag;
    } fcon; /* remote connection */

    fdb_msg_t *msg; /* msg memory */

    Expr *hint;     /* expression passed down by sqlite */
    char *sql_hint; /* precreated sql query including hint */
    int is_schema;  /* special processing for accessing remote sqlite_master */

    enum fdb_cur_stream_state streaming; /* used to track partial streams */
    uuid_t ciduuid;                      /* UUID/fastseed storage for cursor */
    uuid_t tiduuid; /* UUID/fastseed storage for transaction, if any, or 0 */
    char *node;     /* connected to where? */
    int need_ssl;   /* uses ssl */
};

typedef struct fdb_systable_info {
    fdb_systable_ent_t *arr;
    int narr;
} fdb_systable_info_t;

static fdb_cache_t fdbs;

static fdb_t *__cache_fnd_fdb(const char *dbname, int *idx);
static int __cache_link_fdb(fdb_t *fdb);
static void __cache_unlink_fdb(fdb_t *fdb);

static int insert_table_entry_from_packedsqlite(fdb_t *fdb, fdb_tbl_t *tbl,
                                                char *row, int rowlen,
                                                fdb_tbl_ent_t **found_ent,
                                                int versioned);
static int check_table_fdb(fdb_t *fdb, fdb_tbl_t *tbl, int initial,
                           fdb_tbl_ent_t **found_ent, int is_sqlite_master);

static int fdb_num_entries(fdb_t *fdb);

/* REMCUR frontend implementation */
static char *fdb_cursor_id(BtCursor *pCur);
static char *fdb_cursor_get_data(BtCursor *pCur);
static int fdb_cursor_get_datalen(BtCursor *pCur);
static unsigned long long fdb_cursor_get_genid(BtCursor *pCur);
static void fdb_cursor_get_found_data(BtCursor *pCur, unsigned long long *genid,
                                      int *datalen, char **data);
static int fdb_cursor_close(BtCursor *pCur);
static void fdb_cursor_close_on_open(BtCursor *pCur, int cache);
static int fdb_cursor_set_hint(BtCursor *pCur, void *hint);
static void *fdb_cursor_get_hint(BtCursor *pCur);
static int fdb_cursor_set_sql(BtCursor *pCur, const char *sql);
static char *fdb_cursor_name(BtCursor *pCur);
static char *fdb_cursor_tblname(BtCursor *pCur);
static int fdb_cursor_table_has_partidx(BtCursor *pCur);
static int fdb_cursor_table_has_expridx(BtCursor *pCur);
static char *fdb_cursor_dbname(BtCursor *pCur);
static fdb_tbl_ent_t *fdb_cursor_table_entry(BtCursor *pCur);
static int fdb_cursor_access(BtCursor *pCur, int how);

/* REMSQL frontend implementation overrides */
static int fdb_cursor_move_sql(BtCursor *pCur, int how);
static int fdb_cursor_find_sql(BtCursor *pCur, Mem *key, int nfields, int bias);
static int fdb_cursor_find_last_sql(BtCursor *pCur, Mem *key, int nfields,
                                    int bias);

/* REMSQL WRITE frontend */
static int fdb_cursor_insert(BtCursor *pCur, struct sqlclntstate *clnt,
                             fdb_tran_t *trans, unsigned long long genid,
                             int datalen, char *data);
static int fdb_cursor_delete(BtCursor *pCur, struct sqlclntstate *clnt,
                             fdb_tran_t *trans, unsigned long long genid);
static int fdb_cursor_update(BtCursor *pCur, struct sqlclntstate *clnt,
                             fdb_tran_t *trans, unsigned long long oldgenid,
                             unsigned long long genid, int datalen, char *data);

/* NOTE: ALERT! always call this with h_rwlock acquired; as of now add_table_fdb
   has WR lock
   on it and it is undefined behaviour to get the read lock here */
static fdb_tbl_ent_t *get_fdb_tbl_ent_by_name_from_fdb(fdb_t *fdb,
                                                       const char *name);

static int __free_fdb_tbl(void *obj, void *arg);
static int __lock_wrlock_exclusive(char *dbname);

/* Node affinity functions: a clnt tries to stick to one node, unless error in
   which
   case it will move to another one; error will not impact other concurrent
   clnt-s */
static char *_fdb_get_affinity_node(struct sqlclntstate *clnt, const fdb_t *fdb,
                                    int *was_bad);
static int _fdb_set_affinity_node(struct sqlclntstate *clnt, const fdb_t *fdb,
                                  char *host, int status);
void _fdb_clear_clnt_node_affinities(struct sqlclntstate *clnt);

static int _get_protocol_flags(struct sqlclntstate *clnt, fdb_t *fdb,
                               int *flags);
static int _validate_existing_table(fdb_t *fdb, int cls, int local);

/**************  FDB OPERATIONS ***************/

/**
 * Initialize the cache
 *
 */
int fdb_cache_init(int n)
{
    bzero(&fdbs, sizeof(fdbs));
    if (n <= 0) {
        n = 5;
    }
    fdbs.arr = (fdb_t **)calloc(n, sizeof(fdb_t *));
    if (!fdbs.arr) {
        logmsg(LOGMSG_ERROR, "%s:OOM %zu bytes!\n", __func__,
               n * sizeof(fdb_t *));
        return -1;
    }
    fdbs.nalloc = n;
    Pthread_rwlock_init(&fdbs.arr_lock, NULL);

    fdbs.h_curs = hash_init_i4(0);
    Pthread_rwlock_init(&fdbs.h_curs_lock, NULL);

    return 0;
}

/**
 * internal, locate an fdb object based on name
 *
 */
static fdb_t *__cache_fnd_fdb(const char *dbname, int *idx)
{
    int len = strlen(dbname);
    int i = 0;

    if (idx)
        *idx = -1;

    for (i = 0; i < fdbs.nused; i++) {
        if (len == fdbs.arr[i]->dbname_len &&
            strncasecmp(dbname, fdbs.arr[i]->dbname, len) == 0) {
            if (idx)
                *idx = i;
            return fdbs.arr[i];
        }
    }

    return NULL;
}

/**
 * add a fdb to the cache
 * internal, needs caller locking (arr_lock)
 *
 */
static int __cache_link_fdb(fdb_t *fdb)
{
    int rc = FDB_NOERR;
    fdb_t **ptr;

    if (fdbs.nused == fdbs.nalloc) {
        ptr = realloc(fdbs.arr, sizeof(fdb_t *) * fdbs.nalloc * 2);
        if (!ptr) {
            logmsg(LOGMSG_ERROR, "%s: OOM %zu bytes\n", __func__,
                   sizeof(fdb_t *) * fdbs.nalloc * 2);
            rc = FDB_ERR_MALLOC;
            goto done;
        }
        fdbs.arr = ptr;
        fdbs.nalloc *= 2;
        bzero(&fdbs.arr[fdbs.nused],
              sizeof(fdb_t *) * (fdbs.nalloc - fdbs.nused));
    }
    fdbs.arr[fdbs.nused++] = fdb;

done:
    return rc;
}

/**
 * remove a fdb to the cache
 * internal, needs caller locking (fdbs.arr_lock)
 *
 */
static void __cache_unlink_fdb(fdb_t *fdb)
{
    int ix;

    for (ix = 0; ix < fdbs.nused; ix++) {
        if (fdb == fdbs.arr[ix])
            break;
    }
    if (ix == fdbs.nused) {
        logmsg(LOGMSG_ERROR, "%s: bug? for db %s\n", __func__, fdb->dbname);
        return;
    }
    if (fdbs.nused > ix + 1) {
        memmove(&fdbs.arr[ix], &fdbs.arr[ix + 1],
                sizeof(fdbs.arr[0]) * (fdbs.nused - ix - 1));
    }
    fdbs.nused--;
    fdbs.arr[fdbs.nused] = NULL;
}

/**
 * Free an fdb object
 *
 */
void __free_fdb(fdb_t *fdb)
{
    free(fdb->dbname);
    hash_free(fdb->h_ents_rootp);
    hash_free(fdb->h_ents_name);
    hash_free(fdb->h_tbls_name);
    Pthread_rwlock_destroy(&fdb->h_rwlock);
    Pthread_mutex_destroy(&fdb->sqlstats_mtx);
    Pthread_mutex_destroy(&fdb->dbcon_mtx);
    Pthread_mutex_destroy(&fdb->users_mtx);
    free(fdb);
}

/**
 * Add a lockless user
 *
 */
static void __fdb_add_user(fdb_t *fdb, int noTrace)
{
    Pthread_mutex_lock(&fdb->users_mtx);
    fdb->users++;

    if (!noTrace && gbl_fdb_track)
        logmsg(LOGMSG_USER, "%p %s %s users %d\n", (void *)pthread_self(), __func__, fdb->dbname, fdb->users);

    assert(fdb->users > 0);
    Pthread_mutex_unlock(&fdb->users_mtx);
}

/**
 * Remove a lockless user
 *
 */
static void __fdb_rem_user(fdb_t *fdb, int noTrace)
{
    Pthread_mutex_lock(&fdb->users_mtx);
    fdb->users--;

    if (!noTrace && gbl_fdb_track)
        logmsg(LOGMSG_USER, "%p %s %s users %d\n", (void *)pthread_self(), __func__, fdb->dbname, fdb->users);

    assert(fdb->users >= 0);
    Pthread_mutex_unlock(&fdb->users_mtx);
}

/**
 * Retrieve a foreign db object
 * The callers of this function should make sure a table lock is acquired
 * Such by calling fdb_lock_table().
 *
 */
fdb_t *get_fdb(const char *dbname)
{
    fdb_t *fdb = NULL;

    Pthread_rwlock_rdlock(&fdbs.arr_lock);
    fdb = __cache_fnd_fdb(dbname, NULL);
#if 0
   NOTE: we will rely on table locks instead of this! 
   if(fdb)
   {
      __fdb_add_user(fdb, 0);
   }
#endif
    Pthread_rwlock_unlock(&fdbs.arr_lock);
    return fdb;
}

/**
 * Adds a new foreign db to the local cache
 * If it already exists, created is set to 0
 * and users incremented.  Otherwise created
 * is set and the db is created.
 *
 */
static fdb_t *new_fdb(const char *dbname, int *created, enum mach_class class,
                      int local, int class_override)
{
    int rc = 0;
    fdb_t *fdb;

    Pthread_rwlock_wrlock(&fdbs.arr_lock);
    fdb = __cache_fnd_fdb(dbname, NULL);
    if (fdb) {
        assert(class == fdb->class);
        __fdb_add_user(fdb, 0);

        *created = 0;
        goto done;
    }

    fdb = calloc(1, sizeof(*fdb));
    if (!fdb) {
        logmsg(LOGMSG_ERROR, "%s: OOM %zu bytes!\n", __func__, sizeof(*fdb));
        goto done;
    }

    fdb->dbname = strdup(dbname);
    fdb->class = class;
    fdb->class_override = class_override;
    /*
       default remote version we expect

       code will backout on initial connection
     */
    fdb->server_version = FDB_VER;
    fdb->dbname_len = strlen(dbname);
    fdb->users = 1;
    fdb->local = local;
    fdb->h_ents_rootp = hash_init_i4(0);
    fdb->h_ents_name = hash_init_strptr(offsetof(struct fdb_tbl_ent, name));
    fdb->h_tbls_name = hash_init_strptr(0);
    Pthread_rwlock_init(&fdb->h_rwlock, NULL);
    Pthread_mutex_init(&fdb->sqlstats_mtx, NULL);
    Pthread_mutex_init(&fdb->dbcon_mtx, NULL);
    Pthread_mutex_init(&fdb->users_mtx, NULL);

    /* this should be safe to call even though the fdb is not booked in the fdb
     * array */
    __fdb_add_user(fdb, 0);

    rc = __cache_link_fdb(fdb);
    if (rc) {
        /* this was not visible, free it here */
        __free_fdb(fdb);
        fdb = NULL;
        *created = 0;
    } else {
        *created = 1;
    }

done:
    Pthread_rwlock_unlock(&fdbs.arr_lock);
    /* At this point, if we've created a new fdb,
       it is findable by others and users might
       increase/decrease independently */

    if (_test_trap_dlock1 == 1) {
        _test_trap_dlock1 = 2;
        /* wait for second request to arrive */
        while (_test_trap_dlock1 == 2) {
            poll(NULL, 0, 10);
        }
    }

    return fdb;

    /* returns NULL if error or fdb with fdb->users incremented */
}

/**
 * Try to destroy the session;
 * only done when connecting to unexisting dbs
 * If somehow there are other clients, ignore
 * this.
 */
static void destroy_fdb(fdb_t *fdb)
{
    if (!fdb)
        return;

    Pthread_rwlock_wrlock(&fdbs.arr_lock);

    /* if there are any users, don't touch the db */
    Pthread_mutex_lock(&fdb->users_mtx);
    fdb->users--;
    if (fdb->users == 0) {
        __cache_unlink_fdb(fdb);
        Pthread_mutex_unlock(&fdb->users_mtx);
        __free_fdb(fdb);
    } else {
        Pthread_mutex_unlock(&fdb->users_mtx);
    }

    Pthread_rwlock_unlock(&fdbs.arr_lock);
}

/**************  TABLE OPERATIONS ***************/

/**
 * Free an unlinked table structure
 * Unlocked, needs tbls_mtx
 *
 */
void __fdb_free_table(fdb_t *fdb, fdb_tbl_t *tbl)
{
    free(tbl->name);
    Pthread_mutex_destroy(&tbl->ents_mtx);
    free(tbl);
}

/**
 * Add a new table to the foreign db.  Also,
 * retrieves the current sql master row, if possible
 *
 * Note: fdb object cannot go away because it has users>0
 *
 */
static fdb_tbl_t *_alloc_table_fdb(fdb_t *fdb, const char *tblname)
{
    fdb_tbl_t *tbl;

    tbl = (fdb_tbl_t *)calloc(1, sizeof(*tbl));
    if (!tbl) {
        logmsg(LOGMSG_USER, "%s: OOM %zu bytes!\n", __func__,
               sizeof(fdb_tbl_t));
        return NULL;
    }

    tbl->name = strdup(tblname);
    tbl->name_len = strlen(tblname);
    tbl->fdb = fdb;
    Pthread_mutex_init(&tbl->ents_mtx, NULL);
    listc_init(&tbl->ents, offsetof(struct fdb_tbl_ent, lnk));

    return tbl;
}

enum table_status {
    TABLE_MISSING,
    TABLE_EXISTS,
    TABLE_STALE,
};
/**
 * Check if the table exists and has the right version
 * NOTE: registered as a fdb user so fdb does not get removed
 *
 */
static int _table_exists(fdb_t *fdb, const char *table_name,
                         enum table_status *status, int *version)
{
    unsigned long long remote_version;
    fdb_tbl_t *table;
    int rc = FDB_NOERR;

    *status = TABLE_MISSING;

    table = hash_find_readonly(fdb->h_tbls_name, &table_name);
    if (table) {
        *status = TABLE_EXISTS;

        /* ok, table exists, HURRAY!
           Is the table marked obsolete? */
        if (table->need_version &&
            (table->version != (table->need_version - 1))) {
            *status = TABLE_STALE;
        } else {
            if (comdb2_get_verify_remote_schemas()) {
                /* this is a retry for an already */
                rc = fdb_get_remote_version(fdb->dbname, table_name, fdb->class,
                                            fdb->loc == NULL, &remote_version);
                if (rc == FDB_NOERR) {
                    if (table->version != remote_version) {
                        logmsg(LOGMSG_WARN, "Remote table %s.%s new version is "
                                            "%lld, cached %lld\n",
                               fdb->dbname, table_name, remote_version,
                               table->version);
                        table->need_version = remote_version + 1;
                        *status = TABLE_STALE;
                    } else {
                        /* table version correct, make sure to pass this
                         * upstream */
                        *version = table->version;
                    }
                } else {
                    return FDB_ERR_GENERIC;
                }
            } else {
                *version = table->version;
            }
        }

        /* NOTE: we don't prepopulate sql engines at creation
           with schema for already existing fdbs;  therefore, this code
           falts in to update the new sql engine on demand.   This trace
           would spew in such a case, which we don't want to.
         */
        /*
        fprintf(stderr, "%s: table \"%s\" in db \"%s\" already exist %d!\n",
              __func__, table_name, fdb->dbname, *version);
        if(!*version)
            abort();
         */
    }

    return FDB_NOERR;
}

/**
 * Handling sqlite_stats; they have been temporarely added but linked
 * to original table tbl; (I did that to run only a query first time)
 * They really belong to the fdb, lets properly link them now
 *
 * Returns -1 for ENOMEM or if cannot find stat_name
 */
int fix_table_stats(fdb_t *fdb, fdb_tbl_t *tbl, const char *stat_name)
{
    fdb_tbl_t *stat_tbl;
    fdb_tbl_ent_t *stat_ent;

    /* alloc table */
    stat_tbl = _alloc_table_fdb(fdb, stat_name);
    if (!stat_tbl) {
        logmsg(LOGMSG_ERROR, "%s: OOM %s for %p\n", __func__, stat_name, tbl);
        return -1;
    }

    stat_ent = get_fdb_tbl_ent_by_name_from_fdb(fdb, stat_name);
    if (!stat_ent) {
        logmsg(LOGMSG_ERROR, "%s: Cannot find %s for %p\n", __func__, stat_name, tbl);
        return -1;
    }
    /*
       fprintf(stderr, "XYXY: for \"%s\" fixing table from \"%s\" to \"%s\"\n",
       stat_name, found_ent->tbl->name, tbl_stat->name);
     */

    /* we need to move this from ent->tbl->ents to tbl_stat->ents */
    listc_rfl(&stat_ent->tbl->ents, stat_ent);
    stat_ent->tbl = stat_tbl;
    stat_ent->tbl->version = stat_ent->_version;
    listc_abl(&stat_tbl->ents, stat_ent);
    assert(stat_ent->ixnum == -1);

    if (gbl_fdb_track)
        logmsg(LOGMSG_USER, "Linking %s to %s\n", stat_tbl->name, fdb->dbname);
    hash_add(fdb->h_tbls_name, stat_tbl);

    return 0;
}

/**
 * Add a table and index stats if any; acquires exclusive access to fdb cache
 *
 */
static int _add_table_and_stats_fdb(fdb_t *fdb, const char *table_name,
                                    int *version, int in_analysis_load)
{
    enum table_status status;
    int rc = FDB_NOERR;
    fdb_tbl_t *tbl;
    int initial;
    fdb_tbl_ent_t *found_ent;
    int is_sqlite_master; /* corner case when sqlite_master is the first query
                             remote;
                             there is no "sqlite_master" entry for
                             sqlite_master, but
                             that doesn't make the case here to fail */
    /* check if the table exists, and if it does need refreshing
       if it exists and has right version, grab the version and return */
    rc = _table_exists(fdb, table_name, &status, version);
    if (rc == FDB_NOERR && status == TABLE_EXISTS) {
        /* fdb unlocked, users incremented */
        goto nop;
    } else if (rc != FDB_NOERR) {
        logmsg(LOGMSG_WARN, "failure to connect to remote %s.%s\n", fdb->dbname,
               table_name);
        goto nop;
    }

    /* NOTE: since this function is called recursively to add sqlite_stat* for a
     * table as well
     * we make sure we acquire a lock only once, for the initial call.
     * NOTE2: it is possible that the sqlite engine adds another table, but it
     * doesn't have the
     * schema for sqlite_stat, calling into sqlite3AddAndLock again where stats
     * are loaded;
     * in this case we do already have an exclude lock so we skip the locking as
     * well
     */
    if (!in_analysis_load) {
        /* since we removed ourselves, it is possible that the fdb object will
           go away
           in this case, we need to get an exclusive lock while syncronizing
           with the
           destroy_fdb process; we need to use a copy of fdb->dbname instead of
           volative fdb object */
        char *tmpname = strdup(fdb->dbname);

        /* new_fdb bumped this up, we need exclusive lock, get ourselves out */
        __fdb_rem_user(fdb, 0);

        rc = __lock_wrlock_exclusive(tmpname);
        free(tmpname);
        if (rc) {
            if (rc == FDB_ERR_FDB_NOTFOUND) {
                /* the db got deleted from under us, start fresh */
                return rc;
            }
            logmsg(LOGMSG_ERROR, "%s: fail to lock rc=%d!\n", __func__, rc);
            return rc;
        }

        /* add ourselves back */
        __fdb_add_user(fdb, 0);

        /* remove the stale table here */
        /* ok, stale; we need to garbage this one out */
        fdb_tbl_t *remtbl = hash_find_readonly(fdb->h_tbls_name, &table_name);
        /* anything is possible with the table while waiting for exclusive
         * fdb
         * lock */
        if (remtbl) {
            /* table is still around */
            if (!remtbl->need_version ||
                ((remtbl->need_version - 1) == remtbl->version)) {
                /* table was fixed in the meantime!, drop exclusive lock */
                rc = FDB_NOERR;
                *version = remtbl->version;
                goto done;
            } else {
                /* table is still stale, remove */
                if (gbl_fdb_track)
                    logmsg(LOGMSG_USER,
                           "Detected stale table \"%s.%s\" "
                           "version %llu required %d\n",
                           remtbl->fdb->dbname, remtbl->name, remtbl->version,
                           remtbl->need_version - 1);

                if (__free_fdb_tbl(remtbl, fdb)) {
                    logmsg(LOGMSG_ERROR,
                           "Error clearing schema for table "
                           "\"%s\" in db \"%s\"\n",
                           table_name, fdb->dbname);
                }
            }
        }
    }

    /* is this the first table? grab sqlite_stats too */
    initial = fdb_num_entries(fdb) == 0;

    /* create the table object */
    tbl = _alloc_table_fdb(fdb, table_name);
    if (!tbl) {
        rc = FDB_ERR_MALLOC;
        goto done;
    }

    /* this COULD be taken out of tbls_mtx, but I want to clear table
       under lock so I don't add garbage table structures when mispelling
     */
    is_sqlite_master = (strcasecmp(table_name, "sqlite_master") == 0);
    found_ent = NULL;
    rc = check_table_fdb(fdb, tbl, initial, &found_ent, is_sqlite_master);

    if (rc != FDB_NOERR || (!found_ent && !is_sqlite_master)) {
        *version = 0;
        /* we might have populated the tbl with sqlite_stat-s
           remove them */
        __free_fdb_tbl(tbl, fdb);

        if (rc == FDB_NOERR)
            rc = FDB_ERR_FDB_TBL_NOTFOUND;

        goto done;
    }

    /* so, we have a new found the table in remote schema, lets add
       it to the fdb */
    if (!is_sqlite_master) {
        if (gbl_fdb_track)
            logmsg(LOGMSG_USER, "Linking %s to %s\n", tbl->name, fdb->dbname);
        hash_add(fdb->h_tbls_name, tbl);

        *version = fdb_table_version(found_ent->_version);
    } else {
        *version = 0;
    }

    if (initial) {
        /* we have a table, lets get the sqlite_stats */
        if (fdb->has_sqlstat1 &&
            strncasecmp(table_name, "sqlite_stat1", 13) != 0) {
            rc = fix_table_stats(fdb, tbl, "sqlite_stat1");
            if (rc) {
                goto done;
            }
        }

        if (fdb->has_sqlstat4 &&
            strncasecmp(table_name, "sqlite_stat4", 13) != 0) {
            rc = fix_table_stats(fdb, tbl, "sqlite_stat4");
            if (rc) {
                goto done;
            }
        }
    }

    if (is_sqlite_master) {
        /* a dummy sqlite_master tbl was added, we need to remove it here */
        __free_fdb_tbl(tbl, fdb);
        tbl = NULL;
    }

    rc = FDB_NOERR;

done:

    /* unlock the mutex only if acquired */
    if (!in_analysis_load) {
        Pthread_rwlock_unlock(&fdb->h_rwlock);
    }

nop:
    return rc;
}

/* NOT thread safe, need fdb->h_rw_lock */
static int fdb_num_entries(fdb_t *fdb)
{
    int nents;

    /* we use h_ents_rootp instead of h_tbls_name, since this is the last
     * updated */
    hash_info(fdb->h_ents_rootp, NULL, NULL, NULL, NULL, &nents, NULL, NULL);

    return nents;
}

/**
 * Connects to the db and retrieve the current sql master row
 * Checks cached sql master row and updates it and verid if the
 * there was a schema change on the remote db
 * NO thread safe (need exclusive fdb->h_rwlock)
 *
 * NOTE:
 */
static int check_table_fdb(fdb_t *fdb, fdb_tbl_t *tbl, int initial,
                           fdb_tbl_ent_t **found_ent, int is_sqlite_master)
{
    BtCursor *cur;
    int rc = FDB_NOERR;
    int irc = FDB_NOERR;
    fdb_cursor_if_t *fdbc_if;
    fdb_cursor_t *fdbc;
    char *sql = NULL;
    char *row;
    int rowlen;
    int versioned;
    int need_ssl = 0;

    /* fake a BtCursor */
    cur = calloc(1, sizeof(BtCursor) + sizeof(Btree));
    if (!cur) {
        rc = FDB_ERR_MALLOC;
        logmsg(LOGMSG_ERROR, "%s: malloc\n", __func__);
        goto done;
    }
    init_cursor(cur, NULL, (Btree *)(cur + 1));
    cur->bt->fdb = fdb;
    cur->bt->is_remote = 1;
    struct sqlclntstate *clnt = cur->clnt;
    cur->rootpage = 1;

run:
    /* if we have already learnt that fdb is older, do not try newer versioned
     * queries */
    if (fdb->server_version == FDB_VER_LEGACY)
        versioned = 0;
    else
        versioned = 1;

    fdbc_if = fdb_cursor_open(clnt, cur, cur->rootpage, NULL, NULL, need_ssl);
    if (!fdbc_if) {
        rc = clnt->fdb_state.xerr.errval;
        logmsg(LOGMSG_ERROR, 
                "%s: failed to connect remote sqlite_master rc =%d \"%s\"\n",
                __func__, clnt->fdb_state.xerr.errval,
                clnt->fdb_state.xerr.errstr);
        goto done;
    }

    fdbc = fdbc_if->impl;

    /* prepackaged select */
    if (versioned) {
        if (initial) {
            sql = sqlite3_mprintf(
                     "select *, table_version(tbl_name) from sqlite_master"
                     " where tbl_name='%q' collate nocase or tbl_name="
                     "'sqlite_stat1' or "
                     "tbl_name='sqlite_stat4'",
                     tbl->name);
        } else {
            sql = sqlite3_mprintf(
                     "select *, table_version(tbl_name) from sqlite_master"
                     " where tbl_name='%q' collate nocase",
                     tbl->name);
        }
    } else {
        /* fallback to old un-versioned implementation */
        if (initial) {
            sql = sqlite3_mprintf(
                     "select * from sqlite_master"
                     " where tbl_name='%q' or tbl_name='sqlite_stat1' or "
                     "tbl_name='sqlite_stat4' collate nocase",
                     tbl->name);
        } else {
            sql = sqlite3_mprintf(
                     "select * from sqlite_master"
                     " where tbl_name='%q' collate nocase",
                     tbl->name);
        }
    }
    fdbc->sql_hint = sql;

    rc = fdbc_if->move(cur, CFIRST | NORETRY);
    fdbc_if = cur->fdbc; /* retry might get another cursor */
    if (rc != IX_FND && rc != IX_FNDMORE) {
        /* maybe remote is old code, retry in unversioned mode */
        switch (rc) {
        case FDB_ERR_SSL:
            /* remote needs ssl */
            fdb_cursor_close_on_open(cur, 0);
            if (gbl_client_ssl_mode >= SSL_ALLOW) {
                logmsg(LOGMSG_ERROR, "remote required SSl, switching to SSL\n");
                need_ssl = 1;
                assert(fdb->server_version >= FDB_VER_SSL);
                if (sql) {
                    sqlite3_free(sql);
                    sql = NULL;
                }
                fdbc->sql_hint = NULL;
                goto run;
            }
            goto done;

        case FDB_ERR_FDB_VERSION:
            /* retry new version */
            fdb_cursor_close_on_open(cur, 0);
            rc = FDB_NOERR;
            if (sql) {
                sqlite3_free(sql);
                sql = NULL;
            }
            fdbc->sql_hint = NULL;
            goto run;

        case IX_EMPTY:
            rc =
                FDB_NOERR; /* no operational error, but a syntax issue probably
                              */
            break;

        default:
            if (rc > FDB_NOERR) {
                /* catch all */
                rc = FDB_ERR_GENERIC;
            } else {
                /* we expect cursor move to return proper FDB_ERR codes */
                rc = clnt->fdb_state.xerr.errval;
            }
            break;
        }

        if (!is_sqlite_master)
            logmsg(LOGMSG_ERROR, "%s: unable to find schema for %s.%s rc =%d\n",
                   __func__, fdb->dbname, tbl->name, rc);

        if (*found_ent)
            *found_ent = NULL;

        goto close;
    }

    do {
        /* rows ! */
        row = fdbc_if->data(cur);
        rowlen = fdbc_if->datalen(cur);

        if (rowlen <= 0) {
            logmsg(LOGMSG_ERROR, 
                    "%s: failure to retrieve remote schema, row=%p rowlen=%d\n",
                    __func__, row, rowlen);
            rc = FDB_ERR_BUG;
            goto close;
        }

        irc = insert_table_entry_from_packedsqlite(fdb, tbl, row, rowlen,
                                                   found_ent, versioned);
        if (irc) {
            rc = irc;
            goto close;
        }

        if (rc == IX_FNDMORE) {
            rc = fdbc_if->move(cur, CNEXT);
        } else {
            break;
        }
    } while (rc == IX_FNDMORE ||
             rc == IX_FND); /* break if move(next) reports rc=error*/

    if (rc == IX_FND)
        rc = FDB_NOERR;

close:
    irc = fdb_cursor_close(cur);
    if (irc) {
        logmsg(LOGMSG_ERROR, "%s: failed to close cursor rc=%d\n", __func__, irc);
    }

done:
    sqlite3_free(sql);
    return rc;
}

static enum mach_class get_fdb_class(const char **p_dbname, int *local,
                                     int *lvl_override)
{
    const char *dbname = *p_dbname;
    enum mach_class my_lvl = CLASS_UNKNOWN;
    enum mach_class remote_lvl = CLASS_UNKNOWN;
    const char *tmpname;
    char *class;

    *local = 0;

    my_lvl = get_my_mach_class();

    /* extract class if any */
    if ((tmpname = strchr(dbname, '_')) != NULL) {
        class = strndup(dbname, tmpname - dbname);
        dbname = tmpname + 1;
        if (strncasecmp(class, "LOCAL", 6) == 0) {
            *local = 1;
            remote_lvl = my_lvl;
            /* accessed allowed implicitely */
        } else {
            remote_lvl = mach_class_name2class(class);
        }
        free(class); /* class is strndup'd */
        *p_dbname = dbname;
        if (lvl_override)
            *lvl_override = 1;
    } else {
        /* implicit is same class */
        remote_lvl = my_lvl;
        if (lvl_override)
            *lvl_override = 0;
    }

    /* override local */
    if (gbl_fdb_resolve_local) {
        *local = 1;
        remote_lvl = my_lvl; /* accessed allowed implicitely */
    }

    /* TODO: check access permissions */
    /* NOTE: for now, we only allow same class or local overrides.
       I will sleep better */
    if (!gbl_fdb_allow_cross_classes && remote_lvl != my_lvl) {
        logmsg(LOGMSG_ERROR, "%s: trying to access wrong cluster class\n", __func__);
        remote_lvl = CLASS_DENIED;
    }

    return remote_lvl;
}

int comdb2_fdb_check_class(const char *dbname)
{
    fdb_t *fdb;
    enum mach_class requested_lvl = CLASS_UNKNOWN;
    int local;
    int rc = 0;

    requested_lvl = get_fdb_class(&dbname, &local, NULL);
    if (requested_lvl == CLASS_UNKNOWN) {
        return -1;
    }

    fdb = get_fdb(dbname);
    if (!fdb) {
        logmsg(LOGMSG_ERROR, "%s: fdb gone?\n", __func__);
        rc = FDB_ERR_BUG;
        goto done;
    }

    if (fdb->class != requested_lvl) {
        logmsg(LOGMSG_ERROR, "%s: cached fdb is a different class, failing\n",
                __func__);
        rc = FDB_ERR_CLASS_DENIED;
        goto done;
    }

done:

    return rc;
}

static int __check_sqlite_stat(sqlite3 *db, fdb_tbl_ent_t *ent, Table *tab)
{
    /* incorrect version, unlikely */
    if (unlikely(ent && tab && (tab->version != ent->tbl->version))) {
        logmsg(LOGMSG_ERROR, "Stale cache for \"%s.%s\", sql version=%u != "
                             "shared version=%llu\n",
               ent->tbl->fdb->dbname, tab->zName, tab->version,
               ent->tbl->version);

        return SQLITE_SCHEMA_REMOTE;
    }

    /* incorrect rootpage numbers */
    if (ent && tab && (tab->tnum != ent->rootpage)) {
        logmsg(LOGMSG_ERROR, "Stale cache for \"%s.%s\", wrong rootpage number "
                        "sqlite=%d shared=%d\n",
                ent->tbl->fdb->dbname, tab->zName, tab->tnum, ent->rootpage);

        return SQLITE_SCHEMA_REMOTE;
    }

    /* sqlite cached but not shared! */
    if (!ent && tab) {
        logmsg(LOGMSG_ERROR, "Stale cache for \"%s.%s\", wrong rootpage number "
                        "sqlite=%d but not shared\n",
                db->aDb[tab->iDb].zDbSName, tab->zName, tab->tnum);

        return SQLITE_SCHEMA_REMOTE;
    }

    return SQLITE_OK;
}

static int _fdb_check_sqlite3_cached_stats(sqlite3 *db, fdb_t *fdb)
{
    int rc = SQLITE_OK;
    if (sqlite3_is_preparer(db))
        return SQLITE_OK;

    char *dbname = fdb->local == 0 ? fdb->dbname :
        sqlite3_mprintf("LOCAL_%s", fdb->dbname);
    fdb_tbl_ent_t *stat_ent;
    Table *stat_tab;

    stat_ent = get_fdb_tbl_ent_by_name_from_fdb(fdb, "sqlite_stat1");
    stat_tab = sqlite3FindTableCheckOnly(db, "sqlite_stat1", dbname);

    if (__check_sqlite_stat(db, stat_ent, stat_tab) != SQLITE_OK) {
        rc = SQLITE_SCHEMA_REMOTE;
        goto remote;
    }

    stat_ent = get_fdb_tbl_ent_by_name_from_fdb(fdb, "sqlite_stat4");
    stat_tab = sqlite3FindTableCheckOnly(db, "sqlite_stat4", dbname);

    if (__check_sqlite_stat(db, stat_ent, stat_tab) != SQLITE_OK) {
        rc = SQLITE_SCHEMA_REMOTE;
        goto remote;
    }

remote:
    if (dbname != fdb->dbname)
        sqlite3_free(dbname);
    return rc;
}

static int _failed_AddAndLockTable(const char *dbname, int errcode,
                                   const char *prefix)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;

    logmsg(LOGMSG_WARN, "Error rc %d \"%s\" for db \"%s\"\n", errcode, prefix,
           dbname);

    if (clnt->fdb_state.xerr.errval && clnt->fdb_state.preserve_err) {
        logmsg(LOGMSG_ERROR, "Ignored error rc=%d str=\"%s\", got new rc=%d new prefix=\"%s\"\n",
            clnt->fdb_state.xerr.errval, clnt->fdb_state.xerr.errstr, errcode,
            prefix);
    } else {
        /* need to pass error to sqlite */
        errstat_set_rcstrf(&clnt->fdb_state.xerr, errcode, 
                 "%s for db \"%s\"", prefix, dbname);
    }

    return SQLITE_ERROR; /* speak sqlite */
}

/**
 * Sqlite wrapper for adding a new database table
 *
 * version returns the version of the fdb (new or old)
 *
 * NOTE: the function populates clnt->fdb_state.xerr in case of error,
 *       and returns SQLITE_ERROR so that sql can rollback
 *
 */
int sqlite3AddAndLockTable(sqlite3 *db, const char *dbname, const char *table,
                           int *version, int in_analysis_load,
                           int *out_class, int *out_local,
                           int *out_class_override, int *out_proto_version)
{
    fdb_t *fdb;
    int rc = FDB_NOERR;
    int created = 0;
    int local = 0;
    enum mach_class lvl = 0;
    char errstr[256];
    char *perrstr;
    int lvl_override;

    lvl = get_fdb_class(&dbname, &local, &lvl_override);
    if (lvl == CLASS_UNKNOWN || lvl == CLASS_DENIED) {
        return _failed_AddAndLockTable(
            dbname,
            (lvl == CLASS_UNKNOWN) ? FDB_ERR_CLASS_UNKNOWN
                                   : FDB_ERR_CLASS_DENIED,
            (lvl == CLASS_UNKNOWN) ? "unrecognized class" : "denied access");
    }
retry_fdb_creation:
    fdb = new_fdb(dbname, &created, lvl, local, lvl_override);
    if (!fdb) {
        /* we cannot really alloc a new memory string for sqlite here */
        return _failed_AddAndLockTable(dbname, FDB_ERR_MALLOC,
                                       "OOM allocating fdb object");
    }
    if (!created) {
        /* we need to validate requested class to existing class */
        rc = _validate_existing_table(fdb, lvl, local);
        if (rc != FDB_NOERR) {
            __fdb_rem_user(fdb, 1);
            return _failed_AddAndLockTable(dbname, rc, "mismatching class");
        }
    }

    /* NOTE: FROM NOW ON, CREATED FDB IS VISIBLE TO OTHER THREADS! */

    /* hack: sqlite stats are inheriting the present db lvl */
    if (!created && is_sqlite_stat(table)) {
        lvl = fdb->class;
        if (!fdb->loc) {
            local = 1;
        }
    }

    if (!local) {
        rc = fdb_locate(fdb->dbname, fdb->class, 0, &fdb->loc, &fdb->dbcon_mtx);
        if (rc != FDB_NOERR) {
            switch (rc) {
            case FDB_ERR_CLASS_UNKNOWN:
                perrstr = "class unknown";
                break;
            case FDB_ERR_REGISTER_NONODES:
                perrstr = "no nodes";
                break;
            case FDB_ERR_MALLOC:
                perrstr = "out of memory";
                break;
            case FDB_ERR_REGISTER_NORESCPU:
                perrstr = "all nodes are rtcpued";
                break;
            case FDB_ERR_REGISTER_NOTFOUND:
                perrstr = "no comdb2db";
                break;
            case FDB_ERR_REGISTER_IO:
                perrstr = "failed to read from comdb2db";
                break;
            default:
                perrstr = "no destination";
                break;
            }
            goto error; /* new_fdb bumped up users, need to decrement that */
        }
    }

    /* the bellow will exclusively lock fdb, and bump users before releasing
       the lock and returning */
    rc = _add_table_and_stats_fdb(fdb, table, version, in_analysis_load);
    if (rc != FDB_NOERR) {
        if (rc == FDB_ERR_FDB_NOTFOUND) {
            /* fdb deleted from under us by creator thread */
            goto retry_fdb_creation;
        }

        if (rc != FDB_ERR_SSL)
            logmsg(LOGMSG_ERROR,
                   "%s: failed to add foreign table \"%s:%s\" rc=%d\n",
                   __func__, dbname, table, rc);

        switch (rc) {
        case FDB_ERR_FDB_TBL_NOTFOUND: {
            snprintf(errstr, sizeof(errstr), "no such table \"%s\"", table);
            perrstr = errstr;
            break;
        }
        case FDB_ERR_PTHR_LOCK: {
            perrstr = "pthread locks are smashed";
            break;
        }
        case FDB_ERR_MALLOC: {
            perrstr = "out of memory";
            break;
        }
        case FDB_ERR_PI_DISABLED: {
            perrstr = "partial indexes disabled locally";
            break;
        }
        case FDB_ERR_EXPRIDX_DISABLED: {
            perrstr = "expressions indexes disabled locally";
            break;
        }
        case FDB_ERR_SSL: {
            perrstr = "remote db requires SSL";
            break;
        }
        default: {
            perrstr = "error adding remote table";
        }
        }

    error:
        /* decrement the local bump */
        __fdb_rem_user(fdb, 0);

        /* if we've created this now, remove it since it could be a mistype */
        if (created) {
            destroy_fdb(fdb);
            fdb = NULL;
        }

        return _failed_AddAndLockTable(dbname, rc, perrstr);
    }

    /* We have successfully created a shared fdb table on behalf of an sqlite3
       engine
       it is possible that sqlite_stat entries have changed, and prepare will
       need
       them to work (it is possible that they have stale schema/rootpage numbers
       The following clears the entries if the sqlite_stat entries are stale */
    /* we need to check the sqlite_stats also, since they are not really locked
     */
    if (_fdb_check_sqlite3_cached_stats(db, fdb) != SQLITE_OK) {
        /* lets remove the cached sqlite_stat information; it will be retrieved
         * fresh */
        fdb_clear_sqlite_cache(db, fdb->dbname, NULL);
    }

    /* we return SQLITE_OK here, which tells the caller that the db is still
       READ locked!
       the caller will have to release that */

    *out_class = lvl;
    *out_local = local;
    *out_class_override = lvl_override;
    *out_proto_version = fdb->server_version;

    return SQLITE_OK; /* speaks sqlite */
}

/**
 * Decrement users for AddAndLock callers
 *
 * Always able to find a fdb since it was locked
 *
 */
int sqlite3UnlockTable(const char *dbname, const char *table)
{
    fdb_t *fdb;

    fdb = get_fdb(dbname);
    if (!fdb) {
        /* bug */
        logmsg(LOGMSG_FATAL, "Unable to find dbname \"%s\", BUG!\n", dbname);
        abort();
    }

    __fdb_rem_user(fdb, 1); /* matches __fdb_add_user in sqlite3AddAndLockTable */

    return SQLITE_OK;
}

static int __lock_wrlock_shared(fdb_t *fdb)
{
    int rc = FDB_NOERR;

    Pthread_rwlock_rdlock(&fdb->h_rwlock);

    return rc;
}

static int __lock_wrlock_exclusive(char *dbname)
{
    fdb_t *fdb = NULL;
    int rc = FDB_NOERR;
    int idx = -1;
    int len = strlen(dbname) + 1;

    if (_test_trap_dlock1 == 2) {
        _test_trap_dlock1++;
    }

    do {
        Pthread_rwlock_rdlock(&fdbs.arr_lock);
        if (!(idx >= 0 && idx < fdbs.nused && fdbs.arr[idx] == fdb &&
              strncasecmp(dbname, fdbs.arr[idx]->dbname, len) == 0)) {
            fdb = __cache_fnd_fdb(dbname, &idx);
        }

        if (!fdb) {
            Pthread_rwlock_unlock(&fdbs.arr_lock);
            return FDB_ERR_FDB_NOTFOUND;
        }

        Pthread_rwlock_wrlock(&fdb->h_rwlock);

        /* we got the lock, are there any lockless users ? */
        if (fdb->users > 1) {
            Pthread_rwlock_unlock(&fdb->h_rwlock);
            Pthread_rwlock_unlock(&fdbs.arr_lock);

            /* if we loop, make sure this is not a live lock
               deadlocking with another sqlite engine that waits
               for a bdb write lock to be processed */

            struct sql_thread *thd = pthread_getspecific(query_info_key);
            if (!thd)
                continue;

            rc = clnt_check_bdb_lock_desired(thd->clnt);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d recover_deadlock returned %d\n",
                       __func__, __LINE__, rc);
                return FDB_ERR_GENERIC;
            }

            continue;
        } else {
            rc = FDB_NOERR;
            break; /* own fdb */
        }
    } while (1); /* 1 is the creator */

    Pthread_rwlock_unlock(&fdbs.arr_lock);

    return rc;
}

static fdb_tbl_ent_t *get_fdb_tbl_ent_by_rootpage_from_fdb(fdb_t *fdb,
                                                           int rootpage)
{
    fdb_tbl_ent_t *ent;

    __lock_wrlock_shared(fdb);
    ent = hash_find_readonly(fdb->h_ents_rootp, &rootpage);
    Pthread_rwlock_unlock(&fdb->h_rwlock);

    return ent;
}

/* NOTE: ALERT! always call this with h_rwlock acquired; as of now
   add_table_and_stats_fdb
   has WR lock on it and it is undefined behaviour to get the read lock here */
static fdb_tbl_ent_t *get_fdb_tbl_ent_by_name_from_fdb(fdb_t *fdb,
                                                       const char *name)
{
    fdb_tbl_ent_t *ent;
    /*
       Pthread_rwlock_rdlock(&fdb->h_rwlock);
     */
    ent = hash_find_readonly(fdb->h_ents_name, &name);
    /*
       Pthread_rwlock_unlock(&fdb->h_rwlock);
     */

    return ent;
}

/**
 * Retrieve entry for a fdb and a entry name (tbl or index)
 *
 */
fdb_tbl_ent_t *fdb_table_entry_by_name(fdb_t *fdb, const char *name)
{
    fdb_tbl_ent_t *ent;

    __lock_wrlock_shared(fdb);
    ent = hash_find_readonly(fdb->h_ents_name, &name);
    Pthread_rwlock_unlock(&fdb->h_rwlock);

    return ent;
}

static fdb_tbl_ent_t *get_fdb_tbl_ent_by_rootpage(int rootpage)
{
    fdb_t *fdb;
    fdb_tbl_ent_t *ent = NULL;
    int i;

    Pthread_rwlock_rdlock(&fdbs.arr_lock);
    for (i = 0; i < fdbs.nused; i++) {
        fdb = fdbs.arr[i];

        ent = get_fdb_tbl_ent_by_rootpage_from_fdb(fdb, rootpage);

        if (ent)
            break;
    }
    Pthread_rwlock_unlock(&fdbs.arr_lock);

    return ent;
}

/**
 * Retrieve the name for a specific rootpage
 * Caller must free the returned pointer
 *
 */
char *fdb_sqlexplain_get_name(int rootpage)
{
    fdb_tbl_ent_t *ent;
    char tmp[1024];

    ent = get_fdb_tbl_ent_by_rootpage(rootpage);

    /* NOTE: do we support live table removals? */
    if (ent) {
        if (ent->ixnum == -1) {
            snprintf(tmp, sizeof(tmp), "table \"%s.%s\"", ent->tbl->fdb->dbname,
                     ent->name);
        } else {
            snprintf(tmp, sizeof(tmp), "index \"%s\" on table \"%s.%s\"",
                     ent->name, ent->tbl->fdb->dbname, ent->tbl->name);
        }
    } else {
        snprintf(tmp, sizeof(tmp), "UNKNOWN???");
    }

    return strdup(tmp);
}

int create_sqlite_master_table(const char *etype, const char *name,
                               const char *tbl_name, int rootpage,
                               const char *sql, const char *csc2,
                               char **ret_rec, int *ret_rec_len)
{
#define SQLITE_MASTER_ROW_COLS 6
    Mem mems[SQLITE_MASTER_ROW_COLS], *m;
    int rc;

    logmsg(LOGMSG_INFO, "Creating master table for %s %s %s %d \"%s\" \"%s\"\n", etype, name,
           tbl_name, rootpage, sql, csc2);

    bzero(&mems, sizeof(mems));
    *ret_rec = NULL;
    *ret_rec_len = 0;

    /* type */
    m = &mems[0];
    m->z = strdup(etype);
    if (!m->z) {
        logmsg(LOGMSG_ERROR, "ENOMEM: %d Malloc %zu\n", __LINE__,
               strlen(etype));
        return FDB_ERR_MALLOC;
    }
    m->n = strlen(etype);
    m->flags = MEM_Str | MEM_Ephem;
    /* name */
    m++;
    m->z = strdup(name);
    if (!m->z) {
        logmsg(LOGMSG_ERROR, "ENOMEM: %d Malloc %zu\n", __LINE__, strlen(name));
        free(mems[0].z);
        return FDB_ERR_MALLOC;
    }
    m->n = strlen(name);
    m->flags = MEM_Str | MEM_Ephem;
    /* tbl_name */
    m++;
    m->z = strdup(tbl_name);
    if (!m->z) {
        logmsg(LOGMSG_ERROR, "ENOMEM: %d Malloc %zu\n", __LINE__,
               strlen(tbl_name));
        free(mems[0].z);
        free(mems[1].z);
        return FDB_ERR_MALLOC;
    }
    m->n = strlen(tbl_name);
    m->flags = MEM_Str | MEM_Ephem;
    /* rootpage */
    m++;
    m->u.i = rootpage;
    m->flags = MEM_Int;
    /* sql */
    m++;
    m->z = strdup(sql);
    if (!m->z) {
        logmsg(LOGMSG_ERROR, "ENOMEM: %d Malloc %zu\n", __LINE__, strlen(sql));
        free(mems[0].z);
        free(mems[1].z);
        free(mems[2].z);
        return FDB_ERR_MALLOC;
    }
    m->n = strlen(sql);
    m->flags = MEM_Str | MEM_Ephem;
    /* csc2 */
    m++;
    if (csc2) {
        m->z = strdup(csc2);
        if (!m->z) {
            logmsg(LOGMSG_ERROR, "ENOMEM: %d Malloc %zu\n", __LINE__,
                   strlen(csc2));
            free(mems[0].z);
            free(mems[1].z);
            free(mems[2].z);
            free(mems[4].z);
            return FDB_ERR_MALLOC;
        }
        m->n = strlen(csc2);
        m->flags = MEM_Str | MEM_Ephem;
    } else {
        m->flags = MEM_Null;
    }

    rc = sqlite3_unpacked_to_packed(mems, SQLITE_MASTER_ROW_COLS, ret_rec,
                                    ret_rec_len);
    if (rc) {
        logmsg(LOGMSG_ERROR, "ENOMEM: Malloc error\n");
        free(mems[0].z);
        free(mems[1].z);
        free(mems[2].z);
        free(mems[4].z);
        free(mems[5].z);
        return FDB_ERR_MALLOC;
    }

    return FDB_NOERR;
}

/**
 * insert an entry using a packed sqlite row ; no locking here, table is not yet
 * visible
 */
static int insert_table_entry_from_packedsqlite(fdb_t *fdb, fdb_tbl_t *tbl,
                                                char *row, int rowlen,
                                                fdb_tbl_ent_t **found_ent,
                                                int versioned)
{
    fdb_tbl_ent_t *ent = (fdb_tbl_ent_t *)calloc(sizeof(fdb_tbl_ent_t), 1);
    char *etype, *name, *tbl_name, *sql, *csc2;
    int rootpage, source_rootpage;
    unsigned long long version;
    int rc = 0;
    char *where = NULL;

    if (!ent) {
        logmsg(LOGMSG_ERROR, "%s: calloc OOM %zu bytes\n", __func__,
               sizeof(fdb_tbl_ent_t));
        return FDB_ERR_MALLOC;
    }

    /* sqlite_stats are updated under this lock, we don't need it here */
    rootpage = get_rootpage_numbers(1);

    version = 0;
    fdb_packedsqlite_process_sqlitemaster_row(row, rowlen, &etype, &name,
                                              &tbl_name, &source_rootpage, &sql,
                                              &csc2, &version, rootpage);

    if (gbl_fdb_track)
        logmsg(LOGMSG_USER, "%s:%s Inserting table %s:%s rootp=%d src_rootp=%d "
                        "version=%llu, sql %s\n",
                fdb->dbname, tbl->name, name, tbl_name, rootpage,
                source_rootpage, version, sql);

    if (strcasecmp(name, tbl_name) &&
        (where = strstr(sql, ") where (")) != NULL) {
        if (!gbl_partial_indexes) {
            logmsg(LOGMSG_ERROR, "Foreign table has partial indexes but partial "
                            "indexes feature is disabled on this machine\n");
            rc = FDB_ERR_PI_DISABLED;
            goto out;
        }
        tbl->ix_partial = 1;
        where[1] = '\0';
    }
    if (strcasecmp(name, tbl_name) &&
        (strstr(sql, "((") || strstr(sql, "))") || strstr(sql, ", ("))) {
        if (!gbl_expressions_indexes) {
            logmsg(LOGMSG_ERROR, "Foreign table has expressions indexes but "
                            "expressions indexes feature is disabled on this "
                            "machine\n");
            rc = FDB_ERR_EXPRIDX_DISABLED;
            goto out;
        }
        tbl->ix_expr = 1;
    }
    if (where)
        where[1] = ' ';

    /*
    printf("Saved pointer %p\n", row);
    */
    ent->ent = malloc(rowlen);
    if (!ent->ent) {
        logmsg(LOGMSG_ERROR, "Failed malloc!\n");
        rc = FDB_ERR_MALLOC;
        goto out;
    }
    memcpy(ent->ent, row, rowlen);
    ent->entlen = rowlen;

    ent->rootpage = rootpage;
    ent->source_rootpage = source_rootpage;
    if (strcasecmp(name, tbl_name) == 0) {
        /* table */
        ent->ixnum = -1;
    } else {
        /* index */
        ent->ixnum = tbl->nix;
        tbl->nix++;
        if (strstr(sql, ") where ("))
            tbl->ix_partial = 1;
    }
    ent->name = strdup(name);
    ent->tbl = tbl;
    listc_abl(&tbl->ents, ent);
    hash_add(fdb->h_ents_rootp, ent);
    hash_add(fdb->h_ents_name, ent);

    if (strcasecmp(ent->name, "sqlite_stat1") == 0) {
        fdb->has_sqlstat1 = 1;
    }
    if (strcasecmp(ent->name, "sqlite_stat4") == 0) {
        fdb->has_sqlstat4 = 1;
    }
    if (strcasecmp(ent->name, tbl->name) == 0) {
        *found_ent = ent;
    }

    if (versioned) {
        /* Do to the way sqlite_stat tables are added on the first table request
           to the fdb, they get associated as entries to the wrong table here.
           This gets fixed later, but we have to make sure we don't override
           the version of the original table with the version of sqlite_stat.
        */
        if (!(is_sqlite_stat(ent->name)))
            ent->tbl->version = version;
        ent->_version = version; /* sqlite_stats get cached version here */
    }

    rc = FDB_NOERR;

out:
    free(etype);
    free(name);
    free(tbl_name);
    free(sql);
    free(csc2);

    return rc;
}

/**
 * Retrieve the sqlite_master row size for provided entry
 *
 */
int fdb_get_sqlite_master_entry_size(fdb_t *fdb, fdb_tbl_ent_t *ent)
{
    if (!ent) {
        logmsg(LOGMSG_FATAL, "%s; Missing table? \n", __func__);
        abort();
    }

    return ent->entlen;
}

/**
 * Retrieve the sqlite_master row size for rootpage
 *
 */
void *fdb_get_sqlite_master_entry(fdb_t *fdb, fdb_tbl_ent_t *ent)
{
    if (!ent) {
        logmsg(LOGMSG_FATAL, "%s; Missing table? \n", __func__);
        abort();
    }

    return ent->ent;
}

/**
 * Move a cursor on sqlite_master table
 * Since we generate schema for remote tables
 * on demand, this routine is optimized to
 * pick and walk only a fdb, and also to return only
 * rows for table, not all
 *
 */

int fdb_cursor_move_master(BtCursor *pCur, int *pRes, int how)
{
    const char *zTblName;

    if (gbl_old_column_names && pCur->clnt->thd &&
        pCur->clnt->thd->query_preparer_running) {
        /* We must have a query_preparer_plugin installed. */
        assert(pCur->query_preparer_data != 0);
        assert(query_preparer_plugin &&
               query_preparer_plugin->sqlitex_table_name);
        zTblName = query_preparer_plugin->sqlitex_table_name(
            pCur->query_preparer_data);
    } else {
        sqlite3 *sqlite = pCur->sqlite;
        zTblName = sqlite->init.zTblName;
    }
    fdb_t *fdb = pCur->bt->fdb;
    fdb_tbl_t *tbl = NULL;
    int step = 0;

    assert(fdb != NULL);

    /*
     NOTE: there are two types of calls
     1) when a table is attached first time to a sqlite engine:
        the fdb exists and has an sqlite_master already; in this case
        the comdb2_dynamic_attach code sets init.zTblName to point to
        the desired table
     2) after a schema flush; in this case fdb exists but it has no
        sqlite_master; this is called with init.zTblName == NULL, which
        would mean "give me whatever we have local, I am gonna populate
        this engine"
     */

    pCur->eof = 0;

    /* are we walking the sqlite_stats? */
    if (pCur->crt_sqlite_master_row) {
        if (strncasecmp(pCur->crt_sqlite_master_row->name, "sqlite_stat1",
                        12) == 0) {
            goto sqlite_stat1;
        }
        if (strncasecmp(pCur->crt_sqlite_master_row->name, "sqlite_stat4",
                        12) == 0) {
            goto sqlite_stat4;
        }
    } else {
        /* this is the first time we step and locate a table; we
        will need to position on the current table; given the order
        chosen {table, stat1, stat4, done}, if table is stat4, we
        end up skipping stat1.  To fix this, we replace stat4 with
        stat1 since we will get stat4 after this.
        */
        if (strncasecmp(zTblName, "sqlite_stat4", 12) == 0)
            zTblName = "sqlite_stat1";
        /* In addition, if the first remote table from this fdb
        is sqlite_master, we only get stats tables, and the follow-up
        hash_find_readonly returns no entry, since we don't have an
        entry for sqlite_master;  fix this by pointing to sqlite_stat1
        as well */
        if (strncasecmp(zTblName, "sqlite_master", 13) == 0)
            zTblName = "sqlite_stat1";
    }

search:
    __lock_wrlock_shared(fdb);
    tbl = hash_find_readonly(fdb->h_tbls_name, &zTblName);

    if (!tbl) {
        /* this is possible only for wrong tblname? */
        Pthread_rwlock_unlock(&fdb->h_rwlock);
        /* done, the table is gone */
        /* TODO: review drop table case */
        pCur->eof = 1;
        *pRes = 1;
        return SQLITE_OK;
    }
    Pthread_mutex_lock(&tbl->ents_mtx);
    Pthread_rwlock_unlock(&fdb->h_rwlock);

    assert(how == CNEXT || how == CFIRST); /* NEXT w/out FIRST is FIRST */

    if (!pCur->crt_sqlite_master_row) {
        pCur->crt_sqlite_master_row = tbl->ents.top;
        assert(pCur->crt_sqlite_master_row);
    } else {
        if (!pCur->crt_sqlite_master_row->lnk.next) {
            Pthread_mutex_unlock(&tbl->ents_mtx);

            switch (step) {
            case 0:
                pCur->crt_sqlite_master_row = NULL;
                goto sqlite_stat1;
            case 1:
                pCur->crt_sqlite_master_row = NULL;
                goto sqlite_stat4;
            case 2:
                /* fall- through */
                break;
            }
        }
        pCur->crt_sqlite_master_row = pCur->crt_sqlite_master_row->lnk.next;
    }

    if (!pCur->crt_sqlite_master_row) {
        pCur->eof = 1;
        *pRes = 1;
        return SQLITE_OK;
    }

    Pthread_mutex_unlock(&tbl->ents_mtx);

    *pRes = 0;

    return SQLITE_OK;

sqlite_stat1:
    /* NOTE: this is a bit of hack; when we are parsing the sqlite_mastter
       tables,
       we match the table name from zTblName, but also need sqlite_stats */
    /* we still have the fdb->h_rwlock here */
    /* locate btree position */
    zTblName = "sqlite_stat1";
    step = 1;

    goto search;

sqlite_stat4:
    zTblName = "sqlite_stat4";
    step = 2;

    goto search;
}

/**
 * Retrieve the field name for the table identified by "rootpage", index
 * "ixnum",
 * field "fieldnum"
 */
char *fdb_sqlexplain_get_field_name(Vdbe *v, int rootpage, int ixnum,
                                    int fieldnum)
{
    fdb_tbl_ent_t *ent;
    Table *pTab;
    Index *pIdx;
    Column *pCol;

    if (!v)
        return NULL;

    ent = get_fdb_tbl_ent_by_rootpage(rootpage);
    if (!ent)
        return NULL;

    if (ent->ixnum == -1) {
        pTab =
            sqlite3FindTableCheckOnly(v->db, ent->name, ent->tbl->fdb->dbname);
        if (!pTab)
            return NULL;

        if (fieldnum < 0 || fieldnum > pTab->nCol)
            return NULL;

        pCol = &pTab->aCol[fieldnum];
    } else {
        pIdx = sqlite3FindIndex(v->db, ent->name, ent->tbl->fdb->dbname);
        if (!pIdx)
            return NULL;

        if (fieldnum < 0 || fieldnum > pIdx->nColumn)
            return NULL;

        if (pIdx->aiColumn[fieldnum] < 0 ||
            pIdx->aiColumn[fieldnum] > pIdx->pTable->nCol)
            return NULL;

        pCol = &pIdx->pTable->aCol[pIdx->aiColumn[fieldnum]];
    }

    return pCol->zName;
}

/**
 * Retrieve/create space for a Btree schema change (per foreign db)
 *
 */
Schema *fdb_sqlite_get_schema(Btree *pBt, int nbytes)
{

    assert(pBt->is_remote && pBt->fdb != NULL);

    /* TODO: switch to sharing schemas for fdbs */
    /*
    fdb_t *fdb = pBt->fdb;
    Pthread_mutex_lock(&fdb->dbcon_mtx);
    if (fdb->schema == NULL)
    {
       fdb->schema = (Schema*)calloc(1, nbytes);
    }
    Pthread_mutex_unlock(&fdb->dbcon_mtx);

    return fdb->schema;
    */
    return calloc(1, nbytes);
}

static int _fdb_remote_reconnect(fdb_t *fdb, SBUF2 **psb, char *host, int use_cache)
{
    SBUF2 *sb = *psb;
    static uint64_t old = 0ULL;
    uint64_t now = 0, then;

    if (gbl_fdb_track) {
        logmsg(LOGMSG_USER, "Using node %s\n", host);
    }

    if (sb) {
        logmsg(LOGMSG_ERROR, "%s socket opened already???", __func__);
        sbuf2close(sb);
        *psb = sb = NULL;
    }

    if (gbl_fdb_track_times) {
        now = gettimeofday_ms();
    }

    *psb = sb = connect_remote_db("icdb2", fdb->dbname, "remsql", host, use_cache);

    if (gbl_fdb_track_times) {
        then = gettimeofday_ms();

        if (old == 0ULL) {
            logmsg(LOGMSG_USER, "TTTTTT now=%" PRId64 " 0 %" PRId64 "\n", now,
                   then - now);
        } else {
            logmsg(LOGMSG_USER,
                   "TTTTTT now=%" PRId64 " delta=%" PRId64 " %" PRId64 "\n",
                   now, now - old, then - now);
        }
        old = now;
    }

    if (!sb) {
        logmsg(LOGMSG_ERROR, "%s unable to connect to %s %s\n", __func__,
                fdb->dbname, host);
        return FDB_ERR_CONNECT;
    }

    /* we don't want timeouts so we can cache sockets on the source side...  */
    sbuf2settimeout(sb, 0, 0);

    return FDB_NOERR;
}

/**
 * Used to either open a remote transaction or cursor (fdbc==NULL-> transaction
 *begin)
 *
 * 1) Get the affinity node
 * 2) If no affinity, get an initial node and set affinity
 * 3) Do until success or more different nodes
 * 3.1) Connect to the current node
 * 3.2) Sends initial request
 * 3.3) If 3.1 or 3.2 failed,  picks another node and go to 3)
 * 5) If still don't have a connection, return error, otherwise return FDB_NOERR
 *
 */
static int _fdb_send_open_retries(struct sqlclntstate *clnt, fdb_t *fdb,
                                  fdb_cursor_t *fdbc, int source_rootpage,
                                  fdb_tran_t *trans, int flags, int version,
                                  fdb_msg_t *msg, int use_ssl)
{
    enum fdb_location_op op = FDB_LOCATION_REFRESH;
    char *host;
    int avail_nodes;
    int tried_nodes;
    int lcl_nodes;
    int rc = FDB_NOERR;
    int was_bad;
    SBUF2 **psb = NULL;
    int tried_refresh = 0; /* ultimate resort, comdb2db */
    int tran_flags = 0;
    sqlite3 *db;
    const char *sql;

    if (clnt->thd != NULL && (db = clnt->thd->sqldb) != NULL && db->pVdbe != NULL && db->pVdbe->fdb_warn_this_op != 0) {
        /* we only need one trace */
        sql = clnt->sql ? clnt->sql : "unavailable";
        logmsg(LOGMSG_INFO, "%s: Unsupported expr op for fdb cursor hints:%d query:%.16s (see trc.c for full query)\n",
               __func__, db->pVdbe->fdb_warn_this_op, sql);
        ctrace("%s: Unsupported expr op for fdb cursor hints:%d query:%s\n",
               __func__, db->pVdbe->fdb_warn_this_op, sql);
        db->pVdbe->fdb_warn_this_op = 0;
    }

    host = _fdb_get_affinity_node(clnt, fdb, &was_bad);
    if (host == NULL) {
        op = FDB_LOCATION_INITIAL;

    refresh:
        host = fdb_select_node(&fdb->loc, op, 0, &avail_nodes, &lcl_nodes,
                               &fdb->dbcon_mtx);

        if (avail_nodes <= 0) {
            clnt->fdb_state.preserve_err = 1;
            clnt->fdb_state.xerr.errval = FDB_ERR_REGISTER_NONODES;
            snprintf(clnt->fdb_state.xerr.errstr,
                     sizeof(clnt->fdb_state.xerr.errstr),
                     "%s: no available rescpu nodes", __func__);

            return clnt->fdb_state.xerr.errval;
        }
    } else if (was_bad) {
        char *bad_host = host;

        /* we failed earlier on this one, we need the next node */
        op = FDB_LOCATION_NEXT;
        host = fdb_select_node(&fdb->loc, op, host, &avail_nodes, &lcl_nodes,
                               &fdb->dbcon_mtx);

        /* Also, whitelist the node - assuming it's back healthy again. */
        if (was_bad == FDB_ERR_TRANSIENT_IO) {
            if (gbl_fdb_track)
                logmsg(LOGMSG_USER, "%s:%d whitelisting %s\n", __func__,
                       __LINE__, bad_host);
            _fdb_set_affinity_node(clnt, fdb, bad_host, FDB_NOERR);
        }
    }
    if (host == NULL) {
        clnt->fdb_state.preserve_err = 1;
        clnt->fdb_state.xerr.errval = FDB_ERR_REGISTER_NONODES;
        snprintf(clnt->fdb_state.xerr.errstr,
                 sizeof(clnt->fdb_state.xerr.errstr), "%s: unable to find node",
                 __func__);

        return clnt->fdb_state.xerr.errval;
    }

    /* fault tolerant node connect */
    tried_nodes = 0;
    do {
        if (fdbc) {
            psb = &fdbc->fcon.sock.sb;
        } else {
            psb = &trans->sb;
        }

        if ((rc = _fdb_remote_reconnect(fdb, psb, host, (fdbc)?1:0)) == FDB_NOERR) {
            if (fdbc) {
                fdbc->streaming = FDB_CUR_IDLE;

                rc =
                    fdb_send_open(msg, fdbc->cid, trans, source_rootpage, flags,
                                  version, fdbc->fcon.sock.sb);

                /* cache the node info */
                fdbc->node = host;
            } else {

                if (fdb->server_version >= FDB_VER_WR_NAMES)
                    tran_flags = FDB_MSG_TRAN_TBLNAME;
                else
                    tran_flags = 0;

                rc = fdb_send_begin(msg, trans, clnt->dbtran.mode, tran_flags,
                                    trans->sb);
                if (rc == FDB_NOERR) {
                    trans->host = host;
                }
            }
        }

        if (rc == FDB_NOERR) {
            /* successfull connection */
            if (use_ssl) {
                rc = sbuf2flush(*psb);
                if (rc != FDB_NOERR)
                    goto failed;
                rc = sbuf2getc(*psb);
                if (rc != 'Y')
                    goto failed;
                rc = FDB_NOERR;
                /*fprintf(stderr, "READ Y\n");*/

                if (sslio_connect(*psb, gbl_ssl_ctx, fdb->ssl, NULL,
                                  gbl_nid_dbname, 1) != 1) {
                failed:
                    sbuf2close(*psb);
                    *psb = NULL;
#if 0
                    LETS TRY ANOTHER NODE HERE INSTEAD OF FAILING
                    /* don't retry other nodes if SSL configuration is bad */
                    clnt->fdb_state.preserve_err = 1;
                    clnt->fdb_state.xerr.errval = FDB_ERR_CONNECT_CLUSTER;
                    snprintf(clnt->fdb_state.xerr.errstr,
                             sizeof(clnt->fdb_state.xerr.errstr),
                             "SSL config error to %s", host);
                    return FDB_ERR_SSL;
#endif
                }
            }
            break;
        }

        /* send failed, close sbuf */
        if (*psb) {
            sbuf2close(*psb);
            *psb = NULL;
        }

        /* FAIL on current node, NEED to get the next node */
        if (!tried_nodes && op == FDB_LOCATION_REFRESH) {
            op = FDB_LOCATION_INITIAL;
            host = fdb_select_node(&fdb->loc, op, host, &avail_nodes,
                                   &lcl_nodes, &fdb->dbcon_mtx);
            continue; /* try again with the selected node, can be the same */
        }
        else {
            /* either this is the first node, and
                  have location info (avail_nodes, lcl_nodes),
               or this is a retry after first node failed */

            /* did we try too many times? */
            if (tried_nodes >= avail_nodes) {
                break;
            }

            tried_nodes++;

            op = FDB_LOCATION_NEXT;

            /*
               if we already tried all local nodes (first lcl_nodes iterations)
               we ask router to ignore datacenter colocality;
             */
            if (tried_nodes > lcl_nodes) {
                op |= FDB_LOCATION_IGNORE_LCL;
            }

            host = fdb_select_node(&fdb->loc, op, host, NULL, NULL,
                                   &fdb->dbcon_mtx);
            if (host == NULL) {
                break;
            }
        }
    } while (1);

    /* maybe the database migrated... try contact comdb2db again */
    if (!(*psb) && !tried_refresh) {
        tried_refresh = 1;
        op = FDB_LOCATION_INITIAL | FDB_LOCATION_REFRESH;
        goto refresh;
    }

    if (!(*psb)) {
        logmsg(LOGMSG_ERROR, "%s: failed to connect after %d nodes checked\n",
                __func__, tried_nodes);
        clnt->fdb_state.preserve_err = 1;
        clnt->fdb_state.xerr.errval = FDB_ERR_CONNECT_CLUSTER;
        snprintf(clnt->fdb_state.xerr.errstr,
                 sizeof(clnt->fdb_state.xerr.errstr),
                 "failed to connect to cluster %d nodes", tried_nodes);

        rc = clnt->fdb_state.xerr.errval;
    } else {
        assert(rc == FDB_NOERR);

        /* save the node */
        rc = _fdb_set_affinity_node(clnt, fdb, host, FDB_NOERR);
        /* on success rc == FDB_NOERR */
    }

    return rc;
}

static fdb_cursor_if_t *_fdb_cursor_open_remote(struct sqlclntstate *clnt,
                                                fdb_t *fdb, int source_rootpage,
                                                fdb_tran_t *trans, int flags,
                                                int version, int use_ssl)
{
    fdb_cursor_if_t *fdbc_if;
    fdb_cursor_t *fdbc;
    int rc;
    char *tid;
    uuid_t zerouuid;

    if (!trans) {
        /* transactionless stuff */
        comdb2uuid_clear(zerouuid);
        tid = (char *)zerouuid;
    } else {
        tid = trans->tid;
    }

    fdbc_if = (fdb_cursor_if_t *)calloc(
        1, sizeof(fdb_cursor_if_t) + sizeof(fdb_cursor_t) + fdb_msg_size());
    if (!fdbc_if) {
        clnt->fdb_state.preserve_err = 1;
        clnt->fdb_state.xerr.errval = FDB_ERR_MALLOC;
        snprintf(clnt->fdb_state.xerr.errstr,
                 sizeof(clnt->fdb_state.xerr.errstr), "%s out of memory",
                 __func__);

        goto done;
    }

    fdbc_if->impl = fdbc =
        (fdb_cursor_t *)(((char *)fdbc_if) + sizeof(fdb_cursor_if_t));
    fdbc_if->impl->msg =
        (fdb_msg_t *)(((char *)fdbc_if) + sizeof(fdb_cursor_if_t) +
                      sizeof(fdb_cursor_t));
    fdbc_if->close = fdb_cursor_close;
    fdbc_if->id = fdb_cursor_id;
    fdbc_if->data = fdb_cursor_get_data;
    fdbc_if->datalen = fdb_cursor_get_datalen;
    fdbc_if->genid = fdb_cursor_get_genid;
    fdbc_if->get_found_data = fdb_cursor_get_found_data;
    fdbc_if->set_hint = fdb_cursor_set_hint;
    fdbc_if->get_hint = fdb_cursor_get_hint;
    fdbc_if->set_sql = fdb_cursor_set_sql;
    fdbc_if->name = fdb_cursor_name;
    fdbc_if->tblname = fdb_cursor_tblname;
    fdbc_if->tbl_has_partidx = fdb_cursor_table_has_partidx;
    fdbc_if->tbl_has_expridx = fdb_cursor_table_has_expridx;
    fdbc_if->dbname = fdb_cursor_dbname;
    fdbc_if->table_entry = fdb_cursor_table_entry;
    fdbc_if->access = fdb_cursor_access;
    fdbc_if->move = fdb_cursor_move_sql;
    fdbc_if->find = fdb_cursor_find_sql;
    fdbc_if->find_last = fdb_cursor_find_last_sql;
    fdbc_if->insert = fdb_cursor_insert;
    fdbc_if->delete = fdb_cursor_delete;
    fdbc_if->update = fdb_cursor_update;

    fdbc->tid = (char *)fdbc->tiduuid;
    fdbc->cid = (char *)fdbc->ciduuid;

    comdb2uuid(fdbc->ciduuid);
    memcpy(fdbc->tid, tid, sizeof(uuid_t));
    fdbc->flags = flags;
    fdbc->need_ssl = use_ssl;

    fdbc->intf = fdbc_if;

    /* NOTE: expect x_retries to fill in clnt error fields, if any */
    rc = _fdb_send_open_retries(clnt, fdb, fdbc, source_rootpage, trans, flags,
                                version, fdbc->msg, use_ssl);
    if (rc) {
        free(fdbc_if);
        fdbc_if = NULL;
        goto done;
    }

    if (trans != NULL) {
        trans->seq++; /*increment the transaction sequence to track this
                         important update */
    }

    Pthread_rwlock_wrlock(&fdbs.h_curs_lock);
    hash_add(fdbs.h_curs, fdbc);
    Pthread_rwlock_unlock(&fdbs.h_curs_lock);

done:
    return fdbc_if;
}

/**
 * Create a connection to fdb, or a local sqlite_stat cache
 *
 * NOTE: populates clnt->fdb_state error fields, if any error
 *
 */
fdb_cursor_if_t *fdb_cursor_open(struct sqlclntstate *clnt, BtCursor *pCur,
                                 int rootpage, fdb_tran_t *trans, int *ixnum,
                                 int use_ssl)
{
    fdb_cursor_if_t *fdbc_if;
    fdb_cursor_t *fdbc;
    fdb_t *fdb;
    fdb_tbl_ent_t *ent;
    int rc;
    int source_rootpage;
    int flags;

    assert(pCur->bt->is_remote);

    fdb = pCur->bt->fdb;

    assert(fdb != NULL);

    if (pCur->fdbc) {
        logmsg(LOGMSG_ERROR, "%s: fdb cursor already opened, refreshing\n",
                __func__);
        rc = pCur->fdbc->close(pCur);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: closing fdb cursor failed rc=%d\n", __func__, rc);
        }
    }
    pCur->fdbc = NULL;

    if (rootpage == 1) {
        /* if asking for a remote sqlite_master, we know we are asking for
         * remote rootpage 1 */
        ent = NULL;
    } else if (rootpage != -1 /* not shared cursor ! */) {
        ent = get_fdb_tbl_ent_by_rootpage_from_fdb(fdb, rootpage);
        if (!ent) {
            logmsg(LOGMSG_ERROR, "%s: unable to find rootpage %d\n", __func__,
                    rootpage);

            clnt->fdb_state.preserve_err = 1;
            clnt->fdb_state.xerr.errval = FDB_ERR_BUG;
            snprintf(clnt->fdb_state.xerr.errstr,
                     sizeof(clnt->fdb_state.xerr.errstr),
                     "%s: unable to find rootpage %d\n", __func__, rootpage);

            goto done;
        }
    } else {
        /* this include the cursors open to populate the sqlite_stats cache */
        ent = NULL;
    }

    if (_get_protocol_flags(clnt, fdb, &flags)) {
        goto done;
    }
    if (flags & FDB_MSG_CURSOR_OPEN_FLG_SSL)
        use_ssl = 1;

    /* NOTE: R5 used to send source_rootpage for open cursor case;
     *  we will change that in R5 to a magic value that we detect to
     * differentiate
     *  R5 from newer code versions
     */
    source_rootpage = fdb_ver_encoded(fdb->server_version);

    if (ent && is_sqlite_stat(ent->name)) {
        pCur->fdbc = fdbc_if =
            fdb_sqlstat_cache_cursor_open(clnt, fdb, ent->name);
        if (!fdbc_if) {
            logmsg(LOGMSG_ERROR, "%s: failed to open fdb cursor\n", __func__);

            clnt->fdb_state.preserve_err = 1;
            clnt->fdb_state.xerr.errval = FDB_ERR_BUG;
            snprintf(clnt->fdb_state.xerr.errstr,
                     sizeof(clnt->fdb_state.xerr.errstr),
                     "failed to open fdb cursor for stats");

            goto done;
        }
    } else {
        /* NOTE: we expect x_remote to fill in the error, if any */
        pCur->fdbc = fdbc_if = _fdb_cursor_open_remote(
            clnt, fdb, source_rootpage, trans, flags,
            (ent) ? fdb_table_version(ent->tbl->version) : 0, use_ssl);

        if (!fdbc_if) {
            logmsg(LOGMSG_ERROR, "%s: failed to open fdb cursor\n", __func__);
            goto done;
        }

        if (rootpage == 1) /* sqlite_master */
        {
            pCur->fdbc->impl->is_schema = 1;
        }

        fdbc = fdbc_if->impl;

        fdbc->ent = ent;
        fdbc->trans = trans;
    }

    if (ixnum && ent)
        *ixnum = ent->ixnum;

    pCur->fdbc = fdbc_if;

done:
    return pCur->fdbc;
}

/**
 * Close the cursor locally
 *
 */
static void fdb_cursor_close_on_open(BtCursor *pCur, int cache)
{
    if (pCur->fdbc) {
        fdb_cursor_t *fdbc = pCur->fdbc->impl;

        Pthread_rwlock_wrlock(&fdbs.h_curs_lock);
        hash_del(fdbs.h_curs, fdbc);
        Pthread_rwlock_unlock(&fdbs.h_curs_lock);

        if (cache && fdbc->ent && fdbc->ent->tbl &&
            fdbc->streaming == FDB_CUR_IDLE) {
            disconnect_remote_db("icdb2", fdbc->ent->tbl->fdb->dbname, "remsql",
                                 fdbc->node, &fdbc->fcon.sock.sb);
        } else {
            sbuf2close(fdbc->fcon.sock.sb);
            fdbc->fcon.sock.sb = NULL;
        }

        fdb_msg_clean_message(fdbc->msg);
        free(pCur->fdbc);
        pCur->fdbc = NULL;
    }
}

/**
 * Close a connection to fd
 *
 */
static int fdb_cursor_close(BtCursor *pCur)
{
    if (pCur->fdbc) {
        /*TODO: check sqlite_stat cursors and their caching */

        fdb_cursor_t *fdbc = pCur->fdbc->impl;

        fdb_send_close(fdbc->msg, fdbc->cid,
              (fdbc->trans) ? fdbc->trans->tid : 0,
              (fdbc->trans) ? fdbc->trans->seq : 0,
              fdbc->fcon.sock.sb);

        /* closing the cursor locally */
        fdb_cursor_close_on_open(pCur, 1);
    } else {
        logmsg(LOGMSG_ERROR, "%s cursor already closed rootpage=%d?\n", __func__,
                pCur->rootpage);
    }

    return FDB_NOERR;
}

static char *_build_run_sql_from_hint(BtCursor *pCur, Mem *m, int ncols,
                                      int bias, int *p_sqllen, int *error)
{
    fdb_cursor_t *fdbc = pCur->fdbc->impl;
    char *tableName = NULL;
    char *sql = NULL;
    char *whereDesc = NULL;
    char *orderDesc = NULL;
    int hasCondition = 0;
    sqlite3 *sqlitedb = pCur->sqlite;
    char *columnsDesc = NULL;
    int using_col_filter = 0;

    if (!fdbc->ent) {
        tableName = "sqlite_master";
    } else {
        if (fdbc->ent->ixnum >= 0) {
            /* for sql based remote access, use table name, not index name */
            tableName = fdbc->ent->tbl->name;

            orderDesc = sqlite3DescribeIndexOrder(
                sqlitedb, fdbc->ent->name, fdbc->ent->tbl->fdb->dbname, m,
                ncols, &hasCondition, &columnsDesc, bias, pCur->is_equality,
                pCur->col_mask);

            if (orderDesc == NULL) {
                logmsg(LOGMSG_ERROR, 
                        "%s: Failed to get order from sqlite, broken engine!\n",
                        __func__);
                *error = 1;
                return NULL;
            }

            using_col_filter = 1;
        } else {
            tableName = fdbc->ent->name;
        }
    }

    if (fdbc->hint) {
        whereDesc = sqlite3ExprDescribeAtRuntime(pCur->vdbe, fdbc->hint);
        if (!whereDesc) {
            /* unsupported hinting, there is a lower level catch for that */
        }
    }

    if (whereDesc || hasCondition) {
        sql = sqlite3_mprintf("SELECT %s%srowid FROM \"%w\" WHERE %s%s%s",
                 (columnsDesc) ? columnsDesc : ((using_col_filter) ? "" : "*"),
                 (columnsDesc) ? ", " : ((using_col_filter) ? "" : ", "),
                 tableName, whereDesc ? whereDesc : "",
                 (whereDesc != NULL && hasCondition) ? " AND " : "",
                 orderDesc ? orderDesc : "");
    } else {
        sql = sqlite3_mprintf("SELECT %s%srowid FROM \"%w\"%s",
                 (columnsDesc) ? columnsDesc : ((using_col_filter) ? "" : "*"),
                 (columnsDesc) ? ", " : ((using_col_filter) ? "" : ", "),
                 tableName, orderDesc ? orderDesc : "");
    }

    if (!sql) {
        logmsg(LOGMSG_ERROR, "%s: sqlite3_mprintf error\n", __func__);
        goto done;
    }

    /* lets get the actual size here
     *p_sqllen = sqllen;
    */
    *p_sqllen = strlen(sql) + 1;

done:
    if (columnsDesc) {
        sqlite3_free(columnsDesc);
    }

    if (whereDesc) {
        sqlite3_free(whereDesc);
    }

    if (orderDesc) {
        sqlite3_free(orderDesc);
    }

    if (gbl_fdb_track)
        logmsg(LOGMSG_USER, "Build \"%s\"\n", sql);

    return sql;
}


static char *fdb_cursor_get_data(BtCursor *pCur)
{
    assert(pCur->fdbc != NULL);

    if (gbl_fdb_track) {
        int len = fdb_msg_datalen(pCur->fdbc->impl->msg);
        logmsg(LOGMSG_USER, "XXXX: get data %d [", len);
        fsnapf(stderr, fdb_msg_data(pCur->fdbc->impl->msg), len);
        logmsg(LOGMSG_USER, "]\n");
    }
    return fdb_msg_data(pCur->fdbc->impl->msg);
}

static int fdb_cursor_get_datalen(BtCursor *pCur)
{
    assert(pCur->fdbc != NULL);

    if (gbl_fdb_track) {
        logmsg(LOGMSG_USER, "XXXX: get datalen %d\n",
                fdb_msg_datalen(pCur->fdbc->impl->msg));
    }
    return fdb_msg_datalen(pCur->fdbc->impl->msg);
}

static unsigned long long fdb_cursor_get_genid(BtCursor *pCur)
{
    assert(pCur->fdbc != NULL);

    if (gbl_fdb_track) {
        logmsg(LOGMSG_USER, "XXXX: get genid %llx\n",
                fdb_msg_genid(pCur->fdbc->impl->msg));
    }
    return fdb_msg_genid(pCur->fdbc->impl->msg);
}

static void fdb_cursor_get_found_data(BtCursor *pCur, unsigned long long *genid,
                                      int *datalen, char **data)
{
    fdb_cursor_t *cur;

    assert(pCur->fdbc != NULL);

    cur = pCur->fdbc->impl;

#if 0 
   assert(cur->msg.hd.type == FDB_MSG_DATA_ROW);
   assert(cur->msg.dr.rc == IX_FND || cur->msg.dr.rc == IX_FNDMORE || cur->msg.dr.rc == IX_NOTFND);
#endif

    *genid = fdb_msg_genid(cur->msg);
    *datalen = fdb_msg_datalen(cur->msg);
    *data = fdb_msg_data(cur->msg);

    if (gbl_fdb_track) {
        unsigned long long t = osql_log_time();
        logmsg(LOGMSG_USER, "XXXX: %llu get found data genid=%llx len=%d [", t,
                *genid, *datalen);
        fsnapf(stderr, *data, *datalen);
        logmsg(LOGMSG_USER, "]\n");
    }
}

static char *fdb_cursor_id(BtCursor *pCur)
{
    assert(pCur->fdbc);

    return pCur->fdbc->impl->cid;
}

static int fdb_serialize_key(BtCursor *pCur, Mem *key, int nfields)
{
    int fnum = 0;
    u32 type = 0;
    int sz;
    int datasz, hdrsz;
#ifndef NDEBUG
    int remainingsz;
#endif
    char *dtabuf;
    char *hdrbuf;
    u32 len;

    datasz = 0;
    hdrsz = 0;
    for (fnum = 0; fnum < nfields; fnum++) {
        type =
            sqlite3VdbeSerialType(&key[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len);
        sz = sqlite3VdbeSerialTypeLen(type);
        datasz += sz;
        hdrsz += sqlite3VarintLen(type);
        /*fprintf( stderr, "%s:%d type=%d size=%d datasz=%d hdrsz=%d\n",
          __FILE__, __LINE__, type, sz, datasz, hdrsz);*/
    }

    /* to account for size of header in header */
    hdrsz += sqlite3VarintLen(hdrsz);
    /*
       fprintf( stderr, "%s:%d hdrsz=%d ncols=%d maxout=%d\n",
       __FILE__, __LINE__, hdrsz, ncols, maxout);*/

    /* enough room? */
    if ((datasz + hdrsz) != pCur->keybuf_alloc) {
        pCur->keybuf = (char *)realloc(pCur->keybuf, datasz + hdrsz);

        if (!pCur->keybuf) {
            logmsg(LOGMSG_ERROR, "%s: failed to allocate buffer %d bytes\n",
                    __func__, datasz + hdrsz);
            return FDB_ERR_MALLOC;
        }
        pCur->keybuf_alloc = datasz + hdrsz;
    }

    hdrbuf = pCur->keybuf;
    dtabuf = pCur->keybuf + hdrsz;

    /* put header size in header */
    sz = sqlite3PutVarint((unsigned char *)hdrbuf, hdrsz);
    hdrbuf += sz;

    /* keep track of the size remaining */
#ifndef NDEBUG
    remainingsz = datasz;
#endif

    for (fnum = 0; fnum < nfields; fnum++) {
        type =
            sqlite3VdbeSerialType(&key[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len);
        sz = sqlite3VdbeSerialPut((unsigned char *)dtabuf, &key[fnum], type);
        dtabuf += sz;
#ifndef NDEBUG
        remainingsz -= sz;
#endif
        sz =
            sqlite3PutVarint((unsigned char *)hdrbuf,
                             sqlite3VdbeSerialType(
                                 &key[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len));
        hdrbuf += sz;
    }

    pCur->keybuflen = hdrsz + datasz;

#ifndef NDEBUG
    assert(remainingsz == 0);
#endif

    return FDB_NOERR;
}

static int fdb_cursor_set_hint(BtCursor *pCur, void *hint)
{
    assert(pCur->fdbc);
    pCur->fdbc->impl->hint = hint;

    return 0;
}

static void *fdb_cursor_get_hint(BtCursor *pCur)
{
    assert(pCur->fdbc);

    return pCur->fdbc->impl->hint;
}

static int fdb_cursor_reopen(BtCursor *pCur)
{
    struct sql_thread *thd;
    struct sqlclntstate *clnt;
    int rc;
    fdb_tran_t *tran;
    int need_ssl = 0;
    char *sql_hint;

    thd = pthread_getspecific(query_info_key);

    if (!thd) {
        return FDB_ERR_BUG;
    }

    clnt = thd->clnt;
    tran = pCur->fdbc->impl->trans;
    need_ssl = pCur->fdbc->impl->need_ssl;

    if (tran)
        Pthread_mutex_lock(&clnt->dtran_mtx);

    /* preserve the hint */
    sql_hint = pCur->fdbc->impl->sql_hint;

    rc = pCur->fdbc->close(pCur);
    if (rc) {
        /*rc = -1;*/
        goto done;
    }

    pCur->fdbc = fdb_cursor_open(clnt, pCur, pCur->rootpage, tran, &pCur->ixnum,
                                 need_ssl);
    if (!pCur->fdbc) {
        rc = clnt->fdb_state.xerr.errval;
        goto done;
    }

    pCur->fdbc->impl->sql_hint = sql_hint;

done:
    if (tran)
        Pthread_mutex_unlock(&clnt->dtran_mtx);

    return rc;
}

static void _update_fdb_version(BtCursor *pCur, char *errstr)
{
    /* extract protocol number */
    unsigned int protocol_version;

    protocol_version = atoll(errstr);

    logmsg(LOGMSG_ERROR,
           "%s: remote db %s requires protocol "
           "version %d, downgrading from %d\n",
           __func__, pCur->bt->fdb->dbname, protocol_version,
           pCur->bt->fdb->server_version);

    pCur->bt->fdb->server_version = protocol_version;
    /* this socket is possible dirty, because the old version
     * is detected in the first replay; but we have probably sent
     * async requests already to the target.
     *
     * we cannot close and cache it in sockpool;
     * to make sure it is not cached, we mark the socket as
     * "in-the-middle-of-streaming-rows"
     */
    pCur->fdbc->impl->streaming = FDB_CUR_STREAMING;
}

/* implement a slow_start like scheme for polling
 * (tcp slow_start for congestion windows).
 * in this case, we retry immediately a few times to
 * minimize latency, and back-off exponentially if
 * previous fails still
 * returns 1 to retry, 0 to stop
 */
static int _fdb_io_retry(int *pretry, int *pollms)
{
    int retry = *pretry;

    if (retry >= gbl_fdb_io_error_retries)
        return 0;

    if (retry < gbl_fdb_io_error_retries_phase_1) {
        (*pretry)++;
        return 1;
    }
    poll(NULL, 0, *pollms);
    *pollms *= 2;
    (*pretry)++;
    return 1;
}

static int fdb_cursor_move_sql(BtCursor *pCur, int how)
{
    fdb_cursor_t *fdbc = pCur->fdbc->impl;
    sqlclntstate_fdb_t *state = pCur->clnt ? &pCur->clnt->fdb_state : NULL;
    int rc = 0;
    enum run_sql_flags flags = FDB_RUN_SQL_NORMAL;
    unsigned long long start_rpc;
    unsigned long long end_rpc;
    int retry = 0;
    int pollms = gbl_fdb_io_error_retries_phase_2_poll;

    int no_version_retry = how & NORETRY;
    how &= 0x0F;

    if (fdbc) {
retry:
        start_rpc = osql_log_time();

        /* this is a rewind, lets make sure the pipe is clean */
        if ((how == CFIRST || how == CLAST) &&
            (fdbc->streaming != FDB_CUR_IDLE)) {
        version_retry:
            rc = fdb_cursor_reopen(pCur);
            if (rc || !pCur->fdbc /*did we fail to pass error back */) {
                logmsg(LOGMSG_ERROR, "%s: failed to reconnect rc=%d\n", __func__,
                        rc);
                return rc;
            }
            fdbc = pCur->fdbc->impl;
        }

        /* if absolute move, send new query */
        if (how == CFIRST || how == CLAST) {
            int sqllen;
            char *sql;
            int error = 0;

            if (fdbc->sql_hint) {
                /* prepackaged hints */
                sql = fdbc->sql_hint;
                sqllen = strlen(sql) + 1;
                /* for now, this is used only by remote schema retrieval
                in check_table_fdb */
                if (fdbc->is_schema) {
                    flags = FDB_RUN_SQL_SCHEMA;
                }
            } else {
                sql = _build_run_sql_from_hint(
                    pCur, NULL, 0, (how == CLAST) ? OP_Prev : OP_Next, &sqllen,
                    &error);
            }

            if (!sql) {
                if (error)
                    return FDB_ERR_INDEX_DESCRIBE;
                return FDB_ERR_MALLOC;
            }

            rc = fdb_send_run_sql(
                fdbc->msg, fdbc->cid, sqllen, sql,
                (fdbc->ent) ? fdb_table_version(fdbc->ent->tbl->version) : 0, 0,
                NULL, flags, fdbc->fcon.sock.sb);

            if (fdbc->sql_hint != sql) {
                sqlite3_free(sql);
            }
        }

        if (!rc) {
            /* otherwise.read row */
            rc = fdb_recv_row(fdbc->msg, fdbc->cid, fdbc->fcon.sock.sb);

            if (rc != IX_FND && rc != IX_FNDMORE && rc != IX_NOTFND &&
                rc != IX_PASTEOF && rc != IX_EMPTY) {
                char *errstr = fdbc->intf->data(pCur);

                /* sqlite will call reprepare; we need to mark which remote
                 * table cache is stale */
                if (rc == SQLITE_SCHEMA) {
                    unsigned long long remote_version;

                    if (unlikely(!errstr))
                        abort();

                    remote_version = atoll(errstr);

                    logmsg(LOGMSG_ERROR, 
                            "%s: local version %llu is stale, need %llu \"%s\"\n",
                            __func__, fdbc->ent->tbl->version, remote_version,
                        errstr);

                    /* this is just a hint; updated in parallel by possible
                       multiple sql engines, maybe with different
                       values if the remote table is schema changed repeatedly
                       */
                    fdbc->ent->tbl->need_version = remote_version + 1;

                    rc = SQLITE_SCHEMA_REMOTE;
                } else if (rc == FDB_ERR_FDB_VERSION) {
                    _update_fdb_version(pCur, errstr);

                    if (!no_version_retry && (how == CFIRST || how == CLAST)) {
                        no_version_retry = 1;
                        goto version_retry;
                    }
                } else if (rc == FDB_ERR_SSL) {
                    /* extract ssl config */
                    unsigned int ssl_cfg;

                    ssl_cfg = atoll(errstr);

                    logmsg(LOGMSG_INFO, "%s: remote db %s needs ssl %d\n",
                           __func__, pCur->bt->fdb->dbname, ssl_cfg);
                    pCur->bt->fdb->ssl = ssl_cfg;
                } else if (rc == FDB_ERR_READ_IO &&
                           (how == CFIRST || how == CLAST) &&
                           !pCur->clnt->intrans) {
                    /* I/O error. Let's retry the query on some other node by
                     * temporarily blacklisting this node (only when we haven't
                     * read any rows and not in a transaction). */
                    fdbc->streaming = FDB_CUR_ERROR;
                    _fdb_set_affinity_node(pCur->clnt, pCur->bt->fdb,
                                           fdbc->node, FDB_ERR_TRANSIENT_IO);
                    if (gbl_fdb_track)
                        logmsg(LOGMSG_USER,
                               "%s:%d blacklisting %s, retrying..\n", __func__,
                               __LINE__, fdbc->node);
                    if (_fdb_io_retry(&retry, &pollms))
                        goto retry;
                    logmsg(LOGMSG_ERROR,
                           "%s:%d failed to reconnect after %d retries\n",
                           __func__, __LINE__, retry);
                } else {
                    if (rc != FDB_ERR_SSL) {
                        if (state) {
                            state->preserve_err = 1;
                            errstat_set_rc(&state->xerr, FDB_ERR_READ_IO);
                            errstat_set_str(&state->xerr,
                                            errstr ? errstr
                                                   : "error string not set");
                        }
                        logmsg(LOGMSG_ERROR,
                               "%s: failed to retrieve streaming "
                               "row rc=%d \"%s\"\n",
                               __func__, rc,
                               errstr ? errstr : "error string not set");
                        fdbc->streaming = FDB_CUR_ERROR;
                    }
                }

                return rc;
            } else {
                fdbc->streaming =
                    (rc == IX_FNDMORE) ? FDB_CUR_STREAMING : FDB_CUR_IDLE;
            }
        }

        end_rpc = osql_log_time();

        fdb_add_remote_time(pCur, start_rpc, end_rpc);
    } else {
        logmsg(LOGMSG_ERROR, "%s: no fdbc cursor?\n", __func__);
        rc = FDB_ERR_MALLOC;
    }

    return rc;
}

static int fdb_cursor_find_sql_common(BtCursor *pCur, Mem *key, int nfields,
                                      int bias, int last)
{
    /* NOTE: assumption we make here is that the hint should contain all the
       fields that
       determine a certain find operation; recreating that string and passing it
       to the
       remote engine should generate the same plan (avoiding the need to reverse
       engineer
       a where clause from a find/find_last + a followup move)
     */

    fdb_cursor_t *fdbc = pCur->fdbc->impl;
    sqlclntstate_fdb_t *state = pCur->clnt ? &pCur->clnt->fdb_state : NULL;
    int rc = 0;
    char *packed_key = NULL;
    int packed_keylen = 0;
    unsigned long long start_rpc;
    unsigned long long end_rpc;
    int no_version_retry = 0;
    int retry = 0;
    int pollms = gbl_fdb_io_error_retries_phase_2_poll;

    if (fdbc) {
        int sqllen;
        char *sql;
        int error = 0;

retry:
        /* this is a rewind, lets make sure the pipe is clean */
        if (fdbc->streaming != FDB_CUR_IDLE) {
version_retry:
            rc = fdb_cursor_reopen(pCur);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: failed to reconnect rc=%d\n", __func__,
                        rc);
                return rc;
            }
            fdbc = pCur->fdbc->impl;
        }

        if (pCur->ixnum == -1) {
            if (bias != OP_NotExists && bias != OP_SeekRowid && bias != OP_DeferredSeek) {
                logmsg(LOGMSG_FATAL, "%s: not supported op %d\n", __func__, bias);
                abort();
            }

            sql = sqlite3_mprintf("select *, rowid from \"%w\" "
                                  "where rowid = %lld",
                                  fdbc->ent->tbl->name, key->u.i);
            sqllen = strlen(sql) + 1;
        } else {
            if (fdbc->sql_hint) {
                /* prepackaged hints */
                sql = fdbc->sql_hint;
                sqllen = strlen(sql) + 1;
            } else {
                sql = _build_run_sql_from_hint(pCur, key, nfields, bias,
                                               &sqllen, &error);
            }
        }

        if (!sql) {
            if (error)
                return FDB_ERR_INDEX_DESCRIBE;
            return FDB_ERR_MALLOC;
        }

        start_rpc = osql_log_time();

        rc = fdb_send_run_sql(
            fdbc->msg, fdbc->cid, sqllen, sql,
            (fdbc->ent) ? fdb_table_version(fdbc->ent->tbl->version) : 0,
            packed_keylen, packed_key, FDB_RUN_SQL_TRIM,
            fdbc->fcon.sock.sb);

        if (fdbc->sql_hint != sql) {
            sqlite3_free(sql);
        }

        if (!rc) {
            /* otherwise.read row */
            rc = fdb_recv_row(fdbc->msg, fdbc->cid, fdbc->fcon.sock.sb);

            if (rc != IX_FND && rc != IX_FNDMORE && rc != IX_NOTFND &&
                rc != IX_PASTEOF && rc != IX_EMPTY) {
                char *errstr = fdbc->intf->data(pCur);

                /* sqlite will call reprepare; we need to mark which remote
                 * table cache is stale */
                if (rc == SQLITE_SCHEMA) {
                    unsigned long long remote_version;

                    if (unlikely(!errstr))
                        abort();

                    remote_version = atoll(errstr);

                    logmsg(LOGMSG_ERROR, 
                        "%s: local version %llu is stale, need %llu \"%s\"\n",
                        __func__, fdbc->ent->tbl->version, remote_version,
                        errstr);

                    /* this is just a hint; updated in parallel by possible
                       multiple sql engines, maybe with different
                       values if the remote table is schema changed repeatedly
                       */
                    fdbc->ent->tbl->need_version = remote_version + 1;

                    rc = SQLITE_SCHEMA_REMOTE;
                    rc = SQLITE_SCHEMA_REMOTE;
                } else if (rc == FDB_ERR_FDB_VERSION) {
                    _update_fdb_version(pCur, errstr);

                    if (!no_version_retry) {
                        no_version_retry = 1;
                        goto version_retry;
                    }
                } else if (rc == FDB_ERR_READ_IO &&
                           !pCur->clnt->intrans) {
                    /* I/O error. Let's retry the query on some other node by
                     * temporarily blacklisting this node (only when we haven't
                     * read any rows and not in a transaction). */
                    fdbc->streaming = FDB_CUR_ERROR;
                    _fdb_set_affinity_node(pCur->clnt, pCur->bt->fdb,
                                           fdbc->node, FDB_ERR_TRANSIENT_IO);
                    if (gbl_fdb_track)
                        logmsg(LOGMSG_USER,
                               "%s:%d blacklisting %s, retrying..\n", __func__,
                               __LINE__, fdbc->node);
                    if (_fdb_io_retry(&retry, &pollms))
                        goto retry;
                    logmsg(LOGMSG_ERROR,
                           "%s:%d failed to reconnect after %d retries\n",
                           __func__, __LINE__, retry);
                } else {
                    if (rc != FDB_ERR_SSL) {
                        if (state) {
                            state->preserve_err = 1;
                            errstat_set_rc(&state->xerr, FDB_ERR_READ_IO);
                            errstat_set_str(&state->xerr,
                                            errstr ? errstr
                                                   : "error string not set");
                        }
                        logmsg(LOGMSG_ERROR,
                               "%s: failed to retrieve streaming"
                               " row rc=%d \"%s\"\n",
                               __func__, rc,
                               errstr ? errstr : "error string not set");
                        fdbc->streaming = FDB_CUR_ERROR;
                    }
                }

                return rc;
            } else {
                fdbc->streaming =
                    (rc == IX_FNDMORE) ? FDB_CUR_STREAMING : FDB_CUR_IDLE;
            }

            /* if we don't get a row here, it means the concocted sql did not
               match any rows;
               sqlite expect some row nevertheless unless empty;  So we give it
               empty, gorge yourself on thus rows
             */
            if (rc == IX_NOTFND) {
                rc = IX_EMPTY;
            }
            if (rc == IX_FNDMORE) {
                fdbc->streaming = FDB_CUR_STREAMING;
            }
        }

        end_rpc = osql_log_time();

        /*fprintf(stderr, "start=%llu end=%llu RC=%d\n", start_rpc, end_rpc,
         * rc);*/
        fdb_add_remote_time(pCur, start_rpc, end_rpc);
    } else {
        logmsg(LOGMSG_ERROR, "%s: no fdbc cursor?\n", __func__);
        rc = FDB_ERR_BUG;
    }

    return rc;
}

static int fdb_cursor_find_sql(BtCursor *pCur, Mem *key, int nfields, int bias)
{
    return fdb_cursor_find_sql_common(pCur, key, nfields, bias, 0);
}

static int fdb_cursor_find_last_sql(BtCursor *pCur, Mem *key, int nfields,
                                    int bias)
{
    return fdb_cursor_find_sql_common(pCur, key, nfields, bias, 1);
}

/*
   This returns the sqlstats table under a mutex
 */
fdb_sqlstat_cache_t *fdb_sqlstats_get(fdb_t *fdb)
{
    int rc = 0;
    struct sql_thread *thd;
    struct sqlclntstate *clnt;
    int interval = bdb_attr_get(thedb->bdb_attr,
                                BDB_ATTR_FDB_SQLSTATS_CACHE_LOCK_WAITTIME_NSEC);
    if (!interval)
        interval = 100;

    /* this should be an sql thread */
    thd = pthread_getspecific(query_info_key);
    if (!thd) return NULL;

    clnt = thd->clnt;

    /* remote sql stats are implemented as a critical region
       I was told that mutex is faster, lul
       We need to allow bdb lock to recover if we keep waiting
     */
    do {
#       ifdef __APPLE__
        rc = pthread_mutex_trylock(&fdb->sqlstats_mtx);
        if (rc == EBUSY) {
            poll(NULL, 0, 10);
            rc = ETIMEDOUT;
        }
#       else
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += interval;
        if (ts.tv_nsec >= 1000000000) {
            ++ts.tv_sec;
            ts.tv_nsec -= 1000000000;
        }
        rc = pthread_mutex_timedlock(&fdb->sqlstats_mtx, &ts);
#       endif
        if (rc) {
            if (rc == ETIMEDOUT) {
                int irc = clnt_check_bdb_lock_desired(clnt);
                if (irc) {
                    logmsg(LOGMSG_ERROR, "%s: recover_deadlock returned %d\n",
                           __func__, irc);
                    return NULL;
                }
                continue;
            }

            logmsg(LOGMSG_ERROR, "%s: pthread_mutex_timedlock failed with rc=%d\n",
                    __func__, rc);
            return NULL;
        } else {
            break; /* got the lock */
        }
    } while (1);

    if (fdb->sqlstats == NULL) {
        /* create them */
        rc = fdb_sqlstat_cache_create(clnt, fdb, fdb->dbname, &fdb->sqlstats);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to create cache rc=%d\n", __func__, rc);
            fdb->sqlstats = NULL;
            fdb_sqlstats_put(fdb);
        }
    }

    return fdb->sqlstats;
}

void fdb_sqlstats_put(fdb_t *fdb)
{
    Pthread_mutex_unlock(&fdb->sqlstats_mtx);
}

static int fdb_cursor_set_sql(BtCursor *pCur, const char *sql)
{
    assert(pCur->fdbc);
    pCur->fdbc->impl->sql_hint = (char *)sql;

    return 0;
}

static char *fdb_cursor_name(BtCursor *pCur)
{
    assert(pCur->fdbc);

    if (!pCur->fdbc->impl->ent)
        return "sqlite_master";

    return pCur->fdbc->impl->ent->name;
}

static char *fdb_cursor_tblname(BtCursor *pCur)
{
    assert(pCur->fdbc);

    if (!pCur->fdbc->impl->ent)
        return "sqlite_master";

    return pCur->fdbc->impl->ent->tbl->name;
}

static int fdb_cursor_table_has_partidx(BtCursor *pCur)
{
    assert(pCur->fdbc);

    if (!pCur->fdbc->impl->ent)
        return 0;

    return pCur->fdbc->impl->ent->tbl->ix_partial;
}

static int fdb_cursor_table_has_expridx(BtCursor *pCur)
{
    assert(pCur->fdbc);

    if (!pCur->fdbc->impl->ent)
        return 0;

    return pCur->fdbc->impl->ent->tbl->ix_expr;
}

static char *fdb_cursor_dbname(BtCursor *pCur)
{
    assert(pCur->fdbc);

    if (!pCur->fdbc->impl->ent)
        return pCur->bt->fdb->dbname;

    return pCur->fdbc->impl->ent->tbl->fdb->dbname;
}

static fdb_tbl_ent_t *fdb_cursor_table_entry(BtCursor *pCur)
{
    assert(pCur->fdbc);

    return pCur->fdbc->impl->ent;
}

const char *fdb_parse_comdb2_remote_dbname(const char *zDatabase,
                                           const char **fqDbname)
{
    const char *dbName;
    const char *temp_dbname = "temp";
    const char *local_dbname = "main";

    if (!zDatabase) {
        *fqDbname = NULL;
        return NULL;
    }

    dbName = zDatabase;

    if ((strcasecmp(zDatabase, temp_dbname) == 0)) {
        dbName = temp_dbname;
    }
    /* extract location hint, if any */
    else if ((*fqDbname = strchr(dbName, '_')) != NULL) {
        dbName = (++(*fqDbname));
        *fqDbname = zDatabase;
    } else {
        *fqDbname = zDatabase;
    }

    /* NOTE: _ notation is invalidated if dbname is the same as local */
    if (strcasecmp(thedb->envname, dbName) == 0) /* local name */
    {
        dbName = local_dbname;
        *fqDbname = NULL;
    }

    return dbName;
}

/**
 * Get dbname, tablename, and so on
 *
 */
const char *fdb_dbname_name(fdb_t *fdb) { return fdb->dbname; }
const char *fdb_dbname_class_routing(fdb_t *fdb)
{
    if (fdb->local)
        return "LOCAL";
    return mach_class_class2name(fdb->class);
}
const char *fdb_table_entry_tblname(fdb_tbl_ent_t *ent)
{
    return ent->tbl->name;
}
const char *fdb_table_entry_dbname(fdb_tbl_ent_t *ent)
{
    return ent->tbl->fdb->dbname;
}

static fdb_tran_t *fdb_get_subtran(fdb_distributed_tran_t *dtran, fdb_t *fdb)
{
    fdb_tran_t *tran;

    LISTC_FOR_EACH(&dtran->fdb_trans, tran, lnk)
    {
        if (tran->fdb == fdb) {
            return tran;
        }
    }
    return NULL;
}

static inline char *_get_tblname(fdb_cursor_t *fdbc)
{
    return (fdbc->ent->tbl->fdb->server_version >= FDB_VER_WR_NAMES)
               ? strdup(fdbc->ent->tbl->name)
               : NULL;
}

static int fdb_cursor_insert(BtCursor *pCur, struct sqlclntstate *clnt,
                             fdb_tran_t *trans, unsigned long long genid,
                             int datalen, char *data)
{
    fdb_cursor_t *fdbc = pCur->fdbc->impl;
    int rc;
    int ixnum;
    char *tblname = _get_tblname(fdbc);

    if (gbl_fdb_track) {
        uuidstr_t ciduuid;
        uuidstr_t tiduuid;
        logmsg(LOGMSG_USER,
               "Cursor %s: INSERT for transaction %s genid=%llx "
               "seq=%d %s%s\n",
               comdb2uuidstr((unsigned char *)fdbc->cid, ciduuid),
               comdb2uuidstr((unsigned char *)trans->tid, tiduuid), genid,
               trans->seq, (tblname) ? "tblname=" : "",
               (tblname) ? tblname : "");
    }

    if (gbl_expressions_indexes && pCur->fdbc->tbl_has_expridx(pCur)) {
        for (ixnum = 0; ixnum < fdbc->ent->tbl->nix; ixnum++) {
            if (gbl_partial_indexes && pCur->fdbc->tbl_has_partidx(pCur) &&
                !(clnt->ins_keys & (1ULL << ixnum)))
                continue;
            rc = fdb_send_index(fdbc->msg, fdbc->cid, fdbc->ent->tbl->version,
                                fdbc->ent->source_rootpage, genid, 0, ixnum,
                                *((int *)clnt->idxInsert[ixnum]),
                                (char *)clnt->idxInsert[ixnum] + sizeof(int),
                                trans->seq, trans->sb);
            if (rc)
                return rc;
        }
    }

    rc = fdb_send_insert(
        fdbc->msg, fdbc->cid, fdbc->ent->tbl->version,
        fdbc->ent->source_rootpage, tblname, genid,
        (gbl_partial_indexes && pCur->fdbc->tbl_has_partidx(pCur))
            ? clnt->ins_keys
            : -1ULL,
        datalen, data, trans->seq, trans->sb);

    trans->seq++;
    trans->nwrites++;

    return rc;
}

static int fdb_cursor_delete(BtCursor *pCur, struct sqlclntstate *clnt,
                             fdb_tran_t *trans, unsigned long long genid)
{
    fdb_cursor_t *fdbc = pCur->fdbc->impl;
    int rc;
    int ixnum;
    char *tblname = _get_tblname(fdbc);

    if (gbl_fdb_track) {
        uuidstr_t ciduuid;
        uuidstr_t tiduuid;
        logmsg(LOGMSG_USER,
               "Cursor %s: DELETE for transaction %s genid=%llx "
               "seq=%d %s%s\n",
               comdb2uuidstr((unsigned char *)fdbc->cid, ciduuid),
               comdb2uuidstr((unsigned char *)trans->tid, tiduuid), genid,
               trans->seq, (tblname) ? "tblname=" : "",
               (tblname) ? tblname : "");
    }

    if (gbl_expressions_indexes && pCur->fdbc->tbl_has_expridx(pCur)) {
        for (ixnum = 0; ixnum < fdbc->ent->tbl->nix; ixnum++) {
            if (gbl_partial_indexes && pCur->fdbc->tbl_has_partidx(pCur) &&
                !(clnt->del_keys & (1ULL << ixnum)))
                continue;
            rc = fdb_send_index(fdbc->msg, fdbc->cid, fdbc->ent->tbl->version,
                                fdbc->ent->source_rootpage, genid, 1, ixnum,
                                *((int *)clnt->idxDelete[ixnum]),
                                (char *)clnt->idxDelete[ixnum] + sizeof(int),
                                trans->seq, trans->sb);
            if (rc)
                return rc;
        }
    }

    rc = fdb_send_delete(
        fdbc->msg, fdbc->cid, fdbc->ent->tbl->version,
        fdbc->ent->source_rootpage, tblname, genid,
        (gbl_partial_indexes && pCur->fdbc->tbl_has_partidx(pCur))
            ? clnt->del_keys
            : -1ULL,
        trans->seq, trans->sb);

    trans->seq++;
    trans->nwrites++;

    if (rc == 0) {
        rc = fdb_set_genid_deleted(trans, genid);
        if (rc != 0)
            logmsg(LOGMSG_ERROR, "%s: error marking genid deleted, rc %d\n", __func__, rc);
    }

    return rc;
}

static int fdb_cursor_update(BtCursor *pCur, struct sqlclntstate *clnt,
                             fdb_tran_t *trans, unsigned long long oldgenid,
                             unsigned long long genid, int datalen, char *data)
{
    fdb_cursor_t *fdbc = pCur->fdbc->impl;
    int rc;
    int ixnum;
    char *tblname = _get_tblname(fdbc);

    if (gbl_fdb_track) {
        uuidstr_t ciduuid;
        uuidstr_t tiduuid;
        logmsg(LOGMSG_USER, "Cursor %s: UPDATE for transaction %s "
               "oldgenid=%llx to genid=%llx seq=%d %s%s\n",
               comdb2uuidstr((unsigned char *)fdbc->cid, ciduuid),
               comdb2uuidstr((unsigned char *)trans->tid, tiduuid), genid,
               oldgenid, trans->seq, (tblname) ? "tblname=" : "",
               (tblname) ? tblname : "");
    }

    if (gbl_expressions_indexes && pCur->fdbc->tbl_has_expridx(pCur)) {
        for (ixnum = 0; ixnum < fdbc->ent->tbl->nix; ixnum++) {
            if (gbl_partial_indexes && pCur->fdbc->tbl_has_partidx(pCur) &&
                !(clnt->del_keys & (1ULL << ixnum)))
                goto skip;
            rc = fdb_send_index(fdbc->msg, fdbc->cid, fdbc->ent->tbl->version,
                                fdbc->ent->source_rootpage, oldgenid, 1, ixnum,
                                *((int *)clnt->idxDelete[ixnum]),
                                (char *)clnt->idxDelete[ixnum] + sizeof(int),
                                trans->seq, trans->sb);
            if (rc)
                return rc;

        skip:
            if (gbl_partial_indexes && pCur->fdbc->tbl_has_partidx(pCur) &&
                !(clnt->ins_keys & (1ULL << ixnum)))
                continue;
            rc = fdb_send_index(fdbc->msg, fdbc->cid, fdbc->ent->tbl->version,
                                fdbc->ent->source_rootpage, genid, 0, ixnum,
                                *((int *)clnt->idxInsert[ixnum]),
                                (char *)clnt->idxInsert[ixnum] + sizeof(int),
                                trans->seq, trans->sb);
            if (rc)
                return rc;
        }
    }

    rc = fdb_send_update(
        fdbc->msg, fdbc->cid, fdbc->ent->tbl->version,
        fdbc->ent->source_rootpage, tblname, oldgenid, genid,
        (gbl_partial_indexes && pCur->fdbc->tbl_has_partidx(pCur))
            ? clnt->ins_keys
            : -1ULL,
        (gbl_partial_indexes && pCur->fdbc->tbl_has_partidx(pCur))
            ? clnt->del_keys
            : -1ULL,
        datalen, data, trans->seq, trans->sb);

    trans->seq++;
    trans->nwrites++;

    if (rc == 0) {
        rc = fdb_set_genid_deleted(trans, genid);
        if (rc != 0)
            logmsg(LOGMSG_ERROR, "%s: error marking genid deleted, rc %d\n", __func__, rc);
    }

    return rc;
}

static fdb_distributed_tran_t *fdb_trans_create_dtran(struct sqlclntstate *clnt)
{
    fdb_distributed_tran_t *dtran = clnt->dbtran.dtran;

    if (dtran) {
        logmsg(LOGMSG_ERROR, "%s: bug! this looks like an nested sql transaction\n",
                __func__);
        return NULL;
    }

    dtran = clnt->dbtran.dtran =
        (fdb_distributed_tran_t *)calloc(1, sizeof(*dtran));

    if (!dtran) {
        logmsg(LOGMSG_ERROR, "%s: malloc\n", __func__);
        return NULL;
    }

    if (gbl_fdb_track)
        logmsg(LOGMSG_USER, "%s Created D-tran %p\n", __func__, dtran);

    dtran->remoted = 0;
    listc_init(&dtran->fdb_trans, offsetof(struct fdb_tran, lnk));

    return dtran;
}

static fdb_tran_t *fdb_trans_dtran_get_subtran(struct sqlclntstate *clnt,
                                               fdb_distributed_tran_t *dtran,
                                               fdb_t *fdb, int use_ssl)
{
    fdb_tran_t *tran;
    fdb_msg_t *msg;
    int rc = 0;
    uuidstr_t us;

    tran = fdb_get_subtran(dtran, fdb);

    if (!tran) {
        msg = (fdb_msg_t *)calloc(1, fdb_msg_size());
        if (!msg) {
            logmsg(LOGMSG_ERROR, "%s malloc\n", __func__);
            return NULL;
        }

        tran = (fdb_tran_t *)calloc(1, sizeof(*tran));
        if (!tran) {
            logmsg(LOGMSG_ERROR, "%s: malloc 2\n", __func__);
            free(msg);
            return NULL;
        }
        tran->tid = (char *)tran->tiduuid;
        comdb2uuid((unsigned char *)tran->tid);

        tran->fdb = fdb;

        /* NOTE: expect x_retries to fill in clnt error fields, if any */
        rc = _fdb_send_open_retries(clnt, fdb, NULL /* tran_begin */,
                                    -1 /*unused*/, tran, 0 /*flags*/,
                                    0 /*TODO: version */, msg, use_ssl);

        if (rc != FDB_NOERR || !tran->sb) {
            logmsg(LOGMSG_ERROR, "%s unable to connect to %s %s\n", __func__,
                    fdb->dbname, tran->host);
            free(tran);
            free(msg);
            return NULL;
        }

        tran->seq++;

        listc_atl(&dtran->fdb_trans, tran);
        free(msg);

        if (gbl_fdb_track) {
            logmsg(LOGMSG_USER, "%s Created tid=%s db=\"%s\"\n", __func__,
                   comdb2uuidstr((unsigned char *)tran->tid, us),
                   fdb->dbname);
        }
    } else {
        if (gbl_fdb_track) {
            uuidstr_t us;
            logmsg(LOGMSG_USER, "%s Reusing tid=%s db=\"%s\"\n", __func__,
                       comdb2uuidstr((unsigned char *)tran->tid, us),
                       fdb->dbname);
        }
    }

    return tran;
}

fdb_tran_t *fdb_trans_begin_or_join(struct sqlclntstate *clnt, fdb_t *fdb,
                                    char *ptid, int use_ssl)
{
    fdb_distributed_tran_t *dtran;
    fdb_tran_t *tran;

    Pthread_mutex_lock(&clnt->dtran_mtx);

    dtran = clnt->dbtran.dtran;
    if (!dtran) {
        dtran = fdb_trans_create_dtran(clnt);
        if (!dtran) {
            Pthread_mutex_unlock(&clnt->dtran_mtx);
            return NULL;
        }
    }

    tran = fdb_trans_dtran_get_subtran(clnt, dtran, fdb, use_ssl);
    if (tran) {
        comdb2uuidcpy((unsigned char *)ptid, (unsigned char *)tran->tid);
    }

    Pthread_mutex_unlock(&clnt->dtran_mtx);

    return tran;
}

fdb_tran_t *fdb_trans_join(struct sqlclntstate *clnt, fdb_t *fdb, char *ptid)
{
    fdb_distributed_tran_t *dtran = clnt->dbtran.dtran;
    fdb_tran_t *tran = NULL;

    if (dtran) {
        tran = fdb_get_subtran(dtran, fdb);
        if (tran) {
            comdb2uuidcpy((unsigned char *)ptid, (unsigned char *)tran->tid);
        }
    }

    return tran;
}

int fdb_trans_commit(struct sqlclntstate *clnt, enum trans_clntcomm sideeffects)
{
    fdb_distributed_tran_t *dtran = clnt->dbtran.dtran;
    fdb_tran_t *tran, *tmp;
    fdb_msg_t *msg;
    int rc = 0, bdberr;

    if (!dtran)
        return 0;

    /* nop, this is the remote part that reuses the same dbtran data structure
     */
    if (dtran->remoted == 1) {
        /* this is on remote side, the structure is different, see
         * fdb_bend_sql.c */
        fdb_svc_trans_destroy(clnt);
        return 0;
    }

    msg = (fdb_msg_t *)calloc(1, fdb_msg_size());
    if (!msg) {
        logmsg(LOGMSG_ERROR, "%s malloc\n", __func__);
        return FDB_ERR_MALLOC;
    }

    /* TODO: here we replace the trivial 2PC with the actual thing */

    Pthread_mutex_lock(&clnt->dtran_mtx);

    LISTC_FOR_EACH(&dtran->fdb_trans, tran, lnk)
    {
        /*
         * We may need to read from a remote cursor again in the next chunk
         * (for example, INSERT INTO tbl SELECT * FROM remotedb.tbl).
         * Keep such a read transaction open. The final commit of a chunk
         * transaction will call into here with a different `sideeffects'
         * flag, and that will close all remote transactions.
         */
        if (sideeffects == TRANS_CLNTCOMM_CHUNK && tran->nwrites == 0)
            continue;

        rc = fdb_send_commit(msg, tran, clnt->dbtran.mode, tran->sb);

        if (gbl_fdb_track)
            logmsg(LOGMSG_USER, "%s Send Commit tid=%llx db=\"%s\" rc=%d\n",
                    __func__, *(unsigned long long *)tran->tid,
                    tran->fdb->dbname, rc);
    }

    LISTC_FOR_EACH(&dtran->fdb_trans, tran, lnk)
    {
        if (sideeffects == TRANS_CLNTCOMM_CHUNK && tran->nwrites == 0)
            continue;

        rc = fdb_recv_rc(msg, tran);

        if (gbl_fdb_track) {
            uuidstr_t us;
            logmsg(LOGMSG_USER, "%s Commit RC=%d tid=%s db=\"%s\"\n",
                   __func__, rc,
                   comdb2uuidstr((unsigned char *)tran->tid, us),
                   tran->fdb->dbname);
        }

        if (rc) {
            /* rollback all in 2PC here */
            break;
        }
    }

    /* store distributed rc in clnt */
    if (rc) {
        bzero(&clnt->osql.xerr, sizeof(clnt->osql.xerr));
        errstat_set_rc(&clnt->osql.xerr, rc);
        if (tran->errstr) // TODO: this can be non-null even when no error
            errstat_set_str(&clnt->osql.xerr, tran->errstr);
        clnt->osql.error_is_remote = 1;
    } else {
        errstat_set_rc(&clnt->osql.xerr, 0);
        errstat_set_str(&clnt->osql.xerr, NULL);
    }

    /* free the dtran */
    LISTC_FOR_EACH_SAFE(&dtran->fdb_trans, tran, tmp, lnk)
    {
        if (sideeffects == TRANS_CLNTCOMM_CHUNK && tran->nwrites == 0)
            continue;

        listc_rfl(&dtran->fdb_trans, tran);

        if (tran->sb)
            sbuf2close(tran->sb);
        if (tran->errstr)
            free(tran->errstr);

        if (tran->dedup_tbl != NULL) {
            /* tempcursors are automatically closed in bdb_temp_table_close. */
            rc = bdb_temp_table_close(tran->bdb_state, tran->dedup_tbl, &bdberr);
            if (rc != 0)
                logmsg(LOGMSG_ERROR, "%s: error closing temptable, rc %d, bdberr %d\n", __func__, rc, bdberr);
        }

        free(tran);
    }

    /*
     * Keep the remote tran repo alive for next chunk. The final commit of
     * a chunk transaction will call into here with a different `sideeffect' flag,
     * and that will free the remote tran repo.
     */
    if (sideeffects != TRANS_CLNTCOMM_CHUNK) {
        free(clnt->dbtran.dtran);
        clnt->dbtran.dtran = NULL;
    }

    Pthread_mutex_unlock(&clnt->dtran_mtx);

    free(msg);

    return rc;
}

int fdb_trans_rollback(struct sqlclntstate *clnt)
{
    fdb_distributed_tran_t *dtran = clnt->dbtran.dtran;
    fdb_tran_t *tran, *tmp;
    fdb_msg_t *msg;
    int rc, bdberr;

    if (!dtran)
        return 0;

    /* nop, this is the remote part that reuses the same dbtran data structure
     */
    if (dtran->remoted == 1) {
        /* this is on remote side, the structure is different, see
         * fdb_bend_sql.c */
        fdb_svc_trans_destroy(clnt);
        return 0;
    }

    msg = (fdb_msg_t *)calloc(1, fdb_msg_size());
    if (!msg) {
        logmsg(LOGMSG_ERROR, "%s malloc\n", __func__);
        return FDB_ERR_MALLOC;
    }

    /* TODO: here we replace the trivial 2PC with the actual thing */

    Pthread_mutex_lock(&clnt->dtran_mtx);

    LISTC_FOR_EACH(&dtran->fdb_trans, tran, lnk)
    {
        rc = fdb_send_rollback(msg, tran, clnt->dbtran.mode, tran->sb);

        if (gbl_fdb_track)
            logmsg(LOGMSG_USER, "%s Send Commit tid=%llx db=\"%s\" rc=%d\n",
                    __func__, *(unsigned long long *)tran->tid,
                    tran->fdb->dbname, rc);

        if (rc) {
            logmsg(
                LOGMSG_ERROR,
                "%s: sending rollback to node %s tid=%llu failed with rc=%d\n",
                __func__, tran->host, *(unsigned long long *)tran->tid, rc);
        }
    }

    /* free the dtran */
    LISTC_FOR_EACH_SAFE(&dtran->fdb_trans, tran, tmp, lnk)
    {
        listc_rfl(&dtran->fdb_trans, tran);

        if (tran->sb)
            sbuf2close(tran->sb);
        if (tran->errstr)
            free(tran->errstr);

        if (tran->dedup_tbl != NULL) {
            /* tempcursors are automatically closed in bdb_temp_table_close. */
            rc = bdb_temp_table_close(tran->bdb_state, tran->dedup_tbl, &bdberr);
            if (rc != 0)
                logmsg(LOGMSG_ERROR, "%s: error closing temptable, rc %d, bdberr %d\n", __func__, rc, bdberr);
        }

        free(tran);
    }
    free(clnt->dbtran.dtran);
    clnt->dbtran.dtran = NULL;

    Pthread_mutex_unlock(&clnt->dtran_mtx);

    free(msg);

    return 0;
}

char *fdb_trans_id(fdb_tran_t *trans) { return trans->tid; }

int fdb_is_sqlite_stat(fdb_t *fdb, int rootpage)
{
    fdb_tbl_ent_t *ent;

    ent = get_fdb_tbl_ent_by_rootpage_from_fdb(fdb, rootpage);
    if (!ent)
        return 1;

    return strncasecmp(ent->tbl->name, "sqlite_stat", strlen("sqlite_stat")) ==
           0;
}

char *fdb_get_alias(const char **p_tablename)
{
    char *errstr = NULL;
    char *alias = NULL;
    const char *tablename = *p_tablename;
    tran_type *trans;

    trans = curtran_gettran();
    alias = llmeta_get_tablename_alias_tran(trans, tablename, &errstr);
    curtran_puttran(trans);
    if (!alias) {
        if (errstr) {
            logmsg(LOGMSG_ERROR, "%s: error retrieving fdb alias for %s\n", __func__,
                    tablename);
            free(errstr);
        }
    } else {
        char *dot = strchr(alias, '.');
        if (!dot || dot[1] == '\0') {
            logmsg(LOGMSG_ERROR, "%s bad alias %s\n", __func__, alias);
            free(alias);
            return NULL;
        }
        dot[0] = '\0';
        *p_tablename = dot + 1; /* point now to the tablename */
    }

    return alias;
}

void fdb_stat_alias(void) { llmeta_list_tablename_alias(); }

/**
* This function will check some critical regions
* hanging if something is wrong
*
*/
void fdb_sanity_check(void)
{
    /* hook for future watcher enabled checks and stats */
}

/**
 * Check access control for this cursor
 * Returns -1 if access control enabled and access denied
 *         0 otherwise
 *
 */
int fdb_cursor_access(BtCursor *pCur, int how)
{
    struct sql_thread *thd;
    struct sqlclntstate *clnt;
    int rc;
    const char *dbname;
    const char *tblname;

    if (!pCur->bt || !pCur->bt->is_remote)
        return FDB_NOERR;

    thd = pthread_getspecific(query_info_key);
    if (!thd)
        return FDB_NOERR;

    clnt = thd->clnt;
    if (!clnt)
        return FDB_NOERR;

    if (!clnt->fdb_state.access)
        return FDB_NOERR;

    if (!pCur->fdbc)
        return FDB_NOERR;

    dbname = pCur->fdbc->dbname(pCur);
    tblname = pCur->fdbc->tblname(pCur);

    rc = fdb_access_control_check(clnt->fdb_state.access, dbname, tblname, how);

    if (rc) {
        snprintf(clnt->osql.xerr.errstr, sizeof(clnt->osql.xerr.errstr),
                 "%s denied to %s.%s\n",
                 (how == ACCESS_REMOTE_READ) ? "READ" : "WRITE", dbname,
                 tblname);
    }

    return rc;
}

/**
 * Check if master table access if local or remote
 *
 */
int fdb_master_is_local(BtCursor *pCur)
{
    Btree *pBt = pCur->bt;
    /* non fdb case */
    if (!pBt || pBt->is_remote == 0)
        return 1;
    /* this is looking at a remote pBt; check if this is schema initializing
       or a remote lookup */
    if (gbl_old_column_names && pCur->clnt->thd &&
        pCur->clnt->thd->query_preparer_running) {
        /* We must have a query_preparer_plugin installed. */
        assert(pCur->query_preparer_data != 0);
        /* Since query_preparer plugin only prepares the query, the 'init.busy'
         * flag must be set at this point. */
        assert(query_preparer_plugin &&
               query_preparer_plugin->sqlitex_is_initializing &&
               query_preparer_plugin->sqlitex_is_initializing(
                   pCur->query_preparer_data));
        return 1;
    }
    return pCur->sqlite && pCur->sqlite->init.busy == 1;
}

/**
 * Internal function to remove all the ent objects for a table
 * It collects the table associated with the entry, which can
 * be an index or the actual table
 *
 */
static int __free_fdb_tbl(void *obj, void *arg)
{
    fdb_tbl_t *tbl = (fdb_tbl_t *)obj;
    fdb_t *fdb = (fdb_t *)arg;
    fdb_tbl_ent_t *ent, *tmp;

    /* check if this is a sqlite_stat table, for which stat might be present;
       if so, clear it */
    if (is_sqlite_stat(tbl->name)) {
        /* this wipes all the sqlite stats, easier; we could review and
        delete only one stat at a time */
        fdb_sqlstat_cache_destroy(&fdb->sqlstats);
    }

    /* free each entry for table */
    LISTC_FOR_EACH_SAFE(&tbl->ents, ent, tmp, lnk)
    {
        /* unlink the entry from everywhere */
        hash_del(fdb->h_ents_rootp, ent);
        hash_del(fdb->h_ents_name, ent);

        /* free this entry */
        listc_rfl(&tbl->ents, ent);
        if (ent->ent)
            free(ent->ent);
        free(ent->name);
        free(ent);
    }

    /* free table itself */
    hash_del(fdb->h_tbls_name, tbl);
    free(tbl->name);
    Pthread_mutex_destroy(&tbl->ents_mtx);
    free(tbl);

    return FDB_NOERR;
}

/**
 * Purge the schema for a specific db
 * If tbl== NULL, purge all the tables
 *
 * NOTE: caller needs to grab TAGAPI_LK !
 *
 */
static void fdb_clear_schema(const char *dbname, const char *tblname,
                             int need_update)
{
    fdb_t *fdb;
    fdb_tbl_t *tbl;
#if 0
   int         already_updated;
#endif

    /* map name to fdb */
    fdb = get_fdb(dbname);
    if (!fdb) {
        logmsg(LOGMSG_ERROR, "unknown fdb \"%s\"\n", dbname);
        return;
    }

#if 0
   /* if we are trying to update, 
      it is possible that the shared version was already updated 
    */
   already_updated = 0;
   if(need_update)
   {
      tbl = hash_find_readonly(fdb->h_tbls_name, &tblname);
      if (tbl == NULL)
      {
         fprintf(stderr, "Unknown table \"%s\" in db \"%s\"\n", tblname, dbname);
         already_updated = 1;
      }
      else if (tbl->version == tbl->need_version + 1)
      {
         if (gbl_fdb_track)
         {
            fprintf(stderr, "Table %s.%s already at version %u\n",
               dbname, tblname, tbl->version);
         }
         already_updated = 1;
      }
   }

   if (already_updated)
   {
      /* done here */
      return;
   }

   /* NOTE: lets do this during retry */
   return;
#endif

    if (__lock_wrlock_exclusive(fdb->dbname)) {
        return;
    }

    if (tblname == NULL) {
        /* all ours, lets clear the entries */
        hash_for(fdb->h_tbls_name, __free_fdb_tbl, fdb);
    } else {
        tbl = hash_find_readonly(fdb->h_tbls_name, &tblname);
        if (tbl == NULL) {
            logmsg(LOGMSG_ERROR, "Unknown table \"%s\" in db \"%s\"\n", tblname,
                    dbname);
            goto done;
        }

        if (__free_fdb_tbl(tbl, fdb)) {
            logmsg(LOGMSG_ERROR, 
                    "Error clearing schema for table \"%s\" in db \"%s\"\n",
                    tblname, dbname);
        }
    }

done:
    Pthread_rwlock_unlock(&fdb->h_rwlock);
}

/**
 * Drop sqlite stats data
 * Next access to remote data will fetch stats again before proceeding
 *
 */
static void fdb_clear_sqlite_stats(void)
{
    int rc = FDB_ERR_BUG;

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: clear sqlite_stats unimplemented\n", __func__);
    }
}

/**
 * Remove all cached information, bringing fdb cache to the initial state
 * (i.e no fdb entries and so no schemas, and no sqlite_stats data)
 *
 */
static void fdb_init(void)
{
    int rc = FDB_ERR_BUG;

    if (rc) {
        /* TODO: fatal? */
        logmsg(LOGMSG_ERROR, "%s: init unimplemented\n", __func__);
    }
}

static int __fdb_info_ent(void *obj, void *arg)
{
    fdb_tbl_ent_t *ent = (fdb_tbl_ent_t *)obj;

    if (ent->ixnum == -1) {
        logmsg(
            LOGMSG_USER,
            "Db \"%s\" Class \"%s\" Table \"%s\" Rootp %d Remrootp %d "
            "Version=%llx\n",
            ent->tbl->fdb->dbname,
            ent->tbl->fdb->local ? "local"
                                 : mach_class_class2name(ent->tbl->fdb->class),
            ent->name, ent->rootpage, ent->source_rootpage, ent->tbl->version);
    } else {
        logmsg(LOGMSG_USER,
               "Db \"%s\" Class \"%s\" Index \"%s\" for table \"%s\" Rootp %d "
               "Remrootp %d Version=%llx\n",
               ent->tbl->fdb->dbname,
               ent->tbl->fdb->local
                   ? "local"
                   : mach_class_class2name(ent->tbl->fdb->class),
               ent->name, ent->tbl->name, ent->rootpage, ent->source_rootpage,
               ent->tbl->version);
    }

    return FDB_NOERR;
}

static int __fdb_info_ent_save(void *obj, void *arg)
{
    fdb_tbl_ent_t *ent = (fdb_tbl_ent_t *)obj;
    fdb_systable_info_t *info = (fdb_systable_info_t *)arg;
    fdb_systable_ent_t *ient = &info->arr[info->narr++];

    ient->dbname = strdup(ent->tbl->fdb->dbname);
    ient->location = strdup(ent->tbl->fdb->local
                                ? "local"
                                : mach_class_class2name(ent->tbl->fdb->class));
    ient->tablename = strdup(ent->ixnum == -1 ? ent->name : ent->tbl->name);
    ient->indexname = ent->ixnum == -1 ? NULL : strdup(ent->name);
    ient->rootpage = ent->rootpage;
    ient->remoterootpage = ent->source_rootpage;
    ient->version = ent->tbl->version;

    return FDB_NOERR;
}

/**
 * Report the tables for db with their versions
 * If dbname == NULL, report all dbs
 *
 */
static void fdb_info_tables(fdb_t *fdb, fdb_systable_info_t *info)
{
    __lock_wrlock_shared(fdb);
    if (!info) {
        hash_for(fdb->h_ents_name, __fdb_info_ent, NULL);
    } else {
        int nents = fdb_num_entries(fdb);
        info->arr = realloc(info->arr,
                            sizeof(fdb_systable_ent_t) * (info->narr + nents));
        if (!info->arr) {
            logmsg(LOGMSG_ERROR,
                   "%s: unable to allocate virtual table info fdb\n", __func__);
            goto done;
        }
        hash_for(fdb->h_ents_name, __fdb_info_ent_save, info);
    }
done:
    Pthread_rwlock_unlock(&fdb->h_rwlock);
}

/**
 * Report the tables for db with their versions
 * If dbname == NULL, report all dbs
 *
 */
static void fdb_info_db(const char *dbname, fdb_systable_info_t *info)
{
    fdb_t *fdb;

    if (!dbname) {
        int i;

        Pthread_rwlock_rdlock(&fdbs.arr_lock);
        for (i = 0; i < fdbs.nused; i++) {
            fdb = fdbs.arr[i];

            if (!fdb)
                continue;

            __fdb_add_user(fdb, 1);

            fdb_info_tables(fdb, info);

            __fdb_rem_user(fdb, 1);
        }
        Pthread_rwlock_unlock(&fdbs.arr_lock);
    } else {
        fdb = get_fdb(dbname);

        if (!fdb) {
            logmsg(LOGMSG_ERROR, "fdb info db: unknown dbname \"%s\"\n", dbname);
            return;
        }

        __fdb_add_user(fdb, 1);

        fdb_info_tables(fdb, info);

        __fdb_rem_user(fdb, 1);
    }
}

/**
 * Process remote messages
 *
 */
int fdb_process_message(const char *line, int lline)
{
    int st = 0;
    int ltok = 0;
    char *tok;

    tok = segtok((char *)line, lline, &st, &ltok);
    if (ltok == 0) {
        logmsg(LOGMSG_ERROR, "fdb message error: missing command\n");
        return FDB_ERR_GENERIC;
    } else if (tokcmp(tok, ltok, "help") == 0) {
        logmsg(LOGMSG_USER, "Usage:\n"
                        "    fdb init                          = removes all "
                        "schemas and index stats\n"
                        "    fdb clear schema dbname           = removes "
                        "schema caches for all tables in db \"dbname\"\n"
                        "    fdb clear schema dbname tblname   = removes "
                        "schema cache for table \"tblname\" in db \"dbname\"\n"
                        "    fdb clear sqlite_stats            = removes "
                        "cached sqlite stats data\n"
                        "    fdb info db                       = print cached "
                        "tables names and their versions for all dbs\n"
                        "    fdb info db dbname                = print cached "
                        "tables names and their versions in db \"dbname\"\n");
    } else if (tokcmp(tok, ltok, "init") == 0) {
        fdb_init();
    } else if (tokcmp(tok, ltok, "clear") == 0) {
        tok = segtok((char *)line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "fdb schema error: missing command\n");
            return FDB_ERR_GENERIC;
        }
        if (tokcmp(tok, ltok, "schema") == 0) {
            tok = segtok((char *)line, lline, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "fdb clear schema error: missing db name\n");
            } else {
                char *dbname;

                dbname = tokdup(tok, ltok);
                if (!dbname) {
                    logmsg(LOGMSG_ERROR, "Malloc fail!\n");
                    return FDB_ERR_GENERIC;
                }

                tok = segtok((char *)line, lline, &st, &ltok);
                if (ltok == 0) {
                    /* this protects against prepare races */
                    wrlock_schema_lk();

                    /* clear all tables for db "dbname" */
                    fdb_clear_schema(dbname, NULL, 0);

                    unlock_schema_lk();
                } else {
                    char *tblname = tokdup(tok, ltok);
                    if (!tblname) {
                        logmsg(LOGMSG_ERROR, "Malloc fail 2!\n");
                        free(dbname);
                        return FDB_ERR_GENERIC;
                    }

                    /* this protects against prepare races */
                    wrlock_schema_lk();

                    /* clear table "tblname for db "dbname" */
                    fdb_clear_schema(dbname, tblname, 0);

                    unlock_schema_lk();

                    free(tblname);
                }

                free(dbname);
            }
        } else if (tokcmp(tok, ltok, "sqlite_stats") == 0) {
            fdb_clear_sqlite_stats();
        } else {
            logmsg(LOGMSG_ERROR, "fdb clear missing type\n");
            return FDB_ERR_GENERIC;
        }
    } else if (tokcmp(tok, ltok, "info") == 0) {
        tok = segtok((char *)line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "fdb info missing argument\n");
            return FDB_ERR_GENERIC;
        }

        if (tokcmp(tok, ltok, "db") == 0) {
            tok = segtok((char *)line, lline, &st, &ltok);
            if (ltok == 0) {
                fdb_info_db(NULL, NULL);
            } else {
                char *dbname = tokdup(tok, ltok);
                if (!dbname) {
                    logmsg(LOGMSG_ERROR, "Malloc 3!\n");
                    return FDB_ERR_MALLOC;
                }

                fdb_info_db(dbname, NULL);

                free(dbname);
            }
        } else {
            logmsg(LOGMSG_ERROR, "fdb info error: unrecognized argument\n");
            return FDB_ERR_GENERIC;
        }
    } else if (tokcmp(tok, ltok, "test") == 0) {
        tok = segtok((char*) line, lline, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "fdb test error: missing trap name\n");
            return FDB_ERR_GENERIC;
        }

        if (tokcmp(tok, ltok, "dlock1") == 0) {
            _test_trap_dlock1 = 1;
        } else {
            logmsg(LOGMSG_ERROR, "fdb info error: unrecognized argument\n");
            return FDB_ERR_GENERIC;
        }
    } else {
        logmsg(LOGMSG_ERROR, "fdb unrecognized command, try \"fdb help\"\n");
        return FDB_ERR_GENERIC;
    }

    return FDB_NOERR;
}

/**
 * Hack: reduce a 64b version to 32b
 *
 */
int fdb_table_version(unsigned long long version)
{
    return (int)(version % 4294967296LL);
}

/**
 * Clear sqlclntstate fdb_state object
 *
 */
void fdb_clear_sqlclntstate(struct sqlclntstate *clnt)
{
    _fdb_clear_clnt_node_affinities(clnt);

    if (clnt->fdb_state.access) {
        fdb_access_control_destroy(clnt);
    }

    bzero(&clnt->fdb_state, sizeof(clnt->fdb_state));
    clnt->fdb_state.code_release = FDB_VER; /* default */
}

/**
 * Clear sqlite* schema for a certain remote table
 *
 * NOTE: caller needs to grab TAGAPI_LK !
 *
 */
void fdb_clear_sqlite_cache(sqlite3 *sqldb, const char *dbname,
                            const char *tblname)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (gbl_old_column_names && thd && thd->clnt && thd->clnt->thd &&
        thd->clnt->thd->query_preparer_running) {
        /* No need to reset sqlitex stat tables */
        return;
    }

    /* clear the sqlite stored schemas */
    if (tblname)
        sqlite3ResetOneSchemaByName(sqldb, tblname, dbname);

    /* We delete schemas for sqlite_stat*,
       in case these are refreshed.
       Normal cases like this are detected during table lock acquisition,
       except that we don't get locks for sqlite_stat* tables, and we miss
       the case when they are refreshed.
       If they are refreshed, sqlite cached schema has wrong/stale rootpage
       numbers
       */
    sqlite3ResetOneSchemaByName(sqldb, "sqlite_stat1", dbname);
    sqlite3ResetOneSchemaByName(sqldb, "sqlite_stat2", dbname);
    sqlite3ResetOneSchemaByName(sqldb, "sqlite_stat4", dbname);
}

int fdb_table_exists(int rootpage)
{
    fdb_tbl_ent_t *ent = NULL;
    ent = get_fdb_tbl_ent_by_rootpage(rootpage);
    if (ent)
        return 1;
    return 0;
}

/**
 * Lock a remote table schema cache
 *
 * A remote schema change will trigger a flush of local schema cache
 * The lock prevents the flush racing against running remote access
 *
 */
int fdb_lock_table(sqlite3_stmt *pStmt, struct sqlclntstate *clnt, Table *tab,
                   fdb_tbl_ent_t **p_ent)
{
    fdb_tbl_ent_t *ent;
    int rootpage = tab->tnum;
    int version = tab->version;
    Db *db = &((Vdbe *)pStmt)->db->aDb[tab->iDb];

    ent = get_fdb_tbl_ent_by_rootpage(rootpage);

    *p_ent = NULL;

    /* missing or wrong version? */
    if (!ent || ent->tbl->version != tab->version) {
        clnt->osql.error_is_remote = 1;
        clnt->osql.xerr.errval = CDB2ERR_ASYNCERR;

        errstat_set_strf(&clnt->osql.xerr,
                         "schema change table \"%s\" from db \"%s\"",
                         tab->zName, db->zDbSName);

        if (gbl_fdb_track) {
            if (ent) {
                logmsg(
                    LOGMSG_USER, "Stale cache for \"%s.%s\", sql version=%d != "
                                 "shared version=%llu\n",
                    db->zDbSName, tab->zName, tab->version, ent->tbl->version);
            } else {
                logmsg(LOGMSG_USER, "No cache for \"%s.%s\", sql version=%u\n",
                        db->zDbSName, tab->zName, tab->version);
            }
        }
        return SQLITE_SCHEMA_REMOTE;
    }

    if (gbl_fdb_track) {
        char fqname[128];

        snprintf(fqname, sizeof(fqname), "%s.%s", ent->tbl->fdb->dbname,
                 ent->tbl->name);
        fqname[sizeof(fqname) - 1] = '\0';

        logmsg(LOGMSG_USER, "Locking \"%s\" version %u\n", fqname, version);
    }

    /* Lets try something simple, bumping users for fdb */
    __fdb_add_user(ent->tbl->fdb, 0);

    *p_ent = ent;

    return FDB_NOERR;
}

/**
 * Unlock a remote table schema cache
 *
 * This matches fdb_lock_table, allowing again exclusive access to that table
 *
 */
int fdb_unlock_table(fdb_tbl_ent_t *ent)
{
    if (gbl_fdb_track) {
        char fqname[128];

        snprintf(fqname, sizeof(fqname), "%s.%s", ent->tbl->fdb->dbname,
                 ent->tbl->name);
        fqname[sizeof(fqname) - 1] = '\0';

        logmsg(LOGMSG_ERROR, "Unlocking \"%s\" version %llu\n", fqname,
               ent->tbl->version);
    }

    __fdb_rem_user(ent->tbl->fdb, 1);

    return FDB_NOERR;
}

/**
 * Send heartbeats to remote dbs in a distributed transaction
 *
 */
int fdb_heartbeats(struct sqlclntstate *clnt)
{
    Pthread_mutex_lock(&clnt->dtran_mtx);
    fdb_distributed_tran_t *dtran = clnt->dbtran.dtran;
    int out_rc = FDB_NOERR;
    if (!dtran || dtran->remoted) {
        goto out;
    }
    fdb_msg_t *msg = alloca(fdb_msg_size());
    fdb_tran_t *tran;
    LISTC_FOR_EACH(&dtran->fdb_trans, tran, lnk) {
        int rc = fdb_send_heartbeat(msg, tran->tid, tran->sb);
        if (gbl_fdb_track) {
            uuidstr_t us;
            comdb2uuidstr((unsigned char *)tran->tid, us);
            logmsg(LOGMSG_USER, "%s Send heartbeat tid=%s db=\"%s\" rc=%d\n",
                   __func__, us, tran->fdb->dbname, rc);
        }
        if (!out_rc) {
            out_rc = rc;
        }
    }
out:Pthread_mutex_unlock(&clnt->dtran_mtx);
    return out_rc;
}

/* check if the mentioned fdb has a preferred node, and get the status of last
 * op */
static char *_fdb_get_affinity_node(struct sqlclntstate *clnt, const fdb_t *fdb,
                                    int *was_bad)
{
    sqlclntstate_fdb_t *fdb_state = &clnt->fdb_state;
    int i;

    for (i = 0; i < fdb_state->n_fdb_affinities; i++) {
        /* intern? */
        if (strcmp(fdb_state->fdb_ids[i], fdb->dbname) == 0) {
            /* NOTE:
               Currently, the status is not used, since the
               current implementation round-robins through the nodes
               at the time a cursor is opened;  any error following that
               is not transparent;
               I have left the status there for cases where error
               tolerance is improved, and we would like to know
               before opening a fresh node if the previous used node
               failed later on (so we don't stick to it; in the same time
               we don't run the node selection at random points in the code
               so we mark the node bad and wait for the next open to update
               the node with a good one, looking for the next node after the
               current bad one */
            if (likely(was_bad))
                *was_bad = fdb_state->fdb_last_status[i];

            return fdb_state->fdb_nodes[i];
        }
    }

    *was_bad = 1;
    return NULL;
}

/* save the last successful node for this fdb */
static int _fdb_set_affinity_node(struct sqlclntstate *clnt, const fdb_t *fdb,
                                  char *host, int status)
{
    sqlclntstate_fdb_t *fdb_state = &clnt->fdb_state;
    char **arr;
    int i;

    for (i = 0; i < fdb_state->n_fdb_affinities; i++) {
        if (strcmp(fdb_state->fdb_ids[i], fdb->dbname) == 0) {
            break;
        }
    }

    if (i < fdb_state->n_fdb_affinities) {
        fdb_state->fdb_nodes[i] = host;
        fdb_state->fdb_last_status[i] = status;
    } else {
        arr = (char **)realloc(fdb_state->fdb_ids,
                               (fdb_state->n_fdb_affinities + 1) *
                                   sizeof(char *));
        if (!arr)
            return FDB_ERR_MALLOC;

        fdb_state->fdb_ids = arr;

        arr = (char **)realloc(fdb_state->fdb_nodes,
                               (fdb_state->n_fdb_affinities + 1) *
                                   sizeof(char *));
        if (!arr)
            return FDB_ERR_MALLOC;

        fdb_state->fdb_nodes = arr;

        int *iarr;
        iarr =
            (int *)realloc(fdb_state->fdb_last_status,
                           (fdb_state->n_fdb_affinities + 1) * sizeof(char *));
        if (!iarr) return FDB_ERR_MALLOC;

        fdb_state->fdb_last_status = iarr;

        fdb_state->fdb_ids[fdb_state->n_fdb_affinities] = strdup(fdb->dbname);
        fdb_state->fdb_nodes[fdb_state->n_fdb_affinities] = host;
        fdb_state->fdb_last_status[fdb_state->n_fdb_affinities] = status;

        ++fdb_state->n_fdb_affinities;
    }
    return FDB_NOERR;
}

/**
 * Free the cached fdb node affinities
 *
 */
void _fdb_clear_clnt_node_affinities(struct sqlclntstate *clnt)
{
    if (clnt->fdb_state.fdb_ids) {
        for (int i = 0; i < clnt->fdb_state.n_fdb_affinities; i++)
            free(clnt->fdb_state.fdb_ids[i]);
        free(clnt->fdb_state.fdb_ids);
        clnt->fdb_state.fdb_ids = NULL;
    }
    if (clnt->fdb_state.fdb_nodes) {
        free(clnt->fdb_state.fdb_nodes);
        clnt->fdb_state.fdb_nodes = NULL;
    }
    if (clnt->fdb_state.fdb_last_status) {
        free(clnt->fdb_state.fdb_last_status);
        clnt->fdb_state.fdb_last_status = NULL;
    }
    clnt->fdb_state.n_fdb_affinities = 0;
}

/**
 * Convert the protocol version in an appropriate cursor open flag
 *
 */
static int _get_protocol_flags(struct sqlclntstate *clnt, fdb_t *fdb,
                               int *flags)
{
    if (fdb->server_version < FDB_VER_SSL) {
        *flags = FDB_MSG_CURSOR_OPEN_SQL_SID;
        if (clnt->plugin.has_ssl(clnt)) {
            /* Client has SSL, but remote doesn't support SSL */
            clnt->fdb_state.preserve_err = 1;
            clnt->fdb_state.xerr.errval = FDB_ERR_SSL;
            snprintf(clnt->fdb_state.xerr.errstr,
                     sizeof(clnt->fdb_state.xerr.errstr),
                     "client uses SSL but remote db does not support it");
            return -1;
        }
    } else {
        *flags = FDB_MSG_CURSOR_OPEN_SQL_SSL;
        if ((clnt->plugin.has_ssl(clnt) || fdb->ssl >= SSL_REQUIRE) && gbl_ssl_allow_remsql) {
            *flags |= FDB_MSG_CURSOR_OPEN_FLG_SSL;
        }
    }

    return 0;
}

/**
 * Change association of a cursor to a table (see body note)
 *
 */
void fdb_cursor_use_table(fdb_cursor_t *cur, struct fdb *fdb,
                          const char *tblname)
{
    /*
     * NOTE:
     * Cursors running sql are not assigned to a table per-se.
     * An initial table is assigned at the beginning and used to
     * retrieve the table version
     * This function lets re-use the cursor with a different table
     *
     */
    cur->ent = get_fdb_tbl_ent_by_name_from_fdb(fdb, tblname);
}

int fdb_cursor_need_ssl(fdb_cursor_if_t *cur)
{
    return cur->impl->need_ssl;
}

/**
 * Retrieve the schema of a remote table
 *
 */
int fdb_get_remote_version(const char *dbname, const char *table,
                           enum mach_class class, int local,
                           unsigned long long *version)
{
    char *sql = NULL;
    cdb2_hndl_tp *db;
    int rc;
    const char *location;
    int flags;

    if (local) {
        location = "localhost";
        flags = CDB2_DIRECT_CPU;
    } else {
        location = mach_class_class2name(class);
        flags = 0;
    }

    if (gbl_foreign_metadb_config) {
        cdb2_set_comdb2db_info(gbl_foreign_metadb_config);
    }

    sql = sqlite3_mprintf("select table_version('%q')", table);
    if (sql == NULL)
        return FDB_ERR_MALLOC;

    rc = cdb2_open(&db, dbname, location, flags);
    if (rc) {
        sqlite3_free(sql);
        return FDB_ERR_GENERIC;
    }

    rc = cdb2_run_statement(db, sql);
    if (rc) {
        rc = FDB_ERR_GENERIC;
        goto done;
    }

    rc = cdb2_next_record(db);
    if (rc == CDB2_OK) {
        switch (cdb2_column_type(db, 0)) {
        case CDB2_INTEGER:
            *version = *(unsigned long long *)cdb2_column_value(db, 0);
            rc = FDB_NOERR;
            break;
        default:
            rc = FDB_ERR_GENERIC;
            break;
        }
    } else
        rc = FDB_ERR_GENERIC;

done:
    cdb2_close(db);
    sqlite3_free(sql);

    return rc;
}

static int _validate_existing_table(fdb_t *fdb, int cls, int local)
{
    if (fdb->local != local) {
        logmsg(LOGMSG_ERROR,
               "Failed local match fdb %s class %d local %d, asked for class "
               "%d local %d\n",
               fdb->dbname, fdb->class, fdb->local, cls, local);
        /* follow-up instances don't specify LOCAL mode */
        return FDB_ERR_CLASS_DENIED;
    }
    if (fdb->class != cls) {
        logmsg(
            LOGMSG_ERROR,
            "Failed class match fdb %s class %d, asked for class %d local %d\n",
            fdb->dbname, fdb->class, cls, local);
        /* follow-up instances don't specify same class */
        return FDB_ERR_CLASS_DENIED;
    }
    return FDB_NOERR;
}

int fdb_validate_existing_table(const char *zDatabase)
{
    fdb_t *fdb = NULL;
    int rc = FDB_NOERR;
    const char *dbName = zDatabase;
    int local;
    int cls;

    cls = get_fdb_class(&dbName, &local, NULL);

    Pthread_rwlock_rdlock(&fdbs.arr_lock);
    fdb = __cache_fnd_fdb(dbName, NULL);
    if (fdb) {
        rc = _validate_existing_table(fdb, cls, local);
    }
    /* else {}: if the fdb was removed, there is no validation
       to be done; fdb was probably removed and the follow
       up code might actually establish a new fdb */
    Pthread_rwlock_unlock(&fdbs.arr_lock);
    return rc;
}

int fdb_set_genid_deleted(fdb_tran_t *tran, unsigned long long genid)
{
    int rc, bdberr;

    if (tran->dedup_cur == NULL) {
        tran->bdb_state = thedb->bdb_env;

        tran->dedup_tbl = bdb_temp_table_create(tran->bdb_state, &bdberr);
        if (tran->dedup_tbl == NULL) {
            logmsg(LOGMSG_ERROR, "%s: error creating a temptable, bdberr %d\n", __func__, bdberr);
            return -1;
        }

        tran->dedup_cur = bdb_temp_table_cursor(tran->bdb_state, tran->dedup_tbl, NULL, &bdberr);
        if (tran->dedup_cur == NULL) {
            logmsg(LOGMSG_ERROR, "%s: error creating a tempcursor, bdberr %d\n", __func__, bdberr);
            return -1;
        }
    }

    rc = bdb_temp_table_insert(tran->bdb_state, tran->dedup_cur, &genid, sizeof(genid), NULL, 0, &bdberr);
    if (rc != 0)
        logmsg(LOGMSG_ERROR, "%s: error inserting, rc %d, bdberr %d\n", __func__, rc, bdberr);

    return rc;
}

int fdb_is_genid_deleted(fdb_tran_t *tran, unsigned long long genid)
{
    int rc, bdberr;

    if (tran->dedup_cur == NULL)
        return 0;

    rc = bdb_temp_table_find_exact(tran->bdb_state, tran->dedup_cur, &genid, sizeof(genid), &bdberr);
    if (rc < 0) {
        logmsg(LOGMSG_ERROR, "%s: error looking up genid 0x%llx, rc %d, bdberr %d\n", __func__, genid, rc, bdberr);
        return rc;
    }
    return (rc == IX_FND);
}

/******************** SYSTABLE FDB INFO **************************/

int fdb_systable_info_collect(void **data, int *npoints)
{
    fdb_systable_info_t info = {0};

    fdb_info_db(NULL, &info);

    *data = info.arr;
    *npoints = info.narr;

    return 0;
}

void fdb_systable_info_free(void *data, int npoints)
{
    fdb_systable_ent_t *ient = (fdb_systable_ent_t *)data;

    int i;
    for (i = 0; i < npoints; i++) {
        free(ient[i].dbname);
        free(ient[i].location);
        free(ient[i].tablename);
        free(ient[i].indexname);
    }
    free(ient);
}
