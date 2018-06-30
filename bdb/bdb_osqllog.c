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
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <pthread.h>
#include <assert.h>
#include <strings.h>

#include <list.h>
#include <plbitlib.h>
#include <fsnap.h>
#include <bdb_osqllog.h>
#include <bdb_osqltrn.h>
#include <bdb_int.h>
#include <bdb_osqlcur.h>
#include <flibc.h>
#include <locks.h>

#include <logmsg.h>
#include <util.h>

#include <build/db.h>
#include <build/db_int.h>
#include <dbinc_auto/db_auto.h>
#include <dbinc_auto/dbreg_auto.h>
#include <dbinc/txn.h>
#include <dbinc_auto/txn_auto.h>
#include <dbinc_auto/txn_ext.h>
#include <dbinc/btree.h>
#include <dbinc/db_page.h>
#include <dbinc/db_shash.h>
#include <dbinc/db_swap.h>
#include <dbinc/lock.h>
#include <dbinc/log.h>
#include <dbinc/mp.h>

#include <llog_auto.h>
#include <llog_ext.h>

#ifdef NEWSI_STAT
#include <time.h>
#include <sys/time.h>
#include <util.h>
#endif

#define MAXTABLENAME 128
#define LOG_DTA_PTR_BIT 1

static int log_repo_lsns = 0;

static char hex(unsigned char a)
{
    if (a < 10)
        return '0' + a;
    return 'a' + (a - 10);
}

/**
 * Contains a copy of one log record
 *
 */
typedef struct bdb_osql_log_rec {
    int type; /* type of the record */
    DB_LSN lsn;
    unsigned long long genid;
    char *table;
    int dbnum;
    short dtafile;
    short dtastripe;
    DB_LSN complsn;
    struct bdb_osql_log_rec *comprec;
    LINKC_T(struct bdb_osql_log_rec) lnk; /* link to next record */

} bdb_osql_log_rec_t;

/**
 * Log of undo-s
 *
 */
typedef struct bdb_osql_log_impl {
    LISTC_T(bdb_osql_log_rec_t) recs; /* list of undo records */
    int clients;                      /* clean ; num sessions need it */
    int trak;                         /* set this for debug tracing */
    unsigned long long oldest_genid;  /* oldest genid in this log */
    unsigned long long commit_genid;  /* The commit id of log.*/
    int cancelled; /* if clients>0, set this if already cancelled */
} bdb_osql_log_impl_t;

/**
 * Repository of logs
 *
 */
typedef struct bdb_osql_log_repo {
    LISTC_T(bdb_osql_log_t) logs; /* list of transactions */
    pthread_mutex_t clients_mtx;  /* common mutex for transaction counters */
    pthread_rwlock_t tail_lock;   /* tail lock; lock insert path */
    int trak;
} bdb_osql_log_repo_t;

/* transaction repository */
static bdb_osql_log_repo_t *log_repo; /* the log repo */

static int undo_get_prevlsn(bdb_state_type *bdb_state, DBT *logdta,
                            DB_LSN *prevlsn);
static int bdb_osql_log_try_run_optimized(bdb_cursor_impl_t *cur,
                                          DB_LOGC *curlog, bdb_osql_log_t *log,
                                          bdb_osql_log_rec_t *rec,
                                          bdb_osql_trn_t *trn, int *dirty,
                                          int trak, int *bdberr);
static int bdb_osql_log_run_unoptimized(bdb_cursor_impl_t *cur, DB_LOGC *curlog,
                                        bdb_osql_log_t *log,
                                        bdb_osql_log_rec_t *rec, DBT *inlogdta,
                                        void *llog_dta, bdb_osql_trn_t *trn,
                                        int *dirty, int trak, int *bdberr);

/**
 * Initialize bdb_osql log repository
 *
 */
int bdb_osql_log_repo_init(int *bdberr)
{
    bdb_osql_log_repo_t *tmp = NULL;
    int rc = 0;

    if (log_repo) {

        logmsg(LOGMSG_ERROR, "%s: argh\n", __func__);
        *bdberr = BDBERR_BUG_KILLME;
        return -1;
    }

    tmp = (bdb_osql_log_repo_t *)calloc(1, sizeof(bdb_osql_log_repo_t));
    if (!tmp) {
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    listc_init(&tmp->logs, offsetof(bdb_osql_log_t, lnk));

    rc = pthread_rwlock_init(&tmp->tail_lock, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s pthread_mutex_init %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BADARGS;
        free(tmp);
        return -1;
    }

    rc = pthread_mutex_init(&tmp->clients_mtx, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s pthread_mutex_init %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BADARGS;
        pthread_rwlock_destroy(&tmp->tail_lock);
        free(tmp);
        return -1;
    }

    tmp->trak = 0; /* hook this up to a higher lvl */
    log_repo = tmp;
    return 0;
}

/**
 * Clear the bdb osql log repository
 *
 */
int bdb_osql_log_repo_destroy(int *bdberr)
{
    bdb_osql_log_repo_t *tmp = log_repo;
    bdb_osql_log_t *log = NULL, *log2 = NULL;
    int rc = 0;

    if (tmp)
        return 0;

    /* readers will check this upon return from client_mtx */
    log_repo = NULL;

    LISTC_FOR_EACH_SAFE(&tmp->logs, log, log2, lnk)
    {
        /* all clients should be gone by now */
        assert(log->impl->clients == 0);
        if (tmp->trak)
            logmsg(LOGMSG_USER, "REPOLOGS: rem log %p\n", log);
        listc_rfl(&tmp->logs, log);

        bdb_osql_log_destroy(log);
    }

    rc = pthread_rwlock_destroy(&tmp->tail_lock);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_init %d %d\n", rc, errno);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
    }

    rc = pthread_mutex_destroy(&tmp->clients_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_init %d %d\n", rc, errno);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
    }

    free(tmp);

    return rc;
}

/**
 *  Creates a log for this transaction
 *  Called from inside berkdb log processing callback
 *  or transaction commit (the latter for in local mode)
 *
 */
bdb_osql_log_t *bdb_osql_log_begin(int trak, int *bdberr)
{
    bdb_osql_log_t *log = NULL;

    if (!log_repo)
        return NULL;

    log = (bdb_osql_log_t *)calloc(1, sizeof(bdb_osql_log_t));
    if (!log) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }
    log->impl = (bdb_osql_log_impl_t *)calloc(1, sizeof(bdb_osql_log_impl_t));
    if (!log->impl) {
        free(log);
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    listc_init(&log->impl->recs, offsetof(bdb_osql_log_rec_t, lnk));
    log->impl->trak = trak;
    log->impl->oldest_genid = -1ULL;

    /*printf("XXX %p LOG %p created\n", pthread_self(), log);*/

    return log;
}

static inline bdb_osql_log_rec_t *
bdb_osql_deldta_rec(llog_undo_del_dta_args *del_dta, DB_LSN *lsn, int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = del_dta->genid;

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_del_dta;
    rec->genid = genid;
    rec->dtafile = del_dta->dtafile;
    rec->dtastripe = del_dta->dtastripe;
    rec->table = strdup((char *)(del_dta->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_deldta(bdb_osql_log_t *log, DB_LSN *lsn,
                        llog_undo_del_dta_args *del_dta, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_deldta_rec(del_dta, lsn, bdberr);
    unsigned long long genid = del_dta->genid;

    if (!rec) {
        return -1;
    }

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    listc_atl(&log->impl->recs, rec);

    if (log->impl->trak)
        logmsg(LOGMSG_USER, "TRK_LOG: log %p rec %p del_dta file=%d stripe=%d %s "
                        "genid=%llx oldest=%llx\n",
                log, rec, rec->dtafile, rec->dtastripe, rec->table, rec->genid,
                log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_delix_rec(llog_undo_del_ix_args *del_ix, DB_LSN *lsn, int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = del_ix->genid;

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_del_ix;
    rec->genid = genid;
    rec->dtafile = 0;
    rec->dtastripe = del_ix->ix;
    rec->table = strdup((char *)(del_ix->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_delix(bdb_osql_log_t *log, DB_LSN *lsn,
                       llog_undo_del_ix_args *del_ix, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_delix_rec(del_ix, lsn, bdberr);
    unsigned long long genid = del_ix->genid;

    if (!rec) {
        return -1;
    }

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    listc_atl(&log->impl->recs, rec);

    if (log->impl->trak)
        logmsg(LOGMSG_USER, 
            "TRK_LOG: log %p rec %p del_ix ix=%d %s genid=%llx oldest=%llx\n",
            log, rec, rec->dtastripe, rec->table, rec->genid,
            log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_adddta_rec(llog_undo_add_dta_args *add_dta, DB_LSN *lsn, int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = add_dta->genid;

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_add_dta;
    rec->genid = genid;
    rec->dtafile = add_dta->dtafile;
    rec->dtastripe = add_dta->dtastripe;
    rec->table = strdup((char *)(add_dta->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_adddta(bdb_osql_log_t *log, DB_LSN *lsn,
                        llog_undo_add_dta_args *add_dta, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_adddta_rec(add_dta, lsn, bdberr);
    unsigned long long genid = add_dta->genid;

    if (!rec) {
        return -1;
    }

    listc_atl(&log->impl->recs, rec);

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    if (log->impl->trak)
        logmsg(LOGMSG_USER, 
                "TRK_LOG: log %p rec %p add_dta file=%d stripe=%d %s "
                "genid=%llx oldest=%llx\n",
                log, rec, rec->dtafile, rec->dtastripe, rec->table, rec->genid,
                log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_addix_rec(llog_undo_add_ix_args *add_ix, DB_LSN *lsn, int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = add_ix->genid;

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_add_ix;
    rec->genid = genid;
    rec->dtafile = 0;
    rec->dtastripe = add_ix->ix;
    rec->table = strdup((char *)(add_ix->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_addix(bdb_osql_log_t *log, DB_LSN *lsn,
                       llog_undo_add_ix_args *add_ix, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_addix_rec(add_ix, lsn, bdberr);
    unsigned long long genid = add_ix->genid;

    if (!rec) {
        return -1;
    }

    listc_atl(&log->impl->recs, rec);

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    if (log->impl->trak)
        logmsg(LOGMSG_USER, 
            "TRK_LOG: log %p rec %p add_ix ix=%d %s genid=%llx oldest=%llx\n",
            log, rec, rec->dtastripe, rec->table, rec->genid,
            log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_upddta_rec(llog_undo_upd_dta_args *upd_dta, DB_LSN *lsn, int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = upd_dta->oldgenid;

    if (flibc_ntohll(upd_dta->oldgenid) >= flibc_ntohll(upd_dta->newgenid)) {
        logmsg(LOGMSG_FATAL, "%s:%d %s incorrect genid received %lx %lx\n",
               __FILE__, __LINE__, __func__, flibc_ntohll(upd_dta->oldgenid),
               flibc_ntohll(upd_dta->newgenid));
        abort();
    }

    /*
    fprintf (stderr, "%s:%d old = %llx new=%llx\n",
          __FILE__, __LINE__, upd_dta->oldgenid, upd_dta->newgenid);
     */

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_upd_dta;
    rec->genid = genid;
    rec->dtafile = upd_dta->dtafile;
    rec->dtastripe = upd_dta->dtastripe;
    rec->table = strdup((char *)(upd_dta->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_upddta(bdb_osql_log_t *log, DB_LSN *lsn,
                        llog_undo_upd_dta_args *upd_dta, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_upddta_rec(upd_dta, lsn, bdberr);
    unsigned long long genid = upd_dta->oldgenid;

    if (!rec) {
        return -1;
    }

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    listc_atl(&log->impl->recs, rec);

    if (log->impl->trak)
        logmsg(LOGMSG_USER, "TRK_LOG: log %p rec %p upd_dta file=%d stripe=%d %s "
                "genid=%llx oldest=%llx\n",
                log, rec, rec->dtafile, rec->dtastripe, rec->table, rec->genid,
                log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_updix_rec(llog_undo_upd_ix_args *upd_ix, DB_LSN *lsn, int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = upd_ix->oldgenid;

    if (flibc_ntohll(upd_ix->oldgenid) >= flibc_ntohll(upd_ix->newgenid)) {
        logmsg(LOGMSG_FATAL, "Oldgenid %llx >= negenid %llx\n",
               flibc_ntohll(upd_ix->oldgenid), flibc_ntohll(upd_ix->newgenid));
        abort();
    }

    /*
    fprintf (stderr, "%s:%d ix old = %llx new=%llx\n",
          __FILE__, __LINE__, upd_ix->oldgenid, upd_ix->newgenid);
     */

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_upd_ix;
    rec->genid = genid;
    rec->dtafile = 0;
    rec->dtastripe = upd_ix->ix;
    rec->table = strdup((char *)(upd_ix->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_updix(bdb_osql_log_t *log, DB_LSN *lsn,
                       llog_undo_upd_ix_args *upd_ix, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_updix_rec(upd_ix, lsn, bdberr);
    unsigned long long genid = upd_ix->oldgenid;

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    listc_atl(&log->impl->recs, rec);

    if (log->impl->trak)
        logmsg(LOGMSG_USER,
               "TRK_LOG: log %p rec %p upd_ix file=%d stripe=%d %s "
               "genid=%llx->%lx oldest=%llx\n",
               log, rec, rec->dtafile, rec->dtastripe, rec->table, rec->genid,
               upd_ix->newgenid, log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_adddta_lk_rec(llog_undo_add_dta_lk_args *add_dta_lk, DB_LSN *lsn,
                       int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = add_dta_lk->genid;

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_add_dta_lk;
    rec->genid = genid;
    rec->dtafile = add_dta_lk->dtafile;
    rec->dtastripe = add_dta_lk->dtastripe;
    rec->table = strdup((char *)(add_dta_lk->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_adddta_lk(bdb_osql_log_t *log, DB_LSN *lsn,
                           llog_undo_add_dta_lk_args *add_dta_lk, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_adddta_lk_rec(add_dta_lk, lsn, bdberr);
    unsigned long long genid = add_dta_lk->genid;

    if (!rec) {
        return -1;
    }

    listc_atl(&log->impl->recs, rec);

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    if (log->impl->trak)
        logmsg(LOGMSG_USER, "TRK_LOG: log %p rec %p add_dta_lk file=%d stripe=%d "
                        "%s genid=%llx oldest=%llx\n",
                log, rec, rec->dtafile, rec->dtastripe, rec->table, rec->genid,
                log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_addix_lk_rec(llog_undo_add_ix_lk_args *add_ix_lk, DB_LSN *lsn,
                      int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = add_ix_lk->genid;

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_add_ix_lk;
    rec->genid = genid;
    rec->dtafile = 0;
    rec->dtastripe = add_ix_lk->ix;
    rec->table = strdup((char *)(add_ix_lk->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_addix_lk(bdb_osql_log_t *log, DB_LSN *lsn,
                          llog_undo_add_ix_lk_args *add_ix_lk, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_addix_lk_rec(add_ix_lk, lsn, bdberr);
    unsigned long long genid = add_ix_lk->genid;

    listc_atl(&log->impl->recs, rec);

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    if (log->impl->trak)
        logmsg(LOGMSG_USER, "TRK_LOG: log %p rec %p add_ix_lk ix=%d %s genid=%llx "
                        "oldest=%llx\n",
                log, rec, rec->dtastripe, rec->table, rec->genid,
                log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_deldta_lk_rec(llog_undo_del_dta_lk_args *del_dta_lk, DB_LSN *lsn,
                       int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = del_dta_lk->genid;

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_del_dta_lk;
    rec->genid = genid;
    rec->dtafile = del_dta_lk->dtafile;
    rec->dtastripe = del_dta_lk->dtastripe;
    rec->table = strdup((char *)(del_dta_lk->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_deldta_lk(bdb_osql_log_t *log, DB_LSN *lsn,
                           llog_undo_del_dta_lk_args *del_dta_lk, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_deldta_lk_rec(del_dta_lk, lsn, bdberr);
    unsigned long long genid = del_dta_lk->genid;

    if (!rec) {
        return -1;
    }

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    listc_atl(&log->impl->recs, rec);

    if (log->impl->trak)
        logmsg(LOGMSG_USER, "TRK_LOG: log %p rec %p del_dta_lk file=%d stripe=%d "
                        "%s genid=%llx oldest=%llx\n",
                log, rec, rec->dtafile, rec->dtastripe, rec->table, rec->genid,
                log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_delix_lk_rec(llog_undo_del_ix_lk_args *del_ix_lk, DB_LSN *lsn,
                      int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = del_ix_lk->genid;

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_del_ix_lk;
    rec->genid = genid;
    rec->dtafile = 0;
    rec->dtastripe = del_ix_lk->ix;
    rec->table = strdup((char *)(del_ix_lk->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_delix_lk(bdb_osql_log_t *log, DB_LSN *lsn,
                          llog_undo_del_ix_lk_args *del_ix_lk, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_delix_lk_rec(del_ix_lk, lsn, bdberr);
    unsigned long long genid = del_ix_lk->genid;

    if (!rec) {
        return -1;
    }

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    listc_atl(&log->impl->recs, rec);

    if (log->impl->trak)
        logmsg(LOGMSG_USER, "TRK_LOG: log %p rec %p del_ix_lk file=%d stripe=%d %s "
                        "genid=%llx oldest=%llx\n",
                log, rec, rec->dtafile, rec->dtastripe, rec->table, rec->genid,
                log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_upddta_lk_rec(llog_undo_upd_dta_lk_args *upd_dta_lk, DB_LSN *lsn,
                       int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = upd_dta_lk->oldgenid;

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_upd_dta_lk;
    rec->genid = genid;
    rec->dtafile = upd_dta_lk->dtafile;
    rec->dtastripe = upd_dta_lk->dtastripe;
    rec->table = strdup((char *)(upd_dta_lk->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_upddta_lk(bdb_osql_log_t *log, DB_LSN *lsn,
                           llog_undo_upd_dta_lk_args *upd_dta_lk, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_upddta_lk_rec(upd_dta_lk, lsn, bdberr);
    unsigned long long genid = upd_dta_lk->oldgenid;

    if (!rec) {
        return -1;
    }

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    listc_atl(&log->impl->recs, rec);

    if (log->impl->trak)
        logmsg(LOGMSG_USER, "TRK_LOG: log %p rec %p upd_dta_lk file=%d stripe=%d "
                        "%s genid=%llx oldest=%llx\n",
                log, rec, rec->dtafile, rec->dtastripe, rec->table, rec->genid,
                log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_updix_lk_rec(llog_undo_upd_ix_lk_args *upd_ix_lk, DB_LSN *lsn,
                      int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    unsigned long long genid = upd_ix_lk->oldgenid;

    if (flibc_ntohll(upd_ix_lk->oldgenid) >=
        flibc_ntohll(upd_ix_lk->newgenid)) {
        logmsg(LOGMSG_FATAL, "Oldgenid %llx >= negenid %llx\n",
               flibc_ntohll(upd_ix_lk->oldgenid),
               flibc_ntohll(upd_ix_lk->newgenid));
        abort();
    }

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    rec->type = DB_llog_undo_upd_ix_lk;
    rec->genid = genid;
    rec->dtafile = 0;
    rec->dtastripe = upd_ix_lk->ix;
    rec->table = strdup((char *)(upd_ix_lk->table.data));
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    return rec;
}

int bdb_osql_log_updix_lk(bdb_osql_log_t *log, DB_LSN *lsn,
                          llog_undo_upd_ix_lk_args *upd_ix_lk, int *bdberr)
{
    bdb_osql_log_rec_t *rec = bdb_osql_updix_lk_rec(upd_ix_lk, lsn, bdberr);
    unsigned long long genid = upd_ix_lk->oldgenid;

    if (!rec) {
        return -1;
    }

    if (bdb_cmp_genids(genid, log->impl->oldest_genid) < 0)
        log->impl->oldest_genid = genid;

    listc_atl(&log->impl->recs, rec);

    if (log->impl->trak)
        logmsg(LOGMSG_USER, "TRK_LOG: log %p rec %p upd_ix_lk file=%d stripe=%d %s "
                        "genid=%llx oldest=%llx\n",
                log, rec, rec->dtafile, rec->dtastripe, rec->table, rec->genid,
                log->impl->oldest_genid);

    return 0;
}

static inline bdb_osql_log_rec_t *
bdb_osql_comprec_reference_rec(bdb_state_type *bdb_state, DB_LSN *lsn,
                               DBT *logdta, int *bdberr)
{
    u_int32_t rectype = 0;
    int rc = 0;
    llog_undo_add_dta_args *add_dta;
    llog_undo_add_ix_args *add_ix;
    llog_ltran_commit_args *commit;
    llog_ltran_start_args *start;
    llog_ltran_comprec_args *comprec;
    llog_undo_del_dta_args *del_dta;
    llog_undo_del_ix_args *del_ix;
    llog_undo_upd_dta_args *upd_dta;
    llog_undo_upd_ix_args *upd_ix;

    /* Rowlocks types */
    llog_undo_add_dta_lk_args *add_dta_lk;
    llog_undo_add_ix_lk_args *add_ix_lk;
    llog_undo_del_dta_lk_args *del_dta_lk;
    llog_undo_del_ix_lk_args *del_ix_lk;
    llog_undo_upd_dta_lk_args *upd_dta_lk;
    llog_undo_upd_ix_lk_args *upd_ix_lk;
    bdb_osql_log_rec_t *rec = NULL;
    void *logp = NULL;

    LOGCOPY_32(&rectype, logdta->data);

    switch (rectype) {

    /* snapisol logical logging */
    case DB_llog_undo_add_dta:
    case DB_llog_undo_add_ix:
    case DB_llog_undo_del_dta:
    case DB_llog_undo_del_ix:
    case DB_llog_undo_upd_dta:
    case DB_llog_undo_upd_ix:

    /* start/commit/comprec */
    case DB_llog_ltran_commit:
    case DB_llog_ltran_comprec:
    case DB_llog_ltran_start:
        logmsg(LOGMSG_FATAL, "Record type %d lsn %d:%d cannot be a comprec\n",
                rectype, lsn->file, lsn->offset);
        abort();
        rc = -1;
        break;

    case DB_llog_undo_add_dta_lk:
        rc = llog_undo_add_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &add_dta_lk);
        if (rc)
            return NULL;
        logp = add_dta_lk;
        rec = bdb_osql_adddta_lk_rec(add_dta_lk, lsn, bdberr);
        break;

    case DB_llog_undo_add_ix_lk:
        rc = llog_undo_add_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &add_ix_lk);
        if (rc)
            return NULL;
        logp = add_ix_lk;
        rec = bdb_osql_addix_lk_rec(add_ix_lk, lsn, bdberr);
        break;

    case DB_llog_undo_del_dta_lk:
        rc = llog_undo_del_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &del_dta_lk);
        if (rc)
            return NULL;
        logp = del_dta_lk;
        rec = bdb_osql_deldta_lk_rec(del_dta_lk, lsn, bdberr);
        break;

    case DB_llog_undo_del_ix_lk:
        rc = llog_undo_del_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &del_ix_lk);
        if (rc)
            return NULL;
        logp = del_ix_lk;
        rec = bdb_osql_delix_lk_rec(del_ix_lk, lsn, bdberr);
        break;

    case DB_llog_undo_upd_dta_lk:
        rc = llog_undo_upd_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &upd_dta_lk);
        if (rc)
            return NULL;
        logp = upd_dta_lk;
        rec = bdb_osql_upddta_lk_rec(upd_dta_lk, lsn, bdberr);
        break;

    case DB_llog_undo_upd_ix_lk:
        rc = llog_undo_upd_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &upd_ix_lk);
        if (rc)
            return NULL;
        logp = upd_ix_lk;
        rec = bdb_osql_updix_lk_rec(upd_ix_lk, lsn, bdberr);
        break;

    default:
        logmsg(LOGMSG_FATAL, "Unknown log entry type %d at lsn %d:%d\n", rectype,
                lsn->file, lsn->offset);
        abort();
        rc = -1;
        break;
    }

    if (logp)
        free(logp);

    return rec;
}

static inline bdb_osql_log_rec_t *
bdb_osql_comprec_rec(bdb_state_type *bdb_state,
                     llog_ltran_comprec_args *comprec, DB_LSN *lsn, int *bdberr)
{
    bdb_osql_log_rec_t *rec = (bdb_osql_log_rec_t *)calloc(1, sizeof(*rec));
    DB_LOGC *cur;
    DB_LSN comp_lsn = comprec->complsn;
    DBT logdta;
    int rc;

    if (!rec) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    /* Get a log cursor */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed to get a log cursor for comprec\n",
                __func__);
        free(rec);
        *bdberr = BDBERR_MISC;
        return NULL;
    }

    /* Retrieve referenced LSN */
    bzero(&logdta, sizeof(DBT));
    logdta.flags = DB_DBT_REALLOC;
    rc = cur->get(cur, &comp_lsn, &logdta, DB_SET);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Unable to get comprec referenced lsn, %d:%d\n",
                comp_lsn.file, comp_lsn.offset);
        free(rec);
        *bdberr = BDBERR_MISC;
        return NULL;
    }

    /* Close cursor */
    rc = cur->close(cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s fail to close curlog\n", __func__);
    }

    rec->type = DB_llog_ltran_comprec;
    rec->genid = 0;
    rec->dtafile = 0;
    rec->dtastripe = 0;
    rec->table = NULL;
    rec->dbnum = -1;
    memcpy(&rec->lsn, lsn, sizeof(rec->lsn));
    memcpy(&rec->complsn, &comprec->complsn, sizeof(rec->complsn));
    rec->comprec =
        bdb_osql_comprec_reference_rec(bdb_state, &comp_lsn, &logdta, bdberr);

    if (logdta.data)
        free(logdta.data);

    if (!rec->comprec) {
        logmsg(LOGMSG_ERROR, "Failed to create comprec reference for lsn %d:%d\n",
                rec->complsn.file, rec->complsn.offset);
        free(rec);
        *bdberr = BDBERR_MISC;
        return NULL;
    }

    return rec;
}

int bdb_osql_log_comprec(bdb_state_type *bdb_state, bdb_osql_log_t *log,
                         DB_LSN *lsn, llog_ltran_comprec_args *comprec,
                         int *bdberr)
{
    bdb_osql_log_rec_t *rec =
        bdb_osql_comprec_rec(bdb_state, comprec, lsn, bdberr);

    if (!rec) {
        return -1;
    }

    listc_atl(&log->impl->recs, rec);

    if (log->impl->trak)
        logmsg(LOGMSG_USER, 
                "TRK_LOG: log %p rec %p comprec lsn %d:%d oldest-genid=%llx\n",
                log, rec, rec->complsn.file, rec->complsn.offset,
                log->impl->oldest_genid);

    return 0;
}

/**
 *  Remove a log
 *  All the undo records are freed
 *  Called by the last to process sql thread
 *
 */
int bdb_osql_log_destroy(bdb_osql_log_t *log)
{
    bdb_osql_log_rec_t *rec = NULL, *tmp = NULL;

    LISTC_FOR_EACH_SAFE(&log->impl->recs, rec, tmp, lnk)
    {
        if (rec->comprec) {
            if (rec->comprec->table)
                free(rec->comprec->table);
            free(rec->comprec);
        }
        if (rec->table)
            free(rec->table);
        free(rec);
    }
    free(log->impl);
    free(log);
    return 0;
}

/**
 *  Register this many clients;
 *  This is done once, per log creation, by replication log
 *
 */
int bdb_osql_log_register_clients(bdb_osql_log_t *log, int clients, int *bdberr)
{
    int rc = 0;

    *bdberr = 0;
    if (!log_repo) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* printf("XXX %p LOG %p Calling log_register with %d clients\n",
     * pthread_self(), log, clients); */

    rc = pthread_mutex_lock(&log_repo->clients_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
        return -1;
    }

    log->impl->clients = clients;

    /* printf("XXX %p LOG %p has %d clients\n", pthread_self(), log, clients);
     */

    if (log->impl->trak)
        logmsg(LOGMSG_USER, "TRK_LOG: %p registered %d clients\n", log, clients);
    if (log_repo->trak)
        logmsg(LOGMSG_USER, "REPOLOGS: log %p has %d clients\n", log, clients);

    /* since we have the lock anyway, we can do two things here
       - clean any completely processed logs
       - link this new log to the list
     */

    rc = pthread_mutex_unlock(&log_repo->clients_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_unlock %d %d\n", rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
        return -1;
    }

    return 0;
}

/**
 *  Get info about the pointed log
 *
 */
bdb_osql_log_t *bdb_osql_log_first(bdb_osql_log_t *log,
                                   unsigned long long *genid)
{
    if (genid)
        *genid = log->impl->oldest_genid;
    return log;
}

/**
 *  Get info about the next transaction in the log
 * The return, if not null, is the following log;
 * The argument is the return of a previous
 * bdb_osql_log_first or bdb_osql_log_next
 *
 */
bdb_osql_log_t *bdb_osql_log_next(bdb_osql_log_t *log,
                                  unsigned long long *genid)
{
    log = log->lnk.next;
    if (log && genid)
        *genid = log->impl->oldest_genid;

    return log;
}

/**
 * Returns the oldest genid updated by "log"
 *
 */
unsigned long long bdb_osql_log_get_genid(bdb_osql_log_t *log)
{
    return log->impl->oldest_genid;
}

/**
 * Insert a log in the repository
 * (NOTE: logs are first created and populated, before being
 *  inserted in the list)
 * During insert, the clients are checked and updated if needed
 * Sync with bdb_osql_log_next
 */
int bdb_osql_log_insert(bdb_state_type *bdb_state, bdb_osql_log_t *log,
                        int *bdberr, int is_master)
{
    bdb_osql_log_t *log1 = NULL, *log2 = NULL;
    int empty;
    int forced_free;
    int rc = 0;

    *bdberr = 0;

    /* getting this lock effectively prevent transactions
       to process the last of their log, to avoid races
       on the tail
       See also: bdb_osql_log_next
     */
    rc = pthread_rwlock_wrlock(&log_repo->tail_lock);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s wrlock %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
    }

    if (log_repo->trak)
        logmsg(LOGMSG_USER, "REPOLOGS: add log %p\n", log);
    /* insert the log */
    listc_abl(&log_repo->logs, log);

    /* this disable temporary the start of new snapshot/serializable
     * transactions */
    rc = bdb_osql_trn_check_clients(log, &empty, !is_master, bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: fail to check clients %d %d\n", __func__, rc,
                *bdberr);
        *bdberr = BDBERR_BADARGS;
    }

    /* if no clients for this log, delete it */
    if (empty) {
        if (log_repo->trak)
            logmsg(LOGMSG_USER, "REPOLOGS: rem log %p [no clnt]\n", log);
        listc_rfl(&log_repo->logs, log);

        /* printf("XXX %d LOG %p Destroyed (no clients), queue count=%d\n",
         * pthread_self(), log, log_repo->logs.count); */

        rc = bdb_osql_log_destroy(log);
        if (rc) {
            pthread_rwlock_unlock(&log_repo->tail_lock);
            *bdberr = rc;
            return -1;
        }
    } else {
        /* count the new entries */
        log_repo_lsns += log->impl->recs.count;
    }

    /* we can run garbage collection, under client_mtx lock */
    rc = pthread_mutex_lock(&log_repo->clients_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s unlock %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
    }

    /* first pass, free whatever we don't need anymore */
    LISTC_FOR_EACH_SAFE(&log_repo->logs, log1, log2, lnk)
    {
        if (log1->impl->clients == 0) {
            if (log_repo->trak)
                logmsg(LOGMSG_USER, "REPOLOGS: rem log %p [consumed]\n", log1);
            listc_rfl(&log_repo->logs, log1);

            log_repo_lsns -= log1->impl->recs.count;

            /* printf("XXX %d LOG %p Destroyed (insert cleanup), queue
             * count=%d\n", pthread_self(), log1, log_repo->logs.count); */

            rc = bdb_osql_log_destroy(log1);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to destroy log %d %d\n", __func__, rc,
                        *bdberr);
                *bdberr = BDBERR_BADARGS;
            }
        } else
            break; /* prefix not empty, we'll clean next time */
    }

    /* step 2, if we need to free some more, cancel transactions */
    if (bdb_state->attr->max_vlog_lsns &&
        bdb_state->attr->max_vlog_lsns < log_repo_lsns) {
        forced_free = log_repo_lsns - bdb_state->attr->max_vlog_lsns;

        LISTC_FOR_EACH_SAFE(&log_repo->logs, log1, log2, lnk)
        {
            if (forced_free <= 0)
                break;

            /* no need to cancel if no clients are pointing to log1 */
            if (log1->impl->clients > 0 && log1->impl->cancelled == 0) {
                rc = bdb_osql_trn_cancel_clients(log1, !is_master, bdberr);
                if (rc)
                    break;
                log1->impl->cancelled = 1;
            }

            forced_free -= log1->impl->recs.count;
        }
    }

    rc = pthread_mutex_unlock(&log_repo->clients_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s unlock %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
    }

    rc = pthread_rwlock_unlock(&log_repo->tail_lock);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s unlock %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
    }

    /* at this point, the tail was updated, the log counts
       the clients needed, and those clients have the list
       of logs updated correctly (i.e. "first" set if not set
       before getting tail_lock, or last_copy->next set
       correctly
     */

    return rc;
}

/* Apply a page-order update to an add-cursor. */
static int bdb_osql_addcur_apply_ll(bdb_state_type *bdb_state,
                                    bdb_cursor_impl_t *cur, int dbnum,
                                    tran_type *shadow_tran,
                                    unsigned long long genid, int tabletype,
                                    int tableid, void *addptr, int addptr_len,
                                    int trak, int *bdberr)
{
    int rc = 0;

    if (!shadow_tran->startgenid ||
        (bdb_cmp_genids(genid, shadow_tran->startgenid) < 0)) {
        if (trak) {
            logmsg(LOGMSG_USER, 
                "TRK_LOG: %p apply to ADDCUR table for dbnum %d genid=%llx\n",
                shadow_tran, dbnum, genid);
        }

        /* Open the shadows so that we can track which logs have been applied.
         */
        bdb_tran_open_shadow_nocursor(bdb_state, dbnum, shadow_tran, tableid,
                                      tabletype, bdberr);

        shadow_tran->shadow_rows++;

        /* Insert into the temp table. */
        rc = bdb_temp_table_insert(cur->state, cur->addcur, &genid, 8, addptr,
                                   addptr_len, bdberr);

        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_temp_table_insert %d %d.\n", rc, *bdberr);
            return rc;
        }
    } else {
        if (trak) {
            logmsg(LOGMSG_USER, 
                    "TRK_LOG: %p skip ADDCUR add for dbnum %d genid=%llx\n",
                    shadow_tran, dbnum, genid);
        }
    }
    return rc;
}

static char *tabletype_to_str(int tabletype)
{
    if (tabletype == BDBC_SK) {
        return "BDBC_SK";
    }
    if (tabletype == BDBC_IX) {
        return "BDBC_IX";
    }
    if (tabletype == BDBC_BL) {
        return "BDBC_BL";
    }
    if (tabletype == BDBC_DT) {
        return "BDBC_DT";
    }

    if (tabletype == BDBC_UN) {
        return "BDBC_UN";
    }

    return "(UNKNOWN?)";
}

static int bdb_osql_log_apply_ll(bdb_state_type *bdb_state,
                                 tran_type *shadow_tran, bdb_osql_log_t *log,
                                 bdb_osql_log_rec_t *rec, int tabletype,
                                 int tableid, unsigned long long genid,
                                 void *dta, int dtalen, void *collattr,
                                 int collattrlen, int trak, int *bdberr)
{
    tmpcursor_t *cur = NULL;
    int rc = 0;

    if (!shadow_tran->startgenid ||
        (bdb_cmp_genids(genid, shadow_tran->startgenid) < 0)) {
        shadow_tran->shadow_rows++;

        if (trak)
            logmsg(LOGMSG_USER, "TRK_LOG: %p apply to %s %s dbnum=%d file=%d "
                            "stripe=%d genid=%llx startgenid=%llx oldest=%llx "
                            "id=%d\n",
                    shadow_tran, tabletype_to_str(tabletype), rec->table,
                    rec->dbnum, rec->dtafile, rec->dtastripe, genid,
                    shadow_tran->startgenid,
                    (log ? log->impl->oldest_genid : 0), tableid);

        if (tabletype != BDBC_IX) {

            /* blob or data (non-index) */
            cur = bdb_tran_open_shadow(bdb_state, rec->dbnum, shadow_tran,
                                       tabletype == BDBC_DT ? tableid
                                                            : rec->dtafile,
                                       tabletype, 1 /*create*/, bdberr);
            if (!cur) {
                logmsg(LOGMSG_ERROR, "bdb_tran_open_shadow %d\n", *bdberr);
                return -1;
            }

            if (trak & SQL_DBG_SHADOW) {
                logmsg(LOGMSG_USER,
                       "INSERTED DT[%d:%d]:\n\tkeylen=%zu\n\tkey=\"",
                       rec->dbnum, tableid, sizeof(genid));
                hexdump(LOGMSG_USER, (char *)&genid, sizeof(genid));
                logmsg(LOGMSG_USER, "\"\n\tdatalen=%d\n\tdata=\"", dtalen);
                hexdump(LOGMSG_USER, dta, dtalen);
                logmsg(LOGMSG_USER, "\"\n");
            }

            /* If tabletype is blob, insert a masked genid to support our
             * inplace-
             * blob optimization.  We only want to see what was originally
             * there,
             * and ignore updates to already updated blobs.  Make sure this
             * doesn't
             * exist before inserting it.  */
            if (tabletype == BDBC_BL) {
                unsigned long long *gcopy = malloc(sizeof(unsigned long long));
                *gcopy = get_search_genid(bdb_state, genid);

                rc = bdb_temp_table_find_exact(bdb_state, cur, gcopy,
                                               sizeof(*gcopy), bdberr);
                if (rc != IX_FND) {
                    rc = bdb_temp_table_insert(bdb_state, cur, gcopy,
                                               sizeof(*gcopy), dta, dtalen,
                                               bdberr);
                    if (trak & SQL_DBG_SHADOW) {
                        logmsg(LOGMSG_USER, "INSERTING SEARCH-GENID %llx, GENID %llx INTO "
                               "THE BLOB SHADOW TABLE\n",
                               *gcopy, genid);
                    }
                    free(gcopy);
                } else {
                    if (trak & SQL_DBG_SHADOW) {
                        logmsg(LOGMSG_USER, "NOT INSERTING ALREADY EXISTING SEARCH-GENID "
                               "%lld, GENID %lld INTO THE BLOB SHADOW TABLE\n",
                               *gcopy, genid);
                    }
                }
            } else {
                rc = bdb_temp_table_insert(bdb_state, cur, &genid,
                                           sizeof(genid), dta, dtalen, bdberr);
            }
        } else {
            /* indexes have genid as payload, and genid prefixed to key to avoid
               dups;
               We are getting here ONLY for del_ix!!!! (TODO:Maybe move this up
               one level?)
             */
            char newkey[MAXKEYSZ];
            int newkeylen = dtalen + sizeof(genid);
            char *use_data;
            int use_datalen;

            /* index */
            cur =
                bdb_tran_open_shadow(bdb_state, rec->dbnum, shadow_tran,
                                     tableid, tabletype, 1 /*create*/, bdberr);
            if (!cur) {
                logmsg(LOGMSG_ERROR, "bdb_tran_open_shadow %d\n", *bdberr);
                return -1;
            }

            /* Note: dup keys already have the genid inthere, don't add it again
             */
            if (bdb_state->ixdups[tableid]) {
                newkeylen -= sizeof(genid);
                dtalen -= sizeof(genid);

                /*
                 * For the dup-key case:
                 *
                 * Since we append the masked genid to the key rather than the
                 *full genid,
                 * there'll be collisions on multiple updates of the same row.
                 *I could
                 * either unmask the key-genid, or I could do a lookup of the
                 *masked genid
                 * & only add it if it's not in the shadow.
                 *
                 * BTW, I could always append only the masked genid and use the
                 *lookup
                 * strategy for the unique keys as well.  This would put a cap
                 *on how large
                 * this temp table could grow & would be optimal for long
                 *running snapshots.
                 */
            }

            assert(newkeylen <= MAXKEYSZ && newkeylen > 0);
            memcpy(newkey, dta, dtalen);
            /* if (!bdb_state->ixdups[tableid]) */
            {
                memcpy(newkey + dtalen, &genid, sizeof(genid));
            }

            /* we need to add besides genid, also the datacopy/tail */
            if (collattr != NULL && collattrlen > sizeof(unsigned long long)) {
                use_data = collattr;
                use_datalen = collattrlen;
            } else {
                use_data = (char *)&genid;
                use_datalen = sizeof(unsigned long long);
            }

            if (trak & SQL_DBG_SHADOW) {
                logmsg(LOGMSG_USER, "INSERTED IX[%d:%d] (%s)\n\tkeylen=%d\n\tkey=\"",
                       rec->dbnum, tableid,
                       (bdb_state->ixdups[tableid]) ? "dupd" : "uniq",
                       newkeylen);
                hexdump(LOGMSG_USER, newkey, newkeylen);
                logmsg(LOGMSG_USER, "\"\n\tdatalen=%d\n\tdata=\"", use_datalen);
                hexdump(LOGMSG_USER, use_data, use_datalen);
                logmsg(LOGMSG_USER, "\"\n");
            }

            rc = bdb_temp_table_insert(bdb_state, cur, newkey, newkeylen,
                                       use_data, use_datalen, bdberr);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_temp_table_insert %d %d\n", rc, *bdberr);
            return rc;
        }

        shadow_tran->check_shadows = 1;

        rc = bdb_temp_table_close_cursor(bdb_state, cur, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_temp_table_close_cursor %d %d\n", rc, *bdberr);
            return rc;
        }
    } else {
        if (trak)
            logmsg(LOGMSG_USER, "TRK_LOG: %p skip %p %s dbnum=%d file=%d stripe=%d "
                            "genid=%llx oldest=%llx\n",
                    shadow_tran, rec, rec->table, rec->dbnum, rec->dtafile,
                    rec->dtastripe, genid, (log ? log->impl->oldest_genid : 0));
    }

    return rc;
}

static int bdb_osql_log_applicable(bdb_cursor_impl_t *cur,
                                   bdb_osql_log_rec_t *inrec, int *bdberr)
{
    bdb_state_type *bdb_state = NULL;
    bdb_osql_log_rec_t *rec;
    int rc = 0;

    *bdberr = 0;

    if (inrec->type == DB_llog_ltran_comprec) {
        rec = inrec->comprec;
    } else {
        rec = inrec;
    }

    /* check dbnum */
    if (rec->dbnum == -1) {
        /* not initialized  */
        bdb_state =
            bdb_get_table_by_name_dbnum(cur->state, rec->table, &rec->dbnum);
        if (!bdb_state) {
            *bdberr = BDBERR_BADARGS;
            return -1;
        }
    }

    if (cur->dbnum != rec->dbnum) {
        if (cur->trak)
            logmsg(LOGMSG_USER, 
                    "TRK_LOG: cur %p rec %p mismatched dbnum %d vs rec dbnum %d\n",
                cur, rec, cur->dbnum, rec->dbnum);
        return 0;
    }

    /* if data, it is a match */
    if (cur->type == BDBC_DT) {
        if (rec->type == DB_llog_undo_add_dta ||
            rec->type == DB_llog_undo_add_dta_lk ||
            rec->type == DB_llog_undo_del_dta ||
            rec->type == DB_llog_undo_del_dta_lk ||
            rec->type == DB_llog_undo_upd_dta ||
            rec->type == DB_llog_undo_upd_dta_lk) {
            if (cur->trak)
                logmsg(LOGMSG_USER, "TRK_LOG: cur %p (dta=%d) found matching "
                                "record %p (type=%d, dtafile = %d "
                                "dtastripe=%d, lsn %d:%d)\n",
                        cur, cur->idx, rec, rec->type, rec->dtafile,
                        rec->dtastripe, rec->lsn.file, rec->lsn.offset);
            return 1;
        } else if (rec->type == DB_llog_undo_add_ix ||
                   rec->type == DB_llog_undo_add_ix_lk ||
                   rec->type == DB_llog_undo_del_ix ||
                   rec->type == DB_llog_undo_del_ix_lk ||
                   rec->type == DB_llog_undo_upd_ix ||
                   rec->type == DB_llog_undo_upd_ix_lk) {
            return 0; /* ignore index updates for data */
        }
    }

    if (cur->type == BDBC_IX) {
        if (rec->type == DB_llog_undo_add_ix ||
            rec->type == DB_llog_undo_add_ix_lk ||
            rec->type == DB_llog_undo_del_ix ||
            rec->type == DB_llog_undo_del_ix_lk ||
            rec->type == DB_llog_undo_upd_ix ||
            rec->type == DB_llog_undo_upd_ix_lk) {
            rc = (cur->idx == rec->dtastripe);
            if (cur->trak) {
                if (rc)
                    logmsg(LOGMSG_USER, "TRK_LOG: cur %p (ix=%d) found matching "
                                    "record %p (type=%d) lsn %d:%d\n",
                            cur, cur->idx, rec, rec->type, rec->lsn.file,
                            rec->lsn.offset);
                else
                    logmsg(LOGMSG_USER, "TRK_LOG: cur %p (ix=%d) mismatch rec %p "
                                    "id %d (type=%d) lsn %d:%d\n",
                            cur, cur->idx, rec, rec->dtastripe, rec->type,
                            rec->lsn.file, rec->lsn.offset);
            }

            return rc;
        } else if (rec->type == DB_llog_undo_add_dta ||
                   rec->type == DB_llog_undo_add_dta_lk ||
                   rec->type == DB_llog_undo_del_dta ||
                   rec->type == DB_llog_undo_del_dta_lk ||
                   rec->type == DB_llog_undo_upd_dta ||
                   rec->type == DB_llog_undo_upd_dta_lk) {
            return 0; /* ignore data updates for index */
        }
    }

    *bdberr = BDBERR_BADARGS;
    return -1;
}

/**
 * Apply this log to this transaction
 *
 */
extern int gbl_rowlocks;
int bdb_osql_log_apply_log(bdb_cursor_impl_t *cur, DB_LOGC *curlog,
                           bdb_osql_log_t *log, bdb_osql_trn_t *trn, int *dirty,
                           enum log_ops log_op, int trak, int *bdberr)
{
    tran_type *shadow_tran = bdb_osql_trn_get_shadow_tran(trn);
    bdb_osql_log_rec_t *rec = NULL;
    int rc = 0;

    *bdberr = 0;

    /* process each log record */
    LISTC_FOR_EACH(&log->impl->recs, rec, lnk)
    {
        /* if the transaction was cancelled, stop */
        if (bdb_osql_trn_is_cancelled(trn)) {
            if (trak)
                logmsg(LOGMSG_USER, "TRK_TRN: cur %p trn %p trn is cancelled\n",
                        cur, trn);
            *bdberr = BDBERR_TRAN_CANCELLED;
            return -1;
        }

        /* Only apply if this log's gbl_context occurred after our startgenid */
        if (!gbl_rowlocks && log->impl->commit_genid &&
            bdb_cmp_genids(cur->shadow_tran->startgenid,
                           log->impl->commit_genid) >= 0) {
            if (trak)
                logmsg(LOGMSG_USER, "TRK_TRN: log %p rec %p occurred  NOT "
                                "applicable for trn %p\n",
                        log, rec, trn);
            continue;
        }

        /* does this record applies to us? */
        rc = bdb_osql_log_applicable(cur, rec, bdberr);
        if (rc < 0)
            return rc;

        if (!rc) {
            if (trak)
                logmsg(LOGMSG_USER, "TRK_TRN: log %p rec %p is NOT applicable for trn %p\n",
                        log, rec, trn);
            continue;
        }

        rc = bdb_osql_log_try_run_optimized(cur, curlog, log, rec, trn, dirty,
                                            trak, bdberr);
        if (rc < 0)
            return rc;

        if (rc) /*optimized path */
            continue;

        /* cannot run optimized */
        rc = bdb_osql_log_run_unoptimized(cur, curlog, log, rec, NULL, NULL,
                                          trn, dirty, trak, bdberr);
        if (rc < 0) {
            logmsg(LOGMSG_ERROR, "%s: fail to run unoptimized rc=%d bdberr=%d\n",
                    __func__, rc, *bdberr);
            return rc;
        }
    }

    if (log_op == LOG_APPLY) {
        /* mark the this as the last processed log for cursor shadow
           so next time we know from where to resume
           NOTE: for backfill, this is a one time deal and it does not
           involve lastlog (which pairs with first)
         */

        rc = bdb_osql_shadow_set_lastlog(cur->ifn, log, bdberr);
    }

done:

    return rc;
}

/*
 * Creates osql undo logs for logical operations in a physical commit
 */

bdb_osql_log_t *parse_log_for_shadows_int(bdb_state_type *bdb_state,
                                          DB_LOGC *cur,
                                          DB_LSN *last_logical_lsn,
                                          int is_bkfill,
                                          int skip_logical_commit, int *bdberr)
{
    int rc;
    DBT logdta;
    DB_LSN lsn;
    u_int32_t rectype;
    bdb_osql_log_t *undolog = NULL;
    llog_undo_del_dta_args *del_dta = NULL;
    llog_undo_del_dta_lk_args *del_dta_lk = NULL;
    llog_undo_add_dta_args *add_dta = NULL;
    llog_undo_add_dta_lk_args *add_dta_lk = NULL;
    llog_undo_del_ix_args *del_ix = NULL;
    llog_undo_del_ix_lk_args *del_ix_lk = NULL;
    llog_undo_add_ix_args *add_ix = NULL;
    llog_undo_add_ix_lk_args *add_ix_lk = NULL;
    llog_undo_upd_dta_args *upd_dta = NULL;
    llog_undo_upd_dta_lk_args *upd_dta_lk = NULL;
    llog_undo_upd_ix_args *upd_ix = NULL;
    llog_undo_upd_ix_lk_args *upd_ix_lk = NULL;
    llog_ltran_commit_args *commit = NULL;

    /* Rowlockless comprec will need to remove
     * records from addcur */
    llog_ltran_comprec_args *comprec = NULL;

    /* Rowlocks benchmark record */
    llog_rowlocks_log_bench_args *rl_log = NULL;

    unsigned long long genid = 0;
    int done = 0;

    /*
       if we don't support snapshot/serializable (no callback),
       skip this step
       if (!bdb_state->callback->undoshadow_rtn)
       return 0;
     */

    bzero(&logdta, sizeof(DBT));
    logdta.flags = DB_DBT_REALLOC;

    lsn = *last_logical_lsn;

    if (lsn.file == 0 && lsn.offset == 1) {
        /*
        fprintf(stderr, "%d %s:%d Empty transaction\n",
              pthread_self(), __FILE__, __LINE__);
         */
        return NULL;
    }

    rc = cur->get(cur, &lsn, &logdta, DB_SET);
    if (!rc)
        LOGCOPY_32(&rectype, logdta.data);
    else {
        logmsg(LOGMSG_ERROR, "Unable to get last_logical_lsn\n");
        undolog = NULL;
        goto done;
    }

    /* Logical commit is expected in snapisol: skip over it */
    if (skip_logical_commit) {
        /* Verify that the record is a logical commit */
        if (rectype != DB_llog_ltran_commit) {
            return NULL;
        }
        /* Verify that this commit didn't abort */
        rc = llog_ltran_commit_read(bdb_state->dbenv, logdta.data, &commit);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Unable to get logical commit record\n");
            return NULL;
        }
        if (commit->isabort) {
            free(commit);
            return NULL;
        }
        free(commit);
        commit = NULL;
        goto next;
    } else {
        /* Btree operations have their own unique commits in rowlocks */
        if (is_bkfill && rectype == DB_llog_ltran_commit) {
            undolog = NULL;
            goto done;
        }
    }

    while (rc == 0 && rectype != DB_llog_ltran_start && !done) {
        if (lsn.file == 0 && lsn.offset == 1) {
            logmsg(LOGMSG_ERROR,
                   "reached last lsn but not a begin record type %d?\n",
                   rectype);
            break;
        }

        /* check if we have a delete/update undo for data, which
           is the only thing I care for snapshot/serializable;
           Alternatively, I could grab indexes as well to
           avoid recreating them based on data row + schema.
           For simplicity, we chose to not do that.  This is ok
           since sql switches atomically from one schema to a
           new one during schema change, and there is not way
           the indexes to change from the data row copy to their
           recreation
         */
        /*
        fprintf( stderr, "%d %s:%d rectype=%d\n",
              pthread_self(), __FILE__, __LINE__, rectype);
         */

        switch (rectype) {
        case DB_llog_undo_del_dta:

            rc =
                llog_undo_del_dta_read(bdb_state->dbenv, logdta.data, &del_dta);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0 /*trak*/, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_deldta(undolog, &lsn, del_dta, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(del_dta);
            break;

        case DB_llog_undo_del_dta_lk:

            rc = llog_undo_del_dta_lk_read(bdb_state->dbenv, logdta.data,
                                           &del_dta_lk);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0 /*trak*/, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_deldta_lk(undolog, &lsn, del_dta_lk, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }
            free(del_dta_lk);
            break;

        case DB_llog_undo_del_ix:

            rc = llog_undo_del_ix_read(bdb_state->dbenv, logdta.data, &del_ix);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0 /*trak*/, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_delix(undolog, &lsn, del_ix, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(del_ix);
            break;

        case DB_llog_undo_del_ix_lk:

            rc = llog_undo_del_ix_lk_read(bdb_state->dbenv, logdta.data,
                                          &del_ix_lk);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0 /*trak*/, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_delix_lk(undolog, &lsn, del_ix_lk, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(del_ix_lk);
            break;

        case DB_llog_undo_add_dta:

            rc =
                llog_undo_add_dta_read(bdb_state->dbenv, logdta.data, &add_dta);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_adddta(undolog, &lsn, add_dta, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(add_dta);
            break;

        case DB_llog_undo_add_dta_lk:

            rc = llog_undo_add_dta_lk_read(bdb_state->dbenv, logdta.data,
                                           &add_dta_lk);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_adddta_lk(undolog, &lsn, add_dta_lk, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(add_dta_lk);
            break;

        case DB_llog_undo_add_ix:

            rc = llog_undo_add_ix_read(bdb_state->dbenv, logdta.data, &add_ix);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_addix(undolog, &lsn, add_ix, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(add_ix);
            break;

        case DB_llog_undo_upd_dta:

            rc =
                llog_undo_upd_dta_read(bdb_state->dbenv, logdta.data, &upd_dta);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0 /*trak*/, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_upddta(undolog, &lsn, upd_dta, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(upd_dta);
            break;

        case DB_llog_undo_upd_dta_lk:

            rc = llog_undo_upd_dta_lk_read(bdb_state->dbenv, logdta.data,
                                           &upd_dta_lk);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0 /*trak*/, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_upddta_lk(undolog, &lsn, upd_dta_lk, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(upd_dta_lk);
            break;

        case DB_llog_undo_upd_ix:

            rc = llog_undo_upd_ix_read(bdb_state->dbenv, logdta.data, &upd_ix);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0 /*trak*/, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_updix(undolog, &lsn, upd_ix, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(upd_ix);
            break;

        case DB_llog_undo_upd_ix_lk:

            rc = llog_undo_upd_ix_lk_read(bdb_state->dbenv, logdta.data,
                                          &upd_ix_lk);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0 /*trak*/, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_updix_lk(undolog, &lsn, upd_ix_lk, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(upd_ix_lk);
            break;

        case DB_llog_undo_add_ix_lk:

            rc = llog_undo_add_ix_lk_read(bdb_state->dbenv, logdta.data,
                                          &add_ix_lk);
            if (rc) goto done;

            /* queue the record */
            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }
            rc = bdb_osql_log_addix_lk(undolog, &lsn, add_ix_lk, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(add_ix_lk);
            break;

        case DB_llog_ltran_comprec:
            rc = llog_ltran_comprec_read(bdb_state->dbenv, logdta.data,
                                         &comprec);
            if (rc) goto done;

            if (undolog == NULL) {
                undolog = bdb_osql_log_begin(0, bdberr);
                if (undolog == NULL) {
                    logmsg(LOGMSG_ERROR,
                           "%s: fail to create a serial queue %d\n", __func__,
                           *bdberr);
                    rc = -1;
                    goto done;
                }
            }

            rc =
                bdb_osql_log_comprec(bdb_state, undolog, &lsn, comprec, bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to que record %d %d\n", __func__,
                       rc, *bdberr);
                goto done;
            }

            free(comprec);
            break;

        case DB_llog_ltran_commit: break;

        case DB_llog_ltran_start: break;

        case DB_llog_rowlocks_log_bench: break;

        case DB___dbreg_register:
            /* commits for a dbreg */
            done = 1;
            break;

        default:
            logmsg(LOGMSG_ERROR, "%s:%d Unhandled logical record rectype=%d!\n",
                   __FILE__, __LINE__, rectype);
            done = 1;
            break;
        }

        if (bdb_state->attr->shadows_nonblocking)
            break; /* in nonblocking mode, we only process one record at a time
                      */

        if (done && !undolog) /* make sure no partial understood undo log is
                                 generated */
            break;

    next:
        rc = undo_get_prevlsn(bdb_state, &logdta, &lsn);
        if (rc) break;

#if 0
      printf("tran %016llx prevlsn %d:%d\n", tran->logical_tranid, lsn.file, lsn.offset);
#endif

        if (lsn.file == 0 && lsn.offset == 1) break;

        rc = cur->get(cur, &lsn, &logdta, DB_SET);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Unable to get last_logical_lsn\n");
            break;
        }
        LOGCOPY_32(&rectype, logdta.data);
    }

done:
    if (logdta.data)
        free(logdta.data);

    if (rc) {

        if (undolog) {
            rc = bdb_osql_log_destroy(undolog);
            if (rc) logmsg(LOGMSG_ERROR, "%s fail to destroy log\n", __func__);
        }
        return NULL;
    }

    return undolog;
}

bdb_osql_log_t *parse_log_for_snapisol(bdb_state_type *bdb_state, DB_LOGC *cur,
                                       DB_LSN *last_logical_lsn, int is_bkfill,
                                       int *bdberr)
{
    return parse_log_for_shadows_int(bdb_state, cur, last_logical_lsn,
                                     is_bkfill, 1, bdberr);
}

bdb_osql_log_t *parse_log_for_shadows(bdb_state_type *bdb_state, DB_LOGC *cur,
                                      DB_LSN *last_logical_lsn, int is_bkfill,
                                      int *bdberr)
{
    return parse_log_for_shadows_int(bdb_state, cur, last_logical_lsn,
                                     is_bkfill, 0, bdberr);
}

static int undo_get_ltranid(bdb_state_type *bdb_state, DBT *logdta,
                            unsigned long long *ltranid)
{

    u_int32_t rectype = 0;
    int rc = 0;
    llog_undo_add_dta_args *add_dta;
    llog_undo_add_ix_args *add_ix;
    llog_ltran_commit_args *commit;
    llog_ltran_start_args *start;
    llog_ltran_comprec_args *comprec;
    llog_undo_del_dta_args *del_dta;
    llog_undo_del_ix_args *del_ix;
    llog_undo_upd_dta_args *upd_dta;
    llog_undo_upd_ix_args *upd_ix;

    /* Rowlocks types */
    llog_undo_add_dta_lk_args *add_dta_lk;
    llog_undo_add_ix_lk_args *add_ix_lk;
    llog_undo_del_dta_lk_args *del_dta_lk;
    llog_undo_del_ix_lk_args *del_ix_lk;
    llog_undo_upd_dta_lk_args *upd_dta_lk;
    llog_undo_upd_ix_lk_args *upd_ix_lk;
    void *logp = NULL;

    LOGCOPY_32(&rectype, logdta->data);

    switch (rectype) {

    case DB_llog_undo_add_dta:
        rc = llog_undo_add_dta_read(bdb_state->dbenv, logdta->data, &add_dta);
        if (rc) return rc;
        logp = add_dta;

        *ltranid = add_dta->ltranid;
        break;

    case DB_llog_undo_add_ix:
        rc = llog_undo_add_ix_read(bdb_state->dbenv, logdta->data, &add_ix);
        if (rc) return rc;
        logp = add_ix;
        *ltranid = add_ix->ltranid;
        break;

    case DB_llog_ltran_commit:
        rc = llog_ltran_commit_read(bdb_state->dbenv, logdta->data, &commit);
        if (rc) return rc;
        logp = commit;
        *ltranid = commit->ltranid;
        break;

    case DB_llog_ltran_comprec:
        rc = llog_ltran_comprec_read(bdb_state->dbenv, logdta->data, &comprec);
        if (rc) return rc;
        logp = comprec;
        *ltranid = comprec->ltranid;
        break;

    case DB_llog_ltran_start:
        rc = llog_ltran_start_read(bdb_state->dbenv, logdta->data, &start);
        if (rc) return rc;
        logp = start;
        *ltranid = start->ltranid;
        break;

    case DB_llog_undo_del_dta:
        rc = llog_undo_del_dta_read(bdb_state->dbenv, logdta->data, &del_dta);
        if (rc) return rc;
        logp = del_dta;
        *ltranid = del_dta->ltranid;
        break;

    case DB_llog_undo_del_ix:
        rc = llog_undo_del_ix_read(bdb_state->dbenv, logdta->data, &del_ix);
        if (rc) return rc;
        logp = del_ix;
        *ltranid = del_ix->ltranid;
        break;

    case DB_llog_undo_upd_dta:
        rc = llog_undo_upd_dta_read(bdb_state->dbenv, logdta->data, &upd_dta);
        if (rc) return rc;
        logp = upd_dta;
        *ltranid = upd_dta->ltranid;
        break;

    case DB_llog_undo_upd_ix:
        rc = llog_undo_upd_ix_read(bdb_state->dbenv, logdta->data, &upd_ix);
        if (rc) return rc;
        logp = upd_ix;
        *ltranid = upd_ix->ltranid;
        break;

    case DB_llog_undo_add_dta_lk:
        rc = llog_undo_add_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &add_dta_lk);
        if (rc) return rc;
        logp = add_dta_lk;
        *ltranid = add_dta_lk->ltranid;
        break;

    case DB_llog_undo_add_ix_lk:
        rc = llog_undo_add_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &add_ix_lk);
        if (rc) return rc;
        logp = add_ix_lk;
        *ltranid = add_ix_lk->ltranid;
        break;

    case DB_llog_undo_del_dta_lk:
        rc = llog_undo_del_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &del_dta_lk);
        if (rc) return rc;
        logp = del_dta_lk;
        *ltranid = del_dta_lk->ltranid;
        break;

    case DB_llog_undo_del_ix_lk:
        rc = llog_undo_del_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &del_ix_lk);
        if (rc) return rc;
        logp = del_ix_lk;
        *ltranid = del_ix_lk->ltranid;
        break;

    case DB_llog_undo_upd_dta_lk:
        rc = llog_undo_upd_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &upd_dta_lk);
        if (rc) return rc;
        logp = upd_dta_lk;
        *ltranid = upd_dta_lk->ltranid;
        break;

    case DB_llog_undo_upd_ix_lk:
        rc = llog_undo_upd_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &upd_ix_lk);
        if (rc) return rc;
        logp = upd_ix_lk;
        *ltranid = upd_ix_lk->ltranid;
        break;

    default: rc = -1; break;
    }

    if (logp) free(logp);

    return rc;
}

extern int gbl_new_snapisol;

void bdb_update_ltran_lsns(bdb_state_type *bdb_state, DB_LSN regop_lsn,
                           const void *args, unsigned int rectype)
{
    DB_LOGC *logcur = NULL;
    DBT logdta;
    DB_LSN lsn;
    __txn_regop_args *argp = NULL;
    __txn_regop_gen_args *arggenp = NULL;
    __txn_regop_rowlocks_args *argrlp = NULL;
    unsigned long long ltranid;
    tran_type *ltrans = NULL;
    int rc = 0;
    int bdberr;

    if (!gbl_new_snapisol)
        return;

    if (rectype != DB___txn_regop && rectype != DB___txn_regop_rowlocks &&
        rectype != DB___txn_regop_gen)
        return;

    if (rectype == DB___txn_regop)
        argp = (__txn_regop_args *)args;
    else if (rectype == DB___txn_regop_gen)
        arggenp = (__txn_regop_gen_args *)args;
    else
        argrlp = (__txn_regop_rowlocks_args *)args;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    bzero(&logdta, sizeof(DBT));
    logdta.flags = DB_DBT_REALLOC;

    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logcur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d %s log_cursor rc %d\n", __FILE__, __LINE__,
                __func__, rc);
        return;
    }

    if (rectype == DB___txn_regop) {
        lsn = argp->prev_lsn;
        if (lsn.file == 0 || lsn.offset == 0)
            goto done;

        rc = logcur->get(logcur, &lsn, &logdta, DB_SET);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d %s log_cur->get(%u:%u) rc %d\n", __FILE__,
                    __LINE__, __func__, lsn.file, lsn.offset, rc);
            goto done;
        }

        rc = undo_get_ltranid(bdb_state, &logdta, &ltranid);
        if (rc)
            goto done;
    }

    if (rectype == DB___txn_regop_gen) {
        lsn = arggenp->prev_lsn;
        if (lsn.file == 0 || lsn.offset == 0)
            goto done;

        rc = logcur->get(logcur, &lsn, &logdta, DB_SET);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d %s log_cur->get(%u:%u) rc %d\n", __FILE__,
                    __LINE__, __func__, lsn.file, lsn.offset, rc);
            goto done;
        }

        rc = undo_get_ltranid(bdb_state, &logdta, &ltranid);
        if (rc)
            goto done;
    } else if (rectype == DB___txn_regop_rowlocks) {
        lsn = argrlp->prev_lsn;
        ltranid = argrlp->ltranid;
    }

    pthread_mutex_lock(&bdb_state->translist_lk);
    ltrans = hash_find(bdb_state->logical_transactions_hash, &ltranid);
    if (ltrans) {
        ltrans->last_logical_lsn = lsn;
        ltrans->last_physical_commit_lsn = lsn;
        ltrans->last_regop_lsn = regop_lsn;
    }
    pthread_mutex_unlock(&bdb_state->translist_lk);

done:
    if (logdta.data)
        free(logdta.data);
    logcur->close(logcur, 0);
}

extern int gbl_snapisol;

/**
 * Called during commit once we now there is no way back.
 * This retrieves the list of deleted rows that will become
 * visible after commit and call update_shadows_undo to
 * create a snapshot for snapshot/serializable sessions in progress
 *
 * It scan through log the same way abort_logical_transaction does
 * During scan it creates the list of delete ops
 * If there are any delete ops, it makes an upcall to signal
 * snapshot/serializable sql sessions of the need to update their
 * shadow data files
 *
 */
int update_shadows_beforecommit(bdb_state_type *bdb_state,
                                DB_LSN *last_logical_lsn,
                                unsigned long long *commit_genid, int is_master)
{
    DB_LOGC *cur = NULL;
    bdb_osql_log_t *undolog = NULL;
    extern int gbl_disable_update_shadows;
    int rc = 0;
    int count = 0;
    int bdberr;

    if (gbl_disable_update_shadows)
        return 0;

    /* Return immediately if snapisol isn't enabled */
    if (!gbl_snapisol || gbl_new_snapisol)
        return 0;

    /* Skip entirely if there are no clients */
    if ((rc = bdb_osql_trn_count_clients(&count, !is_master, &bdberr)) !=0 ) {
        logmsg(LOGMSG_ERROR, "%s:%d error counting clients, rc %d\n", __FILE__, __LINE__, rc);
        return rc;
    }

    /* Don't parse if no one cares */
    if (count == 0)
        return 0;

    /* get a log cursor */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d log_cursor rc %d\n", __FILE__, __LINE__, rc);
        return rc;
    }

    /* create the transaction log */
    undolog = parse_log_for_shadows(bdb_state, cur, last_logical_lsn,
                                    0 /* not backfill */, &bdberr);

    if (undolog && undolog->impl && commit_genid)
        undolog->impl->commit_genid = *commit_genid;

    /* close log cursor */
    rc = cur->close(cur, 0);
    if (rc)
        logmsg(LOGMSG_ERROR, "%s fail to close curlog\n", __func__);

    /*
    fprintf( stderr, "%d %s:%d created log %p\n",
          pthread_self(), __FILE__, __LINE__, undolog);
     */

    if (rc == 0 && undolog != NULL) {

        /* register transaction with the rest of the system */
        rc = bdb_osql_log_insert(bdb_state, undolog, &bdberr, is_master);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s fail to register clients %d %d\n", __func__, rc,
                    bdberr);
    } else {
        if (rc)
            logmsg(LOGMSG_ERROR, "%s %d\n", __func__, rc);
        if (undolog) {
            /* printf("XXX %d LOG %p Destroyed (rc=%d), queue count=%d\n",
             * pthread_self(), undolog, rc, log_repo->logs.count); */

            bdb_osql_log_destroy(undolog);
        }
    }

    return rc;
}

static int undo_get_prevlsn(bdb_state_type *bdb_state, DBT *logdta,
                            DB_LSN *prevlsn)
{

    u_int32_t rectype = 0;
    int rc = 0;
    llog_undo_add_dta_args *add_dta;
    llog_undo_add_ix_args *add_ix;
    llog_ltran_commit_args *commit;
    llog_ltran_start_args *start;
    llog_ltran_comprec_args *comprec;
    llog_undo_del_dta_args *del_dta;
    llog_undo_del_ix_args *del_ix;
    llog_undo_upd_dta_args *upd_dta;
    llog_undo_upd_ix_args *upd_ix;

    /* Rowlocks types */
    llog_undo_add_dta_lk_args *add_dta_lk;
    llog_undo_add_ix_lk_args *add_ix_lk;
    llog_undo_del_dta_lk_args *del_dta_lk;
    llog_undo_del_ix_lk_args *del_ix_lk;
    llog_undo_upd_dta_lk_args *upd_dta_lk;
    llog_undo_upd_ix_lk_args *upd_ix_lk;

    llog_rowlocks_log_bench_args *rl_log;
    void *logp = NULL;

    LOGCOPY_32(&rectype, logdta->data);

    switch (rectype) {

    case DB_llog_undo_add_dta:
        rc = llog_undo_add_dta_read(bdb_state->dbenv, logdta->data, &add_dta);
        if (rc)
            return rc;
        logp = add_dta;

        *prevlsn = add_dta->prevllsn;
        break;

    case DB_llog_undo_add_ix:
        rc = llog_undo_add_ix_read(bdb_state->dbenv, logdta->data, &add_ix);
        if (rc)
            return rc;
        logp = add_ix;
        *prevlsn = add_ix->prevllsn;
        break;

    case DB_llog_ltran_commit:
        rc = llog_ltran_commit_read(bdb_state->dbenv, logdta->data, &commit);
        if (rc)
            return rc;
        logp = commit;
        *prevlsn = commit->prevllsn;
        break;

    case DB_llog_ltran_comprec:
        rc = llog_ltran_comprec_read(bdb_state->dbenv, logdta->data, &comprec);
        if (rc)
            return rc;
        logp = comprec;
        *prevlsn = comprec->prevllsn;
        break;

    case DB_llog_ltran_start:
        /* no-op, except we need the previous LSN. and there can be no previous
           LSN since it's a start record. */
        prevlsn->file = 0;
        prevlsn->offset = 1;
        logp = NULL;
        break;

    case DB_llog_undo_del_dta:
        rc = llog_undo_del_dta_read(bdb_state->dbenv, logdta->data, &del_dta);
        if (rc)
            return rc;
        logp = del_dta;
        *prevlsn = del_dta->prevllsn;
        break;

    case DB_llog_undo_del_ix:
        rc = llog_undo_del_ix_read(bdb_state->dbenv, logdta->data, &del_ix);
        if (rc)
            return rc;
        logp = del_ix;
        *prevlsn = del_ix->prevllsn;
        break;

    case DB_llog_undo_upd_dta:
        rc = llog_undo_upd_dta_read(bdb_state->dbenv, logdta->data, &upd_dta);
        if (rc)
            return rc;
        logp = upd_dta;
        *prevlsn = upd_dta->prevllsn;
        break;

    case DB_llog_undo_upd_ix:
        rc = llog_undo_upd_ix_read(bdb_state->dbenv, logdta->data, &upd_ix);
        if (rc)
            return rc;
        logp = upd_ix;
        *prevlsn = upd_ix->prevllsn;
        break;

    case DB_llog_undo_add_dta_lk:
        rc = llog_undo_add_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &add_dta_lk);
        if (rc)
            return rc;
        logp = add_dta_lk;
        *prevlsn = add_dta_lk->prevllsn;
        break;

    case DB_llog_undo_add_ix_lk:
        rc = llog_undo_add_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &add_ix_lk);
        if (rc)
            return rc;
        logp = add_ix_lk;
        *prevlsn = add_ix_lk->prevllsn;
        break;

    case DB_llog_undo_del_dta_lk:
        rc = llog_undo_del_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &del_dta_lk);
        if (rc)
            return rc;
        logp = del_dta_lk;
        *prevlsn = del_dta_lk->prevllsn;
        break;

    case DB_llog_undo_del_ix_lk:
        rc = llog_undo_del_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &del_ix_lk);
        if (rc)
            return rc;
        logp = del_ix_lk;
        *prevlsn = del_ix_lk->prevllsn;
        break;

    case DB_llog_undo_upd_dta_lk:
        rc = llog_undo_upd_dta_lk_read(bdb_state->dbenv, logdta->data,
                                       &upd_dta_lk);
        if (rc)
            return rc;
        logp = upd_dta_lk;
        *prevlsn = upd_dta_lk->prevllsn;
        break;

    case DB_llog_undo_upd_ix_lk:
        rc = llog_undo_upd_ix_lk_read(bdb_state->dbenv, logdta->data,
                                      &upd_ix_lk);
        if (rc)
            return rc;
        logp = upd_ix_lk;
        *prevlsn = upd_ix_lk->prevllsn;
        break;

    case DB_llog_rowlocks_log_bench:
        rc = llog_rowlocks_log_bench_read(bdb_state->dbenv, logdta->data,
                                          &rl_log);
        if (rc)
            return rc;
        logp = rl_log;
        *prevlsn = rl_log->prevllsn;
        break;

    default:
        logmsg(LOGMSG_ERROR, "Unknown log entry type %d\n", rectype);
        abort();
        rc = -1;
        break;
    }

    if (logp)
        free(logp);

    return rc;
}

/**
 * Sync-ed return of last log from log_repo
 *
 */
struct bdb_osql_log *bdb_osql_log_last(int *bdberr)
{
    bdb_osql_log_t *ret = NULL;
    int rc = 0;

    rc = pthread_rwlock_rdlock(&log_repo->tail_lock);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s wrlock %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
        return NULL;
    }

    ret = log_repo->logs.bot;
    if (log_repo->trak)
        logmsg(LOGMSG_ERROR, "REPOLOGS: rtn tail log %p\n", ret);

    rc = pthread_rwlock_unlock(&log_repo->tail_lock);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s unlock %d %d\n", __func__, rc, errno);
        *bdberr = BDBERR_BUG_KILLME;
        return NULL;
    }

    return ret;
}

/* Page-order cursors which have passed this genid should ignore it in addcur */
static int bdb_osql_adjust_pageorder_cursors(bdb_cursor_impl_t *hostcur,
                                             unsigned long long genid,
                                             int dbnum, int stripe, int page,
                                             int index, int *bdberr)
{
    tran_type *shadow_tran = hostcur->shadow_tran;
    bdb_cursor_ifn_t *pcur_ifn;
    bdb_cursor_impl_t *cur;
    int rc;

    /* Iterate through all open cursors. */
    LISTC_FOR_EACH(&shadow_tran->open_cursors, pcur_ifn, lnk)
    {
        cur = pcur_ifn->impl;

        /* Skip if a different table, not a data-table, or not pageorder. */
        if (dbnum != cur->dbnum || cur->type != BDBC_DT || !cur->pageorder) {
            continue;
        }

        /* If its on the vs, force cursor to re-find the last genid before
         * calling next. */
        cur->repo_addcur = 1;
        if (cur->trak) {
            logmsg(LOGMSG_USER, "cur %p now has the repo_addcur flag set.\n",
                    hostcur);
        }

        /* Skip if there's no virtual-stripe skiplist. */
        if (NULL == cur->vs_skip) {
            if (cur->trak) {
                logmsg(LOGMSG_USER, 
                        "cur %p has no vs_skip list, target genid 0x%llx\n",
                        cur, genid);
            }
            continue;
        }

        /* Skip if the cursor hasn't made it to this stripe. */
        if (cur->laststripe < stripe) {
            if (cur->trak) {
                logmsg(LOGMSG_USER, 
                        "cur %p stripe %d has not yet seen genid 0x%llx "
                        "stripe %d\n",
                        cur, cur->laststripe, genid, stripe);
            }
            continue;
        }

        /* Skip if the cursor hasn't made it to this page. */
        if (cur->laststripe == stripe && cur->lastpage < page) {
            if (cur->trak) {
                logmsg(LOGMSG_USER, "cur %p page %d has not yet seen genid 0x%llx "
                                "page %d\n",
                        cur, cur->lastpage, genid, page);
            }
            continue;
        }

        /* FOOBAR trace (I don't think this can happen): we would only see this
         * if
         * we've crossed to the next page and released the pagelock. */
        if (cur->laststripe == stripe && cur->lastpage == page &&
            cur->lastindex < index) {
            if (cur->trak) {
                logmsg(LOGMSG_USER, 
                        "cur %p page %d index %d has not yet seen genid "
                        "0x%llx page %d index %d\n",
                        cur, cur->lastpage, cur->lastindex, genid, page, index);
            }
            logmsg(LOGMSG_ERROR, "%s line %d FOOBAR, it happened!\n", __func__,
                    __LINE__);
            continue;
        }

        /* Add to skip-list while i'm traversing the virtual stripe. */
        if (cur->laststripe == cur->state->attr->dtastripe) {
            if (cur->trak) {
                logmsg(LOGMSG_USER, "cur %p is on the virtual stripe: force a skip "
                                "of genid %llx.\n",
                        cur, genid);
            }
        }

        /* Add to the vs_skip list. */
        rc = bdb_temp_table_insert(cur->state, cur->vs_skip, &genid, 8, NULL, 0,
                                   bdberr);

        /* Some error handling. */
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert returns %d %d.\n",
                    __func__, rc, *bdberr);
            return rc;
        }

        /* Tell cursor_move_merge that we've added a new record. */
        cur->new_skip = 1;

        if (cur->trak) {
            logmsg(LOGMSG_USER, "cur %p stripe %d page %d will skip genid 0x%llx "
                            "stripe %d page %d\n",
                    cur, cur->laststripe, cur->lastpage, genid, stripe, page);
        }
    }
    return 0;
}

/**
 * Returns 0 if this has to run unoptimized (like for indexes)
 * Returns 1 if this has been run (so we skip the unoptimized path)
 * Returns negative for error;
 *
 */
static int bdb_osql_log_try_run_optimized(bdb_cursor_impl_t *cur,
                                          DB_LOGC *curlog, bdb_osql_log_t *log,
                                          bdb_osql_log_rec_t *rec,
                                          bdb_osql_trn_t *trn, int *dirty,
                                          int trak, int *bdberr)
{
    tran_type *shadow_tran = bdb_osql_trn_get_shadow_tran(trn);
    bdb_osql_log_dta_ptr_t dtaptr;
    bdb_osql_log_addc_ptr_t addptr;
    int rc = 0;
    int outrc = 0;
    DBT logdta;
    int offset = 0;
    int dttype;
    int updlen;
    int page = 0;
    int index = 0;
    int version = 0;
    int use_addcur = 0;
    u_int32_t rectype;
    llog_undo_del_dta_args *del_dta = NULL;
    llog_undo_upd_dta_args *upd_dta = NULL;

    *bdberr = 0;

    if (!(bdb_attr_get(cur->state->attr, BDB_ATTR_SQL_OPTIMIZE_SHADOWS) &&
          cur->type == BDBC_DT && rec->dtafile == 0 &&
          (rec->type == DB_llog_undo_del_dta ||
           rec->type == DB_llog_undo_upd_dta)))
        return 0;

    if (trak)
        logmsg(LOGMSG_USER, "TRK_TRN: log %p rec %p is being optimized for %p\n",
                log, rec, trn);

    /* Prepare optimization cookie for addcur temptable. */
    if (cur->pageorder) {
        bzero(&addptr, sizeof(addptr));
        addptr.flag = 0xFF;
        addptr.lsn = rec->lsn;
    }
    /* Prepare optimization cookie for shadow table. */
    else {
        bzero(&dtaptr, sizeof(dtaptr));
        dtaptr.flag = 0xFF;
        dtaptr.lsn = rec->lsn;
    }

    outrc = 0;

    if (rec->type == DB_llog_undo_del_dta) {
        /**
         *  Optimized path defers reconstructing the record, and adding it to
         * the
         *  shadow table until the shadow-cursor actually needs it.  Page-order
         *  cursors will reconstruct immediately, as they need to know the page-
         *  number of this record now to determine whether or not we've already
         *  seen it.
         */
        if (rec->dtafile == 0 && cur->pageorder) {
            /* Retrieve the page and index. */
            rc = bdb_reconstruct_delete(cur->state, &rec->lsn, &page, &index,
                                        NULL, 0, NULL, 0, NULL);
            /* Fail if this fails. */
            if (rc) {
                if (rc == BDBERR_NO_LOG)
                    *bdberr = rc;
                logmsg(LOGMSG_ERROR, "%s: Failed to reconstruct delete\n", __func__);
                return -1;
            }

            /* Send this to the addcur table. */
            rc = bdb_osql_addcur_apply_ll(
                cur->state, cur, rec->dbnum, shadow_tran, rec->genid, BDBC_DT,
                rec->dtastripe, &addptr, sizeof(addptr), cur->trak, bdberr);

            if (0 == rc) {
                rc = bdb_osql_adjust_pageorder_cursors(
                    cur, rec->genid, rec->dbnum, rec->dtastripe, page, index,
                    bdberr);
            }
        }
        /* Normal data. */
        else if (rec->dtafile == 0) {
            rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec,
                                       BDBC_DT, rec->dtastripe, rec->genid,
                                       (char *)&dtaptr, sizeof(dtaptr), NULL, 0,
                                       cur->trak, bdberr);
        }
        /* Optimize the blob. */
        else {
            rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec,
                                       BDBC_BL, rec->dtastripe, rec->genid,
                                       (char *)&dtaptr, sizeof(dtaptr), NULL, 0,
                                       cur->trak, bdberr);
        }

        /* Mark as dirty. */
        *dirty = 1;

        outrc = (rc == 0) ? 1 : rc;
    }
    /* In-place update. */
    else if (rec->type == DB_llog_undo_upd_dta) {
        /* Zero it now. */
        bzero(&logdta, sizeof(logdta));

        /* Ask curlog to allocate memory. */
        logdta.flags = DB_DBT_REALLOC;

        /* Retrieve the logfile. */
        rc = curlog->get(curlog, &rec->lsn, &logdta, DB_SET);
        if (!rc) {
            LOGCOPY_32(&rectype, logdta.data);
        } else {
            if (rc == DB_NOTFOUND) {
                *bdberr = BDBERR_NO_LOG;
                return -1;
            }
            logmsg(LOGMSG_ERROR, "%s: Unable to get log lsn %d:%d!\n", __func__,
                    rec->lsn.file, rec->lsn.offset);
            *bdberr = BDBERR_BUG_KILLME;
            return -1;
        }

        /* Another sanity check. */
        assert(rectype == rec->type);

        /* Read this record to retrieve the old and new genids. */
        rc = llog_undo_upd_dta_read(cur->state->dbenv, logdta.data, &upd_dta);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to read upd_data record, rc = %d.\n",
                    __func__, rc);

            /* Free my logdta. */
            free(logdta.data);

            return -1;
        }

        /* Page order case. */
        if (rec->dtafile == 0 && cur->pageorder) {
            /* We need the page and index for pageorder cursors. */
            if (0 == bdb_inplace_cmp_genids(cur->state, upd_dta->oldgenid,
                                            upd_dta->newgenid)) {
                /* Retrieve the page and index for an inplace update. */
                rc = bdb_reconstruct_inplace_update(
                    cur->state, &rec->lsn, NULL, upd_dta->old_dta_len, &offset,
                    NULL, &page, &index);
            } else {
                /* Retrieve the page and index for a normal (addrem) update. */
                rc = bdb_reconstruct_update(cur->state, &rec->lsn, &page,
                                            &index, NULL, 0, NULL, 0);
            }

            /* Fail if this fails. */
            if (rc || offset) {
                if (rc == BDBERR_NO_LOG)
                    *bdberr = rc;

                logmsg(LOGMSG_ERROR, "%s: Failed to reconstruct delete\n", __func__);

                /* Cleanup logfile cursor. */
                free(logdta.data);

                /* Cleanup update record. */
                free(upd_dta);
                return -1;
            }

            /* Add to the addcur log. */
            rc = bdb_osql_addcur_apply_ll(
                cur->state, cur, rec->dbnum, shadow_tran, rec->genid, BDBC_DT,
                rec->dtastripe, &addptr, sizeof(addptr), cur->trak, bdberr);

            if (0 == rc) {
                rc = bdb_osql_adjust_pageorder_cursors(
                    cur, rec->genid, rec->dbnum, rec->dtastripe, page, index,
                    bdberr);
            }

            /* Cleanup logfile cursor. */
            free(logdta.data);

            /* Cleanup update record. */
            free(upd_dta);
        } else if (rec->dtafile == 0) {
            /* Normal case, add optimized record to the shadow table. */
            rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec,
                                       BDBC_DT, rec->dtastripe, rec->genid,
                                       (char *)&dtaptr, sizeof(dtaptr), NULL, 0,
                                       cur->trak, bdberr);
        } else {
            /* Normal case, add optimized record to the shadow table. */
            rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec,
                                       BDBC_BL, rec->dtastripe, rec->genid,
                                       (char *)&dtaptr, sizeof(dtaptr), NULL, 0,
                                       cur->trak, bdberr);
        }

        /* Add the new-genid to the skip-list if this succeeds. */
        if (0 == rc) {
            rc = bdb_osql_log_apply_ll(
                cur->state, shadow_tran, log, rec, BDBC_SK, 0,
                upd_dta->newgenid, &upd_dta->newgenid,
                sizeof(unsigned long long), NULL, 0, cur->trak, bdberr);
        }

        /* Mark as dirty. */
        *dirty = 1;

        outrc = (rc == 0) ? 1 : rc;
    } else
        abort();

    return outrc;
}

int bdb_osql_set_null_blob_in_shadows(bdb_cursor_impl_t *cur,
                                      bdb_osql_trn_t *trn,
                                      unsigned long long genid, int dbnum,
                                      int blobno, int *bdberr)
{
    tmpcursor_t *tmpcur = NULL;
    tran_type *shadow_tran = bdb_osql_trn_get_shadow_tran(trn);
    unsigned long long sgenid;
    int trak = cur->trak;
    int rc;

    tmpcur = bdb_tran_open_shadow(cur->state, dbnum, shadow_tran, blobno - 1,
                                  BDBC_BL, 1, bdberr);

    if (!tmpcur) {
        logmsg(LOGMSG_ERROR, "bdb_tran_open_shadow %d\n", *bdberr);
        return -1;
    }

    sgenid = get_search_genid(cur->state, genid);

    rc = bdb_temp_table_insert(cur->state, tmpcur, &sgenid, sizeof(sgenid),
                               NULL, 0, bdberr);

    if (trak & SQL_DBG_SHADOW) {
        logmsg(LOGMSG_USER, "INSERTED NULL-BLOB IN SHADOWS FOR BLOBNO %d SEARCH-GENID %llx "
               "GENID %llx\n",
               blobno, sgenid, genid);
    }
    return rc;
}

#ifdef NEWSI_STAT
extern struct timeval log_read_time;
extern struct timeval log_read_time2;
extern struct timeval log_apply_time;
extern unsigned long long num_log_read;
extern unsigned long long num_log_applied_opt;
extern unsigned long long num_log_applied_unopt;
#endif
int bdb_osql_update_shadows_with_pglogs(bdb_cursor_impl_t *cur, DB_LSN lsn,
                                        bdb_osql_trn_t *trn, int *dirty,
                                        int trak, int *bdberr)
{
    tran_type *shadow_tran = bdb_osql_trn_get_shadow_tran(trn);
    bdb_state_type *bdb_state = cur->state;
    int rc = 0;
    DBT logdta;
    DB_LSN undolsn;
    DB_LSN prev_lsn;
    u_int32_t rectype = 0;
    void *key;
    int idx;
    DB_LOGC *logcur = NULL;

    void *free_ptr = NULL;
    bdb_osql_log_addc_ptr_t *addptr;
    int use_addcur = 0;
    int inplace = 0;
    void *keybuf;
    int keylen;
    int updlen;
    void *dtabuf;
    void *freeme;
    int page;
    int index;
    int curpage;
    int curidx;
    int offset;
    int dtalen;
    int skip = 0;

    bdb_osql_log_rec_t *rec = NULL;

    llog_undo_add_dta_args *add_dta = NULL;
    llog_undo_add_ix_args *add_ix = NULL;
    llog_ltran_commit_args *commit = NULL;
    llog_ltran_start_args *start = NULL;
    llog_ltran_comprec_args *comprec = NULL;
    llog_undo_del_dta_args *del_dta = NULL;
    llog_undo_del_ix_args *del_ix = NULL;
    llog_undo_upd_dta_args *upd_dta = NULL;
    llog_undo_upd_ix_args *upd_ix = NULL;

    /* Rowlocks types */
    llog_undo_add_dta_lk_args *add_dta_lk = NULL;
    llog_undo_add_ix_lk_args *add_ix_lk = NULL;
    llog_undo_del_dta_lk_args *del_dta_lk = NULL;
    llog_undo_del_ix_lk_args *del_ix_lk = NULL;
    llog_undo_upd_dta_lk_args *upd_dta_lk = NULL;
    llog_undo_upd_ix_lk_args *upd_ix_lk = NULL;
    void *logp = NULL;

    int i;

    bzero(&logdta, sizeof(DBT));
    logdta.flags = DB_DBT_REALLOC;

    /* Should be the child bdb_state, not the parent. */
    assert(bdb_state->parent != NULL);

#ifdef NEWSI_STAT
    struct timeval before, after, diff;
    gettimeofday(&before, NULL);
#endif

    /* get log cursors */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logcur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d %s log_cursor rc %d\n", __FILE__, __LINE__,
                __func__, rc);
        return rc;
    }

    /* get log */
    rc = logcur->get(logcur, &lsn, &logdta, DB_SET);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d %s log_cur->get(%u:%u) rc %d\n", __FILE__,
                __LINE__, __func__, lsn.file, lsn.offset, rc);
        goto done;
    }
    LOGCOPY_32(&rectype, logdta.data);
#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    timeval_add(&log_read_time, &diff, &log_read_time);
    num_log_read++;
    gettimeofday(&before, NULL);
#endif

    switch (rectype) {
    case DB_llog_undo_del_dta:
    case DB_llog_undo_del_dta_lk: {
        if (rectype == DB_llog_undo_del_dta_lk) {
            rc = llog_undo_del_dta_lk_read(bdb_state->dbenv, logdta.data,
                                           &del_dta_lk);
            free_ptr = del_dta_lk;
            if (rc)
                goto done;

            rec = bdb_osql_deldta_lk_rec(del_dta_lk, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        } else {
            rc =
                llog_undo_del_dta_read(bdb_state->dbenv, logdta.data, &del_dta);
            free_ptr = del_dta;
            if (rc)
                goto done;

            rec = bdb_osql_deldta_rec(del_dta, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        }
    } break;

    case DB_llog_undo_del_ix:
    case DB_llog_undo_del_ix_lk: {
        if (rectype == DB_llog_undo_del_ix_lk) {
            rc = llog_undo_del_ix_lk_read(bdb_state->dbenv, logdta.data,
                                          &del_ix_lk);
            free_ptr = del_ix_lk;
            if (rc)
                goto done;

            rec = bdb_osql_delix_lk_rec(del_ix_lk, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        } else {
            rc = llog_undo_del_ix_read(bdb_state->dbenv, logdta.data, &del_ix);
            free_ptr = del_ix;
            if (rc)
                goto done;

            rec = bdb_osql_delix_rec(del_ix, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        }
    } break;

    case DB_llog_undo_add_dta:
    case DB_llog_undo_add_dta_lk: {
        if (rectype == DB_llog_undo_add_dta_lk) {
            rc = llog_undo_add_dta_lk_read(bdb_state->dbenv, logdta.data,
                                           &add_dta_lk);
            free_ptr = add_dta_lk;
            if (rc)
                goto done;

            rec = bdb_osql_adddta_lk_rec(add_dta_lk, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        } else {
            rc =
                llog_undo_add_dta_read(bdb_state->dbenv, logdta.data, &add_dta);
            free_ptr = add_dta;
            if (rc)
                goto done;

            rec = bdb_osql_adddta_rec(add_dta, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        }
    } break;

    case DB_llog_undo_add_ix:
    case DB_llog_undo_add_ix_lk: {
        if (rectype == DB_llog_undo_add_ix_lk) {
            rc = llog_undo_add_ix_lk_read(bdb_state->dbenv, logdta.data,
                                          &add_ix_lk);
            free_ptr = add_ix_lk;
            if (rc)
                goto done;

            rec = bdb_osql_addix_lk_rec(add_ix_lk, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        } else {
            rc = llog_undo_add_ix_read(bdb_state->dbenv, logdta.data, &add_ix);
            free_ptr = add_ix;
            if (rc)
                goto done;

            rec = bdb_osql_addix_rec(add_ix, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        }
    } break;

    case DB_llog_undo_upd_dta:
    case DB_llog_undo_upd_dta_lk: {
        if (rectype == DB_llog_undo_upd_dta_lk) {
            rc = llog_undo_upd_dta_lk_read(bdb_state->dbenv, logdta.data,
                                           &upd_dta_lk);
            free_ptr = upd_dta_lk;
            if (rc)
                goto done;

            rec = bdb_osql_upddta_lk_rec(upd_dta_lk, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        } else {
            rc =
                llog_undo_upd_dta_read(bdb_state->dbenv, logdta.data, &upd_dta);
            free_ptr = upd_dta;
            if (rc)
                goto done;

            rec = bdb_osql_upddta_rec(upd_dta, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        }
    } break;

    case DB_llog_undo_upd_ix:
    case DB_llog_undo_upd_ix_lk: {
        if (rectype == DB_llog_undo_upd_ix_lk) {
            rc = llog_undo_upd_ix_lk_read(bdb_state->dbenv, logdta.data,
                                          &upd_ix_lk);
            free_ptr = upd_ix_lk;
            if (rc)
                goto done;

            rec = bdb_osql_updix_lk_rec(upd_ix_lk, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        } else {

            rc = llog_undo_upd_ix_read(bdb_state->dbenv, logdta.data, &upd_ix);
            free_ptr = upd_ix;
            if (rc)
                goto done;

            rec = bdb_osql_updix_rec(upd_ix, &lsn, bdberr);
            if (rec == NULL) {
                logmsg(LOGMSG_ERROR, "%s:%d %s get rec failed\n", __FILE__, __LINE__,
                        __func__);
                goto done;
            }
        }
    } break;

    case DB_llog_ltran_commit: {
        skip = 1;
    } break;

    case DB_llog_ltran_comprec: {
        /* TODO */
        skip = 1;
    } break;

    default:
        logmsg(LOGMSG_ERROR, "%s unknown log type %d\n", __func__, rectype);
        *bdberr = BDBERR_BADARGS;
        rc = -1;
        goto done;
    }
#ifdef NEWSI_STAT
    gettimeofday(&after, NULL);
    timeval_diff(&before, &after, &diff);
    timeval_add(&log_read_time2, &diff, &log_read_time2);
#endif

    if (!skip) {
        rc = bdb_osql_log_applicable(cur, rec, bdberr);
        if (rc < 0) {
            goto done;
        }

        if (rc) {
#ifdef NEWSI_DEBUG
            logmsg(LOGMSG_DEBUG,
                   "NEWSI tran %p shadow_tran %p birthlsn[%d][%d] applying log "
                   "lsn[%d][%d] type[%d] genid[%llx] dbnum[%d] dtafile[%d] "
                   "dtastripe[%d]\n",
                   trn, shadow_tran, shadow_tran->birth_lsn.file,
                   shadow_tran->birth_lsn.offset, rec->lsn.file,
                   rec->lsn.offset, rec->type, rec->genid, rec->dbnum,
                   rec->dtafile, rec->dtastripe);
#endif

#ifdef NEWSI_STAT
            gettimeofday(&before, NULL);
#endif
            rc = bdb_osql_log_try_run_optimized(cur, logcur, NULL, rec, trn,
                                                dirty, trak, bdberr);
            if (rc < 0)
                goto done;

            if (rc) {
                rc = 0;
#ifdef NEWSI_STAT
                gettimeofday(&after, NULL);
                timeval_diff(&before, &after, &diff);
                timeval_add(&log_apply_time, &diff, &log_apply_time);
                num_log_applied_opt++;
#endif
                goto done;
            }

            rc = bdb_osql_log_run_unoptimized(cur, logcur, NULL, rec, &logdta,
                                              free_ptr, trn, dirty, trak,
                                              bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to run unoptimized rc=%d bdberr=%d\n",
                        __func__, rc, *bdberr);
                goto done;
            }
#ifdef NEWSI_STAT
            gettimeofday(&after, NULL);
            timeval_diff(&before, &after, &diff);
            timeval_add(&log_apply_time, &diff, &log_apply_time);
            num_log_applied_unopt++;
#endif
        }
    }

done:

    if (free_ptr)
        free(free_ptr);
    if (logdta.data)
        free(logdta.data);
    if (rec) {
        if (rec->table)
            free(rec->table);
        free(rec);
    }
    logcur->close(logcur, 0);

    /* coerce error codes to -1 */
    if (rc)
        rc = -1;
    return rc;
}

static int bdb_osql_log_run_unoptimized(bdb_cursor_impl_t *cur, DB_LOGC *curlog,
                                        bdb_osql_log_t *log,
                                        bdb_osql_log_rec_t *rec, DBT *inlogdta,
                                        void *llog_dta, bdb_osql_trn_t *trn,
                                        int *dirty, int trak, int *bdberr)
{
    tran_type *shadow_tran = bdb_osql_trn_get_shadow_tran(trn);
    bdb_state_type *bdb_state = cur->state;
    DBT logdta;
    llog_undo_del_dta_args *del_dta = NULL;
    llog_undo_del_dta_lk_args *del_dta_lk = NULL;
    llog_undo_add_dta_args *add_dta = NULL;
    llog_undo_add_dta_lk_args *add_dta_lk = NULL;
    llog_undo_upd_dta_args *upd_dta = NULL;
    llog_undo_upd_dta_lk_args *upd_dta_lk = NULL;
    llog_undo_add_ix_args *add_ix = NULL;
    llog_undo_add_ix_lk_args *add_ix_lk = NULL;
    llog_undo_del_ix_args *del_ix = NULL;
    llog_undo_del_ix_lk_args *del_ix_lk = NULL;
    llog_undo_upd_ix_args *upd_ix = NULL;
    llog_undo_upd_ix_lk_args *upd_ix_lk = NULL;
    llog_ltran_comprec_args *comprec = NULL;
    void *free_ptr = NULL;
    bdb_osql_log_addc_ptr_t *addptr;
    u_int32_t rectype;
    int use_addcur = 0;
    int inplace = 0;
    void *keybuf;
    int keylen;
    int updlen;
    void *dtabuf;
    void *freeme;
    int page;
    int index;
    int curpage;
    int curidx;
    int rc = 0;
    int offset;
    int dtalen;
    int outdatalen;

    if (trak)
        logmsg(LOGMSG_USER, 
                "TRK_TRN: log %p rec %p is being NOT optimized for %p\n", log,
                rec, trn);

    /* Should be the child bdb_state, not the parent. */
    assert(bdb_state->parent != NULL);

    if (inlogdta == NULL) {
        bzero(&logdta, sizeof(logdta));
        logdta.flags = DB_DBT_REALLOC;
        rc = curlog->get(curlog, &rec->lsn, &logdta, DB_SET);
        if (!rc)
            LOGCOPY_32(&rectype, logdta.data);
        else {
            if (rc == DB_NOTFOUND) {
                *bdberr = BDBERR_NO_LOG;
                return -1;
            }
            logmsg(LOGMSG_ERROR, "Unable to get log lsn %d:%d!\n", rec->lsn.file,
                    rec->lsn.offset);
            *bdberr = BDBERR_BUG_KILLME;
            return -1;
        }
        assert(rectype == rec->type);
    } else {
        memcpy(&logdta, inlogdta, sizeof(DBT));
    }

    switch (rec->type) {
    case DB_llog_undo_del_dta:
    case DB_llog_undo_del_dta_lk: {
        unsigned long long genid;
        short dtafile, dtastripe;
        int dtalen;

        if (rec->type == DB_llog_undo_del_dta_lk) {
            if (llog_dta) {
                del_dta_lk = llog_dta;
            } else {
                rc = llog_undo_del_dta_lk_read(bdb_state->dbenv, logdta.data,
                                               &del_dta_lk);
                if (rc)
                    goto done;
                free_ptr = del_dta_lk;
            }

            genid = del_dta_lk->genid;
            dtafile = del_dta_lk->dtafile;
            dtastripe = del_dta_lk->dtastripe;
            dtalen = del_dta_lk->dtalen;
        } else {
            if (llog_dta) {
                del_dta = llog_dta;
            } else {
                rc = llog_undo_del_dta_read(bdb_state->dbenv, logdta.data,
                                            &del_dta);
                if (rc)
                    goto done;
                free_ptr = del_dta;
            }

            genid = del_dta->genid;
            dtafile = del_dta->dtafile;
            dtastripe = del_dta->dtastripe;
            dtalen = del_dta->dtalen;
        }

        /* Do nothing if this is already being skipped. */
        if (bdb_tran_deltbl_isdeleted(cur->ifn, genid, 0, bdberr) &&
            dtafile == 0) {
            if (free_ptr)
                free(free_ptr);

            goto done;
        }

        /* Prepare to add to the virtual stripe. */
        if (dtafile == 0 && cur->pageorder) {
            /* Allocate an addc_ptr structure. */
            addptr = malloc(dtalen + sizeof(*addptr));

            /* This is the unoptimized case. */
            addptr->flag = 0;

            /* LSN is really only used for the optimized case. */
            addptr->lsn = rec->lsn;

            /* Reconstruct into the dtabuf. */
            dtabuf = (char *)addptr + sizeof(*addptr);

            /* Set 'freeme'. */
            freeme = addptr;

            /* Set the 'virtual-stripe' flag. */
            use_addcur = 1;
        }
        /* Prepare to add to the shadow table. */
        else {
            /* Allocate a dtaptr structure. */
            dtabuf = malloc(dtalen);

            /* Set 'freeme'. */
            freeme = dtabuf;
        }

        /* Reconstruct the delete. */
        rc = bdb_reconstruct_delete(bdb_state, &rec->lsn, &page, &index, NULL,
                                    0, dtabuf, dtalen, NULL);
        if (rc) {
            if (rc == BDBERR_NO_LOG)
                *bdberr = rc;

            logmsg(LOGMSG_ERROR, "%s:%d Failed to reconstruct delete dta\n",
                    __FILE__, __LINE__);
            free(freeme);
            if (free_ptr)
                free(free_ptr);
            goto done;
        }

        /* Apply update for dtafile. */
        if (dtafile == 0) {
            /*
               Page-order inserts go to the virtual stripe.  While we are
               traversing the virtual stripe, we can sniff these out via the
               genid: temporary adds have a virtual genid, while replication
               stream adds will have a real genid.
             */
            if (use_addcur) {
                if (cur->trak) {
                    logmsg(LOGMSG_USER, "Cur %p adding lsn <%d:%d> to addcur.\n",
                            cur, rec->lsn.file, rec->lsn.offset);
                }

                rc = bdb_osql_addcur_apply_ll(
                    cur->state, cur, rec->dbnum, shadow_tran, rec->genid,
                    BDBC_DT, dtastripe, addptr, dtalen + sizeof(*addptr),
                    cur->trak, bdberr);

                if (0 == rc) {
                    rc = bdb_osql_adjust_pageorder_cursors(
                        cur, rec->genid, rec->dbnum, dtastripe, page, index,
                        bdberr);
                }
            } else {
                /* data */
                rc = bdb_osql_log_apply_ll(
                    cur->state, shadow_tran, log, rec, BDBC_DT, dtastripe,
                    rec->genid, dtabuf, dtalen, NULL, 0, cur->trak, bdberr);
            }

        } else {
            /* Blobs are never traversed in page-order.  */
            rc = bdb_osql_log_apply_ll(
                cur->state, shadow_tran, log, rec, BDBC_BL,
                dtafile - 1, /* first blob is dtafile 1 */
                rec->genid, dtabuf, dtalen, NULL, 0, cur->trak, bdberr);
        }
        /* Mark as dirty. */
        *dirty = 1;
        free(freeme);
        if (free_ptr)
            free(free_ptr);
    } break;

    case DB_llog_undo_del_ix:
    case DB_llog_undo_del_ix_lk: {
        int ix;
        if (rec->type == DB_llog_undo_del_ix_lk) {
            if (llog_dta) {
                del_ix_lk = llog_dta;
            } else {
                rc = llog_undo_del_ix_lk_read(bdb_state->dbenv, logdta.data,
                                              &del_ix_lk);
                if (rc)
                    goto done;
                free_ptr = del_ix_lk;
            }

            keylen = del_ix_lk->keylen;
            dtalen = del_ix_lk->dtalen;
            ix = del_ix_lk->ix;
        } else {
            if (llog_dta) {
                del_ix = llog_dta;
            } else {
                rc = llog_undo_del_ix_read(bdb_state->dbenv, logdta.data,
                                           &del_ix);
                if (rc)
                    goto done;
                free_ptr = del_ix;
            }

            keylen = del_ix->keylen;
            dtalen = del_ix->dtalen;
            ix = del_ix->ix;
        }
        keybuf = malloc(keylen);
        dtabuf = malloc(dtalen + 4 * bdb_state->ixcollattr[ix]);
        outdatalen = 0;
        rc = bdb_reconstruct_delete(
            bdb_state, &rec->lsn, NULL, NULL, keybuf, keylen, dtabuf,
            dtalen + 4 * bdb_state->ixcollattr[ix], &outdatalen);
        if (rc) {
            if (rc == BDBERR_NO_LOG)
                *bdberr = rc;

            logmsg(LOGMSG_ERROR, "%s:%d Failed to reconstruct delete ix\n", __FILE__,
                    __LINE__);
            free(keybuf);
            if (free_ptr)
                free(free_ptr);
            goto done;
        }

        rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec, BDBC_IX,
                                   ix, rec->genid, keybuf, keylen, dtabuf,
                                   outdatalen, cur->trak, bdberr);

        /* Mark as dirty. */
        *dirty = 1;

        if (free_ptr)
            free(free_ptr);
        free(keybuf);
    } break;

    case DB_llog_undo_add_dta:
    case DB_llog_undo_add_dta_lk: {
        unsigned long long genid;
        short dtastripe, dtafile;
        int dtalen;

        if (rec->type == DB_llog_undo_add_dta_lk) {
            if (llog_dta) {
                add_dta_lk = llog_dta;
            } else {
                rc = llog_undo_add_dta_lk_read(bdb_state->dbenv, logdta.data,
                                               &add_dta_lk);
                if (rc)
                    goto done;
                free_ptr = add_dta_lk;
            }

            genid = add_dta_lk->genid;
            dtafile = add_dta_lk->dtafile;
            dtastripe = add_dta_lk->dtastripe;
        } else {
            if (llog_dta) {
                add_dta = llog_dta;
            } else {
                rc = llog_undo_add_dta_read(bdb_state->dbenv, logdta.data,
                                            &add_dta);
                if (rc)
                    goto done;
                free_ptr = add_dta;
            }

            genid = add_dta->genid;
            dtafile = add_dta->dtafile;
            dtastripe = add_dta->dtastripe;
        }

        if (dtafile == 0) {
            rc = bdb_osql_log_apply_ll(
                cur->state, shadow_tran, log, rec, BDBC_SK, 0, rec->genid,
                &genid, sizeof(unsigned long long), NULL, 0, cur->trak, bdberr);
        }
        /* An inplace update to a NULL blob results in an add-record if
         * 'inplace_blobs' is not enabled.  Update the shadow with a NULL.
         * If this was previously deleted as part of a non-NULL update, the
         * lower level code will ignore it.  */
        else {
            rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec,
                                       BDBC_BL, dtafile - 1, rec->genid, NULL,
                                       0, NULL, 0, cur->trak, bdberr);
        }

        if (free_ptr)
            free(free_ptr);
    } break;

    case DB_llog_undo_add_ix:
    case DB_llog_undo_add_ix_lk: {
        unsigned long long genid;

        if (rec->type == DB_llog_undo_add_ix_lk) {
            if (llog_dta) {
                add_ix_lk = llog_dta;
            } else {
                rc = llog_undo_add_ix_lk_read(bdb_state->dbenv, logdta.data,
                                              &add_ix_lk);
                if (rc)
                    goto done;
                free_ptr = add_ix_lk;
            }

            genid = add_ix_lk->genid;
        } else {
            if (llog_dta) {
                add_ix = llog_dta;
            } else {
                rc = llog_undo_add_ix_read(bdb_state->dbenv, logdta.data,
                                           &add_ix);
                if (rc)
                    goto done;
                free_ptr = add_ix;
            }

            genid = add_ix->genid;
        }

        rc = bdb_osql_log_apply_ll(
            cur->state, shadow_tran, log, rec, BDBC_SK, 0, rec->genid, &genid,
            sizeof(unsigned long long), NULL, 0, cur->trak, bdberr);

        if (free_ptr)
            free(free_ptr);
    } break;

    case DB_llog_undo_upd_dta:
    case DB_llog_undo_upd_dta_lk: {
        unsigned long long oldgenid, newgenid;
        short dtastripe, dtafile;
        int old_dta_len;

        if (rec->type == DB_llog_undo_upd_dta_lk) {
            if (llog_dta) {
                upd_dta_lk = llog_dta;
            } else {
                rc = llog_undo_upd_dta_lk_read(bdb_state->dbenv, logdta.data,
                                               &upd_dta_lk);
                if (rc)
                    goto done;
                free_ptr = upd_dta_lk;
            }

            oldgenid = upd_dta_lk->oldgenid;
            newgenid = upd_dta_lk->newgenid;
            dtastripe = upd_dta_lk->dtastripe;
            dtafile = upd_dta_lk->dtafile;
            old_dta_len = upd_dta_lk->old_dta_len;
        } else {
            if (llog_dta) {
                upd_dta = llog_dta;
            } else {
                rc = llog_undo_upd_dta_read(bdb_state->dbenv, logdta.data,
                                            &upd_dta);
                if (rc)
                    goto done;
                free_ptr = upd_dta;
            }

            oldgenid = upd_dta->oldgenid;
            newgenid = upd_dta->newgenid;
            dtastripe = upd_dta->dtastripe;
            dtafile = upd_dta->dtafile;
            old_dta_len = upd_dta->old_dta_len;
        }
        /* Check the skip-list to see if this is already deleted. */
        if (bdb_tran_deltbl_isdeleted(cur->ifn, oldgenid, 0, bdberr) &&
            dtafile == 0) {
            rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec,
                                       BDBC_SK, 0, newgenid, &newgenid,
                                       sizeof(unsigned long long), NULL, 0,
                                       cur->trak, bdberr);

            if (free_ptr)
                free(free_ptr);

            goto done;
        }

        if (0 == bdb_inplace_cmp_genids(bdb_state, oldgenid, newgenid)) {
            inplace = 1;
        }

        /* Prepare to add to the virtual stripe. */
        if (dtafile == 0 && cur->pageorder) {
            /* Allocate a addc_ptr structure. */
            addptr = malloc(old_dta_len + sizeof(*addptr));

            /* This is unoptimized. */
            addptr->flag = 0;

            /* Set lsn even though it's only used in the optimized case. */
            addptr->lsn = rec->lsn;

            /* Reconstruct into the dtabuf. */
            dtabuf = (char *)addptr + sizeof(*addptr);

            /* Set the 'virtual-stripe' flag. */
            use_addcur = 1;

            /* Set freeme. */
            freeme = addptr;
        } else {
            /* Allocate a dtaptr structure. */
            dtabuf = malloc(old_dta_len);

            /* Set freeme. */
            freeme = dtabuf;
        }

        /* If this is inplace, search for a berkley repl log entry.  */
        if (inplace && old_dta_len > 0) {
            rc = bdb_reconstruct_inplace_update(bdb_state, &rec->lsn, dtabuf,
                                                old_dta_len, &offset, &updlen,
                                                &page, &index);

            /* Sanity check results. */
            if (0 == rc)
                assert(offset == 0 && updlen == old_dta_len);
        }
        /* Get the addrem's which correlate to this logical update. */
        else {
            rc = bdb_reconstruct_update(bdb_state, &rec->lsn, &page, &index,
                                        NULL, 0, dtabuf, old_dta_len);
        }
        if (rc) {
            if (rc == BDBERR_NO_LOG)
                *bdberr = rc;

            if (free_ptr)
                free(free_ptr);
            free(freeme);
            goto done;
        }

        if (dtafile == 0) {
            /* Page-order cursors add to the addcur table. */
            if (use_addcur) {
                if (cur->trak) {
                    logmsg(LOGMSG_USER, "Cur %p adding lsn <%d:%d> to addcur.\n",
                            cur, rec->lsn.file, rec->lsn.offset);
                }
                rc = bdb_osql_addcur_apply_ll(
                    cur->state, cur, rec->dbnum, shadow_tran, rec->genid,
                    BDBC_DT, dtastripe, addptr, old_dta_len + sizeof(*addptr),
                    cur->trak, bdberr);

                if (0 == rc) {
                    rc = bdb_osql_adjust_pageorder_cursors(
                        cur, rec->genid, rec->dbnum, dtastripe, page, index,
                        bdberr);
                }
            } else {
                rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec,
                                           BDBC_DT, dtastripe, rec->genid,
                                           dtabuf, old_dta_len, NULL, 0,
                                           cur->trak, bdberr);
            }
        } else {
            rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec,
                                       BDBC_BL, dtafile - 1, rec->genid, dtabuf,
                                       old_dta_len, NULL, 0, cur->trak, bdberr);
        }

        /* Mark as dirty. */
        *dirty = 1;

        if (!rc) {
            /*
            fprintf(stderr, "%d %s:%d masking new genid %llx\n",
                 pthread_self(), __FILE__, __LINE__, newgenid);
             */

            rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec,
                                       BDBC_SK, 0, newgenid, &newgenid,
                                       sizeof(unsigned long long), NULL, 0,
                                       cur->trak, bdberr);
        }

        if (free_ptr)
            free(free_ptr);
        free(freeme);
    } break;

    case DB_llog_undo_upd_ix:
    case DB_llog_undo_upd_ix_lk: {
        int ix;
        unsigned long long oldgenid, newgenid;
        DBT *key;
        int collattrlen;
        char *collattr;
        int offset;
        /*
           this is special, it is in place;
           we need to read the new genid
           to reconstruct it; thing is, I have to walk
           the log in reverse order!
         */
        if (rec->type == DB_llog_undo_upd_ix_lk) {
            if (llog_dta) {
                upd_ix_lk = llog_dta;
            } else {
                rc = llog_undo_upd_ix_lk_read(bdb_state->dbenv, logdta.data,
                                              &upd_ix_lk);
                if (rc)
                    return rc;
                free_ptr = upd_ix_lk;
            }

            ix = upd_ix_lk->ix;
            key = &upd_ix_lk->key;
            oldgenid = upd_ix_lk->oldgenid;
            newgenid = upd_ix_lk->newgenid;
        } else {
            if (llog_dta) {
                upd_ix = llog_dta;
            } else {
                rc = llog_undo_upd_ix_read(bdb_state->dbenv, logdta.data,
                                           &upd_ix);
                if (rc)
                    return rc;
                free_ptr = upd_ix;
            }

            ix = upd_ix->ix;
            key = &upd_ix->key;
            oldgenid = upd_ix->oldgenid;
            newgenid = upd_ix->newgenid;
        }

        rc = bdb_reconstruct_key_update(
            bdb_state, &rec->lsn, (void **)&collattr, &offset, &collattrlen);
        if (rc)
            return rc;

        assert(offset == 0);

        /* No reconstruct: use the key from the logical log-record. */
        rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec, BDBC_IX,
                                   ix, rec->genid, key->data, key->size,
                                   collattr, collattrlen, cur->trak, bdberr);

        /* Mark as dirty. */
        *dirty = 1;

        if (!rc) {
            /*
               fprintf(stderr, "%d %s:%d masking new genid %llx\n",
               pthread_self(), __FILE__, __LINE__, newgenid);
             */

            rc = bdb_osql_log_apply_ll(cur->state, shadow_tran, log, rec,
                                       BDBC_SK, 0, newgenid, &newgenid,
                                       sizeof(unsigned long long), NULL, 0,
                                       cur->trak, bdberr);
        }

        if (free_ptr)
            free(free_ptr);

        if (collattr)
            free(collattr);
    } break;

    /*
     * A comprec will undo a previous write, making the "real" tables look the
     * way they did before the beginning of the txn.  find_merge and
     * move_merge can deal with that unless the record is in the addcur stripe.
     *
     * 1) Remove the genid from the virtual stripe
     * 2) If a cursor hasn't passed the genid in the real btree yet, then
     *    great!  There's nothing left to do.  It will find the record in the
     *    real btree.
     * 3) If a cursor has passed it in the virtual stripe, then great!
     *    There's nothing left to do.  It has already found the record
     *    in the virtual stripe.
     * 4) If a cursor has passed the record in the real btree, but hasn't
     *    passed it in the virtual stripe then we have problems.  Because
     *    we're removing it from the addcur stripe (ahead of the cursor), and
     *    adding it back to the btree (behind the cursor), we won't ever find
     *    it.
     * 5) To solve this, I propose a second, cursor-level stripe that will be
     *    traversed after the addcur stripe.  If there is a genid that will
     *    be skipped by a given cursor, then that genid gets added to the
     *    cursor-level stripe.
     */
    case DB_llog_ltran_comprec: {
        /* TODO */
    } break;

    default:
        logmsg(LOGMSG_ERROR, "%s unknown log type %d\n", __func__, rec->type);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

done:
    if (!inlogdta && logdta.data)
        free(logdta.data);

    /* coerce positive error codes to -1 */
    if (rc > 0)
        rc = -1;

    return rc;
}

/**
 * Retrieve a "row" from the log, using the shadow dta pointer
 *
 */
static int bdb_osql_log_get_optim_data_int(bdb_state_type *bdb_state,
                                           DB_LSN *lsn, void **row, int *rowlen,
                                           int addcur, int *bdberr)
{
    DB_LOGC *curlog = NULL;
    DBT logdta;
    int offset;
    int updlen;
    char *dtabuf;
    char *ptr;
    int len;
    int inplace = 0;
    llog_undo_del_dta_args *del_dta = NULL;
    llog_undo_upd_dta_args *upd_dta = NULL;
    u_int32_t rectype;
    int rc = 0;

    *bdberr = 0;
    *row = NULL;
    *rowlen = 0;

    /* retrieve a log cursor */
    rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &curlog, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d error get log cursor rc %d\n", __FILE__,
                __LINE__, rc);
        *bdberr = rc;
        return -1;
    }

    bzero(&logdta, sizeof(logdta));
    logdta.flags = DB_DBT_REALLOC;
    rc = curlog->get(curlog, lsn, &logdta, DB_SET);
    if (!rc)
        LOGCOPY_32(&rectype, logdta.data);
    else {
        if (rc == DB_NOTFOUND) {
            *bdberr = BDBERR_NO_LOG;
            return -1;
        }
        logmsg(LOGMSG_ERROR, "Unable to get log lsn %d:%d!\n", lsn->file,
                lsn->offset);
        return -1;
    }

    switch (rectype) {
    case DB_llog_undo_del_dta:

        rc = llog_undo_del_dta_read(bdb_state->dbenv, logdta.data, &del_dta);
        if (rc)
            goto done;

        /* If this is going into the addcur stripe, leave room for header. */
        if (addcur) {
            len = del_dta->dtalen + sizeof(bdb_osql_log_addc_ptr_t);
            dtabuf = malloc(len);
            ptr = dtabuf + sizeof(bdb_osql_log_addc_ptr_t);
        } else {
            len = del_dta->dtalen;
            dtabuf = malloc(len);
            ptr = dtabuf;
        }

        rc = bdb_reconstruct_delete(bdb_state, lsn, NULL, NULL, NULL, 0, ptr,
                                    del_dta->dtalen, NULL);
        if (rc) {
            if (rc == BDBERR_NO_LOG)
                *bdberr = rc;

            free(dtabuf);
            free(del_dta);
            goto done;
        }

        free(del_dta);

        /* Set row. */
        *row = dtabuf;

        /* Set rowlength. */
        *rowlen = len;

        break;

    case DB_llog_undo_upd_dta:

        rc = llog_undo_upd_dta_read(bdb_state->dbenv, logdta.data, &upd_dta);
        if (rc)
            goto done;

        assert(upd_dta->dtafile == 0); /* data */

        if (0 == bdb_inplace_cmp_genids(bdb_state, upd_dta->oldgenid,
                                        upd_dta->newgenid)) {
            inplace = 1;
        }

        if (addcur) {
            len = upd_dta->old_dta_len + sizeof(bdb_osql_log_addc_ptr_t);
            dtabuf = malloc(len);
            ptr = dtabuf + sizeof(bdb_osql_log_addc_ptr_t);
        } else {
            len = upd_dta->old_dta_len;
            dtabuf = malloc(len);
            ptr = dtabuf;
        }

        if (inplace) {
            rc = bdb_reconstruct_inplace_update(bdb_state, lsn, ptr,
                                                upd_dta->old_dta_len, &offset,
                                                &updlen, NULL, NULL);

        } else {
            rc = bdb_reconstruct_delete(bdb_state, lsn, NULL, NULL, NULL, 0,
                                        ptr, upd_dta->old_dta_len, NULL);
        }
        if (rc) {
            if (rc == BDBERR_NO_LOG)
                *bdberr = rc;

            free(dtabuf);
            free(upd_dta);
            goto done;
        }
        free(upd_dta);

        /* Set row. */
        *row = dtabuf;

        /* Set rowlen. */
        *rowlen = len;

        break;

    default:
        abort();
    }

done:
    if (logdta.data)
        free(logdta.data);

    /* close log cursor */
    rc = curlog->close(curlog, 0);
    if (rc) {
        *bdberr = rc;
        return 0;
    }

    /* coerce positive error codes to -1 */
    if (rc > 0)
        rc = -1;

    return rc;
}

/**
 * Retrieve optimized
 */
int bdb_osql_log_get_optim_data(bdb_state_type *bdb_state, DB_LSN *lsn,
                                void **row, int *rowlen, int *bdberr)
{
    return bdb_osql_log_get_optim_data_int(bdb_state, lsn, row, rowlen, 0,
                                           bdberr);
}

int bdb_osql_log_get_optim_data_addcur(bdb_state_type *bdb_state, DB_LSN *lsn,
                                       void **row, int *rowlen, int *bdberr)
{
    return bdb_osql_log_get_optim_data_int(bdb_state, lsn, row, rowlen, 1,
                                           bdberr);
}

/**
 * Check if a buffer is a log dta pointer
 */
int bdb_osql_log_is_optim_data(void *buf)
{
    bdb_osql_log_dta_ptr_t *rowptr = (bdb_osql_log_dta_ptr_t *)buf;

    if (rowptr && 0xFF == (uint8_t)rowptr->flag)
        return 1;

    return 0;
}

/**
 * Cleanup any unprocessed undo logs
 *
 */
int bdb_osql_log_unregister(tran_type *tran, bdb_osql_log_t *firstlog,
                            bdb_osql_log_t *lastlog, int trak)
{
    int rc = 0;
    int registered = 0;

    if ((firstlog && !lastlog) || (!firstlog && lastlog)) {
        logmsg(LOGMSG_ERROR, "%s: error first=%p last=%p\n", __func__, firstlog, lastlog);
        return -1;
    }

    rc = pthread_mutex_lock(&log_repo->clients_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_lock %d %d\n", rc, errno);
        return -1;
    }

    registered = bdb_osql_log_undo_required(tran, firstlog);

    assert(registered == 1); /* first needs undo ! */

    do {
        if (registered) {
            assert(firstlog->impl->clients > 0);
            --firstlog->impl->clients;

            /* printf("XXX %p LOG %p decremented (cleanup) %d clients\n",
             * pthread_self(), firstlog, firstlog->impl->clients); */

            if (trak)
                logmsg(LOGMSG_USER, "TRK_LOG: %p decremented clients to %d\n",
                        firstlog, firstlog->impl->clients);
        } else {
            /* printf("XXX %p LOG %p skipped (cleanup) %d clients\n",
             * pthread_self(), firstlog, firstlog->impl->clients); */

            if (trak)
                logmsg(LOGMSG_USER, "TRK_LOG: %p skipped clients still %d\n",
                        firstlog, firstlog->impl->clients);
        }

        if (firstlog == lastlog) {
            break;
        }
        firstlog = firstlog->lnk.next;
        assert(firstlog != 0);

        registered = bdb_osql_log_undo_required(tran, firstlog);
    } while (1);

    rc = pthread_mutex_unlock(&log_repo->clients_mtx);
    if (rc) {
        logmsg(LOGMSG_ERROR, "pthread_mutex_unlock %d %d\n", rc, errno);
        return -1;
    }

    return 0;
}

/**
 * Determine if a log is applyable for this
 * transaction.
 * NOTE: for inplace updates, new records
 * look old, so the check is actually more
 * conservative (so working, albeit not perfect)
 *
 */
int bdb_osql_log_undo_required(tran_type *tran, bdb_osql_log_t *log)
{
    if (!tran->startgenid /* before any commit */ ||
        (bdb_cmp_genids(tran->startgenid, log->impl->oldest_genid) > 0 &&
         (!log->impl->commit_genid ||
          (bdb_cmp_genids(tran->startgenid, log->impl->commit_genid) < 0)))) {
#if 0
#include <ctrace.h>
   ctrace("Applying log %p to tran %llx tran->startgenid = %llx log->oldest_genid = %llx log->commit_genid = %llx\n",
           log, tran->logical_tranid, tran->startgenid, log->impl->oldest_genid, log->impl->commit_genid);
#endif
        return 1;
    }

    return 0;
}
/*
vi ts=3:sw=3
*/
