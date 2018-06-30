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
 *
 *  ABSTRACTION FOR A BERKDB BTREE
 *
 *  The berkdb can be a persistent/real file or a temp/shadow file.
 *  (i.e. calling persistent files *  real, as opposed to shadow).
 *  The operations are cursor based:
 *  - open (creator function)    (X)
 *  - close                      (X)
 *  - first                      (X)
 *  - last                       (X)
 *  - prev                       (X)
 *  - next                       (X)
 *  - find                       (X)
 *  - insert                     (X)
 *  - delete                     (X)
 *
 *  Abstracts:
 *    - all DBT management
 *    - bulk processing
 *    - able to use a persistent btree or a temp table as backed
 *
 *  There is NO knowledge of what btree-s contains, this abstraction provides
 *  a uniform way to use temp_tables together with persistent files under the
 *  same api.
 *  There is no knowledge of stripes, indexes vs data, genid, dup keys or
 *simple.
 *  There is no effort to keep the cursor pointed to something if a lookup
 *fails.
 *  Each operation updates only the piece of info that has knowledge of, instead
 *  of updating the whole berkdb state.
 *  All the extra semantic that used to be meshed in this code is now handled
 *  in the upper layer cursor.c (cough bdb_sql.c).
 *
 *  Game Goal:
 *  - Merging temp table DBT management here and maybe unify the api
 *
 *  Dreaming:
 *  - Would be nice to merge this with index path, but for now I need this to
 *work
 *    first
 *
 * NOTE on return codes
 * - there is no PASTEOF return from these functions; a move operation that
 *fails to
 *   find a target returns IX_NOTFND (matchin DB_NOTFOUND);  it is up to the
 *cursor.c
 *   abstraction to convert this to a IX_PASTEOF in those cases when we do a
 *NEXT/PREV
 *   that fail to retrieve a new record!
 * - accepted return codes:
 *   IX_FND       : found what was asked for, row cached in berkdb object
 *   IX_NOTFND    : not found what was looked for; if berkdb was pointing
 *somewhere, it gets
 *                  to keep the position
 *   IX_EMPTY     : returned for a first/last move that does not find a row
 *                  (TODO: WE COULD HANDLE IX_EMPTY IN SAME WAY PASTEOF IS
 *HANDLED)
 *   <0 & bdberr: errors
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <alloca.h>
#include <string.h>
#include <assert.h>

#include <thread_malloc.h>

#include <build/db.h>

#include "bdb_cursor.h"
#include "bdb_int.h"
#include "cursor_ll.h"
#include "bdb_osqlcur.h"
#include "bdb_osqllog.h"
#include "logmsg.h"
#include "util.h"

static unsigned int berkdb_counter = 0;

static int bdb_berkdb_unlock(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_lock(bdb_berkdb_t *pberkdb, cursor_tran_t *curtran,
                           int *bdberr);
static int bdb_berkdb_move(bdb_berkdb_t *pberkdb, int dir, int *bdberr);
static int bdb_berkdb_dta(bdb_berkdb_t *pberkdb, char **dta, int *bdberr);
static int bdb_berkdb_dtasize(bdb_berkdb_t *pberkdb, int *dtasize, int *bdberr);
static int bdb_berkdb_key(bdb_berkdb_t *pberkdb, char **key, int *bdberr);
static int bdb_berkdb_keysize(bdb_berkdb_t *pberkdb, int *keysize, int *bdberr);
static int bdb_berkdb_ver(bdb_berkdb_t *pberkdb, uint8_t *ver, int *bdberr);
static int bdb_berkdb_insert(bdb_berkdb_t *pberkdb, char *key, int keylen,
                             char *dta, int dtalen, int *bdberr);
static int bdb_berkdb_delete(bdb_berkdb_t *pberkdb, int *bdberr);
static void bdb_berkdb_trak(bdb_berkdb_t *pberkdb, int status);
static int bdb_berkdb_get_everything(struct bdb_berkdb *, char **dta,
                                     int *dtasize, char **key, int *keysize,
                                     uint8_t *ver, int *bdberr);
static int bdb_berkdb_is_at_eof(struct bdb_berkdb *pberkdb);

static int bdb_berkdb_prevent_optimized(struct bdb_berkdb *);
static int bdb_berkdb_allow_optimized(struct bdb_berkdb *);

static int bdb_berkdb_close_real(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_first_real(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_next_real(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_prev_real(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_last_real(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_find_real(bdb_berkdb_t *pberkdb, void *key, int keylen,
                                int how, int *bdberr);

static int bdb_berkdb_get_skip_stat(bdb_berkdb_t *pberkdb, u_int64_t *nextcount,
                                    u_int64_t *skipcount);

static int bdb_berkdb_close_shad(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_first_shad(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_next_shad(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_prev_shad(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_last_shad(bdb_berkdb_t *pberkdb, int *bdberr);
static int bdb_berkdb_find_shad(bdb_berkdb_t *pberkdb, void *key, int keylen,
                                int how, int *bdberr);

static int bdb_berkdb_cget(bdb_berkdb_impl_t *berkdb, int use_bulk, int how,
                           int *bdberr);

static int bdb_berkdb_get_pagelsn(bdb_berkdb_t *pberkdb, DB_LSN *lsn,
                                  int *bdberr);
static int bdb_berkdb_get_fileid(bdb_berkdb_t *pberkdb, void *pfileid,
                                 int *bdberr);

static int bdb_berkdb_handle_optimized(bdb_berkdb_t *pberkdb, int *bdberr);

static int prevent_optimized(bdb_berkdb_t *pberkdb);
static int allow_optimized(bdb_berkdb_t *pberkdb);
static int outoforder_set(bdb_berkdb_t *pberkdb, int status);
static int outoforder_get(bdb_berkdb_t *pberkdb);
static int bdb_berkdb_get_pageindex(bdb_berkdb_t *pberkdb, int *page,
                                    int *index, int *bdberr);
static int bdb_berkdb_defer_update_shadows(bdb_berkdb_t *pberkdb);

/* These routines are in cursor_rowlocks.c */
int bdb_berkdb_rowlocks_unlock(struct bdb_berkdb *pberkdb, int *bdberr);
int bdb_berkdb_rowlocks_lock(struct bdb_berkdb *pberkdb,
                             struct cursor_tran *curtran, int *bdberr);
int bdb_berkdb_rowlocks_move(struct bdb_berkdb *berkdb, int dir, int *bdberr);
int bdb_berkdb_rowlocks_first(struct bdb_berkdb *berkdb, int *bdberr);
int bdb_berkdb_rowlocks_next(struct bdb_berkdb *berkdb, int *bdberr);
int bdb_berkdb_rowlocks_prev(struct bdb_berkdb *berkdb, int *bdberr);
int bdb_berkdb_rowlocks_last(struct bdb_berkdb *berkdb, int *bdberr);
int bdb_berkdb_rowlocks_close(struct bdb_berkdb *berkdb, int *bdberr);
int bdb_berkdb_rowlocks_get_pagelsn(struct bdb_berkdb *berkdb, DB_LSN *lsn,
                                    int *bdberr);
int bdb_berkdb_rowlocks_dta(struct bdb_berkdb *berkdb, char **dta, int *bdberr);
int bdb_berkdb_rowlocks_dtasize(struct bdb_berkdb *berkdb, int *dtasize,
                                int *bdberr);
int bdb_berkdb_rowlocks_key(struct bdb_berkdb *berkdb, char **key, int *bdberr);
int bdb_berkdb_rowlocks_keysize(struct bdb_berkdb *berkdb, int *keysize,
                                int *bdberr);
int bdb_berkdb_rowlocks_find(struct bdb_berkdb *berkdb, void *key, int keysize,
                             int how, int *bdberr);
int bdb_berkdb_rowlocks_insert(struct bdb_berkdb *berkdb, char *key, int keylen,
                               char *dta, int dtalen, int *bdberr);
int bdb_berkdb_rowlocks_delete(struct bdb_berkdb *berkdb, int *bdberr);
int bdb_berkdb_rowlocks_get_pageindex(bdb_berkdb_t *pberkdb, int *page,
                                      int *index, int *bdberr);
int bdb_berkdb_rowlocks_get_fileid(bdb_berkdb_t *pberkdb, void *pfileid,
                                   int *bdberr);
int bdb_berkdb_rowlocks_pause(bdb_berkdb_t *pberkdb, int *bdberr);
int bdb_berkdb_rowlocks_init(struct bdb_berkdb *berkdb, DB *db, int *bdberr);
int bdb_berkdb_rowlocks_prevent_optimized(struct bdb_berkdb *berkdb);
int bdb_berkdb_rowlocks_allow_optimized(struct bdb_berkdb *berkdb);
int bdb_berkdb_rowlocks_get_skip_stat(struct bdb_berkdb *berkdb,
                                      u_int64_t *nextcount,
                                      u_int64_t *skipcount);
int bdb_berkdb_rowlocks_defer_update_shadows(struct bdb_berkdb *berkdb);

/* factory method */
bdb_berkdb_t *bdb_berkdb_open(bdb_cursor_impl_t *cur, int type, int maxdata,
                              int maxkey, int *bdberr)
{
    bdb_berkdb_t *pberkdb;
    bdb_berkdb_impl_t *berkdb;
    bdb_state_type *bdb_state = cur->state;
    DB *db = NULL;
    DBC *dbc = NULL;
    DB_LSN curlsn, lwmlsn;
    int rc = 0;
    int lwmrc = 0;
    int cmp;
    u_int32_t curflags = 0;

    *bdberr = 0;

    pberkdb = (bdb_berkdb_t *)thread_malloc(sizeof(bdb_berkdb_t));
    if (!pberkdb) {
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }
    bzero(pberkdb, sizeof(bdb_berkdb_t));

    berkdb = pberkdb->impl =
        (bdb_berkdb_impl_t *)thread_malloc(sizeof(bdb_berkdb_impl_t));
    if (!berkdb) {
        *bdberr = BDBERR_MALLOC;
        thread_free(pberkdb);
        return NULL;
    }
    bzero(berkdb, sizeof(bdb_berkdb_impl_t));

    berkdb->bdb_state = bdb_state;

    if (type == BERKDB_REAL) {
        DBT *dbt = &berkdb->u.rl.data;

        dbt->data = thread_malloc(maxdata + maxkey);
        if (!dbt->data) {
            *bdberr = BDBERR_MALLOC;
            return NULL;
        }
        dbt->ulen = maxdata;
        dbt->flags = DB_DBT_USERMEM;

        DBT *dbk = &berkdb->u.rl.key;
        dbk->data = ((char *)berkdb->u.rl.data.data) + maxdata;
        dbk->ulen = maxkey;
        dbk->flags = DB_DBT_USERMEM;

        /* shadows will be set based on temp_table */
        berkdb->u.rl.bulk.ulen = bdb_state->attr->sqlbulksz;

        if (cur->type == BDBC_DT) {
            DBT *dbth = &berkdb->u.rl.odh;

            dbth->flags = DB_DBT_USERMEM;
            dbth->ulen = MAXRECSZ;

            /* Maximum record size. */
            berkdb->u.rl.odh_tmp = thread_malloc(MAXRECSZ);
            if (!berkdb->u.rl.odh_tmp) {
                *bdberr = BDBERR_MALLOC;
                logmsg(LOGMSG_ERROR, "%s:%d malloc error %d\n", __FILE__, __LINE__,
                        MAXRECSZ);
                thread_free(berkdb->u.rl.data.data);
                thread_free(berkdb);
                thread_free(pberkdb);
                return NULL;
            }
            berkdb->u.rl.use_odh = 1;

            db = bdb_state->dbp_data[0][cur->idx];
        } else {
            if (cur->idx < -1 || cur->idx > cur->state->numix) {
                logmsg(LOGMSG_ERROR, 
                        "%s: incorrect index %d must be between -1 and %d\n",
                        __func__, cur->idx, bdb_state->numix);
                cheap_stack_trace();
                *bdberr = BDBERR_BADARGS;
                thread_free(berkdb->u.rl.data.data);
                thread_free(berkdb);
                thread_free(pberkdb);
                return NULL;
            }
            /* Indexes do not use odh. */
            berkdb->u.rl.use_odh = 0;

            db = cur->state->dbp_ix[cur->idx];
        }

        if (!db) {
            *bdberr = BDBERR_BADARGS;
            if (berkdb->u.rl.use_odh)
                thread_free(berkdb->u.rl.odh_tmp);
            thread_free(berkdb->u.rl.data.data);
            thread_free(berkdb);
            thread_free(pberkdb);
            return NULL;
        }

        berkdb->u.rl.id = berkdb_counter++;

        /* Set appropriate cursor flags. */
        if (cur->pageorder) {
            curflags |= DB_PAGE_ORDER;

            if (cur->discardpages)
                curflags |= DB_DISCARD_PAGES;
        }

        dbc =
            get_cursor_for_cursortran_flags(cur->curtran, db, curflags, bdberr);

        if (!dbc) {
            if (cur->state->attr->dbgberkdbcursor)
                logmsg(LOGMSG_USER, "BERKDB=%d %p cursor dbc=%p rc=%d bdberr=%d "
                                "OPEN [%s %d] (%p %d)n",
                        berkdb->u.rl.id, berkdb, dbc, rc, *bdberr,
                        cursortype(cur->type), cur->idx, cur->curtran,
                        bdb_get_lid_from_cursortran(cur->curtran));
            if (berkdb->u.rl.use_odh)
                thread_free(berkdb->u.rl.odh_tmp);
            thread_free(berkdb->u.rl.data.data);
            thread_free(berkdb);
            thread_free(pberkdb);
            return NULL;
        }

        if (cur->state->attr->dbgberkdbcursor)
            logmsg(LOGMSG_USER, "BERKDB=%d %p cursor dbc=%p rc=%d bdberr=%d OPEN "
                            "[%s %d] (%p %d)\n",
                    berkdb->u.rl.id, berkdb, dbc, rc, *bdberr,
                    cursortype(cur->type), cur->idx, cur->curtran,
                    bdb_get_lid_from_cursortran(cur->curtran));

        /*berkdb->u.rl.db = db;*/
        berkdb->u.rl.dbc = dbc;
        pberkdb->unlock = bdb_berkdb_unlock;
        pberkdb->lock = bdb_berkdb_lock;
        pberkdb->move = bdb_berkdb_move;
        pberkdb->first = bdb_berkdb_first_real;
        pberkdb->next = bdb_berkdb_next_real;
        pberkdb->prev = bdb_berkdb_prev_real;
        pberkdb->last = bdb_berkdb_last_real;
        pberkdb->close = bdb_berkdb_close_real;
        pberkdb->get_pagelsn = bdb_berkdb_get_pagelsn;
        pberkdb->find = bdb_berkdb_find_real;
        pberkdb->get_skip_stat = bdb_berkdb_get_skip_stat;
        pberkdb->pageindex = bdb_berkdb_get_pageindex;
        pberkdb->fileid = bdb_berkdb_get_fileid;
        pberkdb->defer_update_shadows = bdb_berkdb_defer_update_shadows;
    } else if (type == BERKDB_REAL_ROWLOCKS) {
        if (cur->type == BDBC_DT) {
            db = bdb_state->dbp_data[0][cur->idx];
        } else {
            if (cur->idx < -1 || cur->idx > cur->state->numix) {
                logmsg(LOGMSG_ERROR, 
                        "%s: incorrect index %d must be between -1 and %d\n",
                        __func__, cur->idx, bdb_state->numix);
                cheap_stack_trace();
                *bdberr = BDBERR_BADARGS;
                thread_free(berkdb);
                thread_free(pberkdb);
                return NULL;
            }
            db = cur->state->dbp_ix[cur->idx];
        }

        /* Set appropriate cursor flags. */
        if (cur->pageorder) {
            curflags |= DB_PAGE_ORDER;
            if (cur->discardpages)
                curflags |= DB_DISCARD_PAGES;
        }

        /* Rowlocks cursors must be pausible */
        curflags |= DB_PAUSIBLE;

        dbc =
            get_cursor_for_cursortran_flags(cur->curtran, db, curflags, bdberr);

        if (!dbc) {
            if (cur->state->attr->dbgberkdbcursor)
                logmsg(LOGMSG_USER, "BERKDB=%d %p cursor dbc=%p rc=%d bdberr=%d "
                                "OPEN [%s %d] (%p %d)n",
                        berkdb->u.rl.id, berkdb, dbc, rc, *bdberr,
                        cursortype(cur->type), cur->idx, cur->curtran,
                        bdb_get_lid_from_cursortran(cur->curtran));
            if (berkdb->u.rl.use_odh)
                thread_free(berkdb->u.rl.odh_tmp);
            thread_free(berkdb->u.rl.data.data);
            thread_free(berkdb);
            thread_free(pberkdb);
            return NULL;
        }

        pberkdb->impl->u.row.pagelock_cursor = dbc;

        bdb_berkdb_rowlocks_get_fileid(pberkdb, pberkdb->impl->u.row.fileid,
                                       bdberr);

        /* Get the current lsn */
        __log_txn_lsn(bdb_state->dbenv, &curlsn, NULL, NULL);

        /* Get the lwm lsn */
        lwmrc = bdb_get_lsn_lwm(bdb_state, &lwmlsn);

        /* Compare to avoid racing */
        if (lwmlsn.file == 0 || 0 != lwmrc ||
            ((cmp = log_compare(&curlsn, &lwmlsn)) < 0)) {
            /* Lowest is curlsn */
            pberkdb->impl->u.row.lwmlsn.file = curlsn.file;
            pberkdb->impl->u.row.lwmlsn.offset = curlsn.offset;
        } else {
            /* Lowest is lwm */
            pberkdb->impl->u.row.lwmlsn.file = lwmlsn.file;
            pberkdb->impl->u.row.lwmlsn.offset = lwmlsn.offset;
        }

        /* lock/unlock? - what do these do */
        pberkdb->lock = bdb_berkdb_rowlocks_lock;
        pberkdb->unlock = bdb_berkdb_rowlocks_unlock;
        pberkdb->move = bdb_berkdb_rowlocks_move;
        pberkdb->first = bdb_berkdb_rowlocks_first;
        pberkdb->next = bdb_berkdb_rowlocks_next;
        pberkdb->prev = bdb_berkdb_rowlocks_prev;
        pberkdb->last = bdb_berkdb_rowlocks_last;
        pberkdb->close = bdb_berkdb_rowlocks_close;
        pberkdb->get_pagelsn = bdb_berkdb_rowlocks_get_pagelsn;
        pberkdb->dta = bdb_berkdb_rowlocks_dta;
        pberkdb->dtasize = bdb_berkdb_rowlocks_dtasize;
        pberkdb->key = bdb_berkdb_rowlocks_key;
        pberkdb->keysize = bdb_berkdb_rowlocks_keysize;
        pberkdb->find = bdb_berkdb_rowlocks_find;
        pberkdb->get_skip_stat = bdb_berkdb_rowlocks_get_skip_stat;
        pberkdb->pageindex = bdb_berkdb_rowlocks_get_pageindex;
        pberkdb->fileid = bdb_berkdb_rowlocks_get_fileid;
        pberkdb->pause = bdb_berkdb_rowlocks_pause;
        pberkdb->defer_update_shadows =
            bdb_berkdb_rowlocks_defer_update_shadows;
    } else if (type == BERKDB_SHAD || type == BERKDB_SHAD_CREATE) {
        berkdb->u.sd.cur = bdb_osql_open_backfilled_shadows(
            cur, cur->shadow_tran->osql, type, bdberr);
        if (*bdberr) {
            if (*bdberr == BDBERR_TRANTOOCOMPLEX ||
                *bdberr == BDBERR_TRAN_CANCELLED || *bdberr == BDBERR_NO_LOG) {
                thread_free(berkdb);
                thread_free(pberkdb);
                return NULL;
            } else
                logmsg(LOGMSG_ERROR, "%s failure to backfill shadow %d\n", __func__,
                        *bdberr);
        }
#if 0
      berkdb->u.sd.cur = bdb_tran_open_shadow( cur->state, cur->dbnum,
            cur->shadow_tran, cur->idx,
            cur->type, (type==BERKDB_SHAD_CREATE), bdberr);
#endif

        if (!berkdb->u.sd.cur) {
            /* normal case in which the shadow was not present and
               there is no backfill for it */
            thread_free(berkdb);
            thread_free(pberkdb);
            return NULL;
        }

        /* Need odh logic for shadow-tables. */
        if (cur->type == BDBC_DT) {
            berkdb->u.sd.use_odh = 1;
            berkdb->u.sd.odh_tmp = thread_malloc(MAXRECSZ);
            if (!berkdb->u.sd.odh_tmp) {
                *bdberr = BDBERR_MALLOC;
                logmsg(LOGMSG_ERROR, "%s:%d malloc error %d\n", __FILE__, __LINE__,
                        MAXRECSZ);
                thread_free(berkdb);
                thread_free(pberkdb);
                return NULL;
            }
        }
        pberkdb->move = bdb_berkdb_move;
        pberkdb->first = bdb_berkdb_first_shad;
        pberkdb->next = bdb_berkdb_next_shad;
        pberkdb->prev = bdb_berkdb_prev_shad;
        pberkdb->last = bdb_berkdb_last_shad;
        pberkdb->close = bdb_berkdb_close_shad;
        pberkdb->find = bdb_berkdb_find_shad;

        /* Normalize type to BERKDB_SHAD. */
        type = BERKDB_SHAD;
    } else {
        logmsg(LOGMSG_ERROR, "%s: ugh?\n", __func__);
        *bdberr = BDBERR_BUG_KILLME;
        return NULL;
    }

    pberkdb->insert = bdb_berkdb_insert;
    pberkdb->delete = bdb_berkdb_delete;
    pberkdb->outoforder_get = outoforder_get;
    pberkdb->outoforder_set = outoforder_set;
    pberkdb->is_at_eof = bdb_berkdb_is_at_eof;

    pberkdb->ver = bdb_berkdb_ver;

    berkdb->cur = cur;

    berkdb->type = type;

    berkdb->trak = cur->trak;

    if (type == BERKDB_REAL_ROWLOCKS) {
        pberkdb->dta = bdb_berkdb_rowlocks_dta;
        pberkdb->dtasize = bdb_berkdb_rowlocks_dtasize;
        pberkdb->key = bdb_berkdb_rowlocks_key;
        pberkdb->keysize = bdb_berkdb_rowlocks_keysize;
        pberkdb->trak = bdb_berkdb_trak;

        if (cur->type == BDBC_DT)
            db = bdb_state->dbp_data[0][cur->idx];
        else
            db = bdb_state->dbp_ix[cur->idx];

        rc = bdb_berkdb_rowlocks_init(pberkdb, db, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Can't open cursor rc %d bdberr %d\n", rc, *bdberr);
            return NULL;
        }
        pberkdb->prevent_optimized = bdb_berkdb_rowlocks_prevent_optimized;
        pberkdb->allow_optimized = bdb_berkdb_rowlocks_allow_optimized;
    } else {
        pberkdb->dta = bdb_berkdb_dta;
        pberkdb->dtasize = bdb_berkdb_dtasize;
        pberkdb->key = bdb_berkdb_key;
        pberkdb->keysize = bdb_berkdb_keysize;
        pberkdb->insert = bdb_berkdb_insert;
        pberkdb->delete = bdb_berkdb_delete;
        pberkdb->trak = bdb_berkdb_trak;
        pberkdb->get_everything = bdb_berkdb_get_everything;
        pberkdb->prevent_optimized = bdb_berkdb_prevent_optimized;
        pberkdb->allow_optimized = bdb_berkdb_allow_optimized;
    }

    pberkdb->outoforder_set(pberkdb, 1);

    return pberkdb;
}

static inline void reset_bulk_bt(bdb_realdb_tag_t *bt)
{
    bt->bulkptr = NULL;
    bt->lastdtasize = 0;
    bt->lastdta = NULL;
    bt->use_bulk = 0;
}

static inline int bdb_berkdb_get_pagelsn(bdb_berkdb_t *pberkdb, DB_LSN *lsn,
                                         int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    return bt->dbc->c_get_pagelsn(bt->dbc, lsn);
}

static int bdb_berkdb_get_fileid(bdb_berkdb_t *pberkdb, void *pfileid,
                                 int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    int rc;

    if (bt->dbc) {
        if ((rc = bt->dbc->c_get_fileid(bt->dbc, bt->fileid)) != 0)
            return rc;
    }
    memcpy(pfileid, bt->fileid, DB_FILE_ID_LEN);
    return 0;
}

static unsigned long long defer_count = 0;

static int bdb_berkdb_defer_update_shadows(bdb_berkdb_t *pberkdb)
{
    bdb_berkdb_impl_t *cur = pberkdb->impl;
    bdb_realdb_tag_t *bt = &cur->u.rl;

    if (bt->need_update_shadows)
        return 0;

    defer_count++;

#ifdef CONFIRM_DEFER
    static int lastpr = 0;
    int now;

    if ((now = time(NULL)) != lastpr) {
        logmsg(LOGMSG_WARN, "Deferring update shadows, defer-count=%llu\n",
                defer_count);
        lastpr = now;
    }
#endif

    return 1;
}

static int bdb_berkdb_get_pageindex(bdb_berkdb_t *pberkdb, int *page,
                                    int *index, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    if (page)
        *page = bt->pagenum;
    if (index)
        *index = bt->pageidx;
    return 0;
    /*
   return bt->dbc->c_get_pageindex(bt->dbc, page, index);
   */
}

/**
 *  Close a real berkdb
 *    - free buffers
 *    - close cursor
 *    - free it
 *
 */
static int bdb_berkdb_close_real(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    int rc;

    if (bt->use_odh)
        thread_free(bt->odh_tmp);
    if (bt->bulk.data)
        thread_free(bt->bulk.data);
    if (bt->data.data)
        thread_free(bt->data.data);

    /* db->dbc = NULL if bdbcursor was invalidated */
    if (bt->dbc) {
        rc = bt->dbc->c_close(bt->dbc);
        bt->got_fileid = 0;
        berkdb->cur->queue_cursor = NULL;
        if (rc) {
            *bdberr = (rc == DB_LOCK_DEADLOCK || rc == DB_REP_HANDLE_DEAD)
                          ? BDBERR_DEADLOCK
                          : rc;
            if (berkdb->cur->state->attr->dbgberkdbcursor)
                logmsg(LOGMSG_USER, 
                        "BERKDB=%d %p dbc=%p rc=%d bdberr=%d CLOSE (%p %d)\n",
                        berkdb->u.rl.id, berkdb, bt->dbc, rc, *bdberr,
                        berkdb->cur->curtran,
                        bdb_get_lid_from_cursortran(berkdb->cur->curtran));
            return -1;
        }
        if (berkdb->cur->state->attr->dbgberkdbcursor)
            logmsg(LOGMSG_USER, 
                    "BERKDB=%d %p dbc=%p rc=%d bdberr=%d CLOSE (%p %d)\n",
                    berkdb->u.rl.id, berkdb, bt->dbc, rc, *bdberr,
                    berkdb->cur->curtran,
                    bdb_get_lid_from_cursortran(berkdb->cur->curtran));
    }

    thread_free(berkdb);
    thread_free(pberkdb);

    return 0;
}

/**
 * Position cursor on the first/last row:
 *    - Move first; if ok, return IX_FND
 *    - If deadlock or error, return error
 *    - Return IX_EMPTY
 *
 * Returns: IX_FND, IX_EMPTY, -1 if error (bdberr set)
 */
static int bdb_berkdb_firstlast_real(bdb_berkdb_t *pberkdb, int pos,
                                     int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    int rc = IX_OK;

    *bdberr = 0;

    if (bt->use_bulk) {
        reset_bulk_bt(bt);
        pberkdb->impl->num_nexts = 0;
    }

    rc = bdb_berkdb_cget(berkdb, 0 /*no bulk*/, pos, bdberr);
    if (rc == IX_NOTFND) {
        berkdb->at_eof = 1;
        rc = IX_EMPTY;
    } else
        berkdb->at_eof = 0;

    return rc;
}

static inline int bdb_berkdb_first_real(bdb_berkdb_t *pberkdb, int *bdberr)
{
    return bdb_berkdb_firstlast_real(pberkdb, DB_FIRST, bdberr);
}

static inline int bdb_berkdb_last_real(bdb_berkdb_t *pberkdb, int *bdberr)
{
    return bdb_berkdb_firstlast_real(pberkdb, DB_LAST, bdberr);
}

static int process_bulk_odh(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    struct odh odh;
    unsigned long long *genptr;
    bdb_state_type *bdb_state = berkdb->cur->state;
    int rc;

    rc = bdb_unpack(berkdb->cur->state, bt->lastdta, bt->lastdtasize,
                    bt->odh_tmp, bt->odh.ulen, &odh, NULL);
    if (rc != 0) {
        *bdberr = BDBERR_UNPACK;
        return -1;
    }

    /* Copy to bt->data or vtag_to_ondisk will expand into the next record. */
    if (odh.length < bdb_state->lrl) {
        memcpy(bt->data.data, odh.recptr, odh.length);
        bt->odh.data = bt->data.data;
    } else {
        bt->odh.data = odh.recptr;
    }

    bt->odh.size = odh.length;
    bt->ver = odh.csc2vers;
    if (ip_updates_enabled(berkdb->cur->state)) {
        genptr = (unsigned long long *)bt->lastkey;
#ifdef _SUN_SOURCE
        unsigned long long tmp;
        memcpy(&tmp, genptr, sizeof(*genptr));
        tmp = set_updateid(berkdb->cur->state, odh.updateid, tmp);
        memcpy(genptr, &tmp, sizeof(*genptr));
#else
        *genptr = set_updateid(berkdb->cur->state, odh.updateid, *genptr);
#endif
    }

    *bdberr = 0;
    return 0;
}

/**
 * Position cursor on the next row:
 *    - If cursor is not initialized, this is same as "first"
 *    - Move next
 *    - If deadlock or error, return error
 *    - If not found, and return IX_NOTFND
 *    - Return IX_FND
 *
 * Returns: IX_FND, IX_NOTFND, -1 if error (bdberr set)
 */
int bdb_berkdb_next_real(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    int rc = IX_OK;
    int flags = DB_NEXT;

    *bdberr = 0;

    if (berkdb->at_eof)
        return IX_NOTFND;

    /* only enable bulk mode if we did some minimum number of nexts */
    berkdb->num_nexts++;

    if (!bt->use_bulk && bt->bulk.ulen > 0 &&
        berkdb->bdb_state->attr->bulk_sql_mode &&
        berkdb->num_nexts > berkdb->bdb_state->attr->bulk_sql_threshold) {
        /*printf("switching %p to bulk mode after %d nexts\n", pberkdb,
         * berkdb->num_nexts);*/
        bt->use_bulk = 1;
        bt->bulkptr = NULL;
        if (!bt->bulk.data) {
            bt->bulk.data = thread_malloc(bt->bulk.ulen);
        }
        if (!bt->bulk.data) {
            logmsg(LOGMSG_ERROR, "%s: malloc %d\n", __func__, bt->bulk.ulen);
            *bdberr = BDBERR_MALLOC;
            return -1;
        }
        bt->bulk.flags = DB_DBT_USERMEM;

        flags |= DB_MULTIPLE_KEY;
    } else if (bt->use_bulk) {
        flags |= DB_MULTIPLE_KEY;
    }

    /* try use bulk, if any */
    if (bt->bulkptr)
        DB_MULTIPLE_KEY_NEXT(bt->bulkptr, &bt->bulk, bt->lastkey,
                             bt->lastkeysize, bt->lastdta, bt->lastdtasize);
    if (bt->bulkptr) {
        if (bt->use_odh && process_bulk_odh(pberkdb, bdberr))
            return -1;
        return IX_FND;
    }

    rc = bdb_berkdb_cget(berkdb, 1 /*use bulk if any*/, flags, bdberr);
    berkdb->at_eof = (rc != IX_FND);
    if (rc)
        return rc;

    /* got some data, bulk or not */
    if (bt->use_bulk) {
        DB_MULTIPLE_INIT(bt->bulkptr, &bt->bulk);
        DB_MULTIPLE_KEY_NEXT(bt->bulkptr, &bt->bulk, bt->lastkey,
                             bt->lastkeysize, bt->lastdta, bt->lastdtasize);
        if (bt->use_odh) {
            if (process_bulk_odh(pberkdb, bdberr))
                return -1;
        }
    }

    return IX_FND;
}

/**
 * Position cursor on the prev row:
 *    - If cursor is not initialized, this is same as "last"
 *    - Move prev
 *    - If deadlock or error, return error
 *    - If not found, and return IX_NOTFND
 *    - Return IX_FND
 *
 * Returns: IX_FND, IX_NOTFND, -1 if error (bdberr set)
 */
int bdb_berkdb_prev_real(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    int rc = 0;

    if (bt->use_bulk) {
        logmsg(LOGMSG_ERROR, "Detected PREV after NEXT!\n");
        cheap_stack_trace();
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    rc = bdb_berkdb_cget(berkdb, 0 /*no bulk */, DB_PREV, bdberr);
    berkdb->at_eof = (rc != IX_FND);

    return rc;
}

/* =============================================================================*/

/**
 *  Close a real berkdb
 *    - free buffers
 *    - close cursor
 *    - free it
 *
 */
static int bdb_berkdb_close_shad(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_shaddb_tag_t *bt = &berkdb->u.sd;
    int rc = 0;

    if (bt->use_odh)
        thread_free(bt->odh_tmp);

    rc = bdb_temp_table_close_cursor(berkdb->cur->state, berkdb->u.sd.cur,
                                     bdberr);

    thread_free(berkdb);
    thread_free(pberkdb);

    return rc;
}

/* Unpack the shadow-table data. */
static int bdb_berkdb_unpack_shadows(bdb_berkdb_impl_t *berkdb, int *bdberr)
{
    bdb_shaddb_tag_t *bt = &berkdb->u.sd;
    bdb_osql_log_dta_ptr_t *log_hdr;
    char *dta;
    int len;
    int rc;
    struct odh odh;

    /* Skip past the header if it's there. */
    dta = bdb_temp_table_data(bt->cur);
    len = bdb_temp_table_datasize(bt->cur);

    /* Unpack odh to get the version number. */
    if (bt->use_odh) {
        rc = bdb_unpack(berkdb->cur->state, dta, len, bt->odh_tmp, MAXRECSZ,
                        &odh, NULL);

        if (rc != 0) {
            *bdberr = BDBERR_UNPACK;
            return -1;
        }
        bt->dta = odh.recptr;
        bt->dtalen = odh.length;
        bt->ver = odh.csc2vers;
    } else {
        bt->dta = dta;
        bt->dtalen = len;
        bt->ver = berkdb->cur->state->version;
    }

    return 0;
}

/**
 * Position cursor on the first/last row:
 *    - Move first/last; if ok, and return IX_FND
 *    - Return IX_EMPTY
 *
 */
int bdb_berkdb_firstlast_shad(bdb_berkdb_t *pberkdb,
                              int (*func)(bdb_state_type *, tmpcursor_t *,
                                          int *),
                              int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_shaddb_tag_t *bt = &berkdb->u.sd;
    char *dta;
    int len;
    int rc = IX_OK;

    *bdberr = 0;

    rc = func(berkdb->cur->state, bt->cur, bdberr);
    berkdb->at_eof = (rc != IX_FND);
    if (rc == IX_EMPTY) {
        return IX_EMPTY;
    }

    rc = bdb_berkdb_handle_optimized(pberkdb, bdberr);
    if (rc != IX_OK)
        return rc;

    rc = bdb_berkdb_unpack_shadows(berkdb, bdberr);
    if (rc != IX_OK)
        return rc;

    return 0;
}

int inline bdb_berkdb_first_shad(bdb_berkdb_t *pberkdb, int *bdberr)
{
    return bdb_berkdb_firstlast_shad(pberkdb, bdb_temp_table_first, bdberr);
}

int inline bdb_berkdb_last_shad(bdb_berkdb_t *pberkdb, int *bdberr)
{
    return bdb_berkdb_firstlast_shad(pberkdb, bdb_temp_table_last, bdberr);
}

/**
 * Position cursor on the next row:
 *    - If cursor is not initialized, this is same as "first"
 *    - Move next
 *    - If error, return error
 *    - If not found, and return IX_NOTFND
 *    - Return IX_FND
 *
 * Returns: IX_FND, IX_NOTFND, -1 if error (bdberr set)
 */
int bdb_berkdb_next_shad(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_shaddb_tag_t *bt = &berkdb->u.sd;
    int rc = IX_OK;

    *bdberr = 0;
    /*
       rc = bdb_temp_table_next_norewind( berkdb->cur->state, bt->cur, bdberr);
     */
    rc = bdb_temp_table_next(berkdb->cur->state, bt->cur, bdberr);
    if (rc == IX_PASTEOF) {
        rc = IX_NOTFND;
    }

    berkdb->at_eof = (rc != IX_FND);

    if (rc) {
        return rc;
    }

    rc = bdb_berkdb_handle_optimized(pberkdb, bdberr);
    if (rc != IX_OK)
        return rc;

    rc = bdb_berkdb_unpack_shadows(berkdb, bdberr);
    if (rc != IX_OK)
        return rc;

    return 0;
}

/**
 * Position cursor on the prev row:
 *    - If cursor is not initialized, this is same as "first"
 *    - Move next
 *    - If error, return error
 *    - If not found, and return IX_NOTFND
 *    - Return IX_FND
 *
 * Returns: IX_FND, IX_NOTFND, -1 if error (bdberr set)
 */
int bdb_berkdb_prev_shad(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_shaddb_tag_t *bt = &berkdb->u.sd;
    int rc = IX_OK;

    *bdberr = 0;

    /*
       rc = bdb_temp_table_prev_norewind( berkdb->cur->state, bt->cur, bdberr);
    */
    rc = bdb_temp_table_prev(berkdb->cur->state, bt->cur, bdberr);
    if (rc ==
        IX_PASTEOF) { /*TODO: verify that prev returns pasteof when past begin*/
        rc = IX_NOTFND; /* kinda like less codes */
    }

    berkdb->at_eof = (rc != IX_FND);

    if (rc) {
        return rc;
    }

    rc = bdb_berkdb_handle_optimized(pberkdb, bdberr);
    if (rc != IX_OK)
        return rc;

    rc = bdb_berkdb_unpack_shadows(berkdb, bdberr);
    if (rc != IX_OK)
        return rc;

    return rc;
}

/* Don't adjust for headers in the temptable. */
static int bdb_berkdb_dtakey(bdb_berkdb_t *pberkdb, char **dta, int getdata,
                             int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;

    *bdberr = 0;
    *dta = NULL;

    if (berkdb->type == BERKDB_REAL || berkdb->type == BERKDB_REAL_ROWLOCKS) {
        if (getdata) {
            if (berkdb->u.rl.use_bulk) {
                if (berkdb->u.rl.use_odh)
                    *dta = berkdb->u.rl.odh.data;
                else
                    *dta = berkdb->u.rl.lastdta;
            } else {
                *dta = berkdb->u.rl.data.data;
            }
        } else {
            if (berkdb->u.rl.use_bulk)
                *dta = berkdb->u.rl.lastkey;
            else
                *dta = berkdb->u.rl.key.data;
        }
    } else {
        if (getdata) {
            /*
             * If this is a BDBC_DT cursor, the data has been unpacked
             * by bdb_berkdb_unpack_shadows.
             */
            if (berkdb->cur->type == BDBC_DT) {
                bdb_shaddb_tag_t *bt = &berkdb->u.sd;
                *dta = bt->dta;
            } else {
                bdb_shaddb_tag_t *bt = &berkdb->u.sd;
                *dta = bdb_temp_table_data(bt->cur);
            }
        } else {
            *dta = bdb_temp_table_key(berkdb->u.sd.cur);
        }
    }
    return IX_OK;
}

static int bdb_berkdb_dtakeysize(bdb_berkdb_t *pberkdb, int *dtasize,
                                 int getdata, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;

    *bdberr = 0;
    *dtasize = 0;

    assert(BERKDB_REAL == berkdb->type || BERKDB_SHAD == berkdb->type);

    if (berkdb->type == BERKDB_REAL) {
        if (getdata) {
            if (berkdb->u.rl.use_bulk) {
                if (berkdb->u.rl.use_odh)
                    *dtasize = berkdb->u.rl.odh.size;
                else
                    *dtasize = berkdb->u.rl.lastdtasize;
            } else {
                *dtasize = berkdb->u.rl.data.size;
            }
        } else {
            if (berkdb->u.rl.use_bulk)
                *dtasize = berkdb->u.rl.lastkeysize;
            else
                *dtasize = berkdb->u.rl.key.size;
        }
    } else {
        if (getdata) {
            /*
             * If this is a BDBC_DT cursor, the data has been unpacked
             * by bdb_berkdb_unpack_shadows.
             */
            if (berkdb->cur->type == BDBC_DT) {
                bdb_shaddb_tag_t *bt = &berkdb->u.sd;
                *dtasize = bt->dtalen;
            } else {
                bdb_shaddb_tag_t *bt = &berkdb->u.sd;
                *dtasize = bdb_temp_table_datasize(bt->cur);
            }
        } else
            *dtasize = bdb_temp_table_keysize(berkdb->u.sd.cur);
    }
    return IX_OK;
}

static inline int bdb_berkdb_dta(bdb_berkdb_t *pberkdb, char **dta, int *bdberr)
{
    return bdb_berkdb_dtakey(pberkdb, dta, 1, bdberr);
}

static inline int bdb_berkdb_key(bdb_berkdb_t *pberkdb, char **dta, int *bdberr)
{
    return bdb_berkdb_dtakey(pberkdb, dta, 0, bdberr);
}

static inline int bdb_berkdb_dtasize(bdb_berkdb_t *pberkdb, int *dtalen,
                                     int *bdberr)
{
    return bdb_berkdb_dtakeysize(pberkdb, dtalen, 1, bdberr);
}

static inline int bdb_berkdb_keysize(bdb_berkdb_t *pberkdb, int *keylen,
                                     int *bdberr)
{
    return bdb_berkdb_dtakeysize(pberkdb, keylen, 0, bdberr);
}

static int bdb_berkdb_ver(bdb_berkdb_t *pberkdb, uint8_t *ver, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    if (berkdb->type == BERKDB_REAL)
        *ver = pberkdb->impl->u.rl.ver;
    else if (berkdb->type == BERKDB_REAL_ROWLOCKS)
        *ver = pberkdb->impl->cur->ver;
    else if (berkdb->type == BERKDB_SHAD)
        *ver = pberkdb->impl->u.sd.ver;
    else
        *ver = 0;
    return 0;
}

static int bdb_berkdb_is_at_eof(struct bdb_berkdb *pberkdb)
{
    return pberkdb->impl->at_eof;
}

static int bdb_berkdb_get_everything(struct bdb_berkdb *pberkdb, char **dta,
                                     int *dtasize, char **key, int *keysize,
                                     uint8_t *ver, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    if (berkdb->u.rl.use_bulk) {
        if (berkdb->u.rl.use_odh) {
            *dta = berkdb->u.rl.odh.data;
            *dtasize = berkdb->u.rl.odh.size;
        } else {
            *dta = berkdb->u.rl.lastdta;
            *dtasize = berkdb->u.rl.lastdtasize;
        }
        *key = berkdb->u.rl.lastkey;
        *keysize = berkdb->u.rl.lastkeysize;
    } else {
        *dta = berkdb->u.rl.data.data;
        *dtasize = berkdb->u.rl.data.size;
        *key = berkdb->u.rl.key.data;
        *keysize = berkdb->u.rl.key.size;
    }
    *ver = berkdb->u.rl.ver;
    *bdberr = 0;
    return 0;
}

static int bdb_berkdb_move(bdb_berkdb_t *pberkdb, int dir, int *bdberr)
{
    int rc = 0;
    switch (dir) {
    case DB_FIRST:
        rc = pberkdb->first(pberkdb, bdberr);
        if (pberkdb->impl->trak)
            logmsg(LOGMSG_USER, "TRK: %p first stripe=%d %d %d\n", pberkdb,
                    pberkdb->impl->cur->idx, rc, *bdberr);
        return rc;
    case DB_NEXT:
        rc = pberkdb->next(pberkdb, bdberr);
        if (pberkdb->impl->trak)
            logmsg(LOGMSG_USER, "TRK: %p next stripe=%d %d %d\n", pberkdb,
                    pberkdb->impl->cur->idx, rc, *bdberr);
        return rc;
    case DB_PREV:
        rc = pberkdb->prev(pberkdb, bdberr);
        if (pberkdb->impl->trak)
            logmsg(LOGMSG_USER, "TRK: %p prev stripe=%d %d %d\n", pberkdb,
                    pberkdb->impl->cur->idx, rc, *bdberr);
        return rc;
    case DB_LAST:
        rc = pberkdb->last(pberkdb, bdberr);
        if (pberkdb->impl->trak)
            logmsg(LOGMSG_USER, "TRK: %p last stripe=%d %d %d\n", pberkdb,
                    pberkdb->impl->cur->idx, rc, *bdberr);
        return rc;
    default:
        logmsg(LOGMSG_ERROR, "%s: unknown dir %d\n", __func__, dir);
        *bdberr = BDBERR_BADARGS;
        return -1;
    }
}

static int bdb_berkdb_find_real(bdb_berkdb_t *pberkdb, void *key, int keylen,
                                int how, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    int rc = IX_OK;

    berkdb->num_nexts = 0;

    if (keylen > bt->key.ulen) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    if (bt->use_bulk) {
        reset_bulk_bt(bt);
    }

    memmove(bt->key.data, key, keylen);
    bt->key.size = keylen;

    rc = bdb_berkdb_cget(berkdb, 0 /*no bulk*/, how, bdberr);
    if (berkdb->trak) {
        logmsg(LOGMSG_USER, "TRK: %p find real rc=%d bdberr=%d keylen=%d key=0x",
                pberkdb, rc, *bdberr, keylen);
        hexdump(LOGMSG_USER, key, keylen);
        logmsg(LOGMSG_USER, "\n");
    }

    /* if we did not find entry, we are pointing to junk,
     * so at_eof should be set to 1 after positioning to last (for ix btrees)
     */

    berkdb->at_eof = (rc != IX_FND); /* if not found, we are at eof */

    return rc;
}

static int bdb_berkdb_get_skip_stat(bdb_berkdb_t *pberkdb, u_int64_t *nextcount,
                                    u_int64_t *skipcount)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;

    if (!berkdb->u.rl.dbc || berkdb->cur->type != BDBC_DT) {
        return -1;
    }

    return berkdb->u.rl.dbc->c_skip_stat(berkdb->u.rl.dbc, nextcount,
                                         skipcount);
}

static int bdb_berkdb_find_shad(bdb_berkdb_t *pberkdb, void *key, int keylen,
                                int how, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    int rc = 0;

    /* Retrieve my current genid. */
    if (how == DB_NEXT || how == DB_PREV || how == DB_FIRST || how == DB_LAST) {
        rc = bdb_temp_table_move(berkdb->cur->state, berkdb->u.sd.cur, how,
                                 bdberr);
    } else {
        assert(how == DB_SET || how == DB_SET_RANGE);
        rc = bdb_temp_table_find(berkdb->cur->state, berkdb->u.sd.cur, key,
                                 keylen, NULL, bdberr);
    }
#if 0
   bdb_temp_table_debug_dump( berkdb->cur->state, berkdb->u.sd.cur);
#endif

    if (berkdb->trak) {
        logmsg(LOGMSG_USER, "TRK: %p find shad rc=%d bdberr=%d keylen=%d key=0x",
                pberkdb, rc, *bdberr, keylen);
        hexdump(LOGMSG_USER, key, keylen);
        logmsg(LOGMSG_USER, "\n");
    }

    if (rc == IX_FND && berkdb->cur->type == BDBC_DT) {
        rc = bdb_berkdb_handle_optimized(pberkdb, bdberr);
        if (rc != IX_OK)
            return rc;

        rc = bdb_berkdb_unpack_shadows(berkdb, bdberr);
        if (rc != IX_OK)
            return rc;
    }

    if (rc == IX_FND) {
        /* if the rows does not return, this returns the first?
           due to temptable find semantics */
        char *foundkey = bdb_temp_table_key(berkdb->u.sd.cur);
        int foundkeylen = bdb_temp_table_keysize(berkdb->u.sd.cur);

        int assert_keylen = foundkeylen;

        /* In function bdb_cursor_reposition_noupdate for DB_SET_RANGE operation
           in case of duplicate keys genid
            is added as part of key. The record is marked as deleted for the
           next find.*/
        if (berkdb->cur->type == BDBC_IX &&
            bdb_keycontainsgenid(berkdb->cur->state, berkdb->cur->idx))
            assert_keylen += sizeof(unsigned long long);

        /* DB_NEXTs will be for a partial search if the initial SETRANGE is
         * skipped */
        assert(keylen == sizeof(unsigned long long) ||
               /* for partial search. */
               keylen <= (assert_keylen - sizeof(unsigned long long) + 1) ||
               /* for shadow only search */
               (berkdb->cur->type == BDBC_IX && !berkdb->cur->rl &&
                keylen <= foundkeylen));

        if (keylen == foundkeylen - sizeof(unsigned long long)) {
            rc =
                memcmp(foundkey, key, foundkeylen - sizeof(unsigned long long));
        } else if (keylen == foundkeylen - sizeof(unsigned long long) + 1) {
            /* last dup key */
            rc =
                memcmp(foundkey, key, foundkeylen - sizeof(unsigned long long));
            if (rc == 0) {
                /* we were looking for an +epsilon key */
                rc = -1;
            }
        } else {
            if (how == DB_SET_RANGE || how == DB_NEXT) {
                rc = memcmp(foundkey, key, keylen);
            } else {
                /* here we want an exact lookup */
                rc = memcmp(foundkey, key, foundkeylen);
                if (rc != 0)
                    rc = -1;
            }
        }
        if (rc < 0) /* DB_SET_RANGE violation */
            rc = IX_NOTFND;
        else
            rc = IX_FND;
    }
    berkdb->at_eof = (rc != IX_FND); /* if not found, we are at eof */

    return rc;
}

extern int gbl_new_snapisol;

static int bdb_berkdb_cget(bdb_berkdb_impl_t *berkdb, int use_bulk, int how,
                           int *bdberr)
{
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    bdb_cursor_impl_t *cur = berkdb->cur; /* owner cursor */
    int rc = 0;

    *bdberr = 0;

    bt->need_update_shadows = 1;
    if (bt->use_odh && !bt->use_bulk)
        rc = bdb_cget_unpack(berkdb->cur->state, bt->dbc, &bt->key, &bt->data,
                             &bt->ver, how);
    else {
        if (!bt->dbc) {
            *bdberr = BDBERR_DEADLOCK;
            return -1;
        }

        if (bt->use_bulk && use_bulk)
            rc = bt->dbc->c_get(bt->dbc, &bt->key, &bt->bulk, how);
        else
            rc = bt->dbc->c_get(bt->dbc, &bt->key, &bt->data, how);
    }

    if (gbl_new_snapisol && (how == DB_SET_RANGE || how == DB_SET))
        bt->pagenum = bt->dbc->lastpage;

    if (rc == DB_LOCK_DEADLOCK || rc == DB_REP_HANDLE_DEAD) {
        *bdberr = BDBERR_DEADLOCK;
        return -1;
    }
    if (rc == DB_NOTFOUND) {
        return IX_NOTFND;
    }
    if (rc) {
        *bdberr = rc;
        return -1;
    }

    rc = bt->dbc->c_get_pageinfo(bt->dbc, &bt->pagenum, &bt->pageidx,
                                 &bt->pagelsn);

    if (gbl_new_snapisol && (how == DB_SET_RANGE || how == DB_SET))
        bt->pagenum = bt->dbc->lastpage;

    if (rc == 0 && bt->pagelsn.file != 0 && cur->shadow_tran &&
        cur->shadow_tran->snapy_commit_lsn.file != 0 &&
        log_compare(&cur->shadow_tran->snapy_commit_lsn, &bt->pagelsn) > 0) {
        if (berkdb->trak) {
            char *buf;
            hexdumpbuf(bt->key.data, bt->key.size, &buf);
            logmsg(LOGMSG_USER, "Cur %p need_update_shadows -> 0, pagelsn=%d:%d "
                            "snapy_commit_lsn=%d:%d key: 0x%s\n",
                    cur, bt->pagelsn.file, bt->pagelsn.offset,
                    cur->shadow_tran->snapy_commit_lsn.file,
                    cur->shadow_tran->snapy_commit_lsn.offset, buf);
            free(buf);
        }
        bt->need_update_shadows = 0;
    }

    return IX_FND;
}

static inline void bdb_berkdb_trak(bdb_berkdb_t *pberkdb, int status)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;

    berkdb->trak = status;
}

static int bdb_berkdb_insert(bdb_berkdb_t *pberkdb, char *key, int keylen,
                             char *dta, int dtalen, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    int rc = 0;

    if (berkdb->type == BERKDB_REAL || berkdb->type == BERKDB_REAL_ROWLOCKS) {
        logmsg(LOGMSG_ERROR, "%s: bug, unsupported yet\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
#if 0
   DBT               dkey, ddta; 
   dkey.data = key;
   dkey.size = keylen;
   dkey.ulen = keylen;
   dkey.flags = DB_DBT_USERMEM;

   ddta.data = dta;
   ddta.size = dtalen;
   ddta.ulen = dtalen;
   ddta.flags = DB_DBT_USERMEM;

   rc = berkdb->u.sd.cur->c_put( berkdb->u.sd.cur, dkey, ddta, DB_KEYLAST);
   if (rc == DB_LOCK_DEADLOCK || rc == DB_REP_HANDLE_DEAD) 
   {
      *bdberr = BDBERR_DEADLOCK;
      return -1;
   }
   else if (rc) {
      *bdberr = rc;
      return -1;
   }
   return 0;
#endif
    }

    rc = bdb_temp_table_insert(berkdb->cur->state, berkdb->u.sd.cur, key,
                               keylen, dta, dtalen, bdberr);

    if (berkdb->trak) {
        logmsg(LOGMSG_USER, "TRK: %p insert rc=%d bdberr=%d", pberkdb, rc, *bdberr);
        logmsg(LOGMSG_USER, "\n\tkeylen=%d key=0x", keylen);
        hexdump(LOGMSG_USER, key, keylen);
        logmsg(LOGMSG_USER, "\n\tdatalen=%d data=0x", dtalen);
        hexdump(LOGMSG_USER, dta, dtalen);
        logmsg(LOGMSG_USER, "\n");
    }

    return rc;
}

static inline int bdb_berkdb_prevent_optimized(bdb_berkdb_t *pberkdb)
{
    return 0;
}

static inline int bdb_berkdb_allow_optimized(bdb_berkdb_t *pberkdb)
{
    return 0;
}

static int bdb_berkdb_delete(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    int rc = 0;

    if (berkdb->type == BERKDB_REAL || berkdb->type == BERKDB_REAL_ROWLOCKS) {
        logmsg(LOGMSG_ERROR, "%s: bug, unsupported yet\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return -1;
#if 0
      rc = berkdb->u.rl.dbc->c_del( berkdb->u.rl.dbc, 0);
   if (rc == DB_LOCK_DEADLOCK || rc == DB_REP_HANDLE_DEAD) 
   {
      *bdberr = BDBERR_DEADLOCK;
      return -1;
   }
   else if (rc) {
      *bdberr = rc;
      return -1;
   }
   return 0;
#endif
    }

    rc = bdb_temp_table_delete(berkdb->cur->state, berkdb->u.sd.cur, bdberr);

    if (berkdb->trak) {
        logmsg(LOGMSG_USER, "TRK: %p delete rc=%d bdberr=%d genid=%llx\n", pberkdb,
                rc, *bdberr, berkdb->cur->genid);
    }

    return rc;
}

static int bdb_berkdb_unlock(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    int rc = 0;
    int lid = bdb_get_lid_from_cursortran(berkdb->cur->curtran);

    if (berkdb->type != BERKDB_REAL && berkdb->type != BERKDB_REAL_ROWLOCKS) {
        logmsg(LOGMSG_ERROR, "%s: argh?\n", __func__);
        *bdberr = BDBERR_BUG_KILLME;
        return -1;
    }

    bt->need_update_shadows = 1;

    if (bt->dbc) {
        /* close the cursor */
        rc = bt->dbc->c_close(bt->dbc);
        if (rc) {
            if (rc == DB_LOCK_DEADLOCK || rc == DB_REP_HANDLE_DEAD)
                *bdberr = BDBERR_DEADLOCK;
            else
                *bdberr = rc;
            rc = -1;
        }
    }

    if (berkdb->cur->state->attr->dbgberkdbcursor)
        logmsg(LOGMSG_USER, "BERKDB=%d %p cursor dbc=%p rc=%d bdberr=%d UNLOCK (%p %d)\n",
                berkdb->u.rl.id, berkdb, bt->dbc, rc, *bdberr,
                berkdb->cur->curtran, lid);

    /*
       fprintf(stderr, "bdb_berkdb_unlock setting dbc to NULL\n");
     */

    bt->dbc = NULL; /* if anything goes wrong
                       between unlock and lock,
                       make sure we don't reclose it
                       in upper layers */
    return rc;
}

static int bdb_berkdb_lock(bdb_berkdb_t *pberkdb, cursor_tran_t *curtran,
                           int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_realdb_tag_t *bt = &berkdb->u.rl;
    int rc = 0;
    int lid = bdb_get_lid_from_cursortran(berkdb->cur->curtran);
    u_int32_t curflags = 0;
    DB *db;

    if (berkdb->type != BERKDB_REAL) {
        logmsg(LOGMSG_ERROR, "%s: argh?\n", __func__);
        *bdberr = BDBERR_BUG_KILLME;
        return -1;
    }

    if (bt->dbc) {
        /* if this is not present, it means this is the first move here
           (probably
           a first */
        logmsg(LOGMSG_ERROR, "%s: Bug in cursor lock/unlock, cursor was not reset!\n",
                __func__);

        if (berkdb->cur->state->attr->dbgberkdbcursor)
            logmsg(LOGMSG_USER, "BERKDB=%d %p cursor dbc=%p rc=%d bdberr=%d LOCK (%p %d)\n",
                    berkdb->u.rl.id, berkdb, bt->dbc, rc, *bdberr,
                    berkdb->cur->curtran, lid);

        abort();
    }

    if (berkdb->cur->type == BDBC_DT)
        db = berkdb->cur->state->dbp_data[0][berkdb->cur->idx];
    else
        db = berkdb->cur->state->dbp_ix[berkdb->cur->idx];

    /* These are set once when the cursor opens. */
    if (berkdb->cur->pageorder)
        curflags |= DB_PAGE_ORDER;

    if (berkdb->cur->discardpages)
        curflags |= DB_DISCARD_PAGES;

    bt->dbc = get_cursor_for_cursortran_flags(berkdb->cur->curtran, db,
                                              curflags, bdberr);
    if (!bt->dbc)
        return -1;

    /* Increment */

    if (berkdb->cur->state->attr->dbgberkdbcursor)
        logmsg(LOGMSG_USER, "BERKDB=%d %p cursor dbc=%p rc=%d bdberr=%d LOCK (%p %d)\n",
                berkdb->u.rl.id, berkdb, bt->dbc, rc, *bdberr,
                berkdb->cur->curtran, lid);

    return rc;
}

DBC *get_cursor_for_cursortran_flags(cursor_tran_t *curtran, DB *db,
                                     u_int32_t flags, int *bdberr)
{
    DBC *dbc = NULL;
    int lid = bdb_get_lid_from_cursortran(curtran);
    int rc = 0;

    rc = db->paired_cursor_from_lid(db, lid, &dbc, flags);
    if (rc) {
        if (rc == DB_LOCK_DEADLOCK || rc == DB_REP_HANDLE_DEAD)
            *bdberr = BDBERR_DEADLOCK;
        else
            *bdberr = rc;
    } else
        assert(dbc != NULL);

    return dbc;
}

/**
 * Handle optimized shadow files that don't contain
 * the actual btree row, but instead they have a pointer
 * to the berkeley db log
 * This applies to entries produced by replication/local writers
 *
 * Note: again, this is against the original design of berkdb
 * being content agnostic, but the same odh criteria applies here
 * It is much easier to handle it here, transparently.
 */
static int bdb_berkdb_handle_optimized(bdb_berkdb_t *pberkdb, int *bdberr)
{
    bdb_berkdb_impl_t *berkdb = pberkdb->impl;
    bdb_shaddb_tag_t *bt = &berkdb->u.sd;
    char *dta;
    int dtalen;
    char *key;
    int keylen;
    void *row;
    int rowlen;
    int rc = 0;

    assert(berkdb->type == BERKDB_SHAD);

    if (berkdb->cur->type != BDBC_DT)
        return IX_FND;

    dta = bdb_temp_table_data(bt->cur);
    dtalen = bdb_temp_table_datasize(bt->cur);

    if (dtalen > 0 && bdb_osql_log_is_optim_data(dta)) {
        /* We have a berkeley dta_ptr. */
        bdb_osql_log_dta_ptr_t *ptr = (bdb_osql_log_dta_ptr_t *)dta;

        /* retrive the row from the log */
        rc = bdb_osql_log_get_optim_data(berkdb->cur->state, &ptr->lsn, &row,
                                         &rowlen, bdberr);
        if (rc < 0)
            return rc;

        key = bdb_temp_table_key(bt->cur);
        keylen = bdb_temp_table_keysize(bt->cur);

        /* update the temp table */
        rc = bdb_temp_table_update(berkdb->cur->state, bt->cur, key, keylen,
                                   row, rowlen, bdberr);
        if (rc < 0)
            return rc;

        /* refind the newly inserted position */
        rc = bdb_temp_table_find_exact(berkdb->cur->state, bt->cur, key, keylen,
                                       bdberr);
        if (rc != IX_FND) {
            logmsg(LOGMSG_ERROR, 
                "%s: fail to retrieve back the updated row rc=%d bdberr=%d\n",
                __func__, rc, *bdberr);
            rc = -1; /* we have to find this row back */
        }
    }

    return rc;
}

static inline int outoforder_set(bdb_berkdb_t *pberkdb, int status)
{
    int original = pberkdb->impl->outoforder;

    pberkdb->impl->outoforder = status;

    return original;
}

static inline int outoforder_get(bdb_berkdb_t *pberkdb)
{
    return pberkdb->impl->outoforder;
}

/*
   ts=3:sw=3
*/
