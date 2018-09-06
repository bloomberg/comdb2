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

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "bdb_int.h"
#include "bdb_cursor.h"
#include "bdb_osqltrn.h"
#include "bdb_osqllog.h"
#include "bdb_osqlcur.h"
#include "locks.h"
#include "genid.h"
#include "logmsg.h"

typedef struct tran_shadow {
    tmptable_t **
        tbls;   /* temptables for each stripe, 1 for index, blobs, skip */
    int ntbls;  /* how many stripes */
    int nshadows; /* number of shadows (for indexes) */
    int bkfill; /* set if this was bkfilled */
    bdb_osql_log_t *
        lastlog; /* set this to the last log processed for this btree */
} tran_shadow_t;

/* allocated per transaction per db temp_tables */
struct tran_table_shadows {
    tran_shadow_t *ix_shadows; /* index shadow btrees, indexed by index */
    tran_shadow_t *dt_shadows; /* data shadow btrees, only one */
    tran_shadow_t *sk_shadows; /* skip shadow, one */
    tran_shadow_t *bl_shadows; /* blob shadow, one */
};

static tmpcursor_t *open_shadow(bdb_state_type *bdb_state,
                                tran_shadow_t **pshadows, int file, int maxfile,
                                int stripe, int maxstripe, int create,
                                int *bdberr);

static void open_shadow_nocursor(bdb_state_type *bdb_state,
                                 tran_shadow_t **pshadows, int file,
                                 int maxfile, int stripe, int maxstripe,
                                 int *bdberr);

static int bdb_free_shadows_table(bdb_state_type *bdb_state,
                                  tran_shadow_t *shadows);

/**
 * Opens a shadow file for either an index or data
 * For data, stripe indicates the dtastripe
 * cur->ixnum indicates if this is dta or index
 *
 */
static tmpcursor_t *bdb_tran_open_shadow_int(bdb_state_type *bdb_state,
                                             int dbnum, tran_type *shadow_tran,
                                             int idx, int type, int create,
                                             int nocursor, int *bdberr)
{
    *bdberr = 0;

    if (!bdb_state->parent) {
        logmsg(LOGMSG_ERROR, "%s: called for env\n", __func__);
        *bdberr = BDBERR_BADARGS;
        return NULL;
    }

    if (shadow_tran->tables == NULL) {
        if (!create)
            return NULL;

        /* XXX needs to be fixed */

        /* this call is under bdb read lock; numchildren does not
           change from under us; store it */
        shadow_tran->numchildren = bdb_state->parent->numchildren;

        shadow_tran->tables = (tran_table_shadows_t *)calloc(
            shadow_tran->numchildren, sizeof(tran_table_shadows_t));
        if (!shadow_tran) {
            *bdberr = BDBERR_MALLOC;
            return NULL;
        }
    }

    if (type == BDBC_DT) {
        if (nocursor) {
            open_shadow_nocursor(bdb_state,
                                 &shadow_tran->tables[dbnum].dt_shadows, 0, 1,
                                 idx, bdb_state->attr->dtastripe, /* one data */
                                 bdberr);

            return NULL;
        } else {
            return open_shadow(bdb_state,
                               &shadow_tran->tables[dbnum].dt_shadows, 0, 1,
                               idx, bdb_state->attr->dtastripe, /* one data */
                               create, bdberr);
        }
    } else if (type == BDBC_IX) {
        if (nocursor) {
            open_shadow_nocursor(
                bdb_state, &shadow_tran->tables[dbnum].ix_shadows, idx,
                bdb_state->numix, 0, 1, /* no stripes for indexes */
                bdberr);
            return NULL;
        } else {
            return open_shadow(
                bdb_state, &shadow_tran->tables[dbnum].ix_shadows, idx,
                bdb_state->numix, 0, 1, /* no stripes for indexes */
                create, bdberr);
        }
    } else if (type == BDBC_SK) {
        if (nocursor) {
            open_shadow_nocursor(bdb_state,
                                 &shadow_tran->tables[dbnum].sk_shadows, 0, 1,
                                 0, 1, /* one */
                                 bdberr);
            return NULL;
        } else {
            return open_shadow(bdb_state,
                               &shadow_tran->tables[dbnum].sk_shadows, 0, 1, 0,
                               1, /* one */
                               create, bdberr);
        }
    } else if (type == BDBC_BL) {
        if (nocursor) {
            open_shadow_nocursor(bdb_state,
                                 &shadow_tran->tables[dbnum].bl_shadows, idx,
                                 16, 0, 1, /* one */
                                 bdberr);
            return NULL;
        } else {
            return open_shadow(bdb_state,
                               &shadow_tran->tables[dbnum].bl_shadows, idx, 16,
                               0, 1, /* one */
                               create, bdberr);
        }
    }

    logmsg(LOGMSG_ERROR, "%s: unknown bdb shadow type %d\n", __func__, type);
    return NULL;
}

tmpcursor_t *bdb_tran_open_shadow(bdb_state_type *bdb_state, int dbnum,
                                  tran_type *shadow_tran, int idx, int type,
                                  int create, int *bdberr)
{
    return bdb_tran_open_shadow_int(bdb_state, dbnum, shadow_tran, idx, type,
                                    create, 0, bdberr);
}

void bdb_tran_open_shadow_nocursor(bdb_state_type *bdb_state, int dbnum,
                                   tran_type *shadow_tran, int idx, int type,
                                   int *bdberr)
{
    bdb_tran_open_shadow_int(bdb_state, dbnum, shadow_tran, idx, type, 1, 1,
                             bdberr);
}

/**
 * Check if a shadow is backfilled
 *
 */
int bdb_osql_shadow_is_bkfilled(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    tran_type *shadow = cur->shadow_tran;

    *bdberr = 0;
    if (!shadow || !shadow->tables)
        return 0;

    switch (cur->type) {
    case BDBC_DT:
        if (shadow->tables[cur->dbnum].dt_shadows)
            return shadow->tables[cur->dbnum].dt_shadows[0].bkfill;
        break;

    case BDBC_IX:
        if (shadow->tables[cur->dbnum].ix_shadows)
            return shadow->tables[cur->dbnum].ix_shadows[cur->idx].bkfill;
        break;
    case BDBC_UN:
    case BDBC_SK:
    case BDBC_BL:
        break;
    default:
        abort();
        break;
    }
    return 0;

}

/**
 *  Set shadow backfilled
 *
 */
int bdb_osql_shadow_set_bkfilled(bdb_cursor_ifn_t *pcur_ifn, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    tran_type *shadow = cur->shadow_tran;

    *bdberr = 0;
    if (!shadow || !shadow->tables)
        return 0;

    switch (cur->type) {
    case BDBC_DT:
        if (shadow->tables[cur->dbnum].dt_shadows)
            shadow->tables[cur->dbnum].dt_shadows[0].bkfill = 1;
        break;

    case BDBC_IX:
        if (shadow->tables[cur->dbnum].ix_shadows)
            shadow->tables[cur->dbnum].ix_shadows[cur->idx].bkfill = 1;
        break;
    case BDBC_UN:
    case BDBC_SK:
    case BDBC_BL:
        break;
    default:
        abort();
        break;
    }
    return 0;
}

/**
 * Retrieves the last log processed by this transaction
 *
 */
bdb_osql_log_t *bdb_osql_shadow_get_lastlog(bdb_cursor_ifn_t *pcur_ifn,
                                            int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    tran_type *shadow = cur->shadow_tran;

    *bdberr = 0;
    if (!shadow || !shadow->tables)
        return NULL;

    switch (cur->type) {
    case BDBC_BL:
        if (shadow->tables[cur->dbnum].bl_shadows)
            return shadow->tables[cur->dbnum].bl_shadows[0].lastlog;
        break;

    case BDBC_DT:
        if (shadow->tables[cur->dbnum].dt_shadows)
            return shadow->tables[cur->dbnum].dt_shadows[0].lastlog;
        break;

    case BDBC_IX:
        if (shadow->tables[cur->dbnum].ix_shadows)
            return shadow->tables[cur->dbnum].ix_shadows[cur->idx].lastlog;
        break;
    case BDBC_UN:
    case BDBC_SK:
        break;
    default:
        abort();
        break;
    }

    return NULL;
}

/**
 * Stores the last log processed by this cur (which updates
 * the shadow btree status
 *
 */
int bdb_osql_shadow_set_lastlog(bdb_cursor_ifn_t *pcur_ifn,
                                struct bdb_osql_log *log, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    tran_type *shadow = cur->shadow_tran;

    *bdberr = 0;
    if (!shadow || !shadow->tables)
        return 0;

    switch (cur->type) {
    case BDBC_DT:

        /* This might have been applied to addcur.  If we're closed, open now.
         */
        if (!shadow->tables[cur->dbnum].dt_shadows) {
            bdb_tran_open_shadow_nocursor(cur->state, cur->dbnum, shadow, 0,
                                          BDBC_DT, bdberr);
        }

        shadow->tables[cur->dbnum].dt_shadows[0].lastlog = log;
        if (0) {
            logmsg(LOGMSG_ERROR, "LASTLOG: set %p\n", log);
            cheap_stack_trace();
        }
        break;

    case BDBC_IX:

        if (!shadow->tables[cur->dbnum].ix_shadows) {
            bdb_tran_open_shadow_nocursor(cur->state, cur->dbnum, shadow,
                                          cur->idx, BDBC_IX, bdberr);
        }

        shadow->tables[cur->dbnum].ix_shadows[cur->idx].lastlog = log;
        if (0) {
            logmsg(LOGMSG_ERROR, "LASTLOG: set %p\n", log);
            cheap_stack_trace();
        }
        break;
    case BDBC_UN:
    case BDBC_SK:
    case BDBC_BL:
        break;
    default:
        abort();
        break;
    }
    return 0;
}

/**
 *  There are 4 step during a bdbcursor move
 *  1) If current real row consumed, move real berkdb
 *  2) Update shadows
 *  3) If current shadow row consumed, move shadow berkdb
 *    - we need to reposition for relative moves
 *  4) Merge real and shadow
 *
 *  This function is handling the step 2
 *  - get first log
 *  - if there is no log (first == NULL) return
 *  - process each log from first to last (this is the last seen when first log
 *is
 *    retrieved
 *  - if this is the last log, reset log for transactions
 *  If any shadow row is added/deleted, mark dirty
 */

int release_locks(const char *trace);

int bdb_osql_update_shadows(bdb_cursor_ifn_t *pcur_ifn, bdb_osql_trn_t *trn,
                            int *dirty, enum log_ops log_op, int *bdberr)
{
    extern int gbl_sql_release_locks_in_update_shadows;
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    DB_LOGC *logcur = NULL;
    bdb_osql_log_t *log = NULL;
    bdb_osql_log_t *first = NULL;
    bdb_osql_log_t *last = NULL;
    int tmpdirty = 0;
    int rc = 0;
    int released_locks = 0;

    *dirty = 0;
    *bdberr = 0;

    /* try to recover the last log from associated shadow, if one exist
       otherwise, get the first log of the transaction since probably this
       shadow was never initialized before */
    log = bdb_osql_shadow_get_lastlog(pcur_ifn, bdberr);
    if (!log) {
        if (*bdberr) {
            logmsg(LOGMSG_ERROR, "%s last log %d\n", __func__, *bdberr);
            return -1;
        }

        first = bdb_osql_trn_first_log(trn, bdberr);
        if (!first) {
            if (*bdberr) {
                logmsg(LOGMSG_ERROR, "%s first log %d\n", __func__, *bdberr);
                return -1;
            }

            if (cur->trak) {
                logmsg(LOGMSG_ERROR, "Cur %p returns from update shadows with no "
                                "logs to consume.\n",
                        cur);
            }

            /*
            fprintf( stderr, "%d %s:%d no logs for this transaction %p
            startgenid=%llx\n",
                  pthread_self(), __FILE__, __LINE__, cur,
            cur->shadow_tran->startgenid);
             */

            return 0;
        }
    }

    last = bdb_osql_log_last(bdberr);
    if (!last) {
        logmsg(LOGMSG_ERROR, "%s: no tail? bdberr=%d, log=%p, first=%p\n", __func__,
                *bdberr, log, first);
        fflush(stderr);
        fflush(stdout);
        abort();
        *bdberr = BDBERR_BUG_KILLME;
        return -1;
    }
    /* if this is the first time, go ahead and process
       log = first, even if log == last
       if this is not the first time (first == NULL),
       we are done if log == last
     */

    /*
    fprintf( stderr, "%d %s:%d have %slogs for this transaction %p
    startgenid=%llx, first=%p, log=%p, last=%p\n",
          pthread_self(), __FILE__, __LINE__, (!first && log == last)?"NO ":"",
    cur, cur->shadow_tran->startgenid,
          first, log, last);
     */
    if (first) {
        log = first;
    } else {
        if (log == last) {
            if (cur->trak) {
                logmsg(LOGMSG_USER, "Cur %p returning from update-shadows with "
                                "nothing new to consume.\n",
                        cur);
            }
            return 0;
        }

        /* in this case, the current log is always applied; skip to the next */
        log = bdb_osql_trn_next_log(trn, log, bdberr);
        if (*bdberr || !log) {
            logmsg(LOGMSG_ERROR, "%s next_log %d\n", __func__, *bdberr);
            return -1;
        }
    }

    /* retrieve a log cursor */
    rc = cur->state->dbenv->log_cursor(cur->state->dbenv, &logcur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d error get log cursor rc %d\n", __FILE__, __LINE__, rc);
        *bdberr = rc;
        return -1;
    }

    /* we got a log */
    do {
        if (gbl_sql_release_locks_in_update_shadows && !released_locks) {
            extern int gbl_sql_random_release_interval;
            if (bdb_curtran_has_waiters(cur->state, cur->curtran)) {
                rc = release_locks("update shadows");
                released_locks = 1;
            } else if (gbl_sql_random_release_interval &&
                       !(rand() % gbl_sql_random_release_interval)) {
                rc = release_locks("random release update shadows");
                released_locks = 1;
            }
            if (rc != 0) {
                logcur->close(logcur, 0);
                logmsg(LOGMSG_ERROR, "%s release_locks %d\n", __func__, rc);
                /* Generation changed: ask client to retry */
                if (rc == 210 /* SQLITE_CLIENT_CHANGENODE */)
                    *bdberr = BDBERR_NOT_DURABLE;
                else
                    *bdberr = BDBERR_DEADLOCK;
                return -1;
            }
        }

        /* do apply the log to shadow files */
        rc = bdb_osql_log_apply_log(cur, logcur, log, trn, &tmpdirty, log_op,
                                    cur->trak, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s apply log %d %d\n", __func__, rc, *bdberr);
            goto done;
        }
        *dirty |= tmpdirty;
        if (last == log) {
            break; /* we are done here */
        }

        log = bdb_osql_trn_next_log(trn, log, bdberr);
        if (*bdberr) {
            logmsg(LOGMSG_ERROR, "%s next_log %d\n", __func__, *bdberr);
            goto done;
        }

        if (log == NULL)
            logmsg(LOGMSG_ERROR, "FUNKY CASE OCCURS !\n");

    } while (log != NULL);

done:
    /* close log cursor */
    rc = logcur->close(logcur, 0);
    if (rc) {
        *bdberr = rc;
        return -1;
    }

    return 0;
}

static int free_it(void *obj, void *arg)
{
    free(obj);
    return 0;
}

static void free_hash(hash_t *h)
{
    hash_for(h, free_it, NULL);
    hash_clear(h);
    hash_free(h);
}

void bdb_return_pglogs_hashtbl(hash_t *hashtbl);
void bdb_return_pglogs_relink_hashtbl(hash_t *hashtbl);

// void bdb_destory_pglogs_hashtbl(hash_t *hashtbl);
/**
 * Free all shadows upon transaction commit/rollback
 *
 */
int bdb_tran_free_shadows(bdb_state_type *bdb_state, tran_type *tran)
{
    int dbnum = 0;
    int have_errors = 0;
    int bdberr;
    int rc = 0;

    if (!tran)
        return 0;

    if (tran->asof_hashtbl) {
        bdb_return_pglogs_hashtbl(tran->asof_hashtbl);
        tran->asof_hashtbl = NULL;
    }

    if (tran->pglogs_hashtbl) {
        bdb_return_pglogs_hashtbl(tran->pglogs_hashtbl);
        tran->pglogs_hashtbl = NULL;
    }

    if (tran->relinks_hashtbl) {
        bdb_return_pglogs_relink_hashtbl(tran->relinks_hashtbl);
        tran->relinks_hashtbl = NULL;
    }

    if (!tran->tables)
        return 0;

    /* XXX needs to be fixed */
    for (dbnum = 0; dbnum < tran->numchildren; dbnum++) {
        have_errors +=
            bdb_free_shadows_table(bdb_state, tran->tables[dbnum].ix_shadows);

        have_errors +=
            bdb_free_shadows_table(bdb_state, tran->tables[dbnum].dt_shadows);

        have_errors +=
            bdb_free_shadows_table(bdb_state, tran->tables[dbnum].sk_shadows);

        have_errors +=
            bdb_free_shadows_table(bdb_state, tran->tables[dbnum].bl_shadows);
    }

    free(tran->tables);
    tran->tables = NULL;

    return have_errors;
}

/**
 * Returns the first deleted genid of a transaction if one exists
 * and a cursor for retrieving the next records.
 * Returns NULL if no deleted rows.
 *
 *
 */
struct temp_cursor *bdb_tran_deltbl_first(bdb_state_type *bdb_state,
                                          tran_type *shadow_tran, int dbnum,
                                          unsigned long long *genid,
                                          char **data, int *datalen,
                                          int *bdberr)
{
    tmpcursor_t *cur = NULL;
    int rc = 0;

    *bdberr = 0;

    if (!shadow_tran->tables ||
        !shadow_tran->tables[dbnum]
             .sk_shadows) /* no deletes for this transaction*/
        return NULL;

    cur = bdb_temp_table_cursor(bdb_state,
                                shadow_tran->tables[dbnum].sk_shadows->tbls[0],
                                NULL, bdberr);
    if (!cur)
        return NULL;

    rc = bdb_temp_table_first(bdb_state, cur, bdberr);
    if (rc == IX_OK) {
        char *key = bdb_temp_table_key(cur);
        int keylen = bdb_temp_table_keysize(cur);

        assert(keylen == sizeof(*genid));

        memcpy(genid, key, sizeof(*genid));

        *data = bdb_temp_table_data(cur);
        *datalen = bdb_temp_table_datasize(cur);

        return cur;
    }
    *data = NULL;
    *datalen = 0;

    rc = bdb_temp_table_close_cursor(bdb_state, cur, bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR, "%s: close cursor %d %d\n", __func__, rc, *bdberr);

    return NULL;
}

/**
 * Returns the next deleted genid of a transaction
 * if one exists and IX_OK;
 * Returns IX_PASTEOF if done.  The cursor is destroyed
 * immediately when returning IX_PASTEOF.
 *
 */
int bdb_tran_deltbl_next(bdb_state_type *bdb_state, tran_type *shadow_tran,
                         struct temp_cursor *cur, unsigned long long *genid,
                         char **data, int *datalen, int *bdberr)
{
    int rc = 0;

    rc = bdb_temp_table_next(bdb_state, cur, bdberr);
    if (rc == IX_OK) {
        char *key = bdb_temp_table_key(cur);
        int keylen = bdb_temp_table_keysize(cur);

        assert(keylen == sizeof(*genid));

        memcpy(genid, key, sizeof(*genid));

        *data = bdb_temp_table_data(cur);
        *datalen = bdb_temp_table_datasize(cur);

        return IX_FND;
    }

    *data = NULL;
    *datalen = 0;

    rc = bdb_temp_table_close_cursor(bdb_state, cur, bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR, "%s: close cursor %d %d\n", __func__, rc, *bdberr);

    return IX_PASTEOF;
}

/* hack, for now */
extern int sqlglue_release_genid(unsigned long long genid, int *bdberr);

/**
 * Check if a real genid is marked deleted.
 *
 * READ COMMITTED RULES (present skip list)
 * - skip locally deleted rows (in skip list)
 *
 * SNAPSHOT/SERIALIZABLE RULES
 * (present skip list, birth genid and rowlock support)
 * - skip locally deleted rows (in skip list)
 * - skip rows with genid bigger than transaction birth genid
 */
static int _bdb_tran_deltbl_isdeleted(bdb_cursor_ifn_t *pcur_ifn,
                                      unsigned long long genid,
                                      int ignore_limit, int *bdberr, int check)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    unsigned long long *pgenid;
    int rc = 0;

    /* we dont ever expect to get here with a synthetic genid */
    if (is_genid_synthetic(genid)) /* only real genid are marked deleted */
        return 0;

    /* we add this to socksql as an artifact of saving session in socksql */
    if (cur->shadow_tran && cur->shadow_tran->tranclass != TRANCLASS_SOSQL) {
        /* check genid cases the genid limit */
        switch (cur->shadow_tran->tranclass) {
        case TRANCLASS_READCOMMITTED: break;
        case TRANCLASS_SERIALIZABLE:
        case TRANCLASS_SNAPISOL:
            if (/*!ignore_limit &&*/ cur->shadow_tran->startgenid &&
                bdb_cmp_genids(cur->shadow_tran->startgenid, genid) < 0) {
                if (cur->trak) {
                    logmsg(LOGMSG_USER, "Cur %p skipping newer genid 0x%llx "
                                    "(startgenid = 0x%llx)\n",
                            cur, genid, cur->shadow_tran->startgenid);
                }
                /*
                fprintf( stderr, "%s:%d SKIPPING %llx, tran is %llx\n",
                      __FILE__, __LINE__,
                      genid, cur->shadow_tran->startgenid);

                cheap_stack_trace();
                */
                return 1;
            } else {
                if (cur->trak) {
                    logmsg(LOGMSG_USER, "Cur %p NOT skipping older genid 0x%llx "
                                    "(startgenid = 0x%llx)\n",
                            cur, genid, cur->shadow_tran->startgenid);
                }
            }
            break;
        default:
            logmsg(LOGMSG_FATAL, "%s:%d Not recognized transaction mode!\n",
                    __FILE__, __LINE__);
            abort();
            break;
        }

        if (!cur->shadow_tran->check_shadows) {
            return 0;
        }
    }

    /* we can check here also if we are trying to dedup one pass deletes */
    if (!check ||
        (cur->shadow_tran && cur->shadow_tran->tranclass != TRANCLASS_SOSQL)) {
        if (!cur->skip) {
            cur->skip =
                bdb_tran_open_shadow(cur->state, cur->dbnum, cur->shadow_tran,
                                     0, BDBC_SK, 0 /*no create*/, bdberr);
            if (!cur->skip) {
                if (*bdberr)
                    return -1;
            }
        }

        if (cur->skip) {
            /* we have a skip cursor */
            pgenid = (unsigned long long *)malloc(sizeof(*pgenid));
            if (!pgenid) {
                *bdberr = BDBERR_MALLOC;
                return -1;
            }
            *pgenid = genid;

            rc = bdb_temp_table_find_exact(cur->state, cur->skip, pgenid,
                                           sizeof(*pgenid), bdberr);
            if (rc != IX_FND)
                free(pgenid);
            if (rc < 0)
                return rc;

            /* found match in skiplist, return 1 meaning "skip" */
            if (rc == IX_FND) {
                /*
                fprintf( stderr, "%s:%d SKIPPING %llx, tran is %llx\n",
                      __FILE__, __LINE__,
                      genid, cur->shadow_tran->startgenid);
                 */

                if (cur->trak) {
                    logmsg(LOGMSG_USER, "Cur %p skipping deleted genid 0x%llx\n",
                            cur, bdb_genid_to_host_order(genid));
                }

                return 1;
            }
        }
    }

    /* we made it this far - return 0, "dont skip" */
    return 0;
}

int bdb_tran_deltbl_isdeleted(bdb_cursor_ifn_t *pcur_ifn,
                              unsigned long long genid, int ignore_limit,
                              int *bdberr)
{
    return _bdb_tran_deltbl_isdeleted(pcur_ifn, genid, ignore_limit, bdberr, 1);
}

int bdb_tran_deltbl_isdeleted_dedup(bdb_cursor_ifn_t *pcur_ifn,
                                    unsigned long long genid, int ignore_limit,
                                    int *bdberr)
{
    return _bdb_tran_deltbl_isdeleted(pcur_ifn, genid, ignore_limit, bdberr, 0);
}

/**
 * Mark the provided genid deleted
 *
 */
int bdb_tran_deltbl_setdeleted(bdb_cursor_ifn_t *pcur_ifn,
                               unsigned long long genid, void *data,
                               int datalen, int *bdberr)
{
    bdb_cursor_impl_t *cur = pcur_ifn->impl;
    tmpcursor_t *skip = NULL;
    int rc = 0;

    if (!cur->shadow_tran) {
        logmsg(LOGMSG_ERROR, "%s: wrong sql transaction!\n", __func__);
        *bdberr = BDBERR_BUG_KILLME;
        return -1;
    }

    cur->shadow_tran->check_shadows = 1;

    if (!cur->skip) {
        cur->skip =
            bdb_tran_open_shadow(cur->state, cur->dbnum, cur->shadow_tran, 0,
                                 BDBC_SK, 1 /*create*/, bdberr);
        if (!cur->skip) {
            logmsg(LOGMSG_ERROR, "%s: fail to open skip %d %d!\n", __func__, rc,
                    *bdberr);
            return -1;
        }
    }

    rc = bdb_temp_table_insert(cur->state, cur->skip, &genid,
                               sizeof(genid), data, datalen, bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: fail to insert skip %llx %d %d!\n", __func__,
                genid, rc, *bdberr);
        return -1;
    }

    return 0;
}

/*
   open a shadow, create shadow if there is no one
 */
static tmpcursor_t *open_shadow_int(bdb_state_type *bdb_state,
                                    tran_shadow_t **pshadows, int file,
                                    int maxfile, int stripe, int maxstripe,
                                    int create, int nocursor, int *bdberr)
{
    int rc = 0;
    tmpcursor_t *shad_cur = NULL;

    if (!*pshadows) {
        if (!create)
            return NULL;

        *pshadows = (tran_shadow_t *)calloc(maxfile, sizeof(tran_shadow_t));
        if (!*pshadows) {
            logmsg(LOGMSG_ERROR, "%s: calloc error size %zu\n", __func__,
                   maxfile * sizeof(tran_shadow_t));
            *bdberr = BDBERR_BADARGS;
            return NULL;
        }
        (*pshadows)->nshadows = maxfile;
    }

    if (!(*pshadows)[file].tbls) {
        if (!create)
            return NULL;

        (*pshadows)[file].tbls =
            (tmptable_t **)calloc(maxstripe, sizeof(tmptable_t *));
        if (!(*pshadows)[file].tbls) {
            logmsg(LOGMSG_ERROR, "%s: calloc error size %zu\n", __func__,
                   maxstripe * sizeof(tmptable_t *));
            *bdberr = BDBERR_BADARGS;
            return NULL;
        }
        (*pshadows)[file].ntbls = maxstripe;
    }

    if (!(*pshadows)[file].tbls[stripe]) {
        if (!create)
            return NULL;

        /* create a temporary table */
        (*pshadows)[file].tbls[stripe] =
            bdb_temp_table_create(bdb_state, bdberr);
        if (!(*pshadows)[file].tbls[stripe]) {
            logmsg(LOGMSG_ERROR, 
                    "%s: fail create shadow %d %d, rc = %d bdberr = %d\n",
                    __func__, file, stripe, rc, *bdberr);
            return NULL;
        }
    }

    /* at this point, we either have a shadow table (irrespective of create) or
       we
       want to create one (and we did);
       go ahead and create a cursor for it

       NOTE: while shadow tables are managed by shadow_tran, the shadow cursors
       are part of bdbcursor (though sd pointer)
     */
    if (nocursor)
        return NULL;

    /* Create a shadow-table cursor. */
    shad_cur = bdb_temp_table_cursor(bdb_state, (*pshadows)[file].tbls[stripe],
                                     NULL, bdberr);
    if (!shad_cur) {
        logmsg(LOGMSG_ERROR, "%s: fail create shadow %d %d, rc = %d bdberr = %d\n",
                __func__, file, stripe, rc, *bdberr);
        return NULL;
    }

    return shad_cur;
}

static tmpcursor_t *open_shadow(bdb_state_type *bdb_state,
                                tran_shadow_t **pshadows, int file, int maxfile,
                                int stripe, int maxstripe, int create,
                                int *bdberr)
{
    return open_shadow_int(bdb_state, pshadows, file, maxfile, stripe,
                           maxstripe, create, 0, bdberr);
}

/* Create a shadow table, but do not return a cursor to it. */
static void open_shadow_nocursor(bdb_state_type *bdb_state,
                                 tran_shadow_t **pshadows, int file,
                                 int maxfile, int stripe, int maxstripe,
                                 int *bdberr)
{
    open_shadow_int(bdb_state, pshadows, file, maxfile, stripe, maxstripe, 1, 1,
                    bdberr);
}

/* free a set of shadows */
static int bdb_free_shadows_table(bdb_state_type *bdb_state,
                                  tran_shadow_t *shadows)
{
    int rc = 0;
    int bdberr = 0;
    int have_errors = 0;
    int num = 0;
    int tbl = 0;
    int nshadows = 0;

    if (!shadows)
        return 0;

    nshadows = shadows->nshadows;
    for (num = 0; num < nshadows; num++) {
        /* free a tran_shadow */
        if (shadows[num].tbls) {

            for (tbl = 0; tbl < shadows[num].ntbls; tbl++) {
                if (shadows[num].tbls[tbl]) {
                    rc = bdb_temp_table_close(bdb_state, shadows[num].tbls[tbl],
                                              &bdberr);
                    if (rc) {
                        logmsg(LOGMSG_ERROR, 
                                "%s: fail to close temp rc = %d bdberr = %d\n",
                                __func__, rc, bdberr);
                        have_errors++;
                    }
                }
            }

            free(shadows[num].tbls);
        }
    }

    /* free the array */
    free(shadows);

    return have_errors;
}

/* init bdb osql support for snapshot/serializable sql transactions */
int bdb_osql_init(int *bdberr)
{
    int rc = 0;

    rc = bdb_osql_trn_repo_init(bdberr);

    return rc;
}

/* destroy bdb osql support for snapshot/serializable sql transactions */
int bdb_osql_destroy(int *bdberr)
{
    int rc = 0;

    rc = bdb_osql_trn_repo_destroy(bdberr);
    if (rc)
        return rc;

    rc = bdb_osql_log_repo_destroy(bdberr);

    return rc;
}
