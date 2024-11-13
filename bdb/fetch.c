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

/*
 * The fetch monster.
 *
 * just like sam did with db/record.c, the intent here is that there
 * will be 1 uniform way to read a record (be it by index or genid,
 * be it with transactions or not, be it with doing lookahead or
 * not, etc etc, for every dumb permutation and combination we've
 * made lots of pocket routines to do over the years.  all read
 * apis will wrap bdb_fetch_int ultimately.
 *
 */

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <assert.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stddef.h>
#include <pthread.h>

#include <build/db.h>
#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "bdb_cursor.h"
#include "locks.h"
#include "genid.h"
#include "bdb_fetch.h"
#include "logmsg.h"

extern int gbl_rowlocks;
extern int gbl_new_snapisol;
extern int gbl_test_blob_race;

/* if cursor_ser_enabled is enabled and the disabling pbit isn't set */
#define CURSOR_SER_ENABLED(bdb_state) (bdb_state->attr->enable_cursor_ser)

static DB_TXN *resolve_db_txn(bdb_state_type *bdb_state, tran_type *tran)
{
    DB_TXN *rtn = NULL;
    int rc;
    if (tran) {
        if (tran->tranclass == TRANCLASS_LOGICAL && !tran->reptxn) {
            tran_type *pptr;
            if ((rc = get_physical_transaction(bdb_state, tran, &pptr, 0)) !=
                0) {
                logmsg(LOGMSG_ERROR, "%s %d: error getting transaction, rc=%d\n",
                        __FILE__, __LINE__, rc);
                abort();
            }
            rtn = pptr->tid;
        } else {
            rtn = tran->tid;
        }
    }
    return rtn;
}

/* get the requested blobs by rrn+genid, confirming in each case that the
 * rrn matches. */

/* This function is called in two ways: one wants it to fetch all of the blobs.
 * Another wants to fetch only a single blob. */
static int bdb_fetch_blobs_by_rrn_and_genid_int_int(
    bdb_state_type *bdb_state, tran_type *tran, int rrn,
    unsigned long long ingenid, int numblobs, int *dtafilenums,
    size_t *blobsizes, size_t *bloboffs, void **blobptrs,
    bdb_cursor_ifn_t *pparent, bdb_fetch_args_t *args, int *bdberr)
{
    bdb_cursor_impl_t *parent = (pparent) ? pparent->impl : NULL;
    unsigned long long genid = ingenid;
    unsigned long long genid_t;
    int blobn, bloberr, outrc = 0;
    void *dta = NULL, *freeptr;
    struct odh odh;
    int dtalen = 0;
    int called_update_shadows = 0;
    int try_shadow_table = 0;
    int use_shadow_table;
    int dbnum = -1;
    *bdberr = 0;

    if (numblobs > 0) {
        if (!dtafilenums || !blobsizes || !blobptrs || !bloboffs ||
            numblobs > MAXDTAFILES) {
            *bdberr = BDBERR_BADARGS;
            return -1;
        }

        for (blobn = 0; blobn < numblobs; blobn++) {
            if (dtafilenums[blobn] < 0 ||
                dtafilenums[blobn] >= bdb_state->numdtafiles) {
                *bdberr = BDBERR_BADARGS;
                return -1;
            }
            blobsizes[blobn] = 0;
            bloboffs[blobn] = 0;
            blobptrs[blobn] = NULL;
        }
    }
    /* synthetic genids should not make it here */
    if (is_genid_synthetic(genid)) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    /* If the last record was from the shadow-table, try the shadow-table. */
    if (parent && parent->shadow_tran) {
        if ((parent->used_sd && parent->lastkeylen == 0) ||
            (parent->used_rl &&
             parent->state->attr->dtastripe == parent->laststripe) ||
            (parent->invalidated)) {
            try_shadow_table = 1;
            dbnum = get_dbnum_by_handle(bdb_state);
        }
    }

    bloberr = 0;
    for (blobn = 0; blobn < numblobs; blobn++) {
        DBT dbt_key, dbt_data;
        DBT dbt_key_t, dbt_data_t;
        DB *dbp;
        DB_TXN *tid;
        int rc2 = 0;
        int rc = 0;

        int pgno;

        /* Could have changed in get_unpack_blob. */
        genid = ingenid;
        genid_t = ingenid;

        dbp = get_dbp_from_genid(bdb_state, dtafilenums[blobn], genid, NULL);

        tid = resolve_db_txn(bdb_state, tran);

    again:
        /* Reset use_shadow_table. */
        use_shadow_table = try_shadow_table;

        /* now get the dta corresponding to that rrn */
        memset(&dbt_key, 0, sizeof(dbt_key));
        memset(&dbt_data, 0, sizeof(dbt_data));
        memset(&dbt_key_t, 0, sizeof(dbt_key_t));
        memset(&dbt_data_t, 0, sizeof(dbt_data_t));

        dbt_key.data = &genid;
        dbt_key.size = sizeof(genid);
        dbt_key.ulen = sizeof(genid);

        dbt_key.flags |= DB_DBT_USERMEM;
        dbt_data.flags |= DB_DBT_MALLOC;

        dbt_key_t.data = &genid_t;
        dbt_key_t.size = sizeof(genid_t);
        dbt_key_t.ulen = sizeof(genid_t);

        dbt_key_t.flags |= DB_DBT_USERMEM;
        dbt_data_t.flags |= DB_DBT_MALLOC;

        if (tid) {

            /* for transactional mode, preserve the path;
               tid will have the needed lockid
               */

            rc = bdb_get_unpack_blob(bdb_state, dbp, tid, &dbt_key, &dbt_data, &args->ver, 0, args->fn_malloc,
                                     args->fn_free);

        } else {

            /* Switching this to cursor mode; this will let us
               catch potential deadlocks 02262008dh */
            DBC *dbcp = NULL;
            DBC *dbcp_t = NULL;

            if (gbl_new_snapisol && parent && parent->shadow_tran &&
                (parent->shadow_tran->tranclass == TRANCLASS_SNAPISOL ||
                 parent->shadow_tran->tranclass == TRANCLASS_SERIALIZABLE)) {
                if (tran || parent == NULL) {
                    rc = dbp->cursor(dbp, tran ? tran->tid : NULL, &dbcp_t, 0);

                    if (rc) {
                        bdb_get_error(bdb_state, tran ? tran->tid : NULL, rc,
                                      BDBERR_MISC, bdberr, __func__);
                        logmsg(LOGMSG_ERROR, 
                                "^%s: failed to get a cursor bdberr=%d\n",
                                __func__, *bdberr);
                        return -1;
                    }
                } else {
                    dbcp_t = get_cursor_for_cursortran_flags(parent->curtran,
                                                             dbp, 0, bdberr);
                    rc = (dbcp_t == 0);
                    if (rc) {
                        logmsg(LOGMSG_ERROR, 
                                "^%s: failed to get a cursor bdberr=%d\n",
                                __func__, *bdberr);
                        return -1;
                    }
                }

                genid_t = ingenid;

                rc = dbcp_t->c_get(dbcp_t, &dbt_key_t, &dbt_data_t,
                                   DB_SET_RANGE);
                pgno = dbcp_t->lastpage;
                if (dbt_data_t.data)
                    free(dbt_data_t.data);

                if ((rc2 = dbcp_t->c_close(dbcp_t)) != 0) {
                    logmsg(LOGMSG_ERROR, "bdb_fetch_blobs_by_rrn_and_genid_int_int:"
                           "error closing a temporary cursor %d\n",
                           rc2);
                }
                dbcp_t = NULL;

                if (pparent->updateshadows_pglogs(
                        pparent, (unsigned int *)&pgno, dbp->fileid, bdberr))
                    return -1;
            }

            rc = 0;
            rc2 = 0;

            if (use_shadow_table) {
                unsigned long long shadblbgenid;

                /* Shadow-blobs are stored with a masked genid. */
                shadblbgenid = get_search_genid(bdb_state, genid);

                tmpcursor_t *cur = bdb_tran_open_shadow(
                    bdb_state, dbnum, parent->shadow_tran, dtafilenums[blobn],
                    BDBC_BL, 1, bdberr);
                if (!cur) {
                    if (*bdberr) {
                        return -1;
                    }

                    /* If inplace_blob_optimization is enabled and the blob
                     * wasn't
                     * touched on an update, this is expected.  Check data file.
                     */
                    use_shadow_table = 0;
                } else {
                    rc2 =
                        bdb_temp_table_find_exact(bdb_state, cur, &shadblbgenid,
                                                  sizeof(shadblbgenid), bdberr);
                    if (rc2 != IX_FND)
                    if (rc2 < 0)
                        return -1;

                    if (rc2 == IX_FND && bdb_temp_table_datasize(cur) > 0) {
                        /* TODO: optimized codepath for BLOBS will need work
                         * here. */

                        /* Unpack this. */
                        if (bdb_unpack(bdb_state, bdb_temp_table_data(cur),
                                       bdb_temp_table_datasize(cur), dta,
                                       dtalen, &odh, &freeptr)) {
                            *bdberr = BDBERR_MISC;
                            return -1;
                        }

                        /* Copy the length. */
                        dbt_data.size = odh.length;

                        /* Allocate memory and copy the record if we found
                         * anything. */
                        if (dbt_data.size) {
                            /* If bdb_unpack already allocated memory use that.
                             */
                            if (freeptr) {
                                dbt_data.data = freeptr;
                            }
                            /* If malloc fails, return a bad rcode. */
                            else if (NULL ==
                                     (dbt_data.data = malloc(odh.length))) {
                                *bdberr = BDBERR_MALLOC;
                                return -1;
                            }
                            /* Otherwise, copy the data. */
                            else {
                                memmove(dbt_data.data, odh.recptr, odh.length);
                            }
                        } else {
                            dbt_data.data = malloc(
                                1); /* alloc something to diff from NULL */
                        }
                        rc = 0;
                    }
                    /* Size-0 record indicates a NULL blob which has been
                       updated to not-NULL. */
                    else if (rc2 == IX_FND) {
                        rc = DB_NOTFOUND;
                    }
                    /* An inplace-optimized blob won't be touched.  Check the
                       data file. */
                    else {
                        use_shadow_table = 0;
                    }

                    rc2 = bdb_temp_table_close_cursor(bdb_state, cur, bdberr);
                    if (rc2)
                        return -1;
                }
            }

            if (use_shadow_table == 0) {
                if (tran || parent == NULL) {
                    rc = dbp->cursor(dbp, tran ? tran->tid : NULL, &dbcp, 0);

                    if (rc) {
                        bdb_get_error(bdb_state, tran ? tran->tid : NULL, rc,
                                      BDBERR_MISC, bdberr, __func__);
                        logmsg(LOGMSG_ERROR, 
                                "^%s: failed to get a cursor bdberr=%d\n",
                                __func__, *bdberr);
                        return -1;
                    }
                } else {
                    /* rc = dbp->paired_cursor(dbp,
                     * (DBC*)bdb_cursor_dbcp(parent), &dbcp, 0); */

                    dbcp = get_cursor_for_cursortran_flags(parent->curtran, dbp,
                                                           parent->use_snapcur ? DB_CUR_SNAPSHOT : 0, bdberr);

                    rc = (dbcp == 0);
                    if (rc) {
                        logmsg(LOGMSG_ERROR, 
                                "^%s: failed to get a cursor bdberr=%d\n",
                                __func__, *bdberr);
                        return -1;
                    }
                }

                rc = bdb_cget_unpack_blob(bdb_state, dbcp, &dbt_key, &dbt_data, &args->ver, DB_SET, args->fn_malloc,
                                          args->fn_free);

                if ((rc2 = dbcp->c_close(dbcp)) != 0) {
                    logmsg(LOGMSG_ERROR, "bdb_fetch_blobs_by_rrn_and_genid_int_int:"
                           "error closing a temporary cursor %d\n",
                           rc2);
                }

                if (rc == DB_LOCK_DEADLOCK)
                    goto errout;

                /* The blob has been updated since I first found the record.  */
                if (rc == IX_FND &&
                    get_updateid_from_genid(bdb_state, genid) >
                        get_updateid_from_genid(bdb_state, ingenid)) {
                    /* Sanity check. */
                    if (try_shadow_table) {
                        assert(called_update_shadows == 0);

                        /* The blob-file has already been updated- this means
                         * that there's something
                         * on the global-queue for me to consume. */
                        if (pparent->updateshadows(pparent, bdberr))
                            return -1;

                        /* Set the update-shadows flag. */
                        called_update_shadows = 1;

                        goto again;
                    } else {
                        /* this is a verify error for a tag find &blobask pair
                         * of calls */
                        return IX_NOTFND;
                    }
                }

                /* If this is not found and we did a try-shadows first */
                if (try_shadow_table && rc != IX_FND) {
                    /* If we haven't updated shadows, do so now. */
                    if (called_update_shadows == 0) {
                        if (pparent->updateshadows(pparent, bdberr)) {
                            logmsg(LOGMSG_ERROR, "%s: error updating shadows!\n",
                                    __func__);
                            return -1;
                        }

                        /* Set the update-shadows flag. */
                        called_update_shadows = 1;

                        /* We have to try to find this again. */
                        goto again;
                    }

                    /* This is genuinely a NULL blob.  Make this a single
                     * shadow-btree hit from now on. */
                    if (pparent->setnullblob(pparent, ingenid, dbnum,
                                             dtafilenums[blobn], bdberr)) {
                        logmsg(LOGMSG_ERROR, "%s: error in setnullblob!\n",
                                __func__);
                        return -1;
                    }
                }
            }
        }

        if (gbl_test_blob_race && 0 == (rand() % gbl_test_blob_race)) {
            rc = DB_NOTFOUND;
        }

        if (rc == 0) {
            blobsizes[blobn] = dbt_data.size;
            bloboffs[blobn] = 0;
            blobptrs[blobn] = dbt_data.data;
        } else if (rc == DB_NOTFOUND) {
            /* this could mean that there isn't meant to be any data for this
             * blob (a NULL field), or it could mean that someone
             * deleted/updated
             * the record while we were reading it.  at this low level we just
             * don't know.  higher levels should retry if there is meant to be
             * data here. */
            blobsizes[blobn] = 0;
            bloboffs[blobn] = 0;
            blobptrs[blobn] = NULL;
        } else {
        errout:
            bdb_get_error(bdb_state, NULL, rc, BDBERR_FETCH_DTA, bdberr,
                          "bdb_fetch_blobs_by_rrn_and_genid_int_int");
            outrc = -1;
            bloberr = 1;
            break;
        }
    }

    if (bloberr) {
        for (blobn--; blobn >= 0; blobn--) {
            if (blobptrs[blobn])
                myfree(blobptrs[blobn]);
            blobsizes[blobn] = 0;
            bloboffs[blobn] = 0;
            blobptrs[blobn] = NULL;
        }
    }

    return outrc;
}

static int bdb_fetch_blobs_by_rrn_and_genid_int(
    bdb_state_type *bdb_state, int rrn, unsigned long long genid, int numblobs,
    int *dtafilenums, size_t *blobsizes, size_t *bloboffs, void **blobptrs,
    bdb_cursor_ifn_t *pparent, bdb_fetch_args_t *args, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_fetch_blobs_by_rrn_and_genid");

    rc = bdb_fetch_blobs_by_rrn_and_genid_int_int(
        bdb_state, NULL, rrn, genid, numblobs, dtafilenums, blobsizes, bloboffs,
        blobptrs, pparent, args, bdberr);

    BDB_RELLOCK();

    return rc;
}

int bdb_fetch_blobs_by_rrn_and_genid(bdb_state_type *bdb_state, int rrn,
                                     unsigned long long genid, int numblobs,
                                     int *dtafilenums, size_t *blobsizes,
                                     size_t *bloboffs, void **blobptrs,
                                     bdb_fetch_args_t *args, int *bdberr)
{

    return bdb_fetch_blobs_by_rrn_and_genid_int(
        bdb_state, rrn, genid, numblobs, dtafilenums, blobsizes, bloboffs,
        blobptrs, NULL, args, bdberr);
}

int bdb_fetch_blobs_by_rrn_and_genid_cursor(
    bdb_state_type *bdb_state, int rrn, unsigned long long genid, int numblobs,
    int *dtafilenums, size_t *blobsizes, size_t *bloboffs, void **blobptrs,
    bdb_cursor_ifn_t *pparent, bdb_fetch_args_t *args, int *bdberr)
{
    /*
         -this happens naturally for select queries
        if(!parent) {
            fprintf(stderr, "bdb_fetch_blobs_by_rrn_and_genid_cursor: calling w/
       no cursor!\n");
        }
    */
    return bdb_fetch_blobs_by_rrn_and_genid_int(
        bdb_state, rrn, genid, numblobs, dtafilenums, blobsizes, bloboffs,
        blobptrs, pparent, args, bdberr);
}

int bdb_fetch_blobs_by_rrn_and_genid_tran(
    bdb_state_type *bdb_state, tran_type *tran, int rrn,
    unsigned long long genid, int numblobs, int *dtafilenums, size_t *blobsizes,
    size_t *bloboffs, void **blobptrs, bdb_fetch_args_t *args, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_fetch_blobs_by_rrn_and_genid_tran");

    rc = bdb_fetch_blobs_by_rrn_and_genid_int_int(
        bdb_state, tran, rrn, genid, numblobs, dtafilenums, blobsizes, bloboffs,
        blobptrs, NULL, args, bdberr);

    BDB_RELLOCK();

    return rc;
}

static inline int fetch_cget(bdb_state_type *bdb_state, int ixnum, DBC *dbcp,
                             DBT *key, DBT *data, u_int32_t flags)
{
    if (-1 == ixnum) {
        return bdb_cget(bdb_state, dbcp, key, data, flags);
    } else {
        return dbcp->c_get(dbcp, key, data, flags);
    }
}

/******************************************************************************
 *
 * The great fetch routine.
 *
 *****************************************************************************/

/*
  return_dta : 1 == retrieve and return dta record, 0 == dont return dta
               2: retrieve data but not from ixdta - primarily useful
                for verification purposes.
  cur_next_prev :  0 == cur, 1 == next, 2 == prev
*/
/*
  for next/prev:
  search on lastix, ixnum, lastrrn.  key is full length.
  use ix, ixlen to determine rcode only
*/

void bdb_cursor_ser_invalidate(bdb_cursor_ser_t *cur_ser)
{
    ((bdb_cursor_ser_int_t *)cur_ser)->is_valid = 0;
}

#define SAVEGENID                                                              \
    memcpy(&foundgenid, dbt_data.data, sizeof(unsigned long long));            \
    if (havedta) {                                                             \
        llptr = (unsigned long long *)dbt_data.data;                           \
        memcpy(dta, llptr + 1,                                                 \
               MIN(dbt_data.size - sizeof(unsigned long long), dtalen));       \
        *reqdtalen = dbt_data.size - sizeof(unsigned long long);               \
    }

static int bdb_fetch_int_ll(
    int return_dta, int direction, int lookahead, bdb_state_type *bdb_state,
    void *ix, int ixnum, int ixlen, void *lastix, int lastrrn,
    unsigned long long lastgenid, void *dta, int dtalen, int *reqdtalen,
    void *ixfound, int *rrn, int *recnum, unsigned long long *genid,
    int numblobs, int *dtafilenums, size_t *blobsizes, size_t *bloboffs,
    void **blobptrs, int dirty, tran_type *tran, bdb_cursor_ser_int_t *cur_ser,
    bdb_fetch_args_t *args, u_int32_t lockerid, int *bdberr)
{
    DBT dbt_key, dbt_data;
    int rc;
    DBC *dbcp = NULL;
    int outrc;
    int past_three_outrc;
    int foundrrn;
    int found;
    int flags;
    unsigned long long masked_genid;
    int havedta;
    int ixlen_full;
    int keycontainsgenid;
    int ixrecnum;
    DB *dbp;
    unsigned long long foundgenid;
    int dtafile = 0;
    int cursor_flags;
    int get_flags;
    DB_TXN *tid;
    struct odh odh;
    int attempt_deserializaion;
    int initial_rc;
    int page_order = 0;

    /* the "data" for an index can be a genid + record for dtastripe */
    char tmp_data[BDB_RECORD_MAX + sizeof(unsigned long long)];
    char tmp_key[BDB_RECORD_MAX + sizeof(unsigned long long)];
    char tmp_last_key[BDB_RECORD_MAX + sizeof(unsigned long long)];
    char *last_key = NULL;

    unsigned long long *llptr;

    uint8_t *ver = &args->ver;

    foundrrn = 0;
    foundgenid = 0;
    *bdberr = BDBERR_NOERROR;
    tid = resolve_db_txn(bdb_state, tran);
    int paired_cursor = (lockerid > 0 && !tran) ? 1 : 0;
    attempt_deserializaion = 0;

    havedta = 0;
    if ((ixnum < -1) || (ixnum >= bdb_state->numix)) {
        *bdberr = BDBERR_BADARGS;
        *rrn = 0;
        return -1;
    }

    /* ixnum -1 means use the data files directly without an index -
       key is the genid */
    if (ixnum == -1) {
        /* must be data striped and the lookup key must be empty or the length
         * of a genid */
        if (ixlen != 0 && ixlen != sizeof(unsigned long long)) {
            logmsg(LOGMSG_ERROR, "%s: data file ixfind badargs: not dtastripe: %d "
                            "or non zero key length: %d\n",
                    __func__, bdb_state->attr->dtastripe, ixlen);
            *bdberr = BDBERR_BADARGS;
            *rrn = 0;
            return -1;
        }

        ixlen_full = sizeof(unsigned long long); /* len of a gmonid */
        keycontainsgenid = 0;
        ixrecnum = 0;
        lookahead = 0;
        dbp = NULL; /* will be set later */
    } else {
        /* we are using an actual index */
        if ((bdb_state->ixdta[ixnum]) && (return_dta == 1))
            havedta = 1;

        ixlen_full = bdb_state->ixlen[ixnum];
        keycontainsgenid = bdb_keycontainsgenid(bdb_state, ixnum);
        ixrecnum = bdb_state->ixrecnum[ixnum];
        dbp = bdb_state->dbp_ix[ixnum];
    }

    if (return_dta)
        *reqdtalen = 0;

    if (ixlen == -1)
        ixlen = ixlen_full;

    if ((ixlen > ixlen_full) || (ixlen < 0)) {
        *bdberr = BDBERR_BADARGS;
        *rrn = 0;
        return -1;
    }

    memset(&dbt_key, 0, sizeof(dbt_key));
    memset(&dbt_data, 0, sizeof(dbt_data));

    dbt_key.flags = DB_DBT_USERMEM;
    dbt_key.data = tmp_key;
    dbt_key.ulen = sizeof(tmp_key);
    dbt_key.size = ixlen;

    flags = DB_SET_RANGE;

    switch (direction) {
    case FETCH_INT_CUR_LASTDUPE:
        if (ixnum == -1) {
            logmsg(LOGMSG_ERROR, "%s: cannot FETCH_INT_CUR_LASTDUPE on data\n",
                    __func__);
            *bdberr = BDBERR_BADARGS;
            *rrn = 0;
            return -1;
        }
    case FETCH_INT_CUR:
        /* if we're working on the data, find the stripe we are using */
        if (ixnum == -1) {
            if (ixlen == 0)
                dtafile = 0;

            else {
                dtafile = get_dtafile_from_genid(*(unsigned long long *)ix);

                if (dtafile < 0 || dtafile >= bdb_state->attr->dtastripe) {
                    logmsg(LOGMSG_ERROR, 
                            "%s: dtafile=%d out of range genid %016llx\n",
                            __func__, dtafile, *(unsigned long long *)ix);
                    *bdberr = BDBERR_BADARGS;
                    *rrn = 0;
                    return -1;
                }

                /* if this is a FETCH_INT_CUR with ixlen != 0, (ie a direct
                 * lookup on a genid) we don't want the user to be able to
                 * try and use this cursor to move around, it would give
                 * them odd behavior that they probably wouldn't expect
                 * (ie increasing genids that suddenly decrease when the
                 * cursor moves to the next stripe). So we set the ixfound
                 * to be a genid of 0 which will be caught in any future
                 * calls with FETCH_INT_NEXT */
                if (ixfound) {
                    bzero(ixfound, ixlen_full);
                    ixfound = NULL;
                }
            }

            dbp = bdb_state->dbp_data[0][dtafile];
            if (args->for_write) {
                 flags |= DB_RMW;
            }
        }

        memcpy(tmp_key, ix, ixlen);
        break;

    case FETCH_INT_CUR_BY_RECNUM:
        if (ixnum == -1) {
            *bdberr = BDBERR_BADARGS;
            *rrn = 0;
            return -1;
        }
        memcpy(&tmp_key, recnum, sizeof(int));
        dbt_key.size = sizeof(int);
        flags = DB_SET_RECNO;
        break;

    case FETCH_INT_PREV:
        if (ixnum == -1) {
            logmsg(LOGMSG_ERROR, "%s: cannot FETCH_INT_PREV on data\n", __func__);
            *bdberr = BDBERR_BADARGS;
            *rrn = 0;
            return -1;
        }
    /* fall through */
    case FETCH_INT_NEXT:

        /* if we're working on the data, find the stripe we are using */
        if (ixnum == -1) {
            /* if the last call was a FETCH_INT_CUR with ixlen != 0, it
             *_hdr sets its ixfound to a genid of 0 so that we can catch it here
             * and not let someone try to move around with that cursor */
            unsigned long long zero_genid = 0;
            if (memcmp(lastix, &zero_genid, sizeof(zero_genid)) == 0) {
                logmsg(LOGMSG_ERROR, 
                        "%s: unset lastix, possibly trying to move "
                        "to next after doing a direct lookup on a genid?\n",
                        __func__);
                *bdberr = BDBERR_BADARGS;
                *rrn = 0;
                return -1;
            }

            dtafile = get_dtafile_from_genid(*(unsigned long long *)lastix);

            if (dtafile < 0 || dtafile >= bdb_state->attr->dtastripe) {
                logmsg(LOGMSG_ERROR, "%s: dtafile=%d out of range\n", __func__,
                        dtafile);
                *bdberr = BDBERR_BADARGS;
                *rrn = 0;
                return -1;
            }

            dbp = bdb_state->dbp_data[0][dtafile];
        }

        memcpy(tmp_key, lastix, ixlen_full);

        dbt_key.data = tmp_key;
        dbt_key.ulen = sizeof(tmp_key);
        dbt_key.size = ixlen_full;

        if (keycontainsgenid) {
            masked_genid = get_search_genid(bdb_state, lastgenid);
            memcpy(tmp_key + ixlen_full, &masked_genid,
                   sizeof(unsigned long long));
            dbt_key.size += sizeof(unsigned long long);
            memcpy(tmp_last_key, tmp_key, dbt_key.size);
            last_key = tmp_last_key;
        } else
            last_key = lastix;

        attempt_deserializaion = 1;

        break;
    }

    outrc = 0;
    past_three_outrc = 0;

before_first_lookup:
    /* if we just jumped back here because we're looking through the data and
     * didn't find what we were looking for in the previous stripe */
    if (outrc) {
        /* if we found some data record in previous stripe */
        if (outrc == 3)
            past_three_outrc = 1;

        outrc = 0;

        /* we're done with previous stripe's cursor, don't serialize this
         * cursor, we're moving to a different file */
        rc = dbcp->c_close(dbcp);
        attempt_deserializaion = 0;

        /* move on to the next stripe, datafile was checked to be
         * < bdb_state->attr->dtastripe - 1 before the goto */
        ++dtafile;
        dbp = bdb_state->dbp_data[0][dtafile];

        /* grab any record */
        dbt_key.size = 0;
    }

    dbt_data.flags = DB_DBT_USERMEM;

    /* intial read should always go to tmp_data (or you will break compression)
       bdb_unpack will copy this into the user's buffer */
    dbt_data.data = tmp_data;
    dbt_data.ulen = sizeof(tmp_data);

    dbt_key.data = tmp_key;
    dbt_key.ulen = sizeof(tmp_key);

    cursor_flags = 0;

    if (dirty)
        cursor_flags |= DB_DIRTY_READ;

    if (args && args->page_order) {
        cursor_flags |= DB_PAGE_ORDER;
        page_order = args->page_order;
    }

    dbcp = NULL;

    if (cur_ser) {
        if (CURSOR_SER_ENABLED(bdb_state) && attempt_deserializaion &&
            cur_ser->is_valid) {
            if ((rc = dbp->cursor_ser(dbp, tid, &cur_ser->dbcs, &dbcp,
                                      cursor_flags)) != 0) {
                dbcp = NULL;

                /* if it didn't fail because it was stale */
                if (rc != DB_CUR_STALE) {
                    bdb_cursor_error(bdb_state, NULL, rc, bdberr,
                                     "bdb_fetch_int "
                                     "cursor deserialize");
                    *rrn = 0;
                    return -1;
                }
            }
        }

        /* we will mark this valid again if we successfully call
         * dbcp->close_ser() */
        cur_ser->is_valid = 0;
    }

    /* if we didn't successfully deserialize the cursor */
    if (!dbcp) {
        if (paired_cursor)
            rc =
                dbp->paired_cursor_from_lid(dbp, lockerid, &dbcp, cursor_flags);
        else
            rc = dbp->cursor(dbp, tid, &dbcp, cursor_flags);

        if (rc != 0) {
            bdb_cursor_error(bdb_state, NULL, rc, bdberr, "bdb_fetch_int "
                                                          "cursor open");
            *rrn = 0;
            return -1;
        }
    }

    /*
    fprintf(stderr,
       "ixnum %d dbt_key.ulen = %d dbt_key.size %d dbt_data.ulen = %d\n",
       ixnum, dbt_key.ulen, dbt_key.size, dbt_data.ulen);
    */

    initial_rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data, flags);

    /*fprintf(stderr, "fetch_cget rc %d\n", rc);*/

    switch (initial_rc) {
    case 0: /* found something */

        switch (direction) {
        /* find the last record in a set of dupes defined by the partial
           search length */
        case FETCH_INT_CUR_LASTDUPE:
            /*fprintf(stderr, "FETCH_INT_CUR_LASTDUPE: %d %d\n",
              ixlen_full, ixlen);*/
            if ((!(keycontainsgenid)) && (ixlen_full == ixlen))
                goto fetch_int_cur;

            if (bdb_state->ondisk_header && bdb_state->ixdta[ixnum] &&
                bdb_state->datacopy_odh) {
                unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta, dtalen,
                                 reqdtalen, ver);
            } else {
                SAVEGENID;
            }

            /*fprintf(stderr, "current rrn is %d\n", foundrrn);*/

            memset(tmp_key, -1, ixlen_full + sizeof(int));
            memcpy(tmp_key, ix, ixlen);

            memset(&dbt_key, 0, sizeof(dbt_key));
            memset(&dbt_data, 0, sizeof(dbt_data));

            dbt_key.data = tmp_key;
            dbt_key.size = ixlen_full + sizeof(int);
            dbt_key.ulen = sizeof(tmp_key);
            dbt_key.flags = DB_DBT_USERMEM;

            dbt_data.flags = DB_DBT_USERMEM;
            dbt_data.data = tmp_data;
            dbt_data.size = sizeof(tmp_data);
            dbt_data.ulen = sizeof(tmp_data);

            /* go one past the set */
            rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                            DB_SET_RANGE);
            if (rc == DB_NOTFOUND) {
                /*fprintf(stderr, "one past set is end of data\n");*/
                rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                                DB_LAST);
                if ((rc == DB_REP_HANDLE_DEAD) || (rc == DB_LOCK_DEADLOCK)) {
                    *bdberr = BDBERR_DEADLOCK;
                    goto err;
                }

                if (rc == 0) {
                    if (bdb_state->ondisk_header && bdb_state->ixdta[ixnum] &&
                        bdb_state->datacopy_odh) {
                        unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta,
                                         dtalen, reqdtalen, ver);
                    } else {
                        SAVEGENID;
                    }

                    /* copy the ix we found to the user */
                    if (ixfound)
                        memcpy(ixfound, dbt_key.data, ixlen_full);

                    outrc = 3;
                }
            } else if (rc == 0) {
                if (bdb_state->ondisk_header && bdb_state->ixdta[ixnum] &&
                    bdb_state->datacopy_odh) {
                    unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta,
                                     dtalen, reqdtalen, ver);
                } else {
                    SAVEGENID;
                }

                /*fprintf(stderr, "rc %d 1 past set is rrn %d\n", rc,*/
                /*foundrrn);*/

                /* walk backwards till we get into the set */
                found = 0;
                while ((rc == 0) && (!found)) {
                    memset(&dbt_key, 0, sizeof(dbt_key));
                    memset(&dbt_data, 0, sizeof(dbt_data));

                    dbt_key.flags = DB_DBT_USERMEM;
                    dbt_key.data = tmp_key;
                    dbt_key.size = sizeof(tmp_key);
                    dbt_key.ulen = sizeof(tmp_key);

                    dbt_data.flags = DB_DBT_USERMEM;
                    dbt_data.data = tmp_data;
                    dbt_data.size = sizeof(tmp_data);
                    dbt_data.ulen = sizeof(tmp_data);

                    rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                                    DB_PREV);

                    if (rc == 0) {
                        if (memcmp(dbt_key.data, ix, ixlen) == 0) {
                            found = 1;
                        } else if (memcmp(dbt_key.data, ix, ixlen) < 0) {
                            found = 2;
                        }
                    } else if (rc == DB_NOTFOUND) {
                        /* the record we found originally was the first
                         * record in the index.  go back to it - that's the
                         * last dupe. */
                        found = 1;
                        rc = 0;

                        memset(&dbt_key, 0, sizeof(dbt_key));
                        memset(&dbt_data, 0, sizeof(dbt_data));

                        dbt_key.flags = DB_DBT_USERMEM;
                        dbt_key.data = tmp_key;
                        dbt_key.size = ixlen_full;
                        dbt_key.ulen = sizeof(tmp_key);

                        dbt_data.flags = DB_DBT_USERMEM;
                        dbt_data.data = tmp_data;
                        dbt_data.size = sizeof(tmp_data);
                        dbt_data.ulen = sizeof(tmp_data);

                        rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key,
                                        &dbt_data, DB_CURRENT);

                        if (rc != DB_NOTFOUND && rc != 0) {
                            bdb_c_get_error(bdb_state, NULL /*tid*/, &dbcp, rc,
                                            BDBERR_MISC, bdberr,
                                            "bdb_fetch_int:back to last dupe");
                            goto err;
                        }
                    } else {
                        bdb_c_get_error(bdb_state, NULL /*tid*/, &dbcp, rc,
                                        BDBERR_MISC, bdberr,
                                        "bdb_fetch_int:walk back into set");
                        goto err;
                    }
                }

                /* see if we backed up too far, go forward */
                if (found == 2) {
                    memset(&dbt_key, 0, sizeof(dbt_key));
                    memset(&dbt_data, 0, sizeof(dbt_data));

                    dbt_key.flags = DB_DBT_USERMEM;
                    dbt_key.data = tmp_key;
                    dbt_key.size = sizeof(tmp_key);
                    dbt_key.ulen = sizeof(tmp_key);

                    dbt_data.flags = DB_DBT_USERMEM;
                    dbt_data.data = tmp_data;
                    dbt_data.size = sizeof(tmp_data);
                    dbt_data.ulen = sizeof(tmp_data);

                    rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                                    DB_NEXT);

                    if (rc != DB_NOTFOUND && rc != 0) {
                        bdb_c_get_error(bdb_state, NULL /*tid*/, &dbcp, rc,
                                        BDBERR_MISC, bdberr,
                                        "bdb_fetch_int:too far; one forwards");
                        goto err;
                    }
                }
            } else {
                bdb_c_get_error(bdb_state, NULL /*tid*/, &dbcp, rc, BDBERR_MISC,
                                bdberr, "bdb_fetch_int:one forwards");
                goto err;
            }

            if (rc == DB_NOTFOUND) {
                outrc = 99;
            } else {
                if (rc != 0) {
                    *bdberr = BDBERR_MISC;
                    goto err;
                }

                if (bdb_state->ondisk_header && bdb_state->ixdta[ixnum] &&
                    bdb_state->datacopy_odh) {
                    unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta,
                                     dtalen, reqdtalen, ver);
                } else {
                    SAVEGENID;
                }

                /* copy the ix we found to the user */
                if (ixfound)
                    memcpy(ixfound, dbt_key.data, ixlen_full);

                /* make damn sure that the thing we found is >= what they
                   asked for. */
                if (memcmp(dbt_key.data, ix, ixlen) < 0) {
                    *bdberr = BDBERR_DEADLOCK;
                    goto err;
                }
            }
            break;

        /* direct lookup by key (or genid), no going next/prev */
        case FETCH_INT_CUR:
        case FETCH_INT_CUR_BY_RECNUM:
        fetch_int_cur:
            /* if we are working on the data itself */
            if (ixnum == -1) {
                memcpy(&foundgenid, dbt_key.data, sizeof(unsigned long long));

                /* grab the data now */
                if (return_dta) {
                    if (bdb_unpack(bdb_state, dbt_data.data, dbt_data.size, dta,
                                   dtalen, &odh, NULL /*freeptr*/)) {
                        *bdberr = BDBERR_MISC;
                        goto err;
                    }

                    if (odh.recptr != dta)
                        /* this could be a memcpy given bdb_unpack()'s
                         * current behavior, but let's not risk it */
                        memmove(dta, odh.recptr, MIN(odh.length, dtalen));

                    *reqdtalen = odh.length;
                    *ver = odh.csc2vers;
                }
            } else if (bdb_state->ondisk_header && bdb_state->ixdta[ixnum] &&
                       bdb_state->datacopy_odh) {
                unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta, dtalen,
                                 reqdtalen, ver);
            }
            /* else save the genid/rrn we found */
            else {
                SAVEGENID;
            }

            /* copy the ix we found to the user */
            if (ixfound)
                memcpy(ixfound, dbt_key.data, ixlen_full);
            break;

        /* we did an initial lookup on lastkey, now get next */
        case FETCH_INT_NEXT:
            /* we expect to find an exact match here. */
            if (memcmp(dbt_key.data, last_key, dbt_key.size) != 0) {
                /*
                 * if the record got deleted out from under us, we will
                 * miss.  this means we are now looking at the next record.
                 * return this record.
                 */

                /* if we are working on the data itself */
                if (ixnum == -1) {
                    memcpy(&foundgenid, dbt_key.data,
                           sizeof(unsigned long long));

                    /* grab the data now */
                    if (return_dta) {
                        if (bdb_unpack(bdb_state, dbt_data.data, dbt_data.size,
                                       dta, dtalen, &odh, NULL /*freeptr*/)) {
                            *bdberr = BDBERR_MISC;
                            goto err;
                        }

                        if (odh.recptr != dta)
                            /* this could be a memcpy given bdb_unpack()'s
                             * current behavior, but let's not risk it */
                            memmove(dta, odh.recptr, MIN(odh.length, dtalen));

                        *reqdtalen = odh.length;
                        *ver = odh.csc2vers;
                    }
                } else if (bdb_state->ondisk_header &&
                           bdb_state->ixdta[ixnum] && bdb_state->datacopy_odh) {
                    unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta,
                                     dtalen, reqdtalen, ver);
                }
                /* save the genid/rrn we found */
                else {
                    SAVEGENID;
                }

                /* copy the ix we found to the user */
                if (ixfound)
                    memcpy(ixfound, dbt_key.data, ixlen_full);
            } else /* found what we expected to find, now find the next */
            {
                memset(&dbt_key, 0, sizeof(dbt_key));
                memset(&dbt_data, 0, sizeof(dbt_data));

                dbt_key.flags = DB_DBT_USERMEM;
                dbt_key.data = tmp_key;
                dbt_key.size = sizeof(tmp_key);
                dbt_key.ulen = sizeof(tmp_key);

                dbt_data.flags = DB_DBT_USERMEM;
                dbt_data.data = tmp_data;
                dbt_data.size = sizeof(tmp_data);
                dbt_data.ulen = sizeof(tmp_data);

                rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                                DB_NEXT);

                if ((rc == DB_REP_HANDLE_DEAD) || (rc == DB_LOCK_DEADLOCK)) {
                    *bdberr = BDBERR_DEADLOCK;
                    goto err;
                }

                if (rc == 0) {
                    /* if we are working on the data itself */
                    if (ixnum == -1) {
                        memcpy(&foundgenid, dbt_key.data,
                               sizeof(unsigned long long));

                        /* grab the data now */
                        if (return_dta) {
                            if (bdb_unpack(bdb_state, dbt_data.data,
                                           dbt_data.size, dta, dtalen, &odh,
                                           NULL /*freeptr*/)) {
                                *bdberr = BDBERR_MISC;
                                goto err;
                            }

                            if (odh.recptr != dta)
                                /* this could be a memcpy given
                                 * bdb_unpack()'s current behavior, but
                                 * let's not risk it */
                                memmove(dta, odh.recptr,
                                        MIN(odh.length, dtalen));

                            *reqdtalen = odh.length;
                            *ver = odh.csc2vers;
                        }
                    } else if (bdb_state->ondisk_header &&
                               bdb_state->ixdta[ixnum] &&
                               bdb_state->datacopy_odh) {
                        unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta,
                                         dtalen, reqdtalen, ver);
                    }
                    /* save the rrn we found */
                    else {
                        SAVEGENID;
                    }

                    /* copy the ix we found to the user */
                    if (ixfound)
                        memcpy(ixfound, dbt_key.data, ixlen_full);
                } else if (rc == DB_NOTFOUND &&
                           page_order) /* return the last, and a 3 */
                {
                    outrc = 3;
                    if (CURSOR_SER_ENABLED(bdb_state) && cur_ser &&
                        !lookahead) {
                        rc = dbcp->c_close_ser(dbcp, &cur_ser->dbcs);
                        cur_ser->is_valid = !rc;
                    } else {
                        rc = dbcp->c_close(dbcp);
                    }
                    return outrc;
                } else if (rc == DB_NOTFOUND) /* return the last, and a 3 */
                {
                    /* fprintf(stderr, "not found\n"); */
                    memset(&dbt_key, 0, sizeof(dbt_key));
                    memset(&dbt_data, 0, sizeof(dbt_data));

                    dbt_key.flags = DB_DBT_USERMEM;
                    dbt_key.data = tmp_key;
                    dbt_key.size = sizeof(tmp_key);
                    dbt_key.ulen = sizeof(tmp_key);

                    dbt_data.flags = DB_DBT_USERMEM;
                    dbt_data.data = tmp_data;
                    dbt_data.size = sizeof(tmp_data);
                    dbt_data.ulen = sizeof(tmp_data);

                    rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                                    DB_LAST);

                    if ((rc == DB_REP_HANDLE_DEAD) ||
                        (rc == DB_LOCK_DEADLOCK)) {
                        *bdberr = BDBERR_DEADLOCK;
                        goto err;
                    }

                    if (rc == 0) {
                        /* if we are working on the data itself */
                        if (ixnum == -1) {
                            memcpy(&foundgenid, dbt_key.data,
                                   sizeof(unsigned long long));

                            /* grab the data now */
                            if (return_dta) {
                                if (bdb_unpack(bdb_state, dbt_data.data,
                                               dbt_data.size, dta, dtalen, &odh,
                                               NULL /*freeptr*/)) {
                                    *bdberr = BDBERR_MISC;
                                    goto err;
                                }

                                if (odh.recptr != dta)
                                    /* this could be a memcpy given
                                     * bdb_unpack()'s current behavior, but
                                     * let's not risk it */
                                    memmove(dta, odh.recptr,
                                            MIN(odh.length, dtalen));

                                *reqdtalen = odh.length;
                                *ver = odh.csc2vers;
                            }
                        } else if (bdb_state->ondisk_header &&
                                   bdb_state->ixdta[ixnum] &&
                                   bdb_state->datacopy_odh) {
                            unpack_index_odh(bdb_state, &dbt_data, &foundgenid,
                                             dta, dtalen, reqdtalen, ver);
                        }
                        /* save the rrn we found */
                        else {
                            SAVEGENID;
                        }

                        /* copy the ix we found to the user */
                        if (ixfound)
                            memcpy(ixfound, dbt_key.data, ixlen_full);
                        outrc = 3;

                        /* if we're looking through the data itself and we
                         * have more stripes to check */
                        if (ixnum == -1 &&
                            dtafile < bdb_state->attr->dtastripe - 1)
                            goto before_first_lookup;
                    } else if (rc == DB_NOTFOUND) {
                        outrc = 99;

                        /* if we're looking through the data itself */
                        if (ixnum == -1) {
                            /* if we have more stripes to check */
                            if (dtafile < bdb_state->attr->dtastripe - 1)
                                goto before_first_lookup;

                            /* if we found a record in a previous stripe */
                            else if (past_three_outrc)
                                outrc = 3;
                        }
                    } else {
                        *bdberr = BDBERR_MISC;
                        goto err;
                    }
                } else {
                    *bdberr = BDBERR_MISC;
                    goto err;
                }
            }
            break;

        /* we did a lookup by lastkey, now get prev */
        case FETCH_INT_PREV:
            /* we expect to find an exact match here. */
            if (memcmp(dbt_key.data, last_key, dbt_key.size) != 0) {
                /*
                 * if the record got deleted out from under us, we will
                 * miss. this means we are now looking at the next record.
                 * we need to back up until we find a record lower than the
                 * one we searched for.
                 */
                rc = 0;
                found = 0;
                while ((rc == 0) && (!found)) {
                    memset(&dbt_key, 0, sizeof(dbt_key));
                    memset(&dbt_data, 0, sizeof(dbt_data));

                    dbt_key.flags = DB_DBT_USERMEM;
                    dbt_key.data = tmp_key;
                    dbt_key.size = sizeof(tmp_key);
                    dbt_key.ulen = sizeof(tmp_key);

                    dbt_data.flags = DB_DBT_USERMEM;
                    dbt_data.data = tmp_data;
                    dbt_data.size = sizeof(tmp_data);
                    dbt_data.ulen = sizeof(tmp_data);

                    rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                                    DB_PREV);

                    if ((rc == DB_REP_HANDLE_DEAD) ||
                        (rc == DB_LOCK_DEADLOCK)) {
                        *bdberr = BDBERR_DEADLOCK;
                        goto err;
                    }

                    if (rc == 0) {
                        if (memcmp(dbt_key.data, last_key, dbt_key.size) < 0) {
                            /* save the genid we found */

                            if (bdb_state->ondisk_header &&
                                bdb_state->ixdta[ixnum] &&
                                bdb_state->datacopy_odh) {
                                unpack_index_odh(bdb_state, &dbt_data,
                                                 &foundgenid, dta, dtalen,
                                                 reqdtalen, ver);
                            } else {
                                SAVEGENID;
                            }

                            /* copy the ix we found to the user */
                            if (ixfound)
                                memcpy(ixfound, dbt_key.data, ixlen_full);
                            found = 1;
                        }

                    } else if (rc == DB_NOTFOUND) {
                        /* walked to the beginning,get the first and return
                         * that */
                        memset(&dbt_key, 0, sizeof(dbt_key));
                        memset(&dbt_data, 0, sizeof(dbt_data));

                        dbt_key.flags = DB_DBT_USERMEM;
                        dbt_key.data = tmp_key;
                        dbt_key.size = sizeof(tmp_key);
                        dbt_key.ulen = sizeof(tmp_key);

                        dbt_data.flags = DB_DBT_USERMEM;
                        dbt_data.data = tmp_data;
                        dbt_data.size = sizeof(tmp_data);
                        dbt_data.ulen = sizeof(tmp_data);

                        rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key,
                                        &dbt_data, DB_FIRST);

                        if ((rc == DB_REP_HANDLE_DEAD) ||
                            (rc == DB_LOCK_DEADLOCK)) {
                            *bdberr = BDBERR_DEADLOCK;
                            goto err;
                        }

                        if (rc == 0) {
                            /* save the genid we found */

                            if (bdb_state->ondisk_header &&
                                bdb_state->ixdta[ixnum] &&
                                bdb_state->datacopy_odh) {
                                unpack_index_odh(bdb_state, &dbt_data,
                                                 &foundgenid, dta, dtalen,
                                                 reqdtalen, ver);
                            } else {
                                SAVEGENID;
                            }

                            /* copy the ix we found to the user */
                            if (ixfound)
                                memcpy(ixfound, dbt_key.data, ixlen_full);

                            outrc = 3;
                            found = 1;
                        } else if (rc == DB_NOTFOUND) {
                            outrc = 99;
                            found = 1;
                        } else {
                            *bdberr = BDBERR_MISC;
                            goto err;
                        }
                    } else {
                        *bdberr = BDBERR_MISC;
                        goto err;
                    }
                }
            } else {
                /* found what we expected to find, now find the prev
                 * record */
                memset(&dbt_key, 0, sizeof(dbt_key));
                memset(&dbt_data, 0, sizeof(dbt_data));

                dbt_key.flags = DB_DBT_USERMEM;
                dbt_key.data = tmp_key;
                dbt_key.size = sizeof(tmp_key);
                dbt_key.ulen = sizeof(tmp_key);

                dbt_data.flags = DB_DBT_USERMEM;
                dbt_data.data = tmp_data;
                dbt_data.size = sizeof(tmp_data);
                dbt_data.ulen = sizeof(tmp_data);

                rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                                DB_PREV);

                if ((rc == DB_REP_HANDLE_DEAD) || (rc == DB_LOCK_DEADLOCK)) {
                    *bdberr = BDBERR_DEADLOCK;
                    goto err;
                }

                if (rc == 0) {
                    if (bdb_state->ondisk_header && bdb_state->ixdta[ixnum] &&
                        bdb_state->datacopy_odh) {
                        unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta,
                                         dtalen, reqdtalen, ver);
                    } else /* save the genid we found */
                    {
                        SAVEGENID;
                    }

                    /* copy the ix we found to the user */
                    if (ixfound)
                        memcpy(ixfound, dbt_key.data, ixlen_full);
                } else if (rc == DB_NOTFOUND) {
                    /* return the first, and a 3 */
                    memset(&dbt_key, 0, sizeof(dbt_key));
                    memset(&dbt_data, 0, sizeof(dbt_data));

                    dbt_key.flags = DB_DBT_USERMEM;
                    dbt_key.data = tmp_key;
                    dbt_key.size = sizeof(tmp_key);
                    dbt_key.ulen = sizeof(tmp_key);

                    dbt_data.flags = DB_DBT_USERMEM;
                    dbt_data.data = tmp_data;
                    dbt_data.size = sizeof(tmp_data);
                    dbt_data.ulen = sizeof(tmp_data);

                    rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                                    DB_FIRST);

                    if ((rc == DB_REP_HANDLE_DEAD) ||
                        (rc == DB_LOCK_DEADLOCK)) {
                        *bdberr = BDBERR_DEADLOCK;
                        goto err;
                    }

                    if (rc == 0) {
                        if (bdb_state->ondisk_header &&
                            bdb_state->ixdta[ixnum] &&
                            bdb_state->datacopy_odh) {
                            unpack_index_odh(bdb_state, &dbt_data, &foundgenid,
                                             dta, dtalen, reqdtalen, ver);
                        } else /* save the genid we found */
                        {
                            SAVEGENID;
                        }

                        /* copy the ix we found to the user */
                        if (ixfound)
                            memcpy(ixfound, dbt_key.data, ixlen_full);

                        outrc = 3;
                    } else if (rc == DB_NOTFOUND) {
                        outrc = 99;
                    } else {
                        *bdberr = BDBERR_MISC;
                        goto err;
                    }
                } else {
                    *bdberr = BDBERR_MISC;
                    goto err;
                }
            }
            break;
        }
        /* done with switch on direction */

        /* get out now if we have a 99 */
        if (outrc == 99) {
            if (CURSOR_SER_ENABLED(bdb_state) && cur_ser && !lookahead) {
                rc = dbcp->c_close_ser(dbcp, &cur_ser->dbcs);
                cur_ser->is_valid = !rc;
            } else
                rc = dbcp->c_close(dbcp);
            *bdberr = BDBERR_NOERROR;
            *rrn = 0;
            return outrc;
        }

        /* at this point we are looking at the record past what we wanted,
           pointed to by dbt_key/
           dbt_data.  we need to do different things based on whether its
           an exact match or not */

        /*
           printf("found '%.*s' : %.*s\n",
           (int)dbt_key.size, (char *)dbt_key.data,
           (int)dbt_data.size, (char *)dbt_data.data);
           */

        /* exact match on key could be 0 or 1.  depends on next/prev value*/
        if (memcmp(dbt_key.data, ix, ixlen) == 0 && outrc == 0) {
            switch (direction) {
            case FETCH_INT_CUR_LASTDUPE:
                break;

            case FETCH_INT_CUR:
            case FETCH_INT_CUR_BY_RECNUM:
            case FETCH_INT_NEXT:
                if (lookahead) {
                    /*  we need to check the next key to
                        see if it also matches.  if it does we return 1 */
                    memset(&dbt_key, 0, sizeof(dbt_key));
                    memset(&dbt_data, 0, sizeof(dbt_data));

                    dbt_key.flags = DB_DBT_USERMEM;
                    dbt_key.data = tmp_key;
                    dbt_key.size = sizeof(tmp_key);
                    dbt_key.ulen = sizeof(tmp_key);

                    dbt_data.flags = DB_DBT_USERMEM;
                    dbt_data.data = tmp_data;
                    dbt_data.size = sizeof(tmp_data);
                    dbt_data.ulen = sizeof(tmp_data);

                    rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                                    DB_NEXT);

                    if ((rc == DB_REP_HANDLE_DEAD) ||
                        (rc == DB_LOCK_DEADLOCK)) {
                        *bdberr = BDBERR_DEADLOCK;
                        goto err;
                    }

                    if (rc == 0) {
                        if (memcmp(dbt_key.data, ix, ixlen) == 0)
                            outrc = 1;

                        /* zero length keys always match */
                        if (ixlen == 0)
                            outrc = 1;
                    }
                }
                break;

            case FETCH_INT_PREV:
                if (lookahead) {
                    /*  we need to check the prev key to
                        see if it also matches.  if it does we return 1 */
                    memset(&dbt_key, 0, sizeof(dbt_key));
                    memset(&dbt_data, 0, sizeof(dbt_data));

                    dbt_key.flags = DB_DBT_USERMEM;
                    dbt_key.data = tmp_key;
                    dbt_key.size = sizeof(tmp_key);
                    dbt_key.ulen = sizeof(tmp_key);

                    dbt_data.flags = DB_DBT_USERMEM;
                    dbt_data.data = tmp_data;
                    dbt_data.size = sizeof(tmp_data);
                    dbt_data.ulen = sizeof(tmp_data);

                    rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                                    DB_PREV);

                    if ((rc == DB_REP_HANDLE_DEAD) ||
                        (rc == DB_LOCK_DEADLOCK)) {
                        *bdberr = BDBERR_DEADLOCK;
                        goto err;
                    }

                    if (rc == 0) {
                        if (memcmp(dbt_key.data, ix, ixlen) == 0)
                            outrc = 1;

                        /* zero length keys always match */
                        if (ixlen == 0)
                            outrc = 1;
                    }
                }
                break;
            }
        } else {
            /*
               not exact match, but we found something: this is case 2 or 3.
               we already set outrc to 3 if we explicitly fetched the first/last
               so it must be 2 if it's not 3.
            */

            /* TODO wasn't this already done by every possible path above? this
             * seems to be redundant, actually so long as we've found a record
             * we always have to do this exact same thing, so we should really
             * just do it once before we check for outrc 99 above */

            /* if we are working on the data itself, grab the data now */
            if (ixnum == -1) {
                memcpy(&foundgenid, dbt_key.data, sizeof(unsigned long long));

                /* grab the data now */
                if (return_dta) {
                    if (bdb_unpack(bdb_state, dbt_data.data, dbt_data.size, dta,
                                   dtalen, &odh, NULL /*freeptr*/)) {
                        *bdberr = BDBERR_MISC;
                        goto err;
                    }

                    if (odh.recptr != dta)
                        /* this could be a memcpy given bdb_unpack()'s current
                         * behavior, but let's not risk it */
                        memmove(dta, odh.recptr, MIN(odh.length, dtalen));

                    *reqdtalen = odh.length;
                    *ver = odh.csc2vers;
                }
            } else if (bdb_state->ondisk_header && bdb_state->ixdta[ixnum] &&
                       bdb_state->datacopy_odh) {
                unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta, dtalen,
                                 reqdtalen, ver);
            }
            /* save the genid we found */
            else {
                SAVEGENID;
            }

            /* copy the ix we found to the user */
            if (ixfound)
                memcpy(ixfound, dbt_key.data, ixlen_full);

            if (outrc != 3)
                outrc = 2;
        }

        break;

    case DB_NOTFOUND: /* failed to find initial record */
                      /*
                        this could be either case 3 or 99.
                        fetch the first/last record of the db to see if it exists or not.
                        if it exists, return it as the data and return 3.
                        if it doesnt exist, return no data and a 99.
                        */
        switch (direction) {
        case FETCH_INT_CUR:
        case FETCH_INT_CUR_BY_RECNUM:
        case FETCH_INT_NEXT:
        case FETCH_INT_CUR_LASTDUPE:

            memset(&dbt_key, 0, sizeof(dbt_key));
            memset(&dbt_data, 0, sizeof(dbt_data));

            dbt_key.flags = DB_DBT_USERMEM;
            dbt_key.data = tmp_key;
            dbt_key.size = sizeof(tmp_key);
            dbt_key.ulen = sizeof(tmp_key);

            dbt_data.flags = DB_DBT_USERMEM;
            dbt_data.data = tmp_data;
            dbt_data.size = sizeof(tmp_data);
            dbt_data.ulen = sizeof(tmp_data);

            rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                            DB_LAST);

            if ((rc == DB_REP_HANDLE_DEAD) || (rc == DB_LOCK_DEADLOCK)) {
                *bdberr = BDBERR_DEADLOCK;
                goto err;
            }

            if (rc == 0) {
                /* if we are working on the data itself, grab the data
                 * now */
                if (ixnum == -1) {
                    memcpy(&foundgenid, dbt_key.data,
                           sizeof(unsigned long long));

                    /* grab the data now */
                    if (return_dta) {
                        if (bdb_unpack(bdb_state, dbt_data.data, dbt_data.size,
                                       dta, dtalen, &odh, NULL /*freeptr*/)) {
                            *bdberr = BDBERR_MISC;
                            goto err;
                        }

                        if (odh.recptr != dta)
                            /* this could be a memcpy given bdb_unpack()'s
                             * current behavior, but let's not risk it */
                            memmove(dta, odh.recptr, MIN(odh.length, dtalen));

                        *reqdtalen = odh.length;
                        *ver = odh.csc2vers;
                    }
                } else if (bdb_state->ondisk_header &&
                           bdb_state->ixdta[ixnum] && bdb_state->datacopy_odh) {
                    unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta,
                                     dtalen, reqdtalen, ver);
                }
                /* save the rrn we found */
                else {
                    SAVEGENID;
                }

                /* copy the ix we found to the user */
                if (ixfound)
                    memcpy(ixfound, dbt_key.data, ixlen_full);

                outrc = 3;

                /* if we're looking through
                   the data itself and we have more stripes to check */
                if (ixnum == -1 && dtafile < bdb_state->attr->dtastripe - 1)
                    goto before_first_lookup;
            } else if (rc == DB_NOTFOUND) {
                outrc = 99;

                /* if we're looking through the data itself */
                if (ixnum == -1) {
                    /* if we have more stripes to check */
                    if (dtafile < bdb_state->attr->dtastripe - 1)
                        goto before_first_lookup;

                    /* if we found a record in a previous stripe */
                    else if (past_three_outrc)
                        outrc = 3;
                }
            } else {
                *bdberr = BDBERR_MISC;
                goto err;
            }
            break;

        case FETCH_INT_PREV:
            memset(&dbt_key, 0, sizeof(dbt_key));
            memset(&dbt_data, 0, sizeof(dbt_data));

            dbt_key.flags = DB_DBT_USERMEM;
            dbt_key.data = tmp_key;
            dbt_key.size = sizeof(tmp_key);
            dbt_key.ulen = sizeof(tmp_key);

            dbt_data.flags = DB_DBT_USERMEM;
            dbt_data.data = tmp_data;
            dbt_data.size = sizeof(tmp_data);
            dbt_data.ulen = sizeof(tmp_data);

            rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                            DB_LAST);

            if ((rc == DB_REP_HANDLE_DEAD) || (rc == DB_LOCK_DEADLOCK)) {
                *bdberr = BDBERR_DEADLOCK;
                goto err;
            }

            if (rc == 0) {
                /* if we are working on the data itself, grab the data
                 * now */
                if (ixnum == -1) {
                    memcpy(&foundgenid, dbt_key.data,
                           sizeof(unsigned long long));

                    /* grab the data now */
                    if (return_dta) {
                        if (bdb_unpack(bdb_state, dbt_data.data, dbt_data.size,
                                       dta, dtalen, &odh, NULL /*freeptr*/)) {
                            *bdberr = BDBERR_MISC;
                            goto err;
                        }

                        if (odh.recptr != dta)
                            /* this could be a memcpy given bdb_unpack()'s
                             * current behavior, but let's not risk it */
                            memmove(dta, odh.recptr, MIN(odh.length, dtalen));

                        *reqdtalen = odh.length;
                        *ver = odh.csc2vers;
                    }
                } else if (bdb_state->ondisk_header &&
                           bdb_state->ixdta[ixnum] && bdb_state->datacopy_odh) {
                    unpack_index_odh(bdb_state, &dbt_data, &foundgenid, dta,
                                     dtalen, reqdtalen, ver);
                }
                /* save the rrn we found */
                else {
                    SAVEGENID;
                }

                /* copy the ix we found to the user */
                if (ixfound)
                    memcpy(ixfound, dbt_key.data, ixlen_full);

                /* XXX
                 * NOT COMDBG behavior.  comdbg retrurns 3 here!!
                 * we return 2 here ("doesnt match, here is the next/prev")
                 * instead of 3 ("past end of db, here is last") because it
                 * seems to make more sense to the programmer to be able to
                 * set the key to all 1s, and loop on a find_prev until you
                 * see a 3 as a way to iterate through the database
                 * backwards. comdb2 will give you a 3, then 2s, then
                 * another 3.
                 */
                outrc = 2;
            } else if (rc == DB_NOTFOUND) {
                outrc = 99;
            } else {
                *bdberr = BDBERR_MISC;
                goto err;
            }
        }
        break;

    default:
        /* some catastrophic error, couldnt fetch initial record, didnt
           even get DB_NOTFOUND on the initial fetch. */
        *bdberr = BDBERR_FETCH_IX;

        if ((initial_rc == DB_REP_HANDLE_DEAD) ||
            (initial_rc == DB_LOCK_DEADLOCK))
            *bdberr = BDBERR_DEADLOCK;
        else {
            int ii;
            logmsg(LOGMSG_ERROR, "bdb_fetch_int ix %d key '", ixnum);
            for (ii = 0; ii < ixlen; ii++)
                logmsg(LOGMSG_ERROR, "%02x", *(((unsigned char *)ix) + ii));
            logmsg(LOGMSG_ERROR, "' ixlen %d direction %d lastrrn %d rc %d %s\n",
                    ixlen, direction, lastrrn, initial_rc, db_strerror(rc));

            if (initial_rc == ENOMEM) {
                logmsg(LOGMSG_ERROR, "dbt_data.ulen = %d dbt_key.ulen = %d\n",
                        dbt_data.ulen, dbt_key.ulen);
            }
        }
    }

err:
    if (*bdberr != BDBERR_NOERROR) {
        outrc = -1;
        *rrn = 0;
        foundrrn = 0;
        if (dbcp) {
            if (CURSOR_SER_ENABLED(bdb_state) && cur_ser && !lookahead) {
                rc = dbcp->c_close_ser(dbcp, &cur_ser->dbcs);
                cur_ser->is_valid = !rc;
            } else
                rc = dbcp->c_close(dbcp);
        }
        return outrc;
    }

    /* if we were have no data to retrieve (rc=99/empty db) then we are done */
    if (outrc == 99) {
        *bdberr = BDBERR_NOERROR;
        *rrn = 0;
        if (recnum)
            *recnum = -1;

        /* close the cursor */
        if (CURSOR_SER_ENABLED(bdb_state) && cur_ser && !lookahead) {
            rc = dbcp->c_close_ser(dbcp, &cur_ser->dbcs);
            cur_ser->is_valid = !rc;
        } else
            rc = dbcp->c_close(dbcp);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "c_close failed\n");
            *bdberr = BDBERR_MISC;
            outrc = -1;
            *rrn = 0;
            foundrrn = 0;
        }

        return outrc;
    }

    /* if we were told to retrieve the record number, do it now */
    if (recnum && direction != FETCH_INT_CUR_BY_RECNUM) {
        *recnum = -1;

        if (ixrecnum) {
            if (ixfound)
                memcpy(tmp_key, ixfound, ixlen_full);

            memset(&dbt_key, 0, sizeof(dbt_key));
            memset(&dbt_data, 0, sizeof(dbt_data));

            dbt_key.flags = DB_DBT_USERMEM;
            dbt_key.data = tmp_key;
            dbt_key.size = ixlen_full;
            dbt_key.ulen = sizeof(tmp_key);

            dbt_data.flags = DB_DBT_USERMEM;
            dbt_data.data = tmp_data;
            dbt_data.size = sizeof(tmp_data);
            dbt_data.ulen = sizeof(tmp_data);

            if (keycontainsgenid) {
                masked_genid = get_search_genid(bdb_state, foundgenid);
                memcpy(tmp_key + ixlen_full, &masked_genid,
                       sizeof(unsigned long long));
                dbt_key.size += sizeof(unsigned long long);
            }

            rc =
                fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data, DB_SET);

            if (rc != 0) {
                /* return DEADLOCK */
                if (CURSOR_SER_ENABLED(bdb_state) && cur_ser && !lookahead) {
                    rc = dbcp->c_close_ser(dbcp, &cur_ser->dbcs);
                    cur_ser->is_valid = !rc;
                } else
                    rc = dbcp->c_close(dbcp);
                *bdberr = BDBERR_DEADLOCK;

                outrc = -1;
                *recnum = -1;
                return outrc;
            }

            memset(&dbt_data, 0, sizeof(dbt_data));
            dbt_data.data = recnum;
            dbt_data.ulen = sizeof(int);
            dbt_data.flags = DB_DBT_USERMEM;

            rc = fetch_cget(bdb_state, ixnum, dbcp, &dbt_key, &dbt_data,
                            DB_GET_RECNO);

            if ((rc == DB_REP_HANDLE_DEAD) || (rc == DB_LOCK_DEADLOCK)) {
                *bdberr = BDBERR_DEADLOCK;
            }

            if (rc != 0)
                *recnum = -1;
        }
    }
    /********************************************************************/

    /* if we were told not to return data, we are done here. */
    if (return_dta == 0) {
        *bdberr = BDBERR_NOERROR;

        *rrn = 2;

        /* close the cursor */
        if (CURSOR_SER_ENABLED(bdb_state) && cur_ser && !lookahead) {
            rc = dbcp->c_close_ser(dbcp, &cur_ser->dbcs);
            cur_ser->is_valid = !rc;
        } else
            rc = dbcp->c_close(dbcp);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "c_close failed\n");
            *bdberr = BDBERR_MISC;
            outrc = -1;
            *rrn = 0;
            foundrrn = 0;
        }

        if (genid) {
            *genid = foundgenid;
        }

        return outrc;
    }

    /* close the cursor */
    if (CURSOR_SER_ENABLED(bdb_state) && cur_ser && !lookahead) {
        rc = dbcp->c_close_ser(dbcp, &cur_ser->dbcs);
        cur_ser->is_valid = !rc;
    } else
        rc = dbcp->c_close(dbcp);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "c_close failed\n");
        *bdberr = BDBERR_MISC;
        outrc = -1;
        *rrn = 0;
        *genid = 0;
        foundrrn = 0;
    }

    /* we found a genid now fetch the data using genid as key */
    if (foundgenid) {
        if (havedta || ixnum == -1) {
            /* we already put the found dta into "dta" and set the "reqdtalen"
               - just set the rrn here */
            *rrn = 2;
            *genid = foundgenid;
        } else {
            /* now get the dta corresponding to that rrn */
            memset(&dbt_key, 0, sizeof(dbt_key));
            memset(&dbt_data, 0, sizeof(dbt_data));

            dbt_key.data = &foundgenid;
            dbt_key.size = sizeof(unsigned long long);
            dbt_key.ulen = sizeof(unsigned long long);
            dbt_key.flags = DB_DBT_USERMEM;

            dbt_data.data = tmp_data;
            dbt_data.size = sizeof(tmp_data);
            dbt_data.ulen = sizeof(tmp_data);
            dbt_data.flags = DB_DBT_USERMEM;

            dtafile = get_dtafile_from_genid(foundgenid);

            /* if dtafile is out of range then database must be corrupt */
            if (dtafile < 0 || dtafile >= bdb_state->attr->dtastripe) {
                logmsg(LOGMSG_ERROR, "bdb_fetch_int: dtafile=%d out of range\n",
                        dtafile);
                *bdberr = BDBERR_MISC;
                *rrn = 0;
                outrc = -1;
                goto endofbigif;
            }

            get_flags = 0;

            if (dirty)
                get_flags |= DB_DIRTY_READ;

            /* now get the data */
            rc = bdb_get_unpack(bdb_state, bdb_state->dbp_data[0][dtafile], tid,
                                &dbt_key, &dbt_data, ver, get_flags);

            if (rc == 0) {
                *genid = foundgenid;
                *rrn = 2;
                memcpy(dta, dbt_data.data, MIN(dbt_data.size, dtalen));
                *reqdtalen = dbt_data.size;
            } else {
                /* fprintf(stderr, "fetch_dta: %d\n", rc);*/
                if ((rc == DB_REP_HANDLE_DEAD) || (rc == DB_LOCK_DEADLOCK) ||
                    (rc == DB_NOTFOUND))
                    *bdberr = BDBERR_DEADLOCK;
                else
                    *bdberr = BDBERR_FETCH_DTA;
                if (genid)
                    *genid = 0;
                *rrn = 0;
                outrc = -1;
            }
        }

    endofbigif:

        /* if we asked for blobs then get the blob data for each blob.  this
         * gets returned in malloced memory which the caller must free. */
        if (outrc == 0 || outrc == 1) {
            int blbrc;
            unsigned long long usegenid = foundgenid;
            bdb_fetch_args_t blob_args = *args;
            blbrc = bdb_fetch_blobs_by_rrn_and_genid_int_int(
                bdb_state, tran, foundrrn, usegenid, numblobs, dtafilenums,
                blobsizes, bloboffs, blobptrs, NULL, &blob_args, bdberr);
            if (blbrc != 0) {
                if (*bdberr == BDBERR_DTA_MISMATCH) {
                    /* if the genid of the found data is different from what we
                     * got from the index then it's a race, treat it like a
                     * deadlock. */
                    *bdberr = BDBERR_DEADLOCK;
                }
                *rrn = 0;
                if (genid)
                    *genid = 0;

                outrc = -1;
            }
        }
    }

    return outrc;
}

static char *direction_to_str(int direction)
{
    switch (direction) {
    case FETCH_INT_CUR:
        return "fetch_int_cur";
    case FETCH_INT_NEXT:
        return "fetch_int_next";
    case FETCH_INT_PREV:
        return "fetch_int_prev";
    case FETCH_INT_CUR_LASTDUPE:
        return "fetch_int_cur_lastdupe";
    case FETCH_INT_CUR_BY_RECNUM:
        return "fetch_int_cur_by_recnum";
    default:
        return "???";
    }
}

static unsigned long long genid_from_dbt(int ixnum, DBT *dkey, DBT *ddta)
{
    unsigned long long genid;

    if (ixnum != -1)
        memcpy(&genid, ddta->data, sizeof(unsigned long long));
    else
        memcpy(&genid, dkey->data, sizeof(unsigned long long));
    return genid;
}

static int bdb_fetch_prefault_int(
    int return_dta, int direction, int lookahead, bdb_state_type *bdb_state,
    void *ix, int ixnum, int ixlen, void *lastix, int lastrrn,
    unsigned long long lastgenid, void *dta, int dtalen, int *reqdtalen,
    void *ixfound, int *rrn, int *recnum, unsigned long long *genid,
    int numblobs, int *dtafilenums, size_t *blobsizes, size_t *bloboffs,
    void **blobptrs, bdb_fetch_args_t *args, int *bdberr)
{
    u_int32_t lockerid = 0;
    DB_LOCKREQ request;
    int rc;
    int llrc;

    rc = bdb_state->dbenv->lock_id_flags(bdb_state->dbenv, &lockerid,
                                         DB_LOCK_ID_LOWPRI);
    if (rc != 0) {
        *bdberr = BDBERR_MISC;
        return -1;
    }

    rc = bdb_lock_table_read_fromlid(bdb_state, lockerid);
    if (rc != 0) {
        *bdberr = BDBERR_MISC;
        return -1;
    }

    llrc = bdb_fetch_int_ll(return_dta, direction, lookahead, bdb_state, ix,
                            ixnum, ixlen, lastix, lastrrn, lastgenid, dta,
                            dtalen, reqdtalen, ixfound, rrn, recnum, genid,
                            numblobs, dtafilenums, blobsizes, bloboffs,
                            blobptrs, 1, NULL, NULL, args, lockerid, bdberr);

    memset(&request, 0, sizeof(request));
    request.op = DB_LOCK_PUT_ALL;
    bdb_state->dbenv->lock_vec(bdb_state->dbenv, lockerid, 0, &request, 1,
                               NULL);
    bdb_state->dbenv->lock_id_free(bdb_state->dbenv, lockerid);

    return llrc;
}

/* wrapper that handles making a temp transaction when needed */
static int bdb_fetch_int(int return_dta, int direction, int lookahead,
                         bdb_state_type *bdb_state, void *ix, int ixnum,
                         int ixlen, void *lastix, int lastrrn,
                         unsigned long long lastgenid, void *dta, int dtalen,
                         int *reqdtalen, void *ixfound, int *rrn, int *recnum,
                         unsigned long long *genid, int numblobs,
                         int *dtafilenums, size_t *blobsizes, size_t *bloboffs,
                         void **blobptrs, int dirty, tran_type *tran,
                         bdb_cursor_ser_int_t *cur_ser, bdb_fetch_args_t *args,
                         int *bdberr)
{
    int created_temp_tran;
    int rc;
    int llrc;
    char tmpixfound[1024];
    int bdberr2;

    if (!ixfound)
        ixfound = tmpixfound;

    created_temp_tran = 0;

    /* if we dont have a tran, create one */
    if (!tran) {
        created_temp_tran = 1;
        tran =
            bdb_tran_begin_logical_norowlocks_int(bdb_state, 0ULL, 0, bdberr);
        if (!tran) {
            logmsg(LOGMSG_ERROR, "bdb_fetch_int couldnt make temp tran\n");
        }
    }

    if (!args->for_write) {
        rc = bdb_lock_table_read(bdb_state, tran);

        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "bdb_fetch_int unable to get table read lock.\n");
            return -1;
        }
    }

    /* if its not rowlocks mode, just run old way */
    llrc = bdb_fetch_int_ll(return_dta, direction, lookahead, bdb_state, ix,
                            ixnum, ixlen, lastix, lastrrn, lastgenid, dta,
                            dtalen, reqdtalen, ixfound, rrn, recnum, genid,
                            numblobs, dtafilenums, blobsizes, bloboffs,
                            blobptrs, dirty, tran, cur_ser, args, 0, bdberr);

    if (created_temp_tran) {
        int arc;
        /* don't loose bdberr pls */
        arc = bdb_tran_abort_int(bdb_state, tran, &bdberr2, NULL, 0, NULL, 0,
                                 NULL);
        if (arc)
            logmsg(LOGMSG_WARN, "%s:%d arc=%d\n", __FILE__, __LINE__, arc);
    }
    return llrc;
}

/*
  wrap all fetch calls in a read lock on bdb_state->bdb_lock
*/

int bdb_fetch(bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen,
              void *dta, int dtalen, int *reqdtalen, void *ixfound, int *rrn,
              bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch");

    outrc =
        bdb_fetch_int(1,             /* return data */
                      FETCH_INT_CUR, /* current */
                      1,             /* lookahead */
                      bdb_state, ix, ixnum, ixlen, NULL, 0,
                      0, /* lastix, lastrrn, lastgenid */
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      NULL,                                       /* genid */
                      0, NULL, NULL, NULL, NULL,                  /* no blobs */
                      0, NULL,                                    /* no txn */
                      NULL, /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_by_recnum(bdb_state_type *bdb_state, int ixnum, int recnum,
                        void *dta, int dtalen, int *reqdtalen, void *ixfound,
                        int *rrn, bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_by_recnum");

    outrc = bdb_fetch_int(1,                         /* return data */
                          FETCH_INT_CUR_BY_RECNUM,   /* current */
                          1,                         /* lookahead */
                          bdb_state, NULL, ixnum, 0, /* ix, ixnum, ixlen */
                          NULL, 0, 0, /* lastix, lastrrn, lastgenid */
                          dta, dtalen, reqdtalen, ixfound, rrn,
                          &recnum,                   /* recnum */
                          NULL,                      /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta(bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen,
                    void *ixfound, int *rrn, bdb_fetch_args_t *args,
                    int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta");

    outrc = bdb_fetch_int(0,             /* return no data */
                          FETCH_INT_CUR, /* current */
                          1,             /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0,                  /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL,      /* dta, dtalen, reqdtalen */
                          ixfound, rrn, NULL, /* recnum */
                          NULL,               /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_by_recnum_genid(bdb_state_type *bdb_state, int ixnum,
                                    int recnum, void *ixfound, int *rrn,
                                    unsigned long long *genid,
                                    bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_by_recnum_genid");

    outrc = bdb_fetch_int(0,                         /* return no data */
                          FETCH_INT_CUR_BY_RECNUM,   /* current */
                          1,                         /* lookahead */
                          bdb_state, NULL, ixnum, 0, /* ix, ixnum, ixlen */
                          NULL, 0, 0,    /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, &recnum,     /* recnum */
                          genid,                     /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_by_recnum(bdb_state_type *bdb_state, int ixnum, int recnum,
                              void *ixfound, int *rrn, bdb_fetch_args_t *args,
                              int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_by_recnum");

    outrc = bdb_fetch_int(0,                         /* return no data */
                          FETCH_INT_CUR_BY_RECNUM,   /* current */
                          1,                         /* lookahead */
                          bdb_state, NULL, ixnum, 0, /* ix, ixnum, ixlen */
                          NULL, 0, 0,    /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, &recnum,     /* recnum */
                          NULL,                      /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next(bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen,
                   void *lastix, int lastrrn, unsigned long long lastgenid,
                   void *dta, int dtalen, int *reqdtalen, void *ixfound,
                   int *rrn, bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_NEXT, /* next */
                      1,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      NULL,                                       /* genid */
                      0, NULL, NULL, NULL, NULL,                  /* no blobs */
                      0, NULL,                                    /* no txn */
                      NULL, args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_nodta(bdb_state_type *bdb_state, void *ix, int ixnum,
                         int ixlen, void *lastix, int lastrrn,
                         unsigned long long lastgenid, void *ixfound, int *rrn,
                         bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next_nodta");

    outrc = bdb_fetch_int(0,              /* return no data */
                          FETCH_INT_NEXT, /* next */
                          1,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, NULL,       /* recnum */
                          NULL,                     /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev(bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen,
                   void *lastix, int lastrrn, unsigned long long lastgenid,
                   void *dta, int dtalen, int *reqdtalen, void *ixfound,
                   int *rrn, bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_PREV, /* prev */
                      1,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      NULL,                                       /* genid */
                      0, NULL, NULL, NULL, NULL,                  /* no blobs */
                      0, NULL,                                    /* no txn */
                      NULL, /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_nodta(bdb_state_type *bdb_state, void *ix, int ixnum,
                         int ixlen, void *lastix, int lastrrn,
                         unsigned long long lastgenid, void *ixfound, int *rrn,
                         bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_nodta");

    outrc = bdb_fetch_int(0,              /* return no data */
                          FETCH_INT_PREV, /* prev */
                          1,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, NULL,       /* recnum */
                          NULL,                     /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_recnum(bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen,
                     void *dta, int dtalen, int *reqdtalen, void *ixfound,
                     int *rrn, int *recnum, bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_recnum");

    outrc = bdb_fetch_int(1,             /* return data */
                          FETCH_INT_CUR, /* current */
                          1,             /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0, /* lastix, lastrrn, lastgenid */
                          dta, dtalen, reqdtalen, ixfound, rrn, recnum,
                          NULL,                      /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_recnum_by_recnum_genid(bdb_state_type *bdb_state, int ixnum,
                                     int recnum_in, void *dta, int dtalen,
                                     int *reqdtalen, void *ixfound, int *rrn,
                                     int *recnum_out, unsigned long long *genid,
                                     bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_recnum_by_recnum_genid");

    memcpy(recnum_out, &recnum_in, sizeof(int));

    outrc = bdb_fetch_int(1,                         /* return data */
                          FETCH_INT_CUR_BY_RECNUM,   /* current */
                          1,                         /* lookahead */
                          bdb_state, NULL, ixnum, 0, /* ix, ixnum, ixlen */
                          NULL, 0, 0, /* lastix, lastrrn, lastgenid */
                          dta, dtalen, reqdtalen, ixfound, rrn, recnum_out,
                          genid,                     /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_recnum_by_recnum(bdb_state_type *bdb_state, int ixnum,
                               int recnum_in, void *dta, int dtalen,
                               int *reqdtalen, void *ixfound, int *rrn,
                               int *recnum_out, bdb_fetch_args_t *args,
                               int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_recnum_by_recnum");

    memcpy(recnum_out, &recnum_in, sizeof(int));

    outrc = bdb_fetch_int(1,                         /* return data */
                          FETCH_INT_CUR_BY_RECNUM,   /* current */
                          1,                         /* lookahead */
                          bdb_state, NULL, ixnum, 0, /* ix, ixnum, ixlen */
                          NULL, 0, 0, /* lastix, lastrrn, lastgenid */
                          dta, dtalen, reqdtalen, ixfound, rrn, recnum_out,
                          NULL,                      /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_lastdupe_recnum(bdb_state_type *bdb_state, void *ix, int ixnum,
                              int ixlen, void *dta, int dtalen, int *reqdtalen,
                              void *ixfound, int *rrn, int *recnum,
                              bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_lastdupe_recnum");

    outrc = bdb_fetch_int(1,                      /* return data */
                          FETCH_INT_CUR_LASTDUPE, /* current, last dupe */
                          1,                      /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0, /* lastix, lastrrn, lastgenid  */
                          dta, dtalen, reqdtalen, ixfound, rrn, recnum,
                          NULL,                      /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_lastdupe_recnum(bdb_state_type *bdb_state, void *ix,
                                    int ixnum, int ixlen, void *ixfound,
                                    int *rrn, int *recnum,
                                    bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_lastdupe_recnum");

    outrc = bdb_fetch_int(0,                      /* don't return data */
                          FETCH_INT_CUR_LASTDUPE, /* current, last dupe */
                          1,                      /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0,             /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, recnum, NULL, /* genid */
                          0, NULL, NULL, NULL, NULL,  /* no blobs */
                          0, NULL,                    /* no txn */
                          NULL,                       /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_recnum(bdb_state_type *bdb_state, void *ix, int ixnum,
                           int ixlen, void *ixfound, int *rrn, int *recnum,
                           bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_recnum");

    outrc = bdb_fetch_int(0,             /* return no data */
                          FETCH_INT_CUR, /* current */
                          1,             /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0,             /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, recnum, NULL, /* genid */
                          0, NULL, NULL, NULL, NULL,  /* no blobs */
                          0, NULL,                    /* no txn */
                          NULL,                       /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_recnum_by_recnum(bdb_state_type *bdb_state, int ixnum,
                                     int recnum_in, void *ixfound, int *rrn,
                                     int *recnum_out, bdb_fetch_args_t *args,
                                     int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_recnum_by_recnum");

    memcpy(recnum_out, &recnum_in, sizeof(int));

    outrc = bdb_fetch_int(0,                         /* return no data */
                          FETCH_INT_CUR_BY_RECNUM,   /* current */
                          1,                         /* lookahead */
                          bdb_state, NULL, ixnum, 0, /* ix, ixnum, ixlen */
                          NULL, 0, 0,    /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, recnum_out, NULL, /* genid */
                          0, NULL, NULL, NULL, NULL,      /* no blobs */
                          0, NULL,                        /* no txn */
                          NULL,                           /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_recnum(bdb_state_type *bdb_state, void *ix, int ixnum,
                          int ixlen, void *lastix, int lastrrn,
                          unsigned long long lastgenid, void *dta, int dtalen,
                          int *reqdtalen, void *ixfound, int *rrn, int *recnum,
                          bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next_recnum");

    outrc = bdb_fetch_int(1,              /* return data */
                          FETCH_INT_NEXT, /* next */
                          1,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, dta, dtalen, reqdtalen, ixfound, rrn,
                          recnum, NULL,              /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_nodta_recnum(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *ixfound,
                                int *rrn, int *recnum, bdb_fetch_args_t *args,
                                int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next_nodta_recnum");

    outrc = bdb_fetch_int(0,              /* return no data */
                          FETCH_INT_NEXT, /* next */
                          1,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, recnum, NULL, /* genid */
                          0, NULL, NULL, NULL, NULL,  /* no blobs */
                          0, NULL,                    /* no txn */
                          NULL,                       /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_recnum(bdb_state_type *bdb_state, void *ix, int ixnum,
                          int ixlen, void *lastix, int lastrrn,
                          unsigned long long lastgenid, void *dta, int dtalen,
                          int *reqdtalen, void *ixfound, int *rrn, int *recnum,
                          bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_recnum");

    outrc = bdb_fetch_int(1,              /* return data */
                          FETCH_INT_PREV, /* prev */
                          1,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, dta, dtalen, reqdtalen, ixfound, rrn,
                          recnum, NULL,              /* genid */
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_nodta_recnum(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *ixfound,
                                int *rrn, int *recnum, bdb_fetch_args_t *args,
                                int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_nodta_recnum");

    outrc = bdb_fetch_int(0,              /* return no data */
                          FETCH_INT_PREV, /* prev */
                          1,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, recnum, NULL, /* genid */
                          0, NULL, NULL, NULL, NULL,  /* no blobs */
                          0, NULL,                    /* no txn */
                          NULL,                       /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_genid(bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen,
                    void *dta, int dtalen, int *reqdtalen, void *ixfound,
                    int *rrn, unsigned long long *genid, int ignore_ixdta,
                    bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_genid");

    outrc =
        bdb_fetch_int(ignore_ixdta ? 2 : 1, /* return data */
                      FETCH_INT_CUR,        /* current */
                      1,                    /* lookahead */
                      bdb_state, ix, ixnum, ixlen, NULL, 0,
                      0, /* lastix, lastrrn, lastgenid */
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      0, NULL,                                    /* no txn */
                      NULL, /* no cur_ser */
                      args, bdberr);

    if (outrc < 0 && *bdberr == 0)
        logmsg(LOGMSG_ERROR, "Aloha rc=%d bdberr=%d!\n", outrc, *bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_genid_nl_ser(bdb_state_type *bdb_state, void *ix, int ixnum,
                           int ixlen, void *dta, int dtalen, int *reqdtalen,
                           void *ixfound, int *rrn, unsigned long long *genid,
                           int ignore_ixdta, bdb_cursor_ser_t *cur_ser,
                           bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_genid_ser");

    outrc =
        bdb_fetch_int(ignore_ixdta ? 2 : 1, /* return data */
                      FETCH_INT_CUR,        /* current */
                      0,                    /* lookahead */
                      bdb_state, ix, ixnum, ixlen, NULL, 0,
                      0, /* lastix, lastrrn, lastgenid */
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      0, NULL,                                    /* no txn */
                      (bdb_cursor_ser_int_t *)cur_ser, args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_genid_trans(bdb_state_type *bdb_state, void *ix, int ixnum,
                          int ixlen, void *dta, int dtalen, int *reqdtalen,
                          void *ixfound, int *rrn, unsigned long long *genid,
                          int ignore_ixdta, void *tran, bdb_fetch_args_t *args,
                          int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;
    int fetch_dta;

    BDB_READLOCK("bdb_fetch_genid");

    if (dta) {
        if (ignore_ixdta)
            fetch_dta = 2;
        else
            fetch_dta = 1;
    } else
        fetch_dta = 0;

    outrc =
        bdb_fetch_int(fetch_dta,     /* return data */
                      FETCH_INT_CUR, /* current */
                      1,             /* lookahead */
                      bdb_state, ix, ixnum, ixlen, NULL, 0,
                      0, /* lastix, lastrrn, lastgenid */
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      0, tran, NULL, /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_genid_prefault(bdb_state_type *bdb_state, void *ix, int ixnum,
                             int ixlen, void *dta, int dtalen, int *reqdtalen,
                             void *ixfound, int *rrn, unsigned long long *genid,
                             int ignore_ixdta, bdb_fetch_args_t *args,
                             int *bdberr)
{

    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_genid_prefault");

    outrc = bdb_fetch_prefault_int(
        ignore_ixdta ? 2 : 1, /* return data */
        FETCH_INT_CUR,        /* current */
        1,                    /* lookahead */
        bdb_state, ix, ixnum, ixlen, NULL, 0,
        0, /* lastix, lastrrn, lastgenid */
        dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
        genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
        args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_genid_dirty(bdb_state_type *bdb_state, void *ix, int ixnum,
                          int ixlen, void *dta, int dtalen, int *reqdtalen,
                          void *ixfound, int *rrn, unsigned long long *genid,
                          int ignore_ixdta, bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_genid");

    outrc =
        bdb_fetch_int(ignore_ixdta ? 2 : 1, /* return data */
                      FETCH_INT_CUR,        /* current */
                      1,                    /* lookahead */
                      bdb_state, ix, ixnum, ixlen, NULL, 0,
                      0, /* lastix, lastrrn, lastgenid */
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      1, NULL,                                    /* no txn */
                      NULL, /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_blobs_genid(bdb_state_type *bdb_state, void *ix, int ixnum,
                          int ixlen, void *dta, int dtalen, int *reqdtalen,
                          void *ixfound, int *rrn, unsigned long long *genid,
                          int numblobs, int *dtafilenums, size_t *blobsizes,
                          size_t *bloboffs, void **blobptrs, int ignore_ixdta,
                          bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_blobs_genid");

    outrc =
        bdb_fetch_int(ignore_ixdta ? 2 : 1, /* return data */
                      FETCH_INT_CUR,        /* current */
                      1,                    /* lookahead */
                      bdb_state, ix, ixnum, ixlen, NULL, 0,
                      0, /* lastix, lastrrn, lastgenid */
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, numblobs, dtafilenums, blobsizes, bloboffs,
                      blobptrs, 0, NULL, /* no txn */
                      NULL,              /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_blobs_genid_dirty(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *dta, int dtalen,
                                int *reqdtalen, void *ixfound, int *rrn,
                                unsigned long long *genid, int numblobs,
                                int *dtafilenums, size_t *blobsizes,
                                size_t *bloboffs, void **blobptrs,
                                int ignore_ixdta, bdb_fetch_args_t *args,
                                int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_blobs_genid_dirty");

    outrc =
        bdb_fetch_int(ignore_ixdta ? 2 : 1, /* return data */
                      FETCH_INT_CUR,        /* current */
                      1,                    /* lookahead */
                      bdb_state, ix, ixnum, ixlen, NULL, 0,
                      0, /* lastix, lastrrn, lastgenid */
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, numblobs, dtafilenums, blobsizes, bloboffs,
                      blobptrs, 1, NULL, /* no txn */
                      NULL,              /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_genid_by_recnum(bdb_state_type *bdb_state, int ixnum, int recnum,
                              void *dta, int dtalen, int *reqdtalen,
                              void *ixfound, int *rrn,
                              unsigned long long *genid, bdb_fetch_args_t *args,
                              int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_genid_by_recnum");

    outrc = bdb_fetch_int(1,                         /* return data */
                          FETCH_INT_CUR_BY_RECNUM,   /* current */
                          1,                         /* lookahead */
                          bdb_state, NULL, ixnum, 0, /* ix, ixnum, ixlen */
                          NULL, 0, 0, /* lastix, lastrrn, lastgenid */
                          dta, dtalen, reqdtalen, ixfound, rrn,
                          &recnum,                          /* recnum */
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                          /* no txn */
                          NULL,                             /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_genid(bdb_state_type *bdb_state, void *ix, int ixnum,
                          int ixlen, void *ixfound, int *rrn,
                          unsigned long long *genid, bdb_fetch_args_t *args,
                          int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_genid");

    outrc = bdb_fetch_int(0,             /* return no data */
                          FETCH_INT_CUR, /* current */
                          1,             /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0,                  /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL,      /* dta, dtalen, reqdtalen */
                          ixfound, rrn, NULL, /* recnum */
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                          /* no txn */
                          NULL,                             /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_genid_nl_ser(bdb_state_type *bdb_state, void *ix, int ixnum,
                                 int ixlen, void *ixfound, int *rrn,
                                 unsigned long long *genid,
                                 bdb_cursor_ser_t *cur_ser,
                                 bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_genid_ser");

    outrc = bdb_fetch_int(0,             /* return no data */
                          FETCH_INT_CUR, /* current */
                          0,             /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0,                  /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL,      /* dta, dtalen, reqdtalen */
                          ixfound, rrn, NULL, /* recnum */
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                          /* no txn */
                          (bdb_cursor_ser_int_t *)cur_ser, args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_genid_prefault(bdb_state_type *bdb_state, void *ix,
                                   int ixnum, int ixlen, void *ixfound,
                                   int *rrn, unsigned long long *genid,
                                   bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_genid_prefault");

    outrc =
        bdb_fetch_prefault_int(0,             /* return no data */
                               FETCH_INT_CUR, /* current */
                               1,             /* lookahead */
                               bdb_state, ix, ixnum, ixlen, NULL, 0,
                               0,             /* lastix, lastrrn, lastgenid */
                               NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                               ixfound, rrn, NULL,               /* recnum */
                               genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                               args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_genid_dirty(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *ixfound, int *rrn,
                                unsigned long long *genid,
                                bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_genid");

    outrc = bdb_fetch_int(0,             /* return no data */
                          FETCH_INT_CUR, /* current */
                          1,             /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0,                  /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL,      /* dta, dtalen, reqdtalen */
                          ixfound, rrn, NULL, /* recnum */
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          1, NULL,                          /* no txn */
                          NULL,                             /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_genid_by_recnum(bdb_state_type *bdb_state, int ixnum,
                                    int recnum, void *ixfound, int *rrn,
                                    unsigned long long *genid,
                                    bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_genid_by_recnum");

    outrc = bdb_fetch_int(0,                         /* return no data */
                          FETCH_INT_CUR_BY_RECNUM,   /* current */
                          1,                         /* lookahead */
                          bdb_state, NULL, ixnum, 0, /* ix, ixnum, ixlen */
                          NULL, 0, 0,    /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, &recnum,            /* recnum */
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                          /* no txn */
                          NULL,                             /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_genid_nl_ser(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *dta,
                                int dtalen, int *reqdtalen, void *ixfound,
                                int *rrn, unsigned long long *genid,
                                bdb_cursor_ser_t *cur_ser,
                                bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next_genid_ser");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_NEXT, /* next */
                      0,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      0, NULL,                                    /* no txn */
                      (bdb_cursor_ser_int_t *)cur_ser, args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_genid_tran(bdb_state_type *bdb_state, void *ix, int ixnum,
                              int ixlen, void *lastix, int lastrrn,
                              unsigned long long lastgenid, void *dta,
                              int dtalen, int *reqdtalen, void *ixfound,
                              int *rrn, unsigned long long *genid, void *tran,
                              bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK(__func__);

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_NEXT, /* next */
                      1,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      0, tran, NULL, /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_blobs_genid(
    bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen, void *lastix,
    int lastrrn, unsigned long long lastgenid, void *dta, int dtalen,
    int *reqdtalen, void *ixfound, int *rrn, unsigned long long *genid,
    int numblobs, int *dtafilenums, size_t *blobsizes, size_t *bloboffs,
    void **blobptrs, bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next_blobs_genid");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_NEXT, /* next */
                      1,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, numblobs, dtafilenums, blobsizes, bloboffs,
                      blobptrs, 0, NULL, /* no txn */
                      NULL,              /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_nodta_genid_tran(bdb_state_type *bdb_state, void *ix,
                                    int ixnum, int ixlen, void *lastix,
                                    int lastrrn, unsigned long long lastgenid,
                                    void *ixfound, int *rrn,
                                    unsigned long long *genid, void *tran,
                                    bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next_nodta_genid_tran");

    outrc = bdb_fetch_int(0,              /* return no data */
                          FETCH_INT_NEXT, /* next */
                          1,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, NULL,       /* recnum */
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, tran, NULL,                    /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_nodta_genid_nl_ser(bdb_state_type *bdb_state, void *ix,
                                      int ixnum, int ixlen, void *lastix,
                                      int lastrrn, unsigned long long lastgenid,
                                      void *ixfound, int *rrn,
                                      unsigned long long *genid,
                                      bdb_cursor_ser_t *cur_ser,
                                      bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next_nodta_genid_ser");

    outrc = bdb_fetch_int(0,              /* return no data */
                          FETCH_INT_NEXT, /* next */
                          0,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, NULL,       /* recnum */
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                          /* no txn */
                          (bdb_cursor_ser_int_t *)cur_ser, args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_genid_tran(bdb_state_type *bdb_state, void *ix, int ixnum,
                              int ixlen, void *lastix, int lastrrn,
                              unsigned long long lastgenid, void *dta,
                              int dtalen, int *reqdtalen, void *ixfound,
                              int *rrn, unsigned long long *genid, void *tran,
                              bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_genid_tran");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_PREV, /* prev */
                      1,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      0, tran, NULL, /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_genid_nl_ser(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *dta,
                                int dtalen, int *reqdtalen, void *ixfound,
                                int *rrn, unsigned long long *genid,
                                bdb_cursor_ser_t *cur_ser,
                                bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_genid_ser");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_PREV, /* prev */
                      0,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      0, NULL,                                    /* no txn */
                      (bdb_cursor_ser_int_t *)cur_ser, args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_blobs_genid(
    bdb_state_type *bdb_state, void *ix, int ixnum, int ixlen, void *lastix,
    int lastrrn, unsigned long long lastgenid, void *dta, int dtalen,
    int *reqdtalen, void *ixfound, int *rrn, unsigned long long *genid,
    int numblobs, int *dtafilenums, size_t *blobsizes, size_t *bloboffs,
    void **blobptrs, bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_blobs_genid");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_PREV, /* prev */
                      1,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, numblobs, dtafilenums, blobsizes, bloboffs,
                      blobptrs, 0, NULL, /* no txn */
                      NULL,              /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_nodta_genid_tran(bdb_state_type *bdb_state, void *ix,
                                    int ixnum, int ixlen, void *lastix,
                                    int lastrrn, unsigned long long lastgenid,
                                    void *ixfound, int *rrn,
                                    unsigned long long *genid, void *tran,
                                    bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK(__func__);

    outrc = bdb_fetch_int(0,              /* return no data */
                          FETCH_INT_PREV, /* prev */
                          1,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, NULL,       /* recnum */
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, tran, NULL,                    /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_nodta_genid_nl_ser(bdb_state_type *bdb_state, void *ix,
                                      int ixnum, int ixlen, void *lastix,
                                      int lastrrn, unsigned long long lastgenid,
                                      void *ixfound, int *rrn,
                                      unsigned long long *genid,
                                      bdb_cursor_ser_t *cur_ser,
                                      bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_nodta_genid");

    outrc = bdb_fetch_int(0,              /* return no data */
                          FETCH_INT_PREV, /* prev */
                          0,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, NULL,       /* recnum */
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                          /* no txn */
                          (bdb_cursor_ser_int_t *)cur_ser, args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_recnum_genid(bdb_state_type *bdb_state, void *ix, int ixnum,
                           int ixlen, void *dta, int dtalen, int *reqdtalen,
                           void *ixfound, int *rrn, int *recnum,
                           unsigned long long *genid, bdb_fetch_args_t *args,
                           int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_recnum_genid");

    outrc = bdb_fetch_int(1,             /* return data */
                          FETCH_INT_CUR, /* current */
                          1,             /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0, /* lastix, lastrrn, lastgenid */
                          dta, dtalen, reqdtalen, ixfound, rrn, recnum, genid,
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_recnum_genid_by_recnum(bdb_state_type *bdb_state, int ixnum,
                                     int recnum_in, void *dta, int dtalen,
                                     int *reqdtalen, void *ixfound, int *rrn,
                                     int *recnum_out, unsigned long long *genid,
                                     bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_recnum_genid_by_recnum");

    memcpy(recnum_out, &recnum_in, sizeof(int));

    outrc = bdb_fetch_int(1,                         /* return data */
                          FETCH_INT_CUR_BY_RECNUM,   /* current */
                          1,                         /* lookahead */
                          bdb_state, NULL, ixnum, 0, /* ix, ixnum, ixlen */
                          NULL, 0, 0, /* lastix, lastrrn, lastgenid, */
                          dta, dtalen, reqdtalen, ixfound, rrn, recnum_out,
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                          /* no txn */
                          NULL,                             /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_lastdupe_recnum_genid(bdb_state_type *bdb_state, void *ix,
                                    int ixnum, int ixlen, void *dta, int dtalen,
                                    int *reqdtalen, void *ixfound, int *rrn,
                                    int *recnum, unsigned long long *genid,
                                    bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_lastdupe_recnum_genid");

    outrc = bdb_fetch_int(1,                      /* return data */
                          FETCH_INT_CUR_LASTDUPE, /* current, last dupe */
                          1,                      /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0, /* lastix, lastrrn, lastgenid */
                          dta, dtalen, reqdtalen, ixfound, rrn, recnum, genid,
                          0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, NULL,                   /* no txn */
                          NULL,                      /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_lastdupe_recnum_genid(bdb_state_type *bdb_state, void *ix,
                                          int ixnum, int ixlen, void *ixfound,
                                          int *rrn, int *recnum,
                                          unsigned long long *genid,
                                          bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_lastdupe_recnum_genid");

    outrc = bdb_fetch_int(0,                      /* don't return data */
                          FETCH_INT_CUR_LASTDUPE, /* current, last dupe */
                          1,                      /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0,             /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, recnum, genid, 0, NULL, NULL, NULL,
                          NULL,    /* no blobs */
                          0, NULL, /* no txn */
                          NULL,    /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_recnum_genid(bdb_state_type *bdb_state, void *ix, int ixnum,
                                 int ixlen, void *ixfound, int *rrn,
                                 int *recnum, unsigned long long *genid,
                                 bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_recnum_genid");

    outrc = bdb_fetch_int(0,             /* return no data */
                          FETCH_INT_CUR, /* current */
                          1,             /* lookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0,             /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, recnum, genid, 0, NULL, NULL, NULL,
                          NULL,    /* no blobs */
                          0, NULL, /* no txn */
                          NULL,    /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_nodta_recnum_genid_by_recnum(bdb_state_type *bdb_state, int ixnum,
                                           int recnum_in, void *ixfound,
                                           int *rrn, int *recnum_out,
                                           unsigned long long *genid,
                                           bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_nodta_recnum_genid_by_recnum");

    memcpy(recnum_out, &recnum_in, sizeof(int));

    outrc = bdb_fetch_int(0,                         /* return no data */
                          FETCH_INT_CUR_BY_RECNUM,   /* current */
                          1,                         /* lookahead */
                          bdb_state, NULL, ixnum, 0, /* ix, ixnum, ixlen */
                          NULL, 0, 0,    /* lastix, lastrrn, lastgenid */
                          NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, recnum_out, genid, 0, NULL, NULL, NULL,
                          NULL,    /* no blobs */
                          0, NULL, /* no txn */
                          NULL,    /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_recnum_genid(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *dta,
                                int dtalen, int *reqdtalen, void *ixfound,
                                int *rrn, int *recnum,
                                unsigned long long *genid,
                                bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next_recnum_genid");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_NEXT, /* next */
                      1,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, recnum, genid, 0,
                      NULL, NULL, NULL, NULL, /* no blobs */
                      0, NULL,                /* no txn */
                      NULL,                   /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_nodta_recnum_genid(bdb_state_type *bdb_state, void *ix,
                                      int ixnum, int ixlen, void *lastix,
                                      int lastrrn, unsigned long long lastgenid,
                                      void *ixfound, int *rrn, int *recnum,
                                      unsigned long long *genid,
                                      bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next_nodta_recnum_genid");

    outrc = bdb_fetch_int(0,              /* return no data */
                          FETCH_INT_NEXT, /* next */
                          1,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, recnum, genid, 0, NULL, NULL, NULL,
                          NULL,    /* no blobs */
                          0, NULL, /* no txn */
                          NULL,    /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_recnum_genid(bdb_state_type *bdb_state, void *ix, int ixnum,
                                int ixlen, void *lastix, int lastrrn,
                                unsigned long long lastgenid, void *dta,
                                int dtalen, int *reqdtalen, void *ixfound,
                                int *rrn, int *recnum,
                                unsigned long long *genid,
                                bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_recnum_genid");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_PREV, /* prev */
                      1,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, recnum, genid, 0,
                      NULL, NULL, NULL, NULL, /* no blobs */
                      0, NULL,                /* no txn */
                      NULL,                   /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_nodta_recnum_genid(bdb_state_type *bdb_state, void *ix,
                                      int ixnum, int ixlen, void *lastix,
                                      int lastrrn, unsigned long long lastgenid,
                                      void *ixfound, int *rrn, int *recnum,
                                      unsigned long long *genid,
                                      bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_nodta_recnum_genid");

    outrc = bdb_fetch_int(0,              /* return no data */
                          FETCH_INT_PREV, /* prev */
                          1,              /* lookahead */
                          bdb_state, ix, ixnum, ixlen, lastix, lastrrn,
                          lastgenid, NULL, 0, NULL, /* dta, dtalen, reqdtalen */
                          ixfound, rrn, recnum, genid, 0, NULL, NULL, NULL,
                          NULL,    /* no blobs */
                          0, NULL, /* no txn */
                          NULL,    /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_next_genid_nl(bdb_state_type *bdb_state, void *ix, int ixnum,
                            int ixlen, void *lastix, int lastrrn,
                            unsigned long long lastgenid, void *dta, int dtalen,
                            int *reqdtalen, void *ixfound, int *rrn,
                            unsigned long long *genid, bdb_fetch_args_t *args,
                            int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_next_genid_nl");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_NEXT, /* next */
                      0,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      0, NULL,                                    /* no txn */
                      NULL, /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_prev_genid_nl(bdb_state_type *bdb_state, void *ix, int ixnum,
                            int ixlen, void *lastix, int lastrrn,
                            unsigned long long lastgenid, void *dta, int dtalen,
                            int *reqdtalen, void *ixfound, int *rrn,
                            unsigned long long *genid, bdb_fetch_args_t *args,
                            int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_genid_nl");

    outrc =
        bdb_fetch_int(1,              /* return data */
                      FETCH_INT_PREV, /* prev */
                      0,              /* lookahead */
                      bdb_state, ix, ixnum, ixlen, lastix, lastrrn, lastgenid,
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      0, NULL,                                    /* no txn */
                      NULL, /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_by_key_tran(bdb_state_type *bdb_state, tran_type *tran, void *ix,
                          int ixlen, int ixnum, void *ixfound, void *dta,
                          int dtalen, int *reqdtalen, int *rrn,
                          unsigned long long *genid, bdb_fetch_args_t *args,
                          int *bdberr)
{
    int outrc;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_genid_nl");

    outrc =
        bdb_fetch_int(dtalen,           /* return data */
                      FETCH_INT_CUR, 0, /* nolookahead */
                      bdb_state, ix, ixnum, ixlen, NULL, 0,
                      0, /* lastix, lastrrn, lastgenid */
                      dta, dtalen, reqdtalen, ixfound, rrn, NULL, /* recnum */
                      genid, 0, NULL, NULL, NULL, NULL,           /* no blobs */
                      0, tran, NULL, /* no cur_ser */
                      args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_by_primkey_tran(bdb_state_type *bdb_state, tran_type *tran,
                              void *ix, void *dta, int dtalen, int *reqdtalen,
                              int *rrn, unsigned long long *genid,
                              bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    int ixnum;
    int ixlen;

    ixnum = 0;
    ixlen = -1; /* full length */

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_genid_nl");

    outrc = bdb_fetch_int(dtalen,           /* return data */
                          FETCH_INT_CUR, 0, /* nolookahead */
                          bdb_state, ix, ixnum, ixlen, NULL, 0,
                          0, /* lastix, lastrrn, lastgenid */
                          dta, dtalen, reqdtalen, NULL,     /* ixfound */
                          rrn, NULL,                        /* recnum */
                          genid, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          0, tran, NULL,                    /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

static int bdb_fetch_by_genid_int(bdb_state_type *bdb_state, tran_type *tran,
                                  unsigned long long genid, void *dta,
                                  int dtalen, int *dtalenout, int dirty,
                                  bdb_fetch_args_t *args, int *bdberr)
{
    int outrc;
    unsigned long long ixfound;
    int rrnout;
    unsigned long long genidout;

    *bdberr = BDBERR_NOERROR;

    BDB_READLOCK("bdb_fetch_prev_genid_nl");

    outrc = bdb_fetch_int(1,                /* return data */
                          FETCH_INT_CUR, 0, /* nolookahead */
                          bdb_state, &genid /* ix */, -1 /* ixnum */,
                          sizeof(unsigned long long) /* ixlen */, NULL, 0,
                          0, /* lastix, lastrrn, lastgenid */
                          dta, dtalen, dtalenout, &ixfound, &rrnout,
                          NULL,                                 /* recnum */
                          &genidout, 0, NULL, NULL, NULL, NULL, /* no blobs */
                          dirty, tran, NULL,                    /* no cur_ser */
                          args, bdberr);

    BDB_RELLOCK();

    return outrc;
}

int bdb_fetch_by_rrn_and_genid(bdb_state_type *bdb_state, int rrn,
                               unsigned long long genid, void *dta, int dtalen,
                               int *reqdtalen, bdb_fetch_args_t *args,
                               int *bdberr)
{
    int rc;
    BDB_READLOCK("bdb_fetch_by_rrn_and_genid");

    rc = bdb_fetch_by_genid_int(bdb_state, NULL, genid, dta, dtalen, reqdtalen,
                                0, args, bdberr);

    BDB_RELLOCK();

    return rc;
}

int bdb_fetch_by_rrn_and_genid_prefault(bdb_state_type *bdb_state, int rrn,
                                        unsigned long long genid, void *dta,
                                        int dtalen, int *reqdtalen,
                                        bdb_fetch_args_t *args, int *bdberr)
{
    int rc;
    int dtalenout;
    int ixfound;
    int rrnout;
    unsigned long long outgenid;
    BDB_READLOCK("bdb_fetch_by_rrn_and_genid_prefault");

    rc = bdb_fetch_prefault_int(
        1, FETCH_INT_CUR, 0, bdb_state, &genid, -1, sizeof(unsigned long long),
        NULL, 0, 0, dta, dtalen, &dtalenout, &ixfound, &rrnout, NULL, &outgenid,
        0, NULL, NULL, NULL, NULL, args, bdberr);

    BDB_RELLOCK();

    return rc;
}

int bdb_fetch_by_rrn_and_genid_dirty(bdb_state_type *bdb_state, int rrn,
                                     unsigned long long genid, void *dta,
                                     int dtalen, int *reqdtalen,
                                     bdb_fetch_args_t *args, int *bdberr)
{
    int rc;
    BDB_READLOCK("bdb_fetch_by_rrn_and_genid");

    rc = bdb_fetch_by_genid_int(bdb_state, NULL, genid, dta, dtalen, reqdtalen,
                                1, args, bdberr);

    BDB_RELLOCK();

    return rc;
}

int bdb_fetch_by_rrn_and_genid_get_curgenid(bdb_state_type *bdb_state, int rrn,
                                            unsigned long long genid,
                                            unsigned long long *outgenid,
                                            void *dta, int dtalen,
                                            int *reqdtalen,
                                            bdb_fetch_args_t *args, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_fetch_by_rrn_and_genid_get_curgenid");

    rc = bdb_fetch_by_genid_int(bdb_state, NULL, genid, dta, dtalen, reqdtalen,
                                0, args, bdberr);
    *outgenid = genid;

    BDB_RELLOCK();

    return rc;
}

int bdb_fetch_by_rrn_and_genid_tran(bdb_state_type *bdb_state, tran_type *tran,
                                    int rrn, unsigned long long genid,
                                    void *dta, int dtalen, int *reqdtalen,
                                    bdb_fetch_args_t *args, int *bdberr)
{
    int rc;
    BDB_READLOCK("bdb_fetch_by_rrn_and_genid_tran");

    rc = bdb_fetch_by_genid_int(bdb_state, tran, genid, dta, dtalen, reqdtalen,
                                0, args, bdberr);

    BDB_RELLOCK();

    return rc;
}

/* Get the next record in the database in one of the stripes.  If stay_in_stripe
 * is set, don't move on to the next stripe.
 * Returns 0 on success, 1 if there are no more records, -1 on failure. */
int bdb_fetch_next_dtastripe_record(bdb_state_type *bdb_state,
                                    const unsigned long long *p_genid_vector,
                                    unsigned long long *p_genid, int *p_stripe,
                                    int stay_in_stripe, void *p_dta, int dtalen,
                                    int *p_reqdtalen, void *trans,
                                    bdb_fetch_args_t *args, int *p_bdberr)
{
    int rc;
    int stripe_wrapped;
    int cur_stripe;

    /* if stripe is valid */
    if (*p_stripe >= 0 && *p_stripe < bdb_state->attr->dtastripe)
        cur_stripe = *p_stripe;
    else
        cur_stripe = 0;

    stripe_wrapped = 0;
    do {
        int rrn;
        int recnum;
        int direction;
        unsigned long long last_genid;

        direction = FETCH_INT_NEXT;

        last_genid = p_genid_vector[cur_stripe];

        /* if this is the first genid we're getting for this stripe */
        if (!last_genid)
            last_genid = get_lowest_genid_for_datafile(
                cur_stripe); /* smallest genid for this stripe */

        /* if this is the first genid we're getting for the first stripe */
        if (!last_genid)
            /* start looking at the beginning */
            direction = FETCH_INT_CUR;
    again:
        rc = bdb_fetch_int(1,                                /* return data */
                           direction, 0,                     /* lookahead */
                           bdb_state, NULL,                  /* ix */
                           -1,                               /* ixnum */
                           0,                                /* ixlen */
                           &last_genid,                      /* lastix */
                           0,                                /* lastrrn */
                           last_genid,                       /* lastgenid */
                           p_dta, dtalen, p_reqdtalen, NULL, /* ixfound */
                           &rrn, &recnum, p_genid, 0, NULL, NULL, NULL,
                           NULL,  /* no blobs */
                           0,     /* dirty */
                           trans, /* no txn */
                           NULL,  /* cur_ser,  maybe try to use one */
                           args, p_bdberr);
        switch (rc) {
        /* next record found */
        case 0:
        case 1:
        case 2: {
            /* if the genid we found has a different stripe */
            int found_stripe = get_dtafile_from_genid(*p_genid);
            if (found_stripe != cur_stripe) {
                /* also handles found_stripe < 0 */
                if (found_stripe < cur_stripe) {
                    logmsg(LOGMSG_ERROR, "%s: looking for next genid in "
                                    "stripe: %d after: %016llx (%016llx) got: "
                                    "%016llx in stripe %d\n",
                            __func__, cur_stripe, last_genid,
                            p_genid_vector[cur_stripe], *p_genid, found_stripe);

                    *p_bdberr = BDBERR_MISC;
                    return -1;
                }

                if (stay_in_stripe) {
                    /* no more data in stripe */
                    *p_bdberr = BDBERR_NOERROR;
                    return 1;
                }

                cur_stripe = found_stripe;
                break;
            }
        }

            /* An inplace update could have occurred after the add.  Go to the
             * next record. */
            if (bdb_inplace_cmp_genids(bdb_state, *p_genid,
                                       p_genid_vector[cur_stripe]) == 0) {
                direction = FETCH_INT_NEXT;
                memcpy(&last_genid, p_genid, 8);
                goto again;
            }

            /* if the genid we found is less then or equal to the last genid
             * we found in this stripe, there was a problem */
            if ((bdb_cmp_genids(*p_genid, p_genid_vector[cur_stripe]) <= 0) &&
                (!args || !args->page_order)) {
                logmsg(LOGMSG_ERROR, "%s: looking for next genid in stripe: %d "
                                "after: %016llx (%016llx) got: %016llx\n",
                        __func__, cur_stripe, last_genid,
                        p_genid_vector[cur_stripe], *p_genid);

                *p_bdberr = BDBERR_MISC;
                return -1;
            }

            /* found a record */
            *p_stripe = cur_stripe;
            *p_bdberr = BDBERR_NOERROR;
            return 0;

        /* hit end of table */
        case 3:
        case 99:
            /* if we've already wrapped around once before */
            if (stripe_wrapped) {
                logmsg(LOGMSG_ERROR, "%s: stripes wrapped twice\n", __func__);

                *p_bdberr = BDBERR_MISC;
                return 1;
            }

            if (stay_in_stripe) {
                /* no more data in stripe */
                *p_bdberr = BDBERR_NOERROR;
                return 1;
            }

            stripe_wrapped = 1;
            cur_stripe = 0;
            break;

        /* error */
        case -1:
            if (*p_bdberr != BDBERR_DEADLOCK)
                logmsg(LOGMSG_ERROR, "%s: bdb_fetch_int rc %d bdberr %d\n", __func__,
                        rc, *p_bdberr);
            /* dont remap *p_bdberr - percolate it up to caller*/
            return -1;
            break;

        /* dont expect to hit this case */
        default:
            logmsg(LOGMSG_ERROR, "%s: bdb_fetch_int rc %d bdberr %d\n", __func__, rc,
                    *p_bdberr);

            *p_bdberr = BDBERR_MISC;
            return -1;
            break;
        }
    }
    /* continue until we've checked all stripes */
    while (!stripe_wrapped || cur_stripe < *p_stripe);

    /* no more data */
    *p_bdberr = BDBERR_NOERROR;
    return 1;
}

void unpack_index_odh(bdb_state_type *bdb_state, DBT *data, void *foundgenid,
                      void *dta, int dtalen, int *reqdtalen, uint8_t *ver)
{
    struct odh odh;
    void *freeptr;
    const int genid_sz = sizeof(unsigned long long);
    uint8_t *d = data->data;

    memcpy(foundgenid, d, genid_sz);
    if (dtalen == 0)
        return;
    bdb_unpack(bdb_state, d + genid_sz, data->size - genid_sz, dta, dtalen,
               &odh, &freeptr);
    if (dta != odh.recptr) {
        memmove(dta, odh.recptr, MIN(odh.length, dtalen));
    }
    *reqdtalen = odh.length;
    *ver = odh.csc2vers;
}
