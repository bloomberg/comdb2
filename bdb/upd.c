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

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
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
#include <assert.h>

#include <build/db.h>
#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"

#include <plbitlib.h> /* for bset/btst */

#include "genid.h"

#include "logmsg.h"

int ll_dta_upd(bdb_state_type *bdb_state, int rrn, unsigned long long oldgenid,
               unsigned long long *newgenid, DB *dbp, tran_type *tran,
               int dtafile, int dtastripe, int participantstripeid,
               int use_new_genid, DBT *verify_dta, DBT *dta, DBT *old_dta_out);

static int bdb_change_dta_genid_dtastripe(bdb_state_type *bdb_state,
                                          tran_type *tran, int dtanum,
                                          unsigned long long oldgenid,
                                          unsigned long long newgenid,
                                          int has_blob_opt, int *bdberr)
{
    unsigned long long dtagenid = oldgenid;
    int rc;
    DB *dbp;
    int dtastripe;

    /* synthetic genids should not make it here */
    if (is_genid_synthetic(oldgenid)) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    *bdberr = BDBERR_NOERROR;

    int use_new_genid = 1;

    if (0 == bdb_inplace_cmp_genids(bdb_state, oldgenid, newgenid)) {
        use_new_genid = 0;
    }

    /* open a cursor */
    dbp = get_dbp_from_genid(bdb_state, dtanum, oldgenid, &dtastripe);

    if (dtanum >= 1 && has_blob_opt)
        rc = ll_dta_upd_blob_w_opt(bdb_state, 2, oldgenid, &newgenid, dbp, tran,
                                   dtanum, dtastripe, 0, use_new_genid, NULL,
                                   NULL, NULL);
    else
        rc = ll_dta_upd(bdb_state, 2, oldgenid, &newgenid, dbp, tran, dtanum,
                        dtastripe, 0, use_new_genid, NULL, NULL, NULL);

    /* if a blob is null, we won't find it, and don't need to update it */
    if (rc && dtanum > 0 && rc == DB_NOTFOUND)
        rc = 0;
    if (rc) {
        switch (rc) {
        case BDBERR_DTA_MISMATCH:
            *bdberr = BDBERR_DTA_MISMATCH;
            break;
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        default:
            logmsg(LOGMSG_ERROR, "%s:%d ll_dta_upd rc %d\n", __FILE__, __LINE__, rc);
            *bdberr = BDBERR_MISC;
        }
        rc = -1;
    }

    return rc;
}

/* Add or update a blob */
static int bdb_prim_add_upd_int(bdb_state_type *bdb_state, tran_type *tran,
                                int dtanum, void *newdta, int newdtaln, int rrn,
                                unsigned long long oldgenid,
                                unsigned long long newgenid,
                                int participantstripid, int *bdberr)
{

    int rc;
    int stripe;
    int *iptr;
    DB *dbp;
    DBT dbt_newdta;

    if (bdb_write_preamble(bdb_state, bdberr))
        return -1;

    /* if they didnt give us both an old and new genid, then fail */
    if (!oldgenid || !newgenid) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    /* fail if this is a participant-stripe database with no bits
       allocated to the participant id */
    if ((bdb_state->attr->updategenids) &&
        (bdb_state->attr->participantid_bits <= 0)) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    *bdberr = BDBERR_NOERROR;

    stripe = 0;

    /* synthetic genids should not make it here */
    if (is_genid_synthetic(oldgenid) || is_genid_synthetic(newgenid)) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    dbp = get_dbp_from_genid(bdb_state, dtanum, oldgenid, &stripe);

    bzero(&dbt_newdta, sizeof(DBT));
    dbt_newdta.data = newdta;
    dbt_newdta.size = newdtaln;

    rc = ll_dta_upd_blob(bdb_state, rrn, oldgenid, newgenid, dbp, tran, dtanum,
                         stripe, participantstripid, &dbt_newdta);

    if (rc) {
        *bdberr = rc;
        switch (rc) {
        case BDBERR_DTA_MISMATCH:
            *bdberr = BDBERR_DTA_MISMATCH;
            break;
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        default:
            logmsg(LOGMSG_ERROR, "%s:%d ll_dta_upd rc %d\n", __FILE__, __LINE__, rc);
            *bdberr = BDBERR_MISC;
        }
        rc = -1;
    }

    return rc;
}

static int bdb_prim_updvrfy_int(bdb_state_type *bdb_state, tran_type *tran,
                                void *olddta, int olddtaln, void *newdta,
                                int newdtaln, int rrn,
                                unsigned long long oldgenid,
                                unsigned long long *newgenid, int verifydta,
                                int participantstripid, int use_new_genid,
                                int keep_genid_intact, int *bdberr)
{
    DBT dbt_key, dbt_data;
    int rc;
    int stripe;
    int *iptr;
    DB *dbp;
    DBT dbt_newdta;
    DBT dbt_olddta;

    if (bdb_write_preamble(bdb_state, bdberr))
        return -1;

    if (newgenid && !use_new_genid)
        *newgenid = 0;

    /* if they didnt give us a genid, but we arent supposed to verify the
       data, then fail */
    if ((!oldgenid) && (!verifydta)) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    /* fail if this is a participant-stripe database with no bits
       allocated to the participant id */
    if ((bdb_state->attr->updategenids) &&
        (bdb_state->attr->participantid_bits <= 0)) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    *bdberr = BDBERR_NOERROR;

    stripe = 0;

    /* synthetic genids should not make it here */
    if (is_genid_synthetic(oldgenid)) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    dbp = get_dbp_from_genid(bdb_state, 0, oldgenid, &stripe);

    if (verifydta) {
        bzero(&dbt_olddta, sizeof(DBT));
        dbt_olddta.data = olddta;
        dbt_olddta.size = olddtaln;
    }
    bzero(&dbt_newdta, sizeof(DBT));
    dbt_newdta.data = newdta;
    dbt_newdta.size = newdtaln;

    if (keep_genid_intact) {
        rc = ll_dta_upgrade(bdb_state, rrn, oldgenid, dbp, tran, 0, stripe,
                            &dbt_newdta);
    } else {
        rc = ll_dta_upd(bdb_state, rrn, oldgenid, newgenid, dbp, tran, 0,
                        stripe, participantstripid, use_new_genid,
                        verifydta ? &dbt_olddta : NULL, &dbt_newdta, NULL);
    }

    if (rc) {
        *bdberr = rc;
        switch (rc) {
        case DB_NOTFOUND:
            *bdberr = BDBERR_RRN_NOTFOUND;
            break;
        case BDBERR_DTA_MISMATCH:
            *bdberr = BDBERR_DTA_MISMATCH;
            break;
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        default:
            logmsg(LOGMSG_ERROR, "%s:%d ll_dta_upd rc %d\n", __FILE__, __LINE__, rc);
            *bdberr = BDBERR_MISC;
        }
        rc = -1;
    }

    return rc;
}

static int bdb_prim_updkey_genid_int(bdb_state_type *bdb_state, tran_type *tran,
                                     void *key, int keylen, int ixnum,
                                     unsigned long long oldgenid,
                                     unsigned long long genid, void *dta,
                                     int dtalen, int isnull, int *bdberr)
{
    int rc;
    int stripe;
    int *iptr;
    DBT dbt_key;
    void *pKeyMaxBuf = 0;

    if (bdb_write_preamble(bdb_state, bdberr))
        return -1;

    *bdberr = BDBERR_NOERROR;

    stripe = 0;

    /* synthetic genids should not make it here */
    if (is_genid_synthetic(genid)) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    bdb_maybe_use_genid_for_key(bdb_state, &dbt_key, key, ixnum, genid, isnull, &pKeyMaxBuf);
    assert(!bdb_keycontainsgenid(bdb_state, ixnum) || 0 == bdb_inplace_cmp_genids(bdb_state, oldgenid, genid));

    rc = ll_key_upd(bdb_state, tran, bdb_state->name, oldgenid, genid, dbt_key.data, ixnum,
                    dbt_key.size, dta, dtalen);

    if (pKeyMaxBuf)
        free(pKeyMaxBuf);

    if (rc) {
        *bdberr = rc;
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        default:
            logmsg(LOGMSG_ERROR, "%s:%d ll_key_upd rc %d\n", __FILE__, __LINE__, rc);
            *bdberr = BDBERR_MISC;
        }
        rc = -1;
    }

    return rc;
}

int bdb_prim_add_upd_genid(bdb_state_type *bdb_state, tran_type *tran,
                           int dtanum, void *newdta, int newdtaln, int rrn,
                           unsigned long long oldgenid,
                           unsigned long long newgenid, int participantstripeid,
                           int *bdberr)
{
    int rc;

    rc = bdb_prim_add_upd_int(bdb_state, tran, dtanum, newdta, newdtaln, rrn,
                              oldgenid, newgenid, participantstripeid, bdberr);

    return rc;
}

int bdb_prim_updvrfy_genid(bdb_state_type *bdb_state, tran_type *tran,
                                  void *olddta, int oldlen, void *newdta,
                                  int newdtaln, int rrn,
                                  unsigned long long oldgenid,
                                  unsigned long long *newgenid, int verifydta,
                                  int participantstripeid, int use_new_genid,
                                  int *bdberr)
{
    int rc;

    rc = bdb_prim_updvrfy_int(bdb_state, tran, olddta, /* olddta */
                              oldlen,                  /* olddtaln */
                              newdta, newdtaln, rrn, oldgenid, newgenid,
                              verifydta, participantstripeid, use_new_genid,
                              0, /* modify updateid */
                              bdberr);

    return rc;
}

int bdb_upd_genid(bdb_state_type *bdb_state, tran_type *tran, int dtanum,
                  int rrn, unsigned long long oldgenid,
                  unsigned long long newgenid, int has_blob_opt, int *bdberr)
{
    int rc = 0;
    *bdberr = BDBERR_NOERROR;

    rc = bdb_change_dta_genid_dtastripe(bdb_state, tran, dtanum, oldgenid,
                                        newgenid, has_blob_opt, bdberr);

    return rc;
}

int bdb_prim_updkey_genid(bdb_state_type *bdb_state, tran_type *tran, void *key,
                          int keylen, int ixnum, unsigned long long oldgenid,
                          unsigned long long genid, void *dta, int dtalen,
                          int isnull, int *bdberr)
{
    int rc = 0;
    *bdberr = BDBERR_NOERROR;

    rc = bdb_prim_updkey_genid_int(bdb_state, tran, key, keylen, ixnum, genid,
                                   oldgenid, dta, dtalen, isnull, bdberr);

    return rc;
}

int bdb_prim_upgrade(bdb_state_type *bdb_state, tran_type *tran, void *newdta,
                     int newdtaln, unsigned long long oldgenid, int *bdberr)
{
    int rc;

    rc =
        bdb_prim_updvrfy_int(bdb_state, tran, NULL, 0, newdta, newdtaln, 2,
                             oldgenid, NULL, 0, 0, 0, 1, /* keep genid intact */
                             bdberr);

    return rc;
}
/*
    vi:ts=3:sw=3
*/
