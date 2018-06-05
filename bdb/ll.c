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

/* ll : low level.

   add/del/upd instead of making direct berk calls, are to make calls to ll.
   ll provides routines that are easily callable instead, taking close to the
   same arguments when possible.  goal is MINIMAL changes to add/upd/del code
   logic whenever possible.

   based on whether the transaction is "BERK" or "LOGICAL", the ll routines
   make the switch between either directly calling the berk primitive
   (that would have been directly called at various places in
   add/del/upd) or instead calling out to the physical routines in phys.c

   Routines in this file should return unmolested berkdb return code.
   Calling code should be responsible for renaming return codes to bdb if
   needed.
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
#include <limits.h>
#include <assert.h>
#include <alloca.h>

#include <build/db.h>
#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"
#include "llog_auto.h"
#include "missing.h"

#include <plbitlib.h> /* for bset/btst */
#include <list.h>
#include <lockmacro.h>

#include "genid.h"
#include "logmsg.h"

/* This file should contains primitives for adding/deleting data.
   All code in add.c/upd.c/del.c should call this instead of relying
   on berkeley ->put/->del/etc calls. */

#define RCCHECK(rc, close)                                                     \
    if (rc) {                                                                  \
        if (rc != BDBERR_DEADLOCK && rc != BDBERR_DEADLOCK_ROWLOCK)            \
            logmsg(LOGMSG_ERROR, "%s:%d %s get rc %d\n", __FILE__, __LINE__,        \
                    __func__, rc);                                             \
        if (close) {                                                           \
            crc = dbcp->c_close(dbcp);                                         \
            if (crc == DB_LOCK_DEADLOCK)                                       \
                rc = DB_LOCK_DEADLOCK;                                         \
        }                                                                      \
        return rc;                                                             \
    }

/* This is the berkley replication lockid.  We need it on the replicant because
   any page locks we acquire must be on behalf of the replication thread.  */
/* XXX delete after cleanup - we dont need this anymore .. */
extern u_int32_t gbl_rep_lockid;

/* Grab the rowlock for a record in this index */
static int get_row_lock_ix(bdb_state_type *bdb_state, DBC *dbcp,
                           tran_type *tran, int ixnum, unsigned long long genid,
                           DB_LOCK *rlk, DBT *lkname)
{
    /* Acquire rowlocks using this lid */
    int locker_lid = tran->logical_tran->logical_lid;

    /* Make sure this is NULL */
    assert(NULL == tran->logical_tran->tid);

    /* Call lock primitive */
    return bdb_get_row_lock(bdb_state, locker_lid, ixnum, dbcp, genid, rlk,
                            lkname, BDB_LOCK_WRITE);
}

/* Grab min or max lock while the cursor is not positioned on a page */
static int get_row_lock_ix_minmaxlk(bdb_state_type *bdb_state, DBC *dbcp,
                                    int ixnum, tran_type *tran, int minmax,
                                    DB_LOCK *rlk, DBT *lkname)
{
    /* Acquire rowlocks using this lid */
    int locker_lid = tran->logical_tran->logical_lid;

    /* Make sure this is NULL */
    assert(NULL == tran->logical_tran->tid);

    /* Call lock primitive */
    return bdb_get_row_lock_minmaxlk(bdb_state, locker_lid, dbcp, ixnum, -1,
                                     minmax, rlk, lkname, BDB_LOCK_WRITE);
}

/* Grab a row-lock for this genid.  dbcp is positioned in a data file */
static int get_row_lock_dta(bdb_state_type *bdb_state, DBC *dbcp,
                            tran_type *tran, unsigned long long genid,
                            DB_LOCK *rlk, DBT *lkname)
{
    /* Locker id */
    int locker_lid = tran->logical_tran->logical_lid;

    /* Make sure this is NULL */
    assert(NULL == tran->logical_tran->tid);

    /* Call lock primitive */
    return bdb_get_row_lock(bdb_state, locker_lid, -1, dbcp, genid, rlk, lkname,
                            BDB_LOCK_WRITE);
}

/* Grab min-lock for this stripe.  'genid' should be the first record in the
 * table. */
static int get_row_lock_dta_minlk(bdb_state_type *bdb_state, DBC *dbcp,
                                  int stripe, tran_type *tran,
                                  unsigned long long genid, DB_LOCK *rlk,
                                  DBT *lkname)
{
    /* Locker id */
    int locker_lid = tran->logical_tran->logical_lid;

    /* Should be NULL */
    assert(NULL == tran->logical_tran->tid);

    /* Call lock primitive */
    return bdb_get_row_lock_minmaxlk(bdb_state, locker_lid, dbcp, -1, stripe, 0,
                                     rlk, lkname, BDB_LOCK_WRITE);
}

int add_snapisol_logging(bdb_state_type *bdb_state)
{
    if (bdb_state->attr->snapisol && !gbl_rowlocks) {
        return 1;
    } else {
        return 0;
    }
}

/* Why do we pass this both dtafile/dtastripe and a dbp?  Because there's
   some, erm, interesting logic at various points for figuring out which
   file to write to, and I don't want to clone it here. */
int ll_dta_add(bdb_state_type *bdb_state, unsigned long long genid, DB *dbp,
               tran_type *tran, int dtafile, int dtastripe, DBT *dbt_key,
               DBT *dbt_data, int flags)
{
    int outrc, rc;
    int tran_flags;
    int updateid;
    int crc;

    tran_flags = tran ? 0 : DB_AUTO_COMMIT;
    tran_flags |= flags;

    switch (tran->tranclass) {
    case TRANCLASS_BERK:
        if (tran->logical_tran) {
            logmsg(LOGMSG_FATAL, "BUG: dtaadd: got a TRANCLASS_BERK transaction"
                            " in rowlocks mode\n");
            abort();
        }
    /* fall through */
    case TRANCLASS_PHYSICAL:

        if (add_snapisol_logging(bdb_state)) {
            rc = bdb_state->dbenv->lock_clear_tracked_writelocks(
                bdb_state->dbenv, tran->tid->txnid);
            if (rc) {
                logmsg(LOGMSG_FATAL,
                       "%s:%d error clearing tracked writelocks: %d\n",
                       __FILE__, __LINE__, rc);
                abort();
            }
        }

        outrc = bdb_put_pack(bdb_state, dtafile > 0 ? 1 : 0, dbp,
                             tran ? tran->tid : NULL, dbt_key, dbt_data,
                             tran_flags);

        if (!outrc && add_snapisol_logging(bdb_state)) {
            tran_type *parent = (tran->parent) ? tran->parent : tran;
            DBT dbt_tbl = {0};
            int iirc;

            /* prepare tablename and lsn */
            dbt_tbl.data = bdb_state->name;
            dbt_tbl.size = strlen(bdb_state->name) + 1;

            iirc = llog_undo_add_dta_log(
                bdb_state->parent->dbenv, tran->tid, &parent->last_logical_lsn,
                0, &dbt_tbl, dtafile, dtastripe, genid, parent->logical_tranid,
                &parent->last_logical_lsn, NULL);
            if (iirc)
                abort();

            iirc = bdb_state->dbenv->lock_update_tracked_writelocks_lsn(
                bdb_state->dbenv, tran->tid, tran->tid->txnid,
                parent->last_logical_lsn);
            if (iirc)
                abort();
        }

        /* Grab row locks surrounding the record we are modifying.  This
           protects
           us against dirty reads and undeleted records. */
        break;

    case TRANCLASS_LOGICAL:
        outrc = phys_dta_add(bdb_state, tran, genid, dbp, dtafile, dtastripe,
                             dbt_key, dbt_data);
        break;

    default:
        logmsg(LOGMSG_ERROR, "ll_dta_add called with unknown tran type %d\n",
                tran->tranclass);
    }
    return outrc;
}

int ll_dta_del(bdb_state_type *bdb_state, tran_type *tran, int rrn,
               unsigned long long genid, DB *dbp, int dtafile, int dtastripe,
               DBT *dta_out)
{
    int rc;
    DBT dbt_key = {0};
    DBT dta_out_si = {0};
    DBC *dbcp = NULL;
    unsigned long long found_genid;
    unsigned long long search_genid;
    int crc;
    int is_blob = 0;

    if (dta_out) {
        bzero(dta_out, sizeof(DBT));
        dta_out->flags = DB_DBT_MALLOC;
    }

    /* Always delete a masked genid for blobs. */
    if (dtafile > 0)
        is_blob = 1;
    /* NOTE: for logical transactions we are called with a row lock NOT
       acquired, the physical code ends up acquiring the lock and calling
       us with a berkeley transaction with the lock held. */
    switch (tran->tranclass) {
    case TRANCLASS_BERK:
        if (tran->logical_tran) {
            logmsg(LOGMSG_FATAL, "BUG: dtadel: got a TRANCLASS_BERK transaction"
                            " in rowlocks mode\n");
            abort();
        }
    /* fall through */
    case TRANCLASS_PHYSICAL: {

        if (add_snapisol_logging(bdb_state)) {
            rc = bdb_state->dbenv->lock_clear_tracked_writelocks(
                bdb_state->dbenv, tran->tid->txnid);
            if (rc) {
                logmsg(LOGMSG_FATAL,
                       "%s:%d error clearing tracked writelocks: %d\n",
                       __FILE__, __LINE__, rc);
                abort();
            }
        }

        rc = dbp->cursor(dbp, tran->tid, &dbcp, 0);
        if (rc) {
            if (rc != DB_LOCK_DEADLOCK)
                logmsg(LOGMSG_ERROR, "%s:%d %s cursor rc %d\n", __FILE__, __LINE__,
                        __func__, rc);
            return rc;
        }

        /* 1) form the key
           2) position the cursor to the key we are trying to delete */

        dbt_key.flags = DB_DBT_USERMEM;
        dbt_key.ulen = sizeof(unsigned long long);

        /* Retrieve a maximum of eight bytes.  This is enough to check the
           genid.  Unless
           we are deleting as part of a logical transaction, in which case we
           need to log
           the entire record in case the transaction aborts later, so we need to
           get the
           whole record payload. */

        /* If the calling code needs the record value to log an undo,
           fetch it */
        if (dta_out || bdb_state->attr->snapisol || is_blob) {
            /* This codepath does a direct lookup on a masked genid. */
            int updateid = 0;
            int od_updateid = 0;

            search_genid = get_search_genid(bdb_state, genid);
            dbt_key.data = &search_genid;
            dbt_key.size = sizeof(search_genid);

            /* Tell Berkley to allocate memory if we need it.  */
            if (dta_out) {
                dta_out_si.flags = DB_DBT_MALLOC;
            }

            /* Logical logging only needs the record size. */
            else {
                dta_out_si.ulen = 0;
            }
            /*
             * Use a real cursor so we pick up the ondisk headers.  The
             * data will still be 'packed'.  Use the packed-size for the
             * logical log, and return a packed record.
             */
            if (0 != (rc = dbcp->c_get(dbcp, &dbt_key, &dta_out_si,
                                       DB_SET | DB_RMW))) {
                crc = dbcp->c_close(dbcp);
                if (crc == DB_LOCK_DEADLOCK)
                    rc = crc;
                goto done;
            }

            /* Copy the pointers if the user wanted the record. */
            if (dta_out) {
                dta_out->data = dta_out_si.data;
                dta_out->size = dta_out_si.size;
            }

            /* Verify updateid */
            updateid = get_updateid_from_genid(bdb_state, genid);
            od_updateid = bdb_retrieve_updateid(bdb_state, dta_out_si.data,
                                                dta_out_si.size);
            if (od_updateid >= 0) {
                if (dtafile >= 1) {
                    /* blobs */
                    if (od_updateid > updateid)
                        rc = DB_NOTFOUND;
                } else {
                    /* data */
                    if (od_updateid != updateid)
                        rc = DB_NOTFOUND;
                }
            } else {
                logmsg(LOGMSG_ERROR,
                       "%s:%d failed to get updateid from odh for genid %llx\n",
                       __FILE__, __LINE__, genid);
                rc = DB_ODH_CORRUPT;
            }

        } /* dta_out || snapisol || is_blob */
        else {
            /* Use the normal genid. */
            dbt_key.data = &genid;
            dbt_key.size = sizeof(genid);
            rc = bdb_cposition(bdb_state, dbcp, &dbt_key, DB_SET | DB_RMW);
        }

        if (rc != 0) {
            /* this could get a deadlock, and if it does, we
               need to percolate it up */
            crc = dbcp->c_close(dbcp);
            if (crc == DB_LOCK_DEADLOCK)
                rc = crc;
            goto done;
        }

        rc = dbcp->c_del(dbcp, 0);
        if (dtafile != 0 && rc == DB_NOTFOUND)
            rc = 0;
        else if (rc) {
            if (rc != DB_LOCK_DEADLOCK)
                logmsg(LOGMSG_ERROR, "%s:%d %s c_del rc %d\n", __FILE__, __LINE__,
                        __func__, rc);
            crc = dbcp->c_close(dbcp);
            if (crc == DB_LOCK_DEADLOCK)
                rc = crc;
            goto done;
        }

        rc = dbcp->c_close(dbcp);

        if (!rc && add_snapisol_logging(bdb_state)) {
            tran_type *parent = (tran->parent) ? tran->parent : tran;
            DBT dbt_tbl = {0};
            int iirc;

            dbt_tbl.size = strlen(bdb_state->name) + 1;
            dbt_tbl.data = bdb_state->name;

            iirc = llog_undo_del_dta_log(bdb_state->parent->dbenv, tran->tid,
                                         &parent->last_logical_lsn, 0, &dbt_tbl,
                                         genid, parent->logical_tranid,
                                         &parent->last_logical_lsn, dtafile,
                                         dtastripe, dta_out_si.size, NULL);
            if (iirc)
                abort();

            iirc = bdb_state->dbenv->lock_update_tracked_writelocks_lsn(
                bdb_state->dbenv, tran->tid, tran->tid->txnid,
                parent->last_logical_lsn);
            if (iirc)
                abort();
        }

    } break;

    case TRANCLASS_LOGICAL:
        rc = phys_dta_del(bdb_state, tran, rrn, genid, dbp, dtafile, dtastripe);
        break;

    default:
        logmsg(LOGMSG_ERROR, "ll_dta_del called with unknown tranclass %d\n",
                tran->tranclass);
        rc = -1;
        break;
    }

done:

    return rc;
}

int ll_key_del(bdb_state_type *bdb_state, tran_type *tran, int ixnum, void *key,
               int keylen, int rrn, unsigned long long genid, int *payloadsz)
{
    int rc = 0;
    DB *dbp;
    DBC *dbcp;
    DBT dbt_key = {0};
    DBT dbt_dta = {0};
    int *found_rrn;
    unsigned long long *found_genid;
    int keydata[3]; /* genid or rrn + genid for verification */
    int crc;
    int payloadsz_si = 0;

    dbp = bdb_state->dbp_ix[ixnum];
    dbt_key.data = key;
    dbt_key.size = keylen;

    found_rrn = NULL;
    found_genid = (unsigned long long *)(keydata);

    switch (tran->tranclass) {
    case TRANCLASS_BERK:
        if (tran->logical_tran) {
            logmsg(LOGMSG_FATAL, "BUG: keydel: got a TRANCLASS_BERK "
                            "transaction in rowlocks mode\n");
            abort();
        }
    /* fall through */
    case TRANCLASS_PHYSICAL:

        if (add_snapisol_logging(bdb_state)) {
            rc = bdb_state->dbenv->lock_clear_tracked_writelocks(
                bdb_state->dbenv, tran->tid->txnid);
            if (rc) {
                logmsg(LOGMSG_FATAL,
                       "%s:%d error clearing tracked writelocks: %d\n",
                       __FILE__, __LINE__, rc);
                abort();
            }
        }

        if (bdb_state->attr->snapisol && !payloadsz)
            payloadsz = &payloadsz_si;

        /* open a cursor on the index, find exact key to be deleted
           (if key doesn't allow dupes we need to verify the genid),
           then delete */
        rc = dbp->cursor(dbp, tran ? tran->tid : NULL, &dbcp, 0);
        if (rc) {
            if (rc != DB_LOCK_DEADLOCK)
                logmsg(LOGMSG_ERROR, "%s:%d %s cursor rc %d\n", __FILE__, __LINE__,
                        __func__, rc);
            goto done;
        }

        if (bdb_state->ixdta[ixnum]) {
            dbt_dta.data = keydata;
            dbt_dta.ulen = sizeof(int) * 3;
            dbt_dta.dlen = dbt_dta.ulen;
            dbt_dta.flags = DB_DBT_USERMEM;

            /* Get the full record to get the size.  This will fail with ENOMEM
               for most calls since we didn't allocate enough room - this is
               intentional.  All we want is the size.  The call a few lines
               further down will fetch the genid with a partial find. */
            rc = dbcp->c_get(dbcp, &dbt_key, &dbt_dta, DB_SET | DB_RMW);
            if (rc && rc != ENOMEM) {
                crc = dbcp->c_close(dbcp);
                if (crc == DB_LOCK_DEADLOCK)
                    rc = DB_LOCK_DEADLOCK;
                goto done;
            }

            dbt_dta.data = keydata;
            dbt_dta.ulen = sizeof(int) * 3;
            dbt_dta.dlen = dbt_dta.ulen;
            dbt_dta.doff = 0;
            dbt_dta.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
            if (payloadsz)
                *payloadsz = dbt_dta.size;
        } else {
            dbt_dta.data = keydata;
            dbt_dta.ulen = sizeof(int) * 3;
            dbt_dta.dlen = dbt_dta.ulen;
            dbt_dta.doff = 0;
            dbt_dta.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
            if (payloadsz)
                *payloadsz = sizeof(unsigned long long);
        }

        /* call cget here in favor of bdb_cget_unpack since keys aren't
           packed and unpack routines don't support partial finds. */
        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_dta, DB_SET | DB_RMW);
        if (rc) {
            crc = dbcp->c_close(dbcp);
            if (crc == DB_LOCK_DEADLOCK)
                rc = DB_LOCK_DEADLOCK;
            goto done;
        }

        /* see if the genid matches */
        if (*found_genid != genid) {
            rc = DB_NOTFOUND;
            crc = dbcp->c_close(dbcp);
            if (crc == DB_LOCK_DEADLOCK)
                rc = DB_LOCK_DEADLOCK;
            goto done;
        }

        rc = dbcp->c_del(dbcp, 0);
        if (rc != 0) {
            crc = dbcp->c_close(dbcp);
            if (crc == DB_LOCK_DEADLOCK)
                rc = DB_LOCK_DEADLOCK;
            goto done;
        }
        /* now close our cursor */
        rc = dbcp->c_close(dbcp);

        if (!rc && add_snapisol_logging(bdb_state)) {
            tran_type *parent = (tran->parent) ? tran->parent : tran;
            DBT dbt_tbl = {0};
            int iirc;

            dbt_tbl.size = strlen(bdb_state->name) + 1;
            dbt_tbl.data = bdb_state->name;

            iirc = llog_undo_del_ix_log(
                bdb_state->parent->dbenv, tran->tid, &parent->last_logical_lsn,
                0, &dbt_tbl, genid, ixnum, parent->logical_tranid,
                &parent->last_logical_lsn, NULL, keylen, *payloadsz);
            if (iirc)
                abort();

            iirc = bdb_state->dbenv->lock_update_tracked_writelocks_lsn(
                bdb_state->dbenv, tran->tid, tran->tid->txnid,
                parent->last_logical_lsn);
            if (iirc)
                abort();
        }

        break;

    case TRANCLASS_LOGICAL:
        rc = phys_key_del(bdb_state, tran, genid, ixnum, &dbt_key);
        break;

    default:
        logmsg(LOGMSG_ERROR, "ll_key_del called with unknown tran type %d\n",
                tran->tranclass);
        rc = -1;
    }

done:
    return rc;
}

int ll_key_upd(bdb_state_type *bdb_state, tran_type *tran, char *table_name,
               unsigned long long oldgenid, unsigned long long genid, void *key,
               int ixnum, int keylen, void *dta, int dtalen)
{
    int rc = 0;
    DB *dbp;
    DBC *dbcp;
    DBT dbt_key = {0};
    DBT dbt_dta = {0};
    int *found_rrn;
    unsigned long long *found_genid;
    int crc;
    const int genid_sz = sizeof(unsigned long long);
    unsigned char dtacopy_payload[MAXRECSZ + ODH_SIZE_RESERVE + genid_sz];
    unsigned long long keybuf[512 / sizeof(unsigned long long)];
    int dtacopy_payload_len;
    unsigned char keydata[MAXKEYSZ];
    int llog_payload_len = 8;

    dbp = bdb_state->dbp_ix[ixnum];

    bzero(&dbt_key, sizeof(DBT));
    dbt_key.data = key;
    dbt_key.size = keylen;

    if (dta) {
        /* copy the genid */
        memcpy(dtacopy_payload, &genid, genid_sz);

        /* we really need to do this only for datacopy row */
        if (bdb_state->ixdta[ixnum] && bdb_state->ondisk_header &&
            bdb_state->datacopy_odh) {
            /* put odh and dta */
            struct odh odh;
            void *rec = NULL;
            uint32_t recsize = 0;
            void *freeptr = NULL;
            init_odh(bdb_state, &odh, dta, dtalen, 0);
            bdb_pack(bdb_state, &odh, dtacopy_payload + genid_sz,
                     MAXRECSZ + ODH_SIZE_RESERVE, &rec, &recsize, &freeptr);
            llog_payload_len = dtacopy_payload_len = recsize + genid_sz;
        } else {
            /* put dta only */
            memcpy(dtacopy_payload + genid_sz, dta, dtalen);
            llog_payload_len = dtacopy_payload_len = dtalen + genid_sz;
        }
    }

    found_genid = (unsigned long long *)(keydata);

    switch (tran->tranclass) {
    case TRANCLASS_BERK:
        if (tran->logical_tran) {
            logmsg(LOGMSG_FATAL, "BUG: keydel: got a TRANCLASS_BERK "
                            "transaction in rowlocks mode\n");
            abort();
        }
    /* fall through */
    case TRANCLASS_PHYSICAL:

        if (add_snapisol_logging(bdb_state)) {
            rc = bdb_state->dbenv->lock_clear_tracked_writelocks(
                bdb_state->dbenv, tran->tid->txnid);
            if (rc) {
                logmsg(LOGMSG_FATAL,
                       "%s:%d error clearing tracked writelocks: %d\n",
                       __FILE__, __LINE__, rc);
                abort();
            }
        }

        /* open a cursor on the index, find exact key to be deleted
           (if key doesn't allow dupes we need to verify the genid),
           then delete */

        rc = dbp->cursor(dbp, tran ? tran->tid : NULL, &dbcp, 0);
        if (rc) {
            if (rc != DB_LOCK_DEADLOCK)
                logmsg(LOGMSG_ERROR, "%s:%d %s cursor rc %d\n", __FILE__, __LINE__,
                        __func__, rc);
            goto done;
        }

        bzero(&dbt_key, sizeof(DBT));
        bzero(&dbt_dta, sizeof(DBT));

        /* lookup by key */
        dbt_key.data = key;
        dbt_key.size = keylen;
        dbt_key.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

        /* retrieve genid */
        dbt_dta.data = keydata;
        dbt_dta.ulen = sizeof(unsigned long long);
        dbt_dta.dlen = sizeof(unsigned long long);
        dbt_dta.doff = 0;
        dbt_dta.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

        /* do a partial find to support dupe indexes which have a genid
           appended on the right side of the key. */

        /* call cget here in favor of bdb_cget_unpack since keys aren't
           packed and unpack routines don't support partial finds. */
        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_dta, DB_SET | DB_RMW);
        if (rc) {
            crc = dbcp->c_close(dbcp);
            if (crc == DB_LOCK_DEADLOCK)
                rc = DB_LOCK_DEADLOCK;
            goto done;
        }

        /* now do an update to update the genid.  if we're using dtacopy
           then update the genid + changed data payload.  (it must have changed
           if we're updating something). */
        if (dta) {
            dbt_dta.data = dtacopy_payload;
            dbt_dta.ulen = dtacopy_payload_len;
            dbt_dta.dlen = dtacopy_payload_len;
            dbt_dta.size = dtacopy_payload_len;
        } else {
            dbt_dta.data = &genid;
            dbt_dta.ulen = sizeof(unsigned long long);
            dbt_dta.dlen = sizeof(unsigned long long);
        }

        dbt_dta.doff = 0;
        dbt_dta.flags = DB_DBT_USERMEM;

        rc = dbcp->c_put(dbcp, &dbt_key, &dbt_dta, DB_CURRENT);
        if (rc != 0) {
            crc = dbcp->c_close(dbcp);
            if (crc == DB_LOCK_DEADLOCK)
                rc = DB_LOCK_DEADLOCK;
            goto done;
        }
        /* now close our cursor */
        rc = dbcp->c_close(dbcp);

        if (!rc && add_snapisol_logging(bdb_state)) {
            tran_type *parent = (tran->parent) ? tran->parent : tran;
            DBT dbt_tbl = {0};
            int iirc;

            dbt_tbl.size = strlen(bdb_state->name) + 1;
            dbt_tbl.data = bdb_state->name;

            {
                DB_LSN crp = parent->last_logical_lsn;

                /* Send key with our logical-log: we can't get to it from the
                 * berkley logs. */
                iirc = llog_undo_upd_ix_log(
                    bdb_state->parent->dbenv, tran->tid,
                    &parent->last_logical_lsn, 0, &dbt_tbl, oldgenid, genid,
                    parent->logical_tranid, &parent->last_logical_lsn, ixnum,
                    &dbt_key, dtalen);

                /*
                fprintf( stderr, "%s:%d upd ix LLSN %d:%d -> %d:%d\n",
                      __FILE__, __LINE__, crp.file, crp.offset,
                      parent->last_logical_lsn.file,
                parent->last_logical_lsn.offset);
                 */
                if (iirc)
                    abort();
            }

            iirc = bdb_state->dbenv->lock_update_tracked_writelocks_lsn(
                bdb_state->dbenv, tran->tid, tran->tid->txnid,
                parent->last_logical_lsn);
            if (iirc)
                abort();
        }

        break;

    case TRANCLASS_LOGICAL:
        rc = phys_key_upd(bdb_state, tran, bdb_state->name, oldgenid, genid,
                          key, ixnum, keylen, dta, dtalen, llog_payload_len);
        break;

    default:
        logmsg(LOGMSG_ERROR, "ll_key_upd called with unknown tran type %d\n",
                tran->tranclass);
        rc = -1;
    }

done:

    return rc;
}


int ll_key_add(bdb_state_type *bdb_state, unsigned long long ingenid,
               tran_type *tran, int ixnum, DBT *dbt_key, DBT *dbt_data)
{
    DB *dbp;
    DBC *dbcp = NULL;
    int rc;

    dbp = bdb_state->dbp_ix[ixnum];

    switch (tran->tranclass) {
    case TRANCLASS_BERK:
        if (tran->logical_tran) {
            logmsg(LOGMSG_FATAL, "BUG: keyadd: got a TRANCLASS_BERK transaction"
                            " in rowlocks mode\n");
            abort();
        }
    /* fall through */
    case TRANCLASS_PHYSICAL:

        if (add_snapisol_logging(bdb_state)) {
            rc = bdb_state->dbenv->lock_clear_tracked_writelocks(
                bdb_state->dbenv, tran->tid->txnid);
            if (rc) {
                logmsg(LOGMSG_FATAL,
                       "%s:%d error clearing tracked writelocks: %d\n",
                       __FILE__, __LINE__, rc);
                abort();
            }
        }

        rc = bdb_state->dbp_ix[ixnum]->put(bdb_state->dbp_ix[ixnum], tran->tid,
                                           dbt_key, dbt_data, DB_NOOVERWRITE);
        if (rc) {
            return rc;
        }

        if (!rc && add_snapisol_logging(bdb_state)) {
            tran_type *parent = (tran->parent) ? tran->parent : tran;
            DBT dbt_tbl = {0};
            int iirc;

            /* prepare tablename and lsn */
            dbt_tbl.data = bdb_state->name;
            dbt_tbl.size = strlen(bdb_state->name) + 1;

            iirc = llog_undo_add_ix_log(
                bdb_state->parent->dbenv, tran->tid, &parent->last_logical_lsn,
                0, &dbt_tbl, ixnum, ingenid, parent->logical_tranid,
                &parent->last_logical_lsn, dbt_key->size, dbt_data->size);
            if (iirc)
                abort();

            iirc = bdb_state->dbenv->lock_update_tracked_writelocks_lsn(
                bdb_state->dbenv, tran->tid, tran->tid->txnid,
                parent->last_logical_lsn);
            if (iirc)
                abort();
        }

        break;

    case TRANCLASS_LOGICAL:
        rc = phys_key_add(bdb_state, tran, ingenid, ixnum, dbt_key, dbt_data);
        break;

    default:
        logmsg(LOGMSG_ERROR, "ll_key_add called with unknown tran type %d\n",
                tran->tranclass);
        rc = -1;
    }

    return rc;
}

static int ll_dta_upd_int(bdb_state_type *bdb_state, int rrn,
                          unsigned long long oldgenid,
                          unsigned long long *newgenid, DB *dbp,
                          tran_type *tran, int dtafile, int dtastripe,
                          int participantstripid, int use_new_genid,
                          DBT *verify_dta, DBT *dta, DBT *old_dta_out,
                          int is_blob, int has_blob_update_optimization,
                          int keep_genid_intact)
{
    int rc;
    int inplace = 0;
    int updateid = 0;
    DBC *dbcp = NULL;
    DBT dbt_key;
    DBT dbt_data;
    unsigned long long search_genid;
    bdb_state_type *parent;
    void *malloceddta = NULL;
    void *recptr = NULL;
    int bdberr = 0;
    int crc;
    struct odh odh;
    void *freeptr = NULL;
    void *freedtaptr = NULL;
    DB *dbp_add;
    int got_rowlock = 0;
    int is_rowlocks = 0;
    int logical_len = 0;
    int add_blob = 0;
    DBT old_dta_out_lcl;
    char *oldrec = NULL;
    void *formatted_record = NULL;
    uint32_t formatted_record_len;
    int formatted_record_needsfree = 0;
    int got_new_lock = 0;
    int oldsz = -1;
    int newstripe = 0;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    dbp_add = NULL;

    if (tran->logical_tran && dtafile == 0) {
        is_rowlocks = 1;
    }

    if (old_dta_out) {
        old_dta_out->size = 0;
        old_dta_out->data = NULL;
    }

    switch (tran->tranclass) {
    case TRANCLASS_BERK:
        if (tran->logical_tran) {
            logmsg(LOGMSG_FATAL, "BUG: datupd: got a TRANCLASS_BERK "
                            "transaction in rowlocks mode\n");
            abort();
        }
    /* fall through */
    case TRANCLASS_PHYSICAL:

        if (add_snapisol_logging(bdb_state)) {
            rc = bdb_state->dbenv->lock_clear_tracked_writelocks(
                bdb_state->dbenv, tran->tid->txnid);
            if (rc) {
                logmsg(LOGMSG_FATAL,
                       "%s:%d error clearing tracked writelocks: %d\n",
                       __FILE__, __LINE__, rc);
                abort();
            }
        }

        /* open a cursor */
        rc = dbp->cursor(dbp, tran->tid, &dbcp, 0);
        if (rc != 0) {
            bdb_cursor_error(bdb_state, tran->tid, rc, &bdberr, "ll_dta_upd");
            goto done;
        }

        /* Possibly allocate a genid for this update */
        if (keep_genid_intact) {
            inplace = 1;
        } else if (0 == *newgenid) {
            if (ip_updates_enabled(bdb_state)) {
                int newupd = (1 + get_updateid_from_genid(bdb_state, oldgenid));
                if (newupd <= max_updateid(bdb_state)) {
                    *newgenid = set_updateid(bdb_state, newupd, oldgenid);
                    inplace = 1;
                }
            }

            if (!inplace) {
                if (bdb_state->attr->disable_update_stripe_change) {
                    /* old style that prevents update missing rows */
                    newstripe = get_dtafile_from_genid(oldgenid);
                } else {
                    newstripe = bdb_get_active_stripe_int(bdb_state);
                }
                *newgenid = get_genid(bdb_state, newstripe);
            }

            if (parent->attr->updategenids && participantstripid > 0) {
                *newgenid = set_participant_stripeid(
                    bdb_state, participantstripid, *newgenid);
            } else {
                *newgenid = set_participant_stripeid(bdb_state, 0, *newgenid);
            }
        }
        /* Genid was passed in as an argument (i.e. for a post_sc update). */
        else if (0 == bdb_inplace_cmp_genids(bdb_state, oldgenid, *newgenid)) {
            inplace = 1;
        }

        /* Use the inplace update shortcut for only a genid change */
        if ((!bdb_state->attr->snapisol) && (NULL == dta) && (!old_dta_out) &&
            (ip_updates_enabled(bdb_state)) &&
            (((0 == use_new_genid) &&
              (0 == bdb_inplace_cmp_genids(bdb_state, oldgenid, *newgenid))) ||
             (inplace))) {
            /* This is a blobs-only optimization. */
            assert(!is_rowlocks);

            rc = bdb_update_updateid(bdb_state, dbcp, oldgenid, *newgenid);
            if (rc != 0) {
                bdb_c_get_error(bdb_state, tran->tid, &dbcp, rc,
                                BDBERR_RRN_NOTFOUND, &bdberr, "ll_dta_upd");

                if (bdberr == BDBERR_RRN_NOTFOUND) {
                    bdberr = BDBERR_NOERROR;
                    rc = DB_NOTFOUND;
                    goto done;
                }
                goto done;
            }

            crc = dbcp->c_close(dbcp);
            dbcp = NULL;
            if (crc == DB_LOCK_DEADLOCK)
                rc = DB_LOCK_DEADLOCK;

            if (rc) {
                goto done;
            }

            return 0;
        }

        /* From this point on we'll want a masked newgenid. */
        if (inplace && !keep_genid_intact) {
            updateid = get_updateid_from_genid(bdb_state, *newgenid);
            *newgenid = bdb_mask_updateid(bdb_state, *newgenid);
        }

        /* Will be using the key, so zero it now. */
        memset(&dbt_key, 0, sizeof(dbt_key));

        /* Find the record with berkeley apis using the masked genid. */
        search_genid = get_search_genid(bdb_state, oldgenid);
        dbt_key.data = &search_genid;
        dbt_key.size = sizeof(search_genid);
#if 0
            printf("on update isblob %d oldgenid %016llx, search genid %016llx dbp %s haz %d\n", is_blob, oldgenid, search_genid, dbp->fname, has_blob_update_optimization);
#endif

        /* Zero old_dta_out_lcl. */
        bzero(&old_dta_out_lcl, sizeof(old_dta_out_lcl));

        /* Ask Berkeley to allocate memory if we need it.  */
        if (!dta || verify_dta || old_dta_out) {
            old_dta_out_lcl.flags = DB_DBT_MALLOC;
        }
        /* Set ulen to 0 to retrieve only the size. */
        else {
            old_dta_out_lcl.ulen = 0;
        }

        /* Retrieve using a Berkeley cursor. */
        rc = dbcp->c_get(dbcp, &dbt_key, &old_dta_out_lcl, DB_SET | DB_RMW);

        /* Retrieve the actual record length for the logical log. */
        logical_len = old_dta_out_lcl.size;

        /* Unpack if we need the record to return or verify. */
        if (0 == rc && (is_rowlocks || !dta || old_dta_out || verify_dta)) {
            /* Grab malloceddta to possibly free later. */
            malloceddta = old_dta_out_lcl.data;

            /* Zap odh. */
            bzero(&odh, sizeof(odh));

            /* Unpack record. */
            rc = bdb_unpack(bdb_state, old_dta_out_lcl.data,
                            old_dta_out_lcl.size, NULL, 0, &odh, &freeptr);

            /* Latch the verify-record pointer and length.  This is
               also used in the non-update/non-inplace codepath. */
            oldsz = odh.length;
            oldrec = odh.recptr;

            /* The old data should remain compressed & odh'd. */
            if (old_dta_out) {
                *old_dta_out = old_dta_out_lcl;
            }

#if 0
                printf("oldid %016llx  updateid %d odh updateid %d  dtafile %d dtastripe %d haz %d\n",
                        oldgenid, get_updateid_from_genid(bdb_state, oldgenid), odh.updateid, dtafile, dtastripe, has_blob_update_optimization);
#endif

            if (dtafile >= 1) {
                if (odh.updateid > get_updateid_from_genid(bdb_state, oldgenid))
                    rc = (0 == rc ? DB_NOTFOUND : rc);
            } else {
                if (odh.updateid !=
                    get_updateid_from_genid(bdb_state, oldgenid))
                    rc = (0 == rc ? DB_NOTFOUND : rc);
            }
        }

        /* A blob with this genid may or may not be there. */
        if (is_blob && rc == DB_NOTFOUND && dta) {
            add_blob = 1;
        } else if (rc != 0) {
            bdb_c_get_error(bdb_state, tran->tid, &dbcp, rc,
                            bdb_state->attr->dtastripe ? BDBERR_DTA_MISMATCH
                                                       : BDBERR_RRN_NOTFOUND,
                            &bdberr, "bdb_prim_updvrfy_int");
            if (bdberr == BDBERR_DEADLOCK)
                rc = DB_LOCK_DEADLOCK;
            if (bdberr == BDBERR_DTA_MISMATCH || bdberr == BDBERR_RRN_NOTFOUND)
                rc = DB_NOTFOUND;
            if (malloceddta)
                free(malloceddta);
            if (old_dta_out)
                old_dta_out->data = NULL;
            if (freeptr)
                free(freeptr);

            /* bdb_c_get_error closes it */
            goto done;
        }

        /* just finding the dta isnt sufficient if we
           were told to verify the data */
        if (verify_dta) {
            rc = 0;
            if (oldrec && memcmp(oldrec, verify_dta->data, oldsz) != 0) {
                /* in this case we return a bdb return code since there's no
                   corresponding berkdb return code */
                rc = BDBERR_DTA_MISMATCH;
                crc = dbcp->c_close(dbcp);
                dbcp = NULL;
                if (crc != 0) {
                    if (crc == DB_LOCK_DEADLOCK)
                        rc = DB_LOCK_DEADLOCK;
                    else
                        logmsg(LOGMSG_ERROR, "bdb_prim_updvrfy_int: c_close "
                                        "failed\n");
                }
                if (malloceddta)
                    free(malloceddta);
                if (old_dta_out)
                    old_dta_out->data = NULL;
                if (freeptr)
                    free(freeptr);
                goto done;
            }
        }

        /* Zero dbt_data. */
        bzero(&dbt_data, sizeof(dbt_data));

        /*
         * Pageordertablescan only works against splits on the rightmost
         * leaf.  Now that we have the old record size, force this into the
         * delete/add case if the new record is larger.
         *
         * In order to do this correctly, I have to format and add the new
         * record in two separate steps, as a new compressed data-record
         * can be larger or smaller than the old.
         */

        /* Form the record now- reuse the odh. */
        if (dta) {
            /* Zap odh. */
            bzero(&odh, sizeof(odh));

            init_odh(bdb_state, &odh, dta->data, dta->size, is_blob);

            /* Allocate space for new record if it is small. */
            if (dta->size + ODH_SIZE_RESERVE < 16 * 1024) {
                formatted_record = alloca(dta->size + ODH_SIZE_RESERVE);
            }
            /* Otherwise odh.c will allocate for us because it's null. */
            else {
                formatted_record_needsfree = 1;
            }

            /* If the record size was bigger, fall back to delete/add.
             * Otherwise there could be splits in the middle of the btree,
             * which we can't handle under page-order tablescan.  */

            /* Format the payload. */
            rc = bdb_pack(bdb_state, &odh, formatted_record,
                          dta->size + ODH_SIZE_RESERVE, &recptr,
                          &formatted_record_len, &freedtaptr);

            /* Fall back to delete/add. */
            if (inplace && !is_blob &&
                formatted_record_len > old_dta_out_lcl.size && !use_new_genid &&
                bdb_attr_get(bdb_state->attr, BDB_ATTR_PAGE_ORDER_TABLESCAN)) {
                if (keep_genid_intact) {
                    // if we are falling back to delete/add,
                    // we can't use the same genid.
                    // close the cursor and return.
                    rc = dbcp->c_close(dbcp);
                    dbcp = NULL;
                    bdberr = BDBERR_BADARGS;
                    goto done;
                }

                inplace = updateid = 0;

                if (bdb_state->attr->disable_update_stripe_change) {
                    /* old style that prevents update missing rows */
                    newstripe = get_dtafile_from_genid(oldgenid);
                } else {
                    newstripe = bdb_get_active_stripe_int(bdb_state);
                }

                *newgenid = get_genid(bdb_state, newstripe);

                if (parent->attr->updategenids && participantstripid > 0) {
                    *newgenid = set_participant_stripeid(
                        bdb_state, participantstripid, *newgenid);
                } else {
                    *newgenid =
                        set_participant_stripeid(bdb_state, 0, *newgenid);
                }
            }

            dbt_data.data = recptr;
            dbt_data.size = formatted_record_len;
        } else {
            dbt_data.data = old_dta_out_lcl.data;
            dbt_data.size = old_dta_out_lcl.size;
        }

        /*
         * delete the data record.  add_blob records weren't found to
         * begin with.
         */
        if (!inplace && !add_blob) {
            rc = dbcp->c_del(dbcp, 0);
            if (rc != 0) {
                crc = dbcp->c_close(dbcp);
                dbcp = NULL;
                if (malloceddta)
                    free(malloceddta);
                if (old_dta_out)
                    old_dta_out->data = NULL;
                if (freeptr)
                    free(freeptr);
                if (freedtaptr)
                    free(freedtaptr);
                if (crc == DB_LOCK_DEADLOCK)
                    rc = DB_LOCK_DEADLOCK;
                goto done;
            }
        }

        /* If we are given a new data payload, use it.
           Otherwise leave it alone, as it's
           just a genid change */

        /* Add a new data record with the right genid in the key. */
        memset(&dbt_key, 0, sizeof(dbt_key));

        /* New genid should be a masked genid. */
        assert(0 == get_updateid_from_genid(bdb_state, *newgenid));

        /* Handle the inplace update. */
        if (inplace && !add_blob) {
            if (keep_genid_intact) {
                rc = dbcp->c_put(dbcp, &dbt_key, &dbt_data, DB_CURRENT);
            } else {
                /* Write the update id to the correct offset of the ODH. */
                poke_updateid(dbt_data.data, updateid);

                /* Write the record. */
                rc = dbcp->c_put(dbcp, &dbt_key, &dbt_data, DB_CURRENT);

                /* Apply the updateid to the genid for the caller. */
                *newgenid = set_updateid(bdb_state, updateid, *newgenid);
            }
        } else {
            /* Set the key. */
            dbt_key.data = newgenid;
            dbt_key.size = sizeof(unsigned long long);

            /* For the 'add_blob' case, poke the new updateid.
             * For the 'not-inplace' case, updateid will be 0. */
            if (bdb_state->ondisk_header) {
                poke_updateid(dbt_data.data, updateid);
            }

            /* get a new dbp and dbcp on the stripe we're gonna add to  */
            dbp_add = get_dbp_from_genid(bdb_state, dtafile, *newgenid, NULL);

            /* This is either a record we just bdb_pack'd, or the old
               (still compressed, possibly) record. */
            rc = dbp_add->put(dbp_add, tran->tid, &dbt_key, &dbt_data,
                              DB_NOOVERWRITE);

            /* Apply the updateid to the genid for the caller. */
            *newgenid = set_updateid(bdb_state, updateid, *newgenid);
        }

        if (rc != 0) {
            if (malloceddta)
                free(malloceddta);
            if (old_dta_out)
                old_dta_out->data = NULL;
            if (freeptr)
                free(freeptr);
            if (freedtaptr)
                free(freedtaptr);
            if (formatted_record_needsfree)
                free(formatted_record);
            crc = dbcp->c_close(dbcp);
            dbcp = NULL;
            if (crc == DB_LOCK_DEADLOCK)
                rc = DB_LOCK_DEADLOCK;

            goto done;
        }

        /* Normal case- free if the caller doesn't want the record. */
        if (malloceddta && !old_dta_out)
            free(malloceddta);
        if (freeptr)
            free(freeptr);
        if (freedtaptr)
            free(freedtaptr);
        if (formatted_record_needsfree)
            free(formatted_record);
        /* close the cursors */
        rc = dbcp->c_close(dbcp);
        dbcp = NULL;
        if (rc) {
            goto done;
        }

        bdberr = BDBERR_NOERROR;

        if (!rc && add_snapisol_logging(bdb_state)) {
            tran_type *parent = (tran->parent) ? tran->parent : tran;
            DBT dbt_tbl = {0};
            int iirc;

            /*
            fprintf(stderr, "%llx -> %llx dtasdtripe=%d newstripe=%d\n",
                  oldgenid, *newgenid, dtastripe, newstripe);
             */

            /* prepare tablename and lsn */
            dbt_tbl.data = bdb_state->name;
            dbt_tbl.size = strlen(bdb_state->name) + 1;

            /* We discover inplace updates in update_shadows by comparing the
             * two genids. */
            iirc = llog_undo_upd_dta_log(
                bdb_state->parent->dbenv, tran->tid, &parent->last_logical_lsn,
                0, &dbt_tbl, oldgenid, *newgenid, parent->logical_tranid,
                &parent->last_logical_lsn, dtafile, dtastripe, NULL, NULL,
                old_dta_out_lcl.size);

            /*
            fprintf( stderr, "%s:%d upd ix LLSN %d:%d -> %d:%d\n",
                  __FILE__, __LINE__, crp.file, crp.offset,
                  parent->last_logical_lsn.file,
            parent->last_logical_lsn.offset);
             */
            if (iirc)
                abort();

            iirc = bdb_state->dbenv->lock_update_tracked_writelocks_lsn(
                bdb_state->dbenv, tran->tid, tran->tid->txnid,
                parent->last_logical_lsn);
            if (iirc)
                abort();
        }

        break;

    case TRANCLASS_LOGICAL:

        rc = phys_dta_upd(bdb_state, rrn, oldgenid, newgenid, dbp, tran,
                          dtafile, dtastripe, verify_dta, dta);
        break;
    default:
        break;
    }

done:

    return rc;
}

int ll_dta_upd_blob_w_opt(bdb_state_type *bdb_state, int rrn,
                          unsigned long long oldgenid,
                          unsigned long long *newgenid, DB *dbp,
                          tran_type *tran, int dtafile, int dtastripe,
                          int participantstripid, int use_new_genid,
                          DBT *verify_dta, DBT *dta, DBT *old_dta_out)
{
    return ll_dta_upd_int(bdb_state, rrn, oldgenid, newgenid, dbp, tran,
                          dtafile, dtastripe, participantstripid, use_new_genid,
                          verify_dta, dta, old_dta_out, 1, 1, 0);
}

int ll_dta_upd(bdb_state_type *bdb_state, int rrn, unsigned long long oldgenid,
               unsigned long long *newgenid, DB *dbp, tran_type *tran,
               int dtafile, int dtastripe, int participantstripid,
               int use_new_genid, DBT *verify_dta, DBT *dta, DBT *old_dta_out)
{
    return ll_dta_upd_int(bdb_state, rrn, oldgenid, newgenid, dbp, tran,
                          dtafile, dtastripe, participantstripid, use_new_genid,
                          verify_dta, dta, old_dta_out, (dtafile != 0), 0, 0);
}

int ll_dta_upd_blob(bdb_state_type *bdb_state, int rrn,
                    unsigned long long oldgenid, unsigned long long newgenid,
                    DB *dbp, tran_type *tran, int dtafile, int dtastripe,
                    int participantstripid, DBT *dta)
{
    unsigned long long ngenid = newgenid;
    return ll_dta_upd_int(bdb_state, rrn, oldgenid, &ngenid, dbp, tran, dtafile,
                          dtastripe, participantstripid, 1, NULL, dta, NULL, 1,
                          0, 0);
}

int ll_dta_upgrade(bdb_state_type *bdb_state, int rrn, unsigned long long genid,
                   DB *dbp, tran_type *tran, int dtafile, int dtastripe,
                   DBT *dta)
{
    unsigned long long newgenid = 0ULL;
    return ll_dta_upd_int(bdb_state, rrn, genid, &newgenid, dbp, tran, dtafile,
                          dtastripe, 0, 0, NULL, dta, NULL, 0, 0, 1);
}

int ll_commit_bench(bdb_state_type *bdb_state, tran_type *tran, int op,
                    int arg1, int arg2, void *payload, int paylen)
{
    int rc;
    DBT lock1 = {0}, lock2 = {0};
    assert(op == 0);
    rc = bdb_llog_rowlocks_bench(bdb_state, tran, op, arg1, arg2, &lock1,
                                 &lock2, payload, paylen);
    return rc;
}

int ll_rowlocks_bench(bdb_state_type *bdb_state, tran_type *tran, int op,
                      int arg1, int arg2, void *payload, int paylen)
{
    int rc = 0;
    assert(op > 0);
    switch (tran->tranclass) {
    case TRANCLASS_BERK:
        /* Fall through */
        if (tran->logical_tran) {
            logmsg(LOGMSG_FATAL, "BUG: %s: got a TRANCLASS_BERK "
                            "transaction in rowlocks mode\n",
                    __func__);
            abort();
        }

    case TRANCLASS_PHYSICAL:
        /* No physical operation for this */
        break;

    case TRANCLASS_LOGICAL:
        rc = phys_rowlocks_log_bench_lk(bdb_state, tran, op, arg1, arg2,
                                        payload, paylen);
        break;
    default:
        break;
    }
    return rc;
}

/* If we enabled rowlocks we need to record the lowest
   LSN of the oldest logical transaction in flight.
   This is the new low limit for log file deletion
   which will override berkeley's. Note that this code
   is only needed when a db comes up cold. If a replicant
   becomes master he has a better picture of the LWM from
   the replication stream.  That's why we don't need to
   write this more often than per checkpoint. */

extern int gbl_rowlocks;
extern int gbl_fullrecovery;

int ll_checkpoint(bdb_state_type *bdb_state, int force)
{
    DB_LSN lwm, lwmlsn, curlsn;
    tran_type *trans;
    int rc;
    int cmp;
    int bdberr;

    if (gbl_rowlocks &&
        (gbl_fullrecovery ||
         bdb_state->repinfo->master_host == bdb_state->repinfo->myhost)) {

        /* Grab current lsn first */
        __log_txn_lsn(bdb_state->dbenv, &curlsn, NULL, NULL);

        /* grab the LWM */
        rc = bdb_get_lsn_lwm(bdb_state, &lwmlsn);

        if (lwmlsn.file == 0 || 0 != rc ||
            ((cmp = log_compare(&curlsn, &lwmlsn)) < 0)) {
            lwm.file = curlsn.file;
            lwm.offset = curlsn.offset;
        } else {
            lwm.file = lwmlsn.file;
            lwm.offset = lwmlsn.offset;
        }

        rc = bdb_set_file_lwm(bdb_state, NULL, &lwm, &bdberr);
        if (rc)
           logmsg(LOGMSG_ERROR, "rc %d saving watermark %d:%d\n", rc, lwm.file, lwm.offset);
    }

    /* do the real checkpoint */
    rc = bdb_state->dbenv->txn_checkpoint(bdb_state->dbenv, 0, 0,
                                          force ? DB_FORCE : 0);

    return rc;
}
