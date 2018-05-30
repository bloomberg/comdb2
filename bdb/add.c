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

/* if dta is not null, it will be tailed after the genid in btree data part */
static int bdb_prim_addkey_int(bdb_state_type *bdb_state, tran_type *tran,
                               void *ixdta, int ixnum, int rrn,
                               unsigned long long genid, void *dta, int dtalen,
                               int isnull, int *bdberr)
{
    DBT dbt_key, dbt_data;
    int rc;
    unsigned int *keydata;
    int keydata_len;
    unsigned int *iptr;
    void *mallocedkeydata;
    unsigned int stackkeydata[3];
    void *pKeyMaxBuf = 0;

    *bdberr = BDBERR_NOERROR;

    if (bdb_write_preamble(bdb_state, bdberr))
        return -1;

    /* if we require the dta, but they didnt give us the dta, fail */
    if ((bdb_state->ixdta[ixnum]) && ((dta == NULL) || (dtalen == 0))) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    /* if we were given a dta without a genid, fail */
    if ((dta) && (!genid)) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    if ((isnull) && (!bdb_state->ixnulls[ixnum])) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    if (bdb_state->ixlen[ixnum] > bdb_state->keymaxsz) {
        logmsg(LOGMSG_FATAL, "calling abort 3\n");
        abort();
    }

    /* JJM 2018-05-02: This value is not actually used by this function. */
    /* rrn = 2; */

    /* for fixed format (rrn+genid, or genid) we dont malloc */
    mallocedkeydata = NULL;

    /* establish the size of the data payload on the index */
    keydata_len = sizeof(unsigned long long);

    if (dta) {
        keydata_len += dtalen;
        if (bdb_state->ixdta[ixnum] && bdb_state->ondisk_header &&
            bdb_state->datacopy_odh) {
            keydata_len += ODH_SIZE_RESERVE;
        }
    }

    /* get storage for the payload, mallocing if needed */
    if (dta) {
        mallocedkeydata = malloc(keydata_len);
        if (!mallocedkeydata) {
            *bdberr = BDBERR_MALLOC;
            return -1;
        }
        keydata = mallocedkeydata;
    } else
        keydata = stackkeydata;

    /* form the payload */
    iptr = (unsigned int *)keydata;

    memcpy(iptr, &genid, sizeof(unsigned long long));

    if (dta) {
        /* if this is datacopy, update the cached row properly */
        if (bdb_state->ixdta[ixnum] && bdb_state->ondisk_header &&
            bdb_state->datacopy_odh) {
            struct odh odh;
            void *rec = NULL;
            uint32_t recsize = 0;
            void *freeptr = NULL;
            init_odh(bdb_state, &odh, dta, dtalen, 0);
            bdb_pack(bdb_state, &odh, iptr + 2,
                     keydata_len - sizeof(unsigned long long), &rec, &recsize,
                     &freeptr);
            /* freeptr cannot be set. Provided buffer as big as rec.
             * Compression does not occur if rec expands */
            assert(freeptr == NULL);
            assert(rec == (iptr + 2));
            keydata_len = sizeof(unsigned long long) + recsize;
        } else {
            /* collattr or non-odh datacopy */
            memcpy(iptr + 2, dta, dtalen);
        }
    }

    /* keydata and keydata_len are set now */

    /*
      now add to the ix.

      if we allow dups:         key is ixdta + genid
      if we dont allow dups:    key is ixdta

      if we dont use dta:       data is genid
      if we use dta:            data is genid + dta

      */

    /* depending on the index flags and the provided field (column?) values, we
     * may want to use the genid as part of the index key.  currently, this is
     * only done when supporting multiple NULL values in a UNIQUE index. */
    bdb_maybe_use_genid_for_key(bdb_state, &dbt_key, ixdta, ixnum, genid, isnull, &pKeyMaxBuf);

    /* set up the dbt_data */
    memset(&dbt_data, 0, sizeof(dbt_data));
    dbt_data.data = keydata;
    dbt_data.size = keydata_len;

    /* write to the index */
    rc = ll_key_add(bdb_state, genid, tran, ixnum, &dbt_key, &dbt_data);

    if (pKeyMaxBuf)
        free(pKeyMaxBuf);
    if (mallocedkeydata)
        free(mallocedkeydata);
    if (rc == DB_KEYEXIST) {
        /* caught a dupe */
        *bdberr = BDBERR_ADD_DUPE;
        return -1;
    } else if (rc != 0) {
        switch (rc) {
        case DB_LOCK_DEADLOCK:
        case DB_REP_HANDLE_DEAD:
            *bdberr = BDBERR_DEADLOCK;
            break;
        default:
            *bdberr = BDBERR_MISC;
            break;
        }
        return -1;
    }

    return 0;
}

int bdb_prim_addkey_genid(bdb_state_type *bdb_state, tran_type *tran,
                          void *ixdta, int ixnum, int rrn,
                          unsigned long long genid, void *dta, int dtalen,
                          int isnull, int *bdberr)
{
    int rc;

    rc = bdb_prim_addkey_int(bdb_state, tran, ixdta, ixnum, rrn, genid, dta,
                             dtalen, isnull, bdberr);

    return rc;
}

static int bdb_prim_allocdta_int(bdb_state_type *bdb_state, tran_type *tran,
                                 void *dta, int dtalen,
                                 unsigned long long *genid,
                                 int participantstripid, int *bdberr)
{
    DBT dbt_key, dbt_data;
    int rc;
    int rrn;
    unsigned long long saved_genid = 0, *chkgenid = NULL;
    int dtafile;
    bdb_state_type *parent;
    int *genid_dta;
    tran_type *parent_tran;
    DB *dbp;
    int *mallocedtid;

    if (bdb_write_preamble(bdb_state, bdberr))
        return -1;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    if (tran->parent)
        parent_tran = tran->parent;
    else
        parent_tran = tran;

#ifdef FOO
    /* we DO NOT support variable length dta.  (for dta[0], blobs are above
       that).  therefore, we place a sanity check here to enforce this */
    if (bdb_state->lrl && (dtalen != bdb_state->lrl)) {
        logmsg(LOGMSG_ERROR, "bdb_prim_allocdta_int: size %d %d\n", dtalen,
                bdb_state->lrl);
        *bdberr = BDBERR_BADARGS;
        return 0;
    }
#endif

    *bdberr = BDBERR_NOERROR;

    rrn = 2; /* ALWAYS RETURN RRN 2!  HAHAHAHAHAAA! */

    dtafile = bdb_get_active_stripe_int(bdb_state);

    /*fprintf(stderr, "tid=%d rrn=%d dtafile=%d\n", tid, rrn, dtafile);*/

    /* get a genid and formulate a data buffer
       with the genid as the first 8 bytes and the data payload after it */
    *genid = get_genid(bdb_state, dtafile);

    if (parent->attr->updategenids && participantstripid > 0) {
        *genid =
            set_participant_stripeid(bdb_state, participantstripid, *genid);
    }

    /* add data to the dta file, with key being rrn */
    memset(&dbt_key, 0, sizeof(dbt_key));
    memset(&dbt_data, 0, sizeof(dbt_data));
    dbt_key.flags |= DB_DBT_USERMEM;
    dbt_data.flags |= DB_DBT_USERMEM;

    dbt_key.data = genid;
    dbt_key.size = sizeof(unsigned long long);

    dbt_data.data = dta;
    dbt_data.size = dtalen;

    genid_dta = NULL;

    dbp = bdb_state->dbp_data[0][dtafile];

    rc = ll_dta_add(bdb_state, *genid, dbp, tran, 0, dtafile /* stripe! */,
                    &dbt_key, &dbt_data, DB_NOOVERWRITE);

    if (genid_dta)
        free(genid_dta);

    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        default:
            logmsg(LOGMSG_ERROR, "%s:%d ll_dta_add rc=%d\n", __FILE__, __LINE__, rc);
            *bdberr = BDBERR_MISC;
            break;
        }

        return 0;
    }

    return rrn;
}

int bdb_prim_allocdta(bdb_state_type *bdb_state, tran_type *tran, void *dta,
                      int dtalen, int *bdberr)
{
    int rc;

    rc = bdb_prim_allocdta_int(bdb_state, tran, dta, dtalen, NULL, /* genid */
                               0, bdberr);

    return rc;
}

int bdb_prim_allocdta_genid(bdb_state_type *bdb_state, tran_type *tran,
                            void *dta, int dtalen, unsigned long long *genid,
                            int participantstripid, int *bdberr)
{
    int rc;

    rc = bdb_prim_allocdta_int(bdb_state, tran, dta, dtalen, genid,
                               participantstripid, bdberr);

    return rc;
}

static int bdb_prim_adddta_n_genid_int(bdb_state_type *bdb_state,
                                       tran_type *tran, int dtanum,
                                       void *dtaptr, size_t dtalen, int rrn,
                                       unsigned long long genid, int *bdberr)
{
    DBT dbt_key, dbt_data;
    unsigned long long *genid_dta = NULL;
    int rc;
    int stripe;
    DB *dbp;

    if (bdb_write_preamble(bdb_state, bdberr))
        return -1;

    if ((unsigned)dtanum >= (unsigned)bdb_state->numdtafiles) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    /* This is now allowed - we need it for schema changes that keep
     * genids the same. */
    /*
    if (dtanum == 0)
    {
       *bdberr = BDBERR_BADARGS;
       return 1;
    }
    */

    *bdberr = BDBERR_NOERROR;

    /* synthetic genids should not make it here */
    if (is_genid_synthetic(genid)) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    /* add data to the dta file, with key being rrn (or genid if dtastripe) */
    memset(&dbt_key, 0, sizeof(dbt_key));
    memset(&dbt_data, 0, sizeof(dbt_data));
    dbt_key.flags |= DB_DBT_USERMEM;
    dbt_data.flags |= DB_DBT_USERMEM;

    dbt_key.data = &genid;
    dbt_key.size = sizeof(genid);

    if (genid_dta) {
        dbt_data.data = genid_dta;
        dbt_data.size = dtalen + sizeof(unsigned long long);
    } else {
        dbt_data.data = dtaptr;
        dbt_data.size = dtalen;
    }

#if 0
   /* Now it gets really crazy.  For a schema change on a db that has been
    * blobstriped we need to make sure that blobs inserted before a given
    * time remain in stripe 0. */
   unsigned long long genid_fudged = genid;
   if(dtanum > 0 && genid < bdb_state->blobstripe_convert_genid)
      genid_fudged &= ~0xf;

   dbp = get_dbp_from_genid(bdb_state, dtanum, genid_fudged, &stripe);
#endif

    dbp = get_dbp_from_genid(bdb_state, dtanum, genid, &stripe);

    rc = ll_dta_add(bdb_state, genid, dbp, tran, dtanum, stripe, &dbt_key,
                    &dbt_data, DB_NOOVERWRITE);

    if (genid_dta)
        free(genid_dta);
    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        default:
            logmsg(LOGMSG_ERROR, "bdb_prim_adddta_n_genid_int: put failed %d %s\n",
                    rc, db_strerror(rc));
            *bdberr = BDBERR_MISC;
            break;
        }

        return 0;
    }

    return 0;
}

int bdb_prim_adddta_n_genid(bdb_state_type *bdb_state, tran_type *tran,
                            int dtanum, void *dtaptr, size_t dtalen, int rrn,
                            unsigned long long genid, int *bdberr)
{
    int rc;

    rc = bdb_prim_adddta_n_genid_int(bdb_state, tran, dtanum, dtaptr, dtalen,
                                     rrn, genid, bdberr);

    return rc;
}
