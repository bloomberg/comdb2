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

#include <build/db.h>
#include "bdb_int.h"
#include "bdb_osqlbkfill.h"

struct bfillhndl {
    DB_LSN *lsns;
    int n_lsns;
    int crt_lsn;
};

/**
 * Provide a list of "current" lsn-s for the pending
 * rowlock transactions.  It keeps the commit process
 * waiting while this list is created (the lsn for
 * current logical transactions cannot advance )
 * The handle is NULL if no lsn-s are available (no
 * logical transactions for which there is a committed
 * physical transaction).
 * If error, return -1 and set bdberr
 *
 * If epoch>0, create a backfill required to generate a snapshot
 *
 */
int bdb_osqlbkfill_create(bdb_state_type *bdb_state, bfillhndl_t **bhndl,
                          int *bdberr, int epoch, int file, int offset,
                          tran_type *shadow_tran)
{
    bfillhndl_t *ret = NULL;
    int rc = 0;

    *bdberr = 0;
    ret = (bfillhndl_t *)calloc(1, sizeof(bfillhndl_t));
    if (!ret) {
        *bdberr = BDBERR_MALLOC;
        return -1;
    }

    ret->lsns = NULL;

    if (epoch > 0 || file > 0) {
        /* this looks up at commit lsn in the log */
        rc = get_committed_lsns(bdb_state->dbenv, &ret->lsns, &ret->n_lsns,
                                epoch, file, offset);
    } else {
        /* this looks up for logical transactions */
        rc = bdb_get_active_logical_transaction_lsns(
            bdb_state, &ret->lsns, &ret->n_lsns, bdberr, shadow_tran);
    }
    if (rc) {
        if (ret->lsns)
            free(ret->lsns);
        free(ret);
        if (*bdberr == 0)
            *bdberr = rc;
        return -1;
    }

    if (ret->n_lsns == 0) {
        free(ret->lsns);
        free(ret);
        return 0;
    }

    *bhndl = ret;
    return 0;
}

/**
 * Retrieve the first lsn for backfill
 * in the provided handle;
 * It is assumed that if there is a handle, there is a first lsn
 * Returns -1 if error and sets bdberr
 *
 */
int bdb_osqlbkfill_firstlsn(bfillhndl_t *bfillhndl, DB_LSN *lsn, int *bdberr)
{
    *bdberr = 0;
    if (bfillhndl->n_lsns > 0) {
        bfillhndl->crt_lsn = 0;
        *lsn = bfillhndl->lsns[bfillhndl->crt_lsn];
        return 0;
    }

    *bdberr = BDBERR_BUG_KILLME;
    return -1;
}

/**
 * Retrieve the next lsn for backfill
 * Return 1 if there are no more lsn-s for the current handle
 * Returns -1 if error and sets bdberr
 *
 */
int bdb_osqlbkfill_next_lsn(bfillhndl_t *bfillhndl, DB_LSN *lsn, int *bdberr)
{
    *bdberr = 0;
    if (bfillhndl->crt_lsn < bfillhndl->n_lsns - 1) {
        bfillhndl->crt_lsn++;
        *lsn = bfillhndl->lsns[bfillhndl->crt_lsn];
        return 0;
    }
    return 1; /*eof*/
}

int bdb_osqlbkfill_lastlsn(bfillhndl_t *bfillhndl, DB_LSN *lsn, int *bdberr) 
{
    *bdberr = 0;
    if (bfillhndl->n_lsns) {
        *lsn = bfillhndl->lsns[bfillhndl->n_lsns - 1];
        return 0;
    }
    *bdberr = BDBERR_BUG_KILLME;
    return -1;
}

/**
 * Close handle and free the lsn entries
 * Returns -1 if error and sets bdberr
 *
 */
int bdb_osqlbkfill_close(bfillhndl_t *bfillhndl, int *bdberr)
{
    *bdberr = 0;
    if (bfillhndl->lsns)
        free(bfillhndl->lsns);
    free(bfillhndl);
    return 0;
}
