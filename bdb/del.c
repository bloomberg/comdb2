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

#include <build/db.h>
#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"

#include <plbitlib.h> /* for bset/btst */

#include "genid.h"

#include "logmsg.h"

static int bdb_prim_delkey_int(bdb_state_type *bdb_state, tran_type *tran,
                               void *ixdta, int ixnum, int rrn, long long genid,
                               int isnull, int *bdberr)
{
    int rc;
    DBT dbt_key;
    DBC *dbcp;
    unsigned int keydata[3];
    unsigned int *iptr;
    void *pKeyMaxBuf = 0;

    if (bdb_write_preamble(bdb_state, bdberr))
        return -1;

    *bdberr = BDBERR_NOERROR;

    rrn = 2;

    if (bdb_state->ixlen[ixnum] > bdb_state->keymaxsz) {
        logmsg(LOGMSG_FATAL, "calling abort 3\n");
        abort();
    }

    bdb_maybe_use_genid_for_key(bdb_state, &dbt_key, ixdta, ixnum, genid, isnull, &pKeyMaxBuf);

    rc = ll_key_del(bdb_state, tran, ixnum, dbt_key.data, dbt_key.size, rrn, genid, NULL);

    if (pKeyMaxBuf)
        free(pKeyMaxBuf);

    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        case DB_NOTFOUND:
            *bdberr = BDBERR_DELNOTFOUND;
            break;
        default:
            logmsg(LOGMSG_ERROR, "%s:%d ll_key_del rc=%d\n", __FILE__, __LINE__, rc);
            *bdberr = BDBERR_MISC;
            break;
        }
        rc = -1;
    }

    return rc;
}

int bdb_prim_delkey(bdb_state_type *bdb_state, tran_type *tran, void *ixdta,
                    int ixnum, int rrn, int isnull, int *bdberr)
{
    int rc;

    rc = bdb_prim_delkey_int(bdb_state, tran, ixdta, ixnum, rrn, 0, /* genid */
                             isnull, bdberr);

    return rc;
}

int bdb_prim_delkey_genid(bdb_state_type *bdb_state, tran_type *tran,
                          void *ixdta, int ixnum, int rrn,
                          unsigned long long genid, int isnull, int *bdberr)
{
    int rc;

    rc = bdb_prim_delkey_int(bdb_state, tran, ixdta, ixnum, rrn, genid, isnull, bdberr);

    return rc;
}

static int bdb_prim_deallocdta_stripe_int(bdb_state_type *bdb_state,
                                          tran_type *tran,
                                          unsigned long long genid, int *bdberr)
{
    DBT dbt_key;
    int rc;
    int dtafile;
    DB *db;

    if (bdb_write_preamble(bdb_state, bdberr))
        return -1;

    *bdberr = BDBERR_NOERROR;

    dtafile = get_dtafile_from_genid(genid);
    if (dtafile < 0 || dtafile >= bdb_state->attr->dtastripe) {
        *bdberr = BDBERR_DELNOTFOUND;
        return -1;
    }

    db = bdb_state->dbp_data[0][dtafile];
    rc = ll_dta_del(bdb_state, tran, 2 /*rrn*/, genid, db, 0, dtafile, NULL);
    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        case DB_NOTFOUND:
            *bdberr = BDBERR_DELNOTFOUND;
            break;
        default:
            *bdberr = BDBERR_MISC;
            break;
        }
        return -1;
    }

    return 0;
}

static int bdb_prim_deallocdta_n_int(bdb_state_type *bdb_state, tran_type *tran,
                                     int rrn, unsigned long long genid,
                                     int dtanum, int *bdberr)
{
    DBT dbt_key, dbt_data;
    int rc, dtafile;
    DBC *dbcp;
    DB *dbp;

    if (bdb_write_preamble(bdb_state, bdberr))
        return -1;

    if (dtanum < 0 || dtanum > bdb_state->numdtafiles) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    *bdberr = BDBERR_NOERROR;

    /* synthetic genids should not make it here */
    if (is_genid_synthetic(genid)) {
        *bdberr = BDBERR_BADARGS;
        return 1;
    }

    dbp = get_dbp_from_genid(bdb_state, dtanum, genid, &dtafile);
    rc = ll_dta_del(bdb_state, tran, rrn, genid, dbp, dtanum, dtafile, NULL);
    if (rc) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        case DB_NOTFOUND:
            *bdberr = BDBERR_DELNOTFOUND;
            break;
        default:
            logmsg(LOGMSG_ERROR, "%s:%d ll_key_del rc=%d\n", __FILE__, __LINE__, rc);
            *bdberr = BDBERR_MISC;
            break;
        }
        rc = -1;
    }

    return rc;
}

static int bdb_prim_deallocdta_int(bdb_state_type *bdb_state, tran_type *tran,
                                   int rrn, unsigned long long genid,
                                   int *bdberr)
{
    DBT dbt_key, dbt_data;
    int rc;
    int keyval;
    int dtanum;

    /*fprintf(stderr, "bdb_prim_deallocdta_int called\n");*/

    *bdberr = BDBERR_NOERROR;

    if (bdb_write_preamble(bdb_state, bdberr))
        return -1;

    /* delete the dta record */
    rc = bdb_prim_deallocdta_stripe_int(bdb_state, tran, genid, bdberr);

    if (rc == 0) {
        /* delete blobs */
        for (dtanum = 1; dtanum < bdb_state->numdtafiles; dtanum++) {
            rc = bdb_prim_deallocdta_n_int(bdb_state, tran, rrn, genid, dtanum,
                                           bdberr);

            if (rc != 0) {
                if (*bdberr == BDBERR_DELNOTFOUND) {
                    /* only the first data file must have a record.. other ones
                     * can contain null. */
                    *bdberr = BDBERR_NOERROR;
                    rc = 0;
                } else {
                    return -1;
                }
            }
        }
    }

    return rc;
}

int bdb_prim_deallocdta(bdb_state_type *bdb_state, tran_type *tran, int rrn,
                        int *bdberr)
{
    int rc;

    rc = bdb_prim_deallocdta_int(bdb_state, tran, rrn, 0, /* genid */
                                 bdberr);

    return rc;
}

int bdb_prim_deallocdta_genid(bdb_state_type *bdb_state, tran_type *tran,
                              int rrn, unsigned long long genid, int *bdberr)
{
    int rc;

    rc = bdb_prim_deallocdta_int(bdb_state, tran, rrn, genid, bdberr);

    return rc;
}

int bdb_prim_deallocdta_n_genid(bdb_state_type *bdb_state, tran_type *tran,
                                int rrn, unsigned long long genid, int dtanum,
                                int *bdberr)
{
    int rc;

    rc = bdb_prim_deallocdta_n_int(bdb_state, tran, rrn, genid, dtanum, bdberr);

    return rc;
}
