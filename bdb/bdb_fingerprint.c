/*
   Copyright 2026 Bloomberg Finance L.P.

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

/* Master-side write half of fingerprint stats on the replication stream: put
 * the statement's fingerprint in the log so replicants can attribute the
 * page-ins they do applying it (read back in berkdb/rep/rep_record.c). */

#include <stdlib.h>
#include <string.h>

#include <build/db.h>

#include "bdb_int.h"

#include "llog_auto.h"
#include "llog_ext.h"
#include "llog_handlers.h"

#include "logmsg.h"

/* Keep off until the whole cluster is upgraded, and turn off before downgrading
 * any node: an older binary aborts on the unknown rectype. */
int gbl_log_fingerprint = 0;

/* Log the fingerprint into the block processor's txn, once per statement (the
 * osql stream already collapses runs of rows), not once per row. */
int bdb_llog_fingerprint_tran(bdb_state_type *bdb_state, tran_type *tran, const unsigned char *fingerprint, int *bdberr)
{
    DBT dfp = {0};
    DB_LSN lsn;
    int rc;

    *bdberr = BDBERR_NOERROR;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    dfp.data = (void *)fingerprint;
    dfp.size = BDB_FINGERPRINTSZ;

    rc = llog_fingerprint_log(bdb_state->dbenv, tran->tid, &lsn, 0, &dfp);
    if (rc) {
        *bdberr = BDBERR_MISC;
        return -1;
    }
    return 0;
}

/* Read the payload back out for berkdb, which must not know the llog layout.
 * fplen is checked, not assumed: the caller sizes that buffer itself. */
int bdb_fingerprint_from_logrec(DB_ENV *dbenv, void *logrec, unsigned char *fingerprint, size_t fplen)
{
    llog_fingerprint_args *argp = NULL;
    int found = 0;

    if (fplen != BDB_FINGERPRINTSZ)
        return 0;

    if (llog_fingerprint_read(dbenv, logrec, &argp) != 0)
        return 0;

    if (argp->fingerprint.size == BDB_FINGERPRINTSZ) {
        memcpy(fingerprint, argp->fingerprint.data, BDB_FINGERPRINTSZ);
        found = 1;
    }

    free(argp);
    return found;
}

/* No physical state to change, so every op is a no-op beyond prev_lsn. Arming
 * happens in rep_record.c, before parallel rep reorders the records. */
int handle_fingerprint(DB_ENV *dbenv, u_int32_t rectype, llog_fingerprint_args *fpop, DB_LSN *lsn, db_recops op)
{
    switch (op) {
    /* for an UNDO record, berkeley expects us to set prev_lsn */
    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        *lsn = fpop->prev_lsn;
        break;

    case DB_TXN_APPLY:
    case DB_TXN_SNAPISOL:
        break;

    case DB_TXN_PRINT:
        printf("[%lu][%lu]fingerprint: rec: %lu txnid %lx prevlsn[%lu][%lu]\n", (u_long)lsn->file, (u_long)lsn->offset,
               (u_long)rectype, (u_long)fpop->txnid->txnid, (u_long)fpop->prev_lsn.file, (u_long)fpop->prev_lsn.offset);
        printf("\tfingerprint: ");
        for (unsigned int i = 0; i < fpop->fingerprint.size; i++)
            printf("%02x", ((unsigned char *)fpop->fingerprint.data)[i]);
        printf("\n");
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_fingerprint\n", (int)op);
        break;
    }
    return 0;
}
