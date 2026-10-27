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

#include <inttypes.h>
#include <stdlib.h>

#include <build/db.h>

#include "bdb_int.h"
#include "comdb2_trace.h"
#include "epochlib.h"

#include "llog_auto.h"
#include "llog_ext.h"
#include "llog_handlers.h"

struct comdb2_trace_hooks *gbl_trace_hooks = NULL;

/* Keep off until the whole cluster is upgraded, and turn off before downgrading
 * any node: an older binary aborts on the unknown rectype. */
int gbl_log_trace = 0;

int bdb_llog_trace_tran(bdb_state_type *bdb_state, tran_type *tran, const void *payload, int len, int *bdberr)
{
    DBT dpayload = {0};
    DB_LSN lsn;

    *bdberr = BDBERR_NOERROR;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    dpayload.data = (void *)payload;
    dpayload.size = len;

    if (llog_trace_log(bdb_state->dbenv, tran->tid, &lsn, 0, &dpayload)) {
        *bdberr = BDBERR_MISC;
        return -1;
    }
    return 0;
}

struct trace_rep {
    void *h;
    int startms;
};

void *bdb_trace_from_logrec(DB_ENV *dbenv, void *logrec)
{
    struct comdb2_trace_hooks *hooks = gbl_trace_hooks;
    llog_trace_args *argp = NULL;
    struct trace_rep *r;
    void *h;

    if (hooks == NULL || llog_trace_read(dbenv, logrec, &argp) != 0)
        return NULL;
    h = hooks->rep_start(argp->payload.data, argp->payload.size);
    free(argp);
    if (h == NULL)
        return NULL;
    if ((r = malloc(sizeof(*r))) == NULL) {
        hooks->release(h);
        return NULL;
    }
    r->h = h;
    r->startms = comdb2_time_epochms();
    return r;
}

void bdb_trace_rep_free(void *p)
{
    struct trace_rep *r = p;

    if (r == NULL)
        return;
    gbl_trace_hooks->release(r->h);
    free(r);
}

void bdb_trace_rep_done(void *p, int rc, uint64_t pagein, uint64_t pagein_io)
{
    struct trace_rep *r = p;

    if (r == NULL)
        return;
    gbl_trace_hooks->rep_end(r->h, comdb2_time_epochms() - r->startms, pagein, pagein_io, rc);
    bdb_trace_rep_free(r);
}

int handle_trace(DB_ENV *dbenv, u_int32_t rectype, llog_trace_args *trop, DB_LSN *lsn, db_recops op)
{
    switch (op) {
    /* for an UNDO record, berkeley expects us to set prev_lsn */
    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        *lsn = trop->prev_lsn;
        break;

    case DB_TXN_APPLY:
    case DB_TXN_SNAPISOL:
        break;

    case DB_TXN_PRINT:
        printf("[%lu][%lu]trace: rec: %lu txnid %lx prevlsn[%lu][%lu] utxnid %" PRIx64 " prevcksum %08lx\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype, (u_long)trop->txnid->txnid,
               (u_long)trop->prev_lsn.file, (u_long)trop->prev_lsn.offset, trop->txnid->utxnid,
               (u_long)trop->prev_cksum);
        printf("\tpayload: ");
        for (unsigned int i = 0; i < trop->payload.size; i++)
            printf("%02x", ((unsigned char *)trop->payload.data)[i]);
        printf("\n");
        printf("\n");
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_trace\n", (int)op);
        break;
    }
    return 0;
}
