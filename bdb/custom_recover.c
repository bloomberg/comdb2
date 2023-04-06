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

#include <str0.h>

#include <build/db.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"

#include <build/db_int.h>
#include "llog_auto.h"
#include "llog_ext.h"
#include "llog_handlers.h"
#include "dbinc/db_swap.h"

#include <ctrace.h>
#include <logmsg.h>

extern int __db_count_cursors(DB *db);
extern int __dbenv_count_cursors_dbenv(DB_ENV *dbenv);

int handle_undo_upd_ix(DB_ENV *dbenv, u_int32_t rectype,
                       llog_undo_upd_ix_args *updop, DB_LSN *lsn, db_recops op);
int handle_repblob(DB_ENV *dbenv, u_int32_t rectype, llog_repblob_args *repblob,
                   DB_LSN *lsn, db_recops op);
int bdb_blkseq_recover(DB_ENV *dbenv, u_int32_t rectype,
                       llog_blkseq_args *repblob, DB_LSN *lsn, db_recops op);

int bdb_apprec(DB_ENV *dbenv, DBT *log_rec, DB_LSN *lsn, db_recops op)
{
    u_int32_t rectype, *bp;
    void *logp = NULL;
    llog_undo_add_dta_args *add_dta;
    llog_undo_add_ix_args *add_ix;
    llog_ltran_commit_args *commit;
    llog_ltran_start_args *start;
    llog_ltran_comprec_args *comprec;
    llog_scdone_args *scdoneop;
    llog_undo_del_dta_args *del_dta;
    llog_undo_del_ix_args *del_ix;
    llog_undo_upd_dta_args *upd_dta;
    llog_undo_upd_ix_args *upd_ix;
    llog_repblob_args *repblob;

    llog_undo_add_dta_lk_args *add_dta_lk;
    llog_undo_add_ix_lk_args *add_ix_lk;
    llog_undo_del_dta_lk_args *del_dta_lk;
    llog_undo_del_ix_lk_args *del_ix_lk;
    llog_undo_upd_dta_lk_args *upd_dta_lk;
    llog_undo_upd_ix_lk_args *upd_ix_lk;

    llog_blkseq_args *blkseq;

    llog_rowlocks_log_bench_args *rl_log_bench;
    llog_commit_log_bench_args *c_log_bench;

    int rc;
    bdb_state_type *bdb_state;

    bp = log_rec->data;
    bdb_state = (bdb_state_type *)dbenv->app_private;

    LOGCOPY_32(&rectype, bp);
    normalize_rectype(&rectype);

    if (bdb_state->attr->snapisol && !gbl_rowlocks &&
        (op == DB_TXN_FORWARD_ROLL || op == DB_TXN_APPLY)) {
        switch (rectype) {
        case DB_llog_scdone:
        case DB_llog_blkseq:
            break;

        case DB_llog_ltran_commit:
            if (op == DB_TXN_FORWARD_ROLL)
                break;
            else
                op = DB_TXN_SNAPISOL;
            break;

        default:
            rc = 0;
            goto err;
        }
    }

    switch (rectype) {
    case DB_llog_undo_add_dta:
        rc = llog_undo_add_dta_read(dbenv, log_rec->data, &add_dta);
        if (rc)
            goto err;
        logp = add_dta;
        rc = handle_undo_add_dta(dbenv, rectype, add_dta, lsn, op);
        break;

    case DB_llog_undo_add_ix:
        rc = llog_undo_add_ix_read(dbenv, log_rec->data, &add_ix);
        if (rc)
            goto err;
        logp = add_ix;
        rc = handle_undo_add_ix(dbenv, rectype, add_ix, lsn, op);
        break;

    case DB_llog_scdone:
        rc = llog_scdone_read(dbenv, log_rec->data, &scdoneop);
        if (rc)
            goto err;
        logp = scdoneop;
        rc = handle_scdone(dbenv, rectype, scdoneop, lsn, op);
        break;

    case DB_llog_ltran_commit:
        rc = llog_ltran_commit_read(dbenv, log_rec->data, &commit);
        if (rc)
            goto err;
        logp = commit;
        rc = handle_commit(dbenv, rectype, commit, lsn, log_rec->app_data, op);
        break;

    case DB_llog_ltran_start:
        rc = llog_ltran_start_read(dbenv, log_rec->data, &start);
        if (rc)
            goto err;
        logp = start;
        rc = handle_start(dbenv, rectype, start, lsn, op);
        break;

    case DB_llog_ltran_comprec:
        rc = llog_ltran_comprec_read(dbenv, log_rec->data, &comprec);
        if (rc)
            return rc;
        logp = comprec;
        rc = handle_comprec(dbenv, rectype, comprec, lsn, op);
        break;

    case DB_llog_undo_del_dta:
        rc = llog_undo_del_dta_read(dbenv, log_rec->data, &del_dta);
        if (rc)
            return rc;
        logp = del_dta;
        rc = handle_undo_del_dta(dbenv, rectype, del_dta, lsn, op);
        break;

    case DB_llog_undo_del_ix:
        rc = llog_undo_del_ix_read(dbenv, log_rec->data, &del_ix);
        if (rc)
            return rc;
        logp = del_ix;
        rc = handle_undo_del_ix(dbenv, rectype, del_ix, lsn, op);
        break;

    case DB_llog_undo_upd_dta:
        rc = llog_undo_upd_dta_read(dbenv, log_rec->data, &upd_dta);
        if (rc)
            return rc;
        logp = upd_dta;
        rc = handle_undo_upd_dta(dbenv, rectype, upd_dta, lsn, op);
        break;

    case DB_llog_undo_upd_ix:
        rc = llog_undo_upd_ix_read(dbenv, log_rec->data, &upd_ix);
        if (rc)
            return rc;
        logp = upd_ix;
        rc = handle_undo_upd_ix(dbenv, rectype, upd_ix, lsn, op);
        break;

    case DB_llog_repblob:
        rc = llog_repblob_read(dbenv, log_rec->data, &repblob);
        if (rc)
            return rc;
        logp = repblob;
        rc = handle_repblob(dbenv, rectype, repblob, lsn, op);
        break;

    case DB_llog_undo_add_dta_lk:
        rc = llog_undo_add_dta_lk_read(dbenv, log_rec->data, &add_dta_lk);
        if (rc)
            return rc;
        logp = add_dta_lk;
        rc = handle_undo_add_dta_lk(dbenv, rectype, add_dta_lk, lsn, op);
        break;

    case DB_llog_undo_add_ix_lk:
        rc = llog_undo_add_ix_lk_read(dbenv, log_rec->data, &add_ix_lk);
        if (rc)
            return rc;
        logp = add_ix_lk;
        rc = handle_undo_add_ix_lk(dbenv, rectype, add_ix_lk, lsn, op);
        break;

    case DB_llog_undo_del_dta_lk:
        rc = llog_undo_del_dta_lk_read(dbenv, log_rec->data, &del_dta_lk);
        if (rc)
            return rc;
        logp = del_dta_lk;
        rc = handle_undo_del_dta_lk(dbenv, rectype, del_dta_lk, lsn, op);
        break;

    case DB_llog_undo_del_ix_lk:
        rc = llog_undo_del_ix_lk_read(dbenv, log_rec->data, &del_ix_lk);
        if (rc)
            return rc;
        logp = del_ix_lk;
        rc = handle_undo_del_ix_lk(dbenv, rectype, del_ix_lk, lsn, op);
        break;

    case DB_llog_undo_upd_dta_lk:
        rc = llog_undo_upd_dta_lk_read(dbenv, log_rec->data, &upd_dta_lk);
        if (rc)
            return rc;
        logp = upd_dta_lk;
        rc = handle_undo_upd_dta_lk(dbenv, rectype, upd_dta_lk, lsn, op);
        break;

    case DB_llog_undo_upd_ix_lk:
        rc = llog_undo_upd_ix_lk_read(dbenv, log_rec->data, &upd_ix_lk);
        if (rc)
            return rc;
        logp = upd_ix_lk;
        rc = handle_undo_upd_ix_lk(dbenv, rectype, upd_ix_lk, lsn, op);
        break;

    case DB_llog_blkseq:
        rc = llog_blkseq_read(dbenv, log_rec->data, &blkseq);
        if (rc)
            return rc;
        logp = blkseq;
        rc = bdb_blkseq_recover(dbenv, rectype, blkseq, lsn, op);
        break;

    case DB_llog_rowlocks_log_bench:
        rc = llog_rowlocks_log_bench_read(dbenv, log_rec->data, &rl_log_bench);
        if (rc)
            return rc;
        logp = rl_log_bench;
        rc = handle_rowlocks_log_bench(dbenv, rectype, rl_log_bench, lsn, op);
        break;

    case DB_llog_commit_log_bench:
        rc = llog_commit_log_bench_read(dbenv, log_rec->data, &c_log_bench);
        if (rc)
            return rc;
        logp = c_log_bench;
        rc = handle_commit_log_bench(dbenv, rectype, c_log_bench, lsn, op);
        break;

    default:
        __db_err(dbenv, "unknown record type %d in app recovery\n", rectype);
        rc = EINVAL;
        goto err;
    }

err:
    if (rc && rc != DB_LOCK_DEADLOCK)
        logmsg(LOGMSG_ERROR, "at %u:%u rc %d\n", lsn->file, lsn->offset, rc);

    if (logp)
        free(logp);

    return rc;
}
