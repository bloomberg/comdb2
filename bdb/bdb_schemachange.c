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

#include <db.h>
#include <epochlib.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"

#include "llog_auto.h"
#include "llog_int.h"
#include "llog_handlers.h"

#include <plbitlib.h> /* for bset/btst */
#include <ctrace.h>
#include <alloca.h>
#include "bdb_schemachange.h"
#include "logmsg.h"

/* bdb routines to support schema change */

int bdb_scdone_int(bdb_state_type *bdb_state_in, DB_TXN *txnid,
                   const char table[], int fastinit)
{
    int rc;
    bdb_state_type *bdb_state;

    if (bdb_state_in == NULL)
        return 0;

    if (bdb_state_in->parent)
        bdb_state = bdb_state_in->parent;
    else
        bdb_state = bdb_state_in;

    if (!bdb_state->passed_dbenv_open)
        return 0;

    if (!bdb_state->callback->scdone_rtn) {
        logmsg(LOGMSG_ERROR, "bdb_scdone_int: no scdone callback\n");
        return -1;
    }

    /* TODO fail gracefully now that inline? */
    /* reload the changed table (if necesary) and update the schemas in memory*/
    if ((rc = bdb_state->callback->scdone_rtn(bdb_state_in, table, fastinit))) {
        if (rc == BDBERR_DEADLOCK)
            rc = DB_LOCK_DEADLOCK;
        logmsg(LOGMSG_ERROR, "bdb_scdone_int: callback failed\n");
        return rc;
    }

    return 0;
}

int handle_scdone(DB_ENV *dbenv, u_int32_t rectype, llog_scdone_args *scdoneop,
                  DB_LSN *lsn, db_recops op)
{
    int rc = 0;
    const char *table = (const char *)scdoneop->table.data;

    uint32_t type;
    assert(sizeof(type) == scdoneop->fastinit.size);
    memcpy(&type, scdoneop->fastinit.data, sizeof(type));
    scdone_t sctype = ntohl(type);

    switch (op) {
    /* for an UNDO record, berkeley expects us to set prev_lsn */
    case DB_TXN_FORWARD_ROLL:
    case DB_TXN_BACKWARD_ROLL:
    case DB_TXN_ABORT:
        *lsn = scdoneop->prev_lsn;
        rc = 0;
        break;

    /* make a copy of the row that's being deleted. */
    case DB_TXN_APPLY:
        rc = bdb_scdone_int(dbenv->app_private, scdoneop->txnid, table, sctype);
        break;

    case DB_TXN_SNAPISOL:
    case DB_TXN_PRINT:
        /* keep format similar to berkeley - except for the raw data -
           dump that in more readable format */
        printf("[%lu][%lu]scdone: rec: %lu txnid %lx prevlsn[%lu][%lu]\n",
               (u_long)lsn->file, (u_long)lsn->offset, (u_long)rectype,
               (u_long)scdoneop->txnid->txnid, (u_long)scdoneop->prev_lsn.file,
               (u_long)scdoneop->prev_lsn.offset);
        printf("\ttype: %d", sctype);
        printf("\ttable: %.*s\n", scdoneop->table.size, table);
        printf("\n");
        break;

    default:
        __db_err(dbenv, "unknown op type %d in handle_scdone\n", (int)op);
        break;
    }
    return rc;
}

// Must be done a transaction by itself so this func creates a trans internally
static int do_llog_int(bdb_state_type *ch_bdb_state, DBT *tbl, DBT *type,
                       int wait, int *bdberr)
{
    int retries = 0;
    bdb_state_type *p_bdb_state = ch_bdb_state;
    if (ch_bdb_state->parent)
        p_bdb_state = ch_bdb_state->parent;

retry:
    *bdberr = BDBERR_NOERROR;
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    tran_type *ltran = bdb_tran_begin_logical(p_bdb_state, 0, bdberr);
    if (ltran == NULL) {
        if (*bdberr == BDBERR_DEADLOCK)
            goto retry;
        logmsg(LOGMSG_ERROR, "%s: bdb_tran_begin bdberr:%d\n", __func__, *bdberr);
        return -1;
    }
    ltran->single_physical_transaction = 1;
    ltran->get_schema_lock = 1;

    int rc;
    tran_type *tran;
    if ((rc = get_physical_transaction(p_bdb_state, ltran, &tran)) != 0) {
        if (bdb_tran_abort(p_bdb_state, ltran, bdberr) != 0)
            abort();
        logmsg(LOGMSG_ERROR, "%s: get_physical_transaction: %d\n", __func__, rc);
        return -1;
    }

    uint32_t sctype = ntohl(*((uint32_t *)type->data));
    if ((sctype == alter || sctype == fastinit) &&
        (strncmp(ch_bdb_state->name, "sqlite_stat",
                 sizeof("sqlite_stat") - 1) != 0))
        bdb_lock_table_write(ch_bdb_state, tran);

    DB_LSN lsn;
    rc = llog_scdone_log(p_bdb_state->dbenv, tran->tid, &lsn, 0, tbl, type);
    if (rc) {
        bdb_tran_abort(p_bdb_state, tran, bdberr);
        *bdberr = BDBERR_MISC;
        return -1;
    }
    uint64_t transize;
    seqnum_type seqnum;
    rc = bdb_tran_commit_with_seqnum_size(p_bdb_state, ltran, &seqnum,
                                          &transize, bdberr);
    if (rc || *bdberr != BDBERR_NOERROR) {
        if (*bdberr == BDBERR_DEADLOCK)
            goto retry;
    }
    if (wait) {
        rc = bdb_wait_for_seqnum_from_all(p_bdb_state, &seqnum);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, 
                    "%s: bdb_wait_for_seqnum_from_all rc: %d bdberr:%d\n",
                    __func__, rc, *bdberr);
        }
    }
    return rc;
}

static int do_llog(bdb_state_type *bdb_state, scdone_t sctype, char *tbl,
                   int wait, int *bdberr)
{
    DBT *dtbl = NULL;
    if (tbl) {
        dtbl = alloca(sizeof(DBT));
        bzero(dtbl, sizeof(DBT));
        dtbl->data = tbl;
        dtbl->size = strlen(tbl) + 1;
    }

    DBT dtype = {0};
    uint32_t type = htonl(sctype);
    dtype.data = &type;
    dtype.size = sizeof(type);

    return do_llog_int(bdb_state, dtbl, &dtype, wait, bdberr);
}

int llog_scdone_tran(bdb_state_type *p_bdb_state, char *name, scdone_t type,
                     tran_type *tran, int *bdberr)
{
    int rc = 0;
    DBT *dtbl = NULL;
    DBT dtype = {0};
    uint32_t sctype = htonl(type);
    DB_LSN lsn;

    ++gbl_dbopen_gen;
    if (name) {
        dtbl = alloca(sizeof(DBT));
        bzero(dtbl, sizeof(DBT));
        dtbl->data = name;
        dtbl->size = strlen(name) + 1;
    }

    dtype.data = &sctype;
    dtype.size = sizeof(sctype);

    rc = llog_scdone_log(p_bdb_state->dbenv, tran->tid, &lsn, 0, dtbl, &dtype);
    if (rc) {
        *bdberr = BDBERR_MISC;
        return -1;
    }
    return rc;
}

int bdb_llog_scdone_tran(bdb_state_type *bdb_state, scdone_t type,
                         tran_type *tran, int *bdberr)
{
    bdb_state_type *p_bdb_state = bdb_state;
    if (bdb_state->parent) p_bdb_state = bdb_state->parent;

    return llog_scdone_tran(p_bdb_state, bdb_state->name, type, tran, bdberr);
}

int bdb_llog_sequences_tran(bdb_state_type *bdb_state, char *name,
                            scdone_t type, tran_type *tran, int *bdberr)
{
    return llog_scdone_tran(bdb_state, name, type, tran, bdberr);
}

int bdb_llog_scdone(bdb_state_type *bdb_state, scdone_t type, int wait,
                    int *bdberr)
{
    ++gbl_dbopen_gen;
    return do_llog(bdb_state, type, bdb_state->name, wait, bdberr);
}

int bdb_llog_analyze(bdb_state_type *bdb_state, int wait, int *bdberr)
{
    ++gbl_analyze_gen;
    return do_llog(bdb_state, sc_analyze, NULL, wait, bdberr);
}

int bdb_llog_views(bdb_state_type *bdb_state, char *name, int wait, int *bdberr)
{
    ++gbl_views_gen;
    return do_llog(bdb_state, views, name, wait, bdberr);
}

int bdb_llog_luareload(bdb_state_type *bdb_state, int wait, int *bdberr)
{
    return do_llog(bdb_state, luareload, NULL, wait, bdberr);
}

int bdb_llog_luafunc(bdb_state_type *bdb_state, scdone_t type, int wait,
                     int *bdberr)
{
    return do_llog(bdb_state, type, bdb_state->name, wait, bdberr);
}

extern int gbl_rowlocks;

int bdb_llog_rowlocks(bdb_state_type *bdb_state, scdone_t type, int *bdberr)
{
    char *str;
    int rc;

    assert(type == rowlocks_on || type == rowlocks_off);

    if (type == rowlocks_on) {
        assert(!gbl_rowlocks);
        str = "enable_rowlocks";
    } else {
        assert(gbl_rowlocks);
        str = "disable_rowlocks";
    }

    if (type == rowlocks_on)
        gbl_rowlocks = 1;
    else
        gbl_rowlocks = 0;
    rc = do_llog(bdb_state, type, NULL, 0, bdberr);
    return rc;
}

int bdb_llog_genid_format(bdb_state_type *bdb_state, scdone_t type, int *bdberr)
{
    char *str;
    int rc, format;

    assert(type == genid48_enable || type == genid48_disable);

    format = bdb_genid_format(bdb_state);
    if (type == genid48_enable) {
        assert(format == LLMETA_GENID_ORIGINAL);
        format = LLMETA_GENID_48BIT;
        str = "enable_genid48";
    } else {
        assert(format == LLMETA_GENID_48BIT);
        format = LLMETA_GENID_ORIGINAL;
        str = "disable_genid48";
    }

    bdb_genid_set_format(bdb_state, format);
    rc = do_llog(bdb_state, type, NULL, 0, bdberr);
    return rc;
}

int bdb_reload_rowlocks(bdb_state_type *bdb_state, scdone_t type, int *bdberr)
{
    char *str;

    assert(type == rowlocks_on || type == rowlocks_on_master_only ||
           type == rowlocks_off);

    if (type == rowlocks_on) {
        str = "enable_rowlocks";
    } else {
        assert(gbl_rowlocks);
        str = "disable_rowlocks";
    }

    BDB_WRITELOCK(str);
    if (type == rowlocks_on)
        gbl_rowlocks = 1;
    else
        gbl_rowlocks = 0;
    BDB_RELLOCK();
    return 0;
}

