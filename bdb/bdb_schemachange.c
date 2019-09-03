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
#include <epochlib.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"
#include "locks_wrap.h"

#include "llog_auto.h"
#include "llog_ext.h"
#include "llog_handlers.h"

#include <ctrace.h>
#include <alloca.h>
#include "bdb_schemachange.h"
#include "logmsg.h"
#include "comdb2_atomic.h"

extern int sc_ready(void);

/* bdb routines to support schema change */

static int bdb_scdone_int(bdb_state_type *bdb_state_in, DB_TXN *txnid,
                          const char table[], const char *newtable,
                          int fastinit)
{
    int rc;
    bdb_state_type *bdb_state;
    char *db_newtable = NULL;

    if (newtable && newtable[0])
        db_newtable = strdup(newtable);

    if (bdb_state_in == NULL)
        return 0;

    if (bdb_state_in->parent)
        bdb_state = bdb_state_in->parent;
    else
        bdb_state = bdb_state_in;

    if (!sc_ready()) {
        logmsg(LOGMSG_INFO, "Skipping bdb_scdone, files not opened yet!\n");
        return 0;
    }

    if (!bdb_state->callback->scdone_rtn) {
        logmsg(LOGMSG_ERROR, "%s: no scdone callback\n", __func__);
        return -1;
    }

    /* TODO fail gracefully now that inline? */
    /* reload the changed table (if necesary) and update the schemas in memory*/
    if ((rc = bdb_state->callback->scdone_rtn(bdb_state_in, table, db_newtable,
                                              fastinit))) {
        if (rc == BDBERR_DEADLOCK)
            rc = DB_LOCK_DEADLOCK;
        logmsg(LOGMSG_ERROR, "%s: callback failed\n", __func__);
        return rc;
    }

    return 0;
}

int handle_scdone(DB_ENV *dbenv, u_int32_t rectype, llog_scdone_args *scdoneop,
                  DB_LSN *lsn, db_recops op)
{
    int rc = 0;
    const char *table = (const char *)scdoneop->table.data;
    const char *newtable = NULL;

    uint32_t type;
    assert(sizeof(type) == scdoneop->fastinit.size);
    memcpy(&type, scdoneop->fastinit.data, sizeof(type));
    scdone_t sctype = ntohl(type);

    if (sctype == rename_table) {
        assert(strlen(table) + 1 < scdoneop->table.size);
        newtable = &table[strlen(table) + 1];
    }

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
        rc = bdb_scdone_int(dbenv->app_private, scdoneop->txnid, table,
                            newtable, sctype);
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
    if ((rc = get_physical_transaction(p_bdb_state, ltran, &tran, 0)) != 0) {
        if (bdb_tran_abort(p_bdb_state, ltran, bdberr) != 0)
            abort();
        logmsg(LOGMSG_ERROR, "%s: get_physical_transaction: %d\n", __func__, rc);
        return -1;
    }

    uint32_t sctype = ntohl(*((uint32_t *)type->data));
    if ((sctype == alter || sctype == fastinit || sctype == bulkimport) &&
        (strncmp(ch_bdb_state->name, "sqlite_stat",
                 sizeof("sqlite_stat") - 1) != 0))
        bdb_lock_table_write(ch_bdb_state, tran);
    /* analyze does NOT need schema_lk */
    if (sctype == sc_analyze)
        ltran->get_schema_lock = 0;

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
                   int wait, const char *origtable, int *bdberr)
{
    DBT *dtbl = NULL;
    if (tbl) {
        dtbl = alloca(sizeof(DBT));
        bzero(dtbl, sizeof(DBT));
        dtbl->data = tbl;
        dtbl->size = strlen(tbl) + 1;
        if (sctype == rename_table) {
            assert(origtable);
            int origlen = strlen(origtable) + 1;
            int len = dtbl->size + origlen;
            char *mashup = alloca(len);
            memcpy(mashup, origtable, origlen);
            memcpy(mashup + origlen, dtbl->data, dtbl->size);
            dtbl->data = mashup;
            dtbl->size = len;
        }
    }

    DBT dtype = {0};
    uint32_t type = htonl(sctype);
    dtype.data = &type;
    dtype.size = sizeof(type);

    return do_llog_int(bdb_state, dtbl, &dtype, wait, bdberr);
}

int bdb_llog_scdone_tran(bdb_state_type *bdb_state, scdone_t type,
                         tran_type *tran, const char *origtable, int *bdberr)
{
    int rc = 0;
    DBT *dtbl = NULL;
    DBT dtype = {0};
    uint32_t sctype = htonl(type);
    bdb_state_type *p_bdb_state = bdb_state;
    DB_LSN lsn;

    ++gbl_dbopen_gen;
    if (bdb_state->name) {
        dtbl = alloca(sizeof(DBT));
        bzero(dtbl, sizeof(DBT));
        dtbl->data = bdb_state->name;
        dtbl->size = strlen(bdb_state->name) + 1;
        if (type == rename_table) {
            assert(origtable);
            int origlen = strlen(origtable) + 1;
            int len = dtbl->size + origlen;
            char *mashup = alloca(len);
            memcpy(mashup, origtable, origlen);
            memcpy(mashup + origlen, dtbl->data, dtbl->size);
            dtbl->data = mashup;
            dtbl->size = len;
        }
    }

    dtype.data = &sctype;
    dtype.size = sizeof(sctype);

    if (bdb_state->parent) p_bdb_state = bdb_state->parent;

#if 0 /* finalize_schema_change already got the write lock? */
    if ((type == alter || type == fastinit) &&
        (strncmp(bdb_state->name, "sqlite_stat",
                 sizeof("sqlite_stat") - 1) != 0))
        bdb_lock_table_write(bdb_state, tran);
#endif

    rc = llog_scdone_log(p_bdb_state->dbenv, tran->tid, &lsn, 0, dtbl, &dtype);
    if (rc) {
        *bdberr = BDBERR_MISC;
        return -1;
    }
    return rc;
}

int bdb_llog_scdone(bdb_state_type *bdb_state, scdone_t type, int wait,
                    int *bdberr)
{
    ++gbl_dbopen_gen;
    return do_llog(bdb_state, type, bdb_state->name, wait, NULL, bdberr);
}

int bdb_llog_scdone_origname(bdb_state_type *bdb_state, scdone_t type, int wait,
                             const char *origtable, int *bdberr)
{
    ++gbl_dbopen_gen;
    return do_llog(bdb_state, type, bdb_state->name, wait, origtable, bdberr);
}

int bdb_llog_analyze(bdb_state_type *bdb_state, int wait, int *bdberr)
{
    ATOMIC_ADD32(gbl_analyze_gen, 1);
    return do_llog(bdb_state, sc_analyze, NULL, wait, NULL, bdberr);
}

int bdb_llog_views(bdb_state_type *bdb_state, char *name, int wait, int *bdberr)
{
    ++gbl_views_gen;
    return do_llog(bdb_state, views, name, wait, NULL, bdberr);
}

int bdb_llog_luareload(bdb_state_type *bdb_state, int wait, int *bdberr)
{
    return do_llog(bdb_state, luareload, NULL, wait, NULL, bdberr);
}

int bdb_llog_luafunc(bdb_state_type *bdb_state, scdone_t type, int wait,
                     int *bdberr)
{
    return do_llog(bdb_state, type, bdb_state->name, wait, NULL, bdberr);
}

extern int gbl_rowlocks;

int bdb_llog_rowlocks(bdb_state_type *bdb_state, scdone_t type, int *bdberr)
{
    // char *str;
    int rc;

    assert(type == rowlocks_on || type == rowlocks_off);

    if (type == rowlocks_on) {
        assert(!gbl_rowlocks);
        // str = "enable_rowlocks";
    } else {
        assert(gbl_rowlocks);
        // str = "disable_rowlocks";
    }

    if (type == rowlocks_on)
        gbl_rowlocks = 1;
    else
        gbl_rowlocks = 0;
    rc = do_llog(bdb_state, type, NULL, 0, NULL, bdberr);
    return rc;
}

int bdb_llog_genid_format(bdb_state_type *bdb_state, scdone_t type, int *bdberr)
{
    // char *str;
    int rc, format;

    assert(type == genid48_enable || type == genid48_disable);

    format = bdb_genid_format(bdb_state);
    if (type == genid48_enable) {
        assert(format == LLMETA_GENID_ORIGINAL);
        format = LLMETA_GENID_48BIT;
        // str = "enable_genid48";
    } else {
        assert(format == LLMETA_GENID_48BIT);
        format = LLMETA_GENID_ORIGINAL;
        // str = "disable_genid48";
    }

    bdb_genid_set_format(bdb_state, format);
    rc = do_llog(bdb_state, type, NULL, 0, NULL, bdberr);
    return rc;
}

int bdb_reload_rowlocks(bdb_state_type *bdb_state, scdone_t type, int *bdberr)
{
    assert(type == rowlocks_on || type == rowlocks_on_master_only ||
           type == rowlocks_off);

    if (type != rowlocks_on) {
        assert(gbl_rowlocks);
    }

    if (type == rowlocks_on)
        gbl_rowlocks = 1;
    else
        gbl_rowlocks = 0;
    return 0;
}

int bdb_set_logical_live_sc(bdb_state_type *bdb_state, int lock)
{
    int rc = 0, bdberr = 0;
    tran_type *trans = NULL;

    if (bdb_state == NULL) {
        logmsg(LOGMSG_ERROR, "%s(NULL)!!\n", __func__);
        return -1;
    }

    if (lock) {
        trans = bdb_tran_begin(bdb_state, NULL, &bdberr);
        if (!trans) {
            logmsg(LOGMSG_ERROR, "%s: failed to get transaction, bdberr=%d\n",
                   __func__, bdberr);
            return -1;
        }
        rc = bdb_lock_table_write(bdb_state, trans);
        if (rc) {
            bdb_tran_abort(bdb_state, trans, &bdberr);
            logmsg(LOGMSG_ERROR, "%s: failed to lock table, rc=%d\n", __func__,
                   rc);
            return -1;
        }
        // no one can write to this table at this point
    }

    bdb_state->logical_live_sc = 1;
    Pthread_mutex_init(&(bdb_state->sc_redo_lk), NULL);
    Pthread_cond_init(&(bdb_state->sc_redo_wait), NULL);
    listc_init(&bdb_state->sc_redo_list, offsetof(struct sc_redo_lsn, lnk));

    if (lock) {
        rc = bdb_tran_abort(bdb_state, trans, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to abort trans, rc=%d, bdberr=%d\n", __func__,
                   rc, bdberr);
            return -1;
        }
    }

    return 0;
}

int bdb_clear_logical_live_sc(bdb_state_type *bdb_state, int lock)
{
    int rc = 0, bdberr = 0;
    tran_type *trans = NULL;
    struct sc_redo_lsn *redo;

    if (bdb_state == NULL) {
        logmsg(LOGMSG_ERROR, "%s(NULL)!!\n", __func__);
        return -1;
    }

    if (bdb_state->logical_live_sc == 0)
        return 0;

    if (lock) {
        trans = bdb_tran_begin(bdb_state, NULL, &bdberr);
        if (!trans) {
            logmsg(LOGMSG_ERROR, "%s: failed to get transaction, bdberr=%d\n",
                   __func__, bdberr);
            return -1;
        }
        rc = bdb_lock_table_write(bdb_state, trans);
        if (rc) {
            bdb_tran_abort(bdb_state, trans, &bdberr);
            logmsg(LOGMSG_ERROR, "%s: failed to lock table, rc=%d\n", __func__,
                   rc);
            return -1;
        }
        // no one can write to this table at this point
    }

    bdb_state->logical_live_sc = 0;
    Pthread_mutex_destroy(&(bdb_state->sc_redo_lk));
    Pthread_cond_destroy(&(bdb_state->sc_redo_wait));
    while ((redo = listc_rtl(&bdb_state->sc_redo_list)) != NULL) {
        free(redo);
    }

    if (lock) {
        rc = bdb_tran_abort(bdb_state, trans, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to abort trans, rc=%d, bdberr=%d\n", __func__,
                   rc, bdberr);
            return -1;
        }
    }

    return 0;
}
