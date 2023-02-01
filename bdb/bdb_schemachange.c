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
extern int gbl_debug_systable_locks;
extern int32_t gbl_rep_lockid;

/* bdb routines to support schema change */

static const char *const bdb_scdone_type_names[] = {
    "invalid",                 // -1
    "alter",                   //  0
    "fastinit / add",          //  1
    "drop",                    //  2
    "bulkimport",              //  3
    "setcompr",                //  4
    "luareload",               //  5
    "sc_analyze",              //  6
    "bthash",                  //  7
    "rowlocks_on",             //  8
    "rowlocks_on_master_only", //  9
    "rowlocks_off",            // 10
    "views",                   // 11
    "llmeta_queue_add",        // 12
    "llmeta_queue_alter",      // 13
    "llmeta_queue_drop",       // 14
    "genid48_enable",          // 15
    "genid48_disable",         // 16
    "lua_sfunc",               // 17
    "lua_afunc",               // 18
    "rename_table",            // 19
    "change_stripe",           // 20
    "user_view",               // 21
    "add_queue_file",          // 22
    "del_queue_file",          // 23
    "alias_table"              // 24
};

const char *bdb_get_scdone_str(scdone_t type)
{
    int maxIndex = sizeof(bdb_scdone_type_names) /
                   sizeof(bdb_scdone_type_names[0]);
    static __thread char buf[100];
    if (type >= invalid && type <= del_queue_file) {
        int index = ((int)type) + 1; // -1 ==> 0
        if (index >= 0 && index <= maxIndex) {
            snprintf(buf, sizeof(buf), "\"%s\" (%d)",
                     bdb_scdone_type_names[index], (int)type);
        } else {
            snprintf(buf, sizeof(buf), "BAD_INDEX %d (%d)",
                     index, (int)type);
        }
    } else {
        snprintf(buf, sizeof(buf), "UNKNOWN (%d)", (int)type);
    }
    return buf;
}

static int32_t dbopen_gen = 0;

int32_t bdb_get_dbopen_gen(void)
{
    return ATOMIC_LOAD32(dbopen_gen);
}

int bdb_bump_dbopen_gen(const char *type, const char *message,
                        const char *funcName, const char *fileName, int lineNo)
{
    int rc = ATOMIC_ADD32(dbopen_gen, 1);
    if (message == NULL) message = "<null>";
    logmsg(LOGMSG_WARN,
           "DBOPEN_GEN is now %d (for type %s via %s, %s, line #%d): %s\n",
           rc, type, funcName, fileName, lineNo, message);
    return rc;
}

static int bdb_scdone_int(bdb_state_type *bdb_state, DB_TXN *txnid,
                          const char table[], const char *newtable,
                          int fastinit)
{
    int rc;
    char *db_newtable = NULL;

    if (newtable && newtable[0])
        db_newtable = strdup(newtable);

    if (bdb_state == NULL)
        return 0;

    /* we pass here the environment, not a table */
    assert(!bdb_state->parent);

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
    if ((rc = bdb_state->callback->scdone_rtn(bdb_state, table, db_newtable,
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

    if (gbl_debug_systable_locks) {
        assert(bdb_has_tablename_locked(dbenv->app_private, "_comdb2_systables", gbl_rep_lockid,
                                        TABLENAME_LOCKED_WRITE));
    }

    if (sctype == rename_table || sctype == alias_table) {
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
int bdb_llog_scdone(bdb_state_type *bdb_state, scdone_t sctype, const char *tbl,
                   int tbllen, int wait, int *bdberr)
{
    int retries = 0;

    if (bdb_state->parent)
        abort();

retry:
    *bdberr = BDBERR_NOERROR;
    if (++retries >= gbl_maxretries) {
        logmsg(LOGMSG_ERROR, "%s: giving up after %d retries\n", __func__, retries);
        return -1;
    }

    tran_type *ltran = bdb_tran_begin_logical(bdb_state, 0, bdberr);
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
    if ((rc = get_physical_transaction(bdb_state, ltran, &tran, 0)) != 0) {
        if (bdb_tran_abort(bdb_state, ltran, bdberr) != 0)
            abort();
        logmsg(LOGMSG_ERROR, "%s: get_physical_transaction: %d\n", __func__, rc);
        return -1;
    }

    if ((sctype == alter || sctype == fastinit || sctype == bulkimport || sctype == drop) &&
        (strncmp(tbl, "sqlite_stat", sizeof("sqlite_stat") - 1) != 0)) {
        bdb_lock_tablename_write(bdb_state, tbl, tran);
    }
    /* analyze does NOT need schema_lk */
    if (sctype == sc_analyze)
        ltran->get_schema_lock = 0;

    rc = bdb_llog_scdone_tran(bdb_state, sctype, tran, tbl, tbllen, bdberr);

    if (rc) {
        bdb_tran_abort(bdb_state, tran, bdberr);
        *bdberr = BDBERR_MISC;
        return -1;
    }
    uint64_t transize;
    seqnum_type seqnum;
    rc = bdb_tran_commit_with_seqnum_size(bdb_state, ltran, &seqnum,
                                          &transize, bdberr);
    if (rc || *bdberr != BDBERR_NOERROR) {
        if (*bdberr == BDBERR_DEADLOCK)
            goto retry;
    }
    if (wait) {
        rc = bdb_wait_for_seqnum_from_all(bdb_state, &seqnum);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, 
                    "%s: bdb_wait_for_seqnum_from_all rc: %d bdberr:%d\n",
                    __func__, rc, *bdberr);
        }
    }
    return rc;
}

#define IS_QUEUEDB_ROLLOVER_SCHEMA_CHANGE_TYPE(a)                              \
    (((a) == add_queue_file) || ((a) == del_queue_file))
int bdb_llog_scdone_tran(bdb_state_type *bdb_state, scdone_t type,
                         tran_type *tran, const char *tbl, int tbllen, int *bdberr)
{
    int rc = 0;

    if (bdb_state->parent)
        abort();

    if (!IS_QUEUEDB_ROLLOVER_SCHEMA_CHANGE_TYPE(type))
        BDB_BUMP_DBOPEN_GEN(type, NULL);

    DBT *dtbl = NULL;
    if (tbl) {
        dtbl = alloca(sizeof(DBT));
        bzero(dtbl, sizeof(DBT));
        dtbl->data = (char*)tbl;
        dtbl->size = tbllen;
    }

    DBT dtype = {0};
    uint32_t sctype = htonl(type);
    dtype.data = &sctype;
    dtype.size = sizeof(sctype);


    if (gbl_debug_systable_locks) {
        bdb_lock_tablename_write(bdb_state, "_comdb2_systables", tran);
    }

    DB_LSN lsn;
    rc = llog_scdone_log(bdb_state->dbenv, tran->tid, &lsn, 0, dtbl, &dtype);
    if (rc) {
        *bdberr = BDBERR_MISC;
        return -1;
    }
    return rc;
}

int bdb_llog_analyze(bdb_state_type *bdb_state, int wait, int *bdberr)
{
    ATOMIC_ADD32(gbl_analyze_gen, 1);
    return bdb_llog_scdone(bdb_state, sc_analyze, NULL, 0, wait, bdberr);
}

int bdb_llog_views(bdb_state_type *bdb_state, char *name, int wait, int *bdberr)
{
    ++gbl_views_gen;
    return bdb_llog_scdone(bdb_state, views, name, strlen(name) + 1, wait, bdberr);
}

int bdb_llog_partition(bdb_state_type *bdb_state, tran_type *tran, char *name,
                       int *bdberr)
{
    ++gbl_views_gen;
    return bdb_llog_scdone_tran(bdb_state, views, tran, name, strlen(name) + 1,
                                bdberr);
}

int bdb_llog_luareload(bdb_state_type *bdb_state, int wait, int *bdberr)
{
    return bdb_llog_scdone(bdb_state, luareload, NULL, 0, wait, bdberr);
}

int bdb_llog_luafunc(bdb_state_type *bdb_state, scdone_t type, int wait,
                     int *bdberr)
{
    return bdb_llog_scdone(bdb_state, type, NULL, 0, wait, bdberr);
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
    rc = bdb_llog_scdone(bdb_state, type, NULL, 0, 0, bdberr);
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
    rc = bdb_llog_scdone(bdb_state, type, NULL, 0, 0, bdberr);
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

    if (bdb_state->logical_live_sc == 0) {
        return 0;
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
