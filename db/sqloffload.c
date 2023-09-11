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

/*
   offload sql old/main interface

 */
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <poll.h>

#include <sbuf2.h>
#include <plhash.h>
#include <list.h>
#include <queue.h>
#include <epochlib.h>
#include <limits.h>

#include "sql.h"
#include "sqloffload.h"
#include "sqlinterfaces.h"
#include "block_internal.h"
#include "comdb2.h"
#include "comdb2uuid.h"

#include "osqlrepository.h"
#include "osqlcheckboard.h"
#include "osqlcomm.h"
#include "osqlblockproc.h"
#include "osqlsqlthr.h"
#include "osqlshadtbl.h"
#include "osqlblkseq.h"
#include "schemachange.h"
#include <net_types.h>

#include <logmsg.h>

#include <autoanalyze.h>
#include "sc_callbacks.h"
#include "views.h"

#if 0
#define TEST_QSQL_REQ
#define TEST_OSQL
#define TEST_OSQL_DATA
#define TEST_BLOCKSOCK
#define TEST_RECOM
#endif

int scdone_abort_cleanup(struct ireq *iq);

int gbl_master_swing_osql_verbose = 1;

int g_osql_ready = 0;
int tran2netreq(int dbtran)
{
    switch (dbtran) {
    case TRANLEVEL_SOSQL:
        return NET_OSQL_SOCK_REQ;

    case TRANLEVEL_RECOM:
        return NET_OSQL_RECOM_REQ;

    case TRANLEVEL_SNAPISOL:
        return NET_OSQL_SNAPISOL_REQ;

    case TRANLEVEL_SERIAL:
        return NET_OSQL_SERIAL_REQ;
    }

    logmsg(LOGMSG_ERROR, "%s: unknown transaction mode %d\n", __func__, dbtran);
    {
        int once = 0;
        if (!once) {
            cheap_stack_trace();
            once = 1;
        }
    }

    return -1;
}

int tran2netrpl(int dbtran)
{
    switch (dbtran) {
    case TRANLEVEL_SOSQL:
        return NET_OSQL_SOCK_RPL;

    case TRANLEVEL_RECOM:
        return NET_OSQL_RECOM_RPL;

    case TRANLEVEL_SNAPISOL:
        return NET_OSQL_SNAPISOL_RPL;

    case TRANLEVEL_SERIAL:
        return NET_OSQL_SERIAL_RPL;
    }

    logmsg(LOGMSG_ERROR, "%s: unknown transaction mode %d\n", __func__, dbtran);
    {
        int once = 0;
        if (!once) {
            cheap_stack_trace();
            once = 1;
        }
    }

    return -1;
}

/******************************** API *****************************************/

/* TODO: create a cache of reusable osql_sess_t objects */

/* main control */
int osql_open(struct dbenv *dbenv)
{
    int rc = 0;
    int bdberr = 0;

    /* create repository, needed if we're ever becomming master */
    rc = osql_repository_init();
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to init repository\n", __func__);
        return -1;
    }

    /* create checkboard */
    rc = osql_checkboard_init();
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to init checkboard\n", __func__);
        osql_repository_destroy();
        return -2;
    }

    /* init osql structures */
    rc = bdb_osql_init(&bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to init bdb osql %d %d\n", __func__, rc,
                bdberr);
        return -2;
    }

    /* create comm endpoint and kickoff the communication */
    rc = osql_comm_init(dbenv);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to init network\n", __func__);
        osql_repository_destroy();
        return -2;
    }

    g_osql_ready = 1;
    logmsg(LOGMSG_INFO, "osql ready.\n");

    return 0;
}

void osql_cleanup(void)
{
    int bdberr = 0;

    logmsg(LOGMSG_INFO, "Clearing osql structures...\n");
    g_osql_ready = 0;

    osql_comm_destroy();
    osql_checkboard_destroy();
    osql_repository_destroy();
    bdb_osql_destroy(&bdberr);
}

void block2_sorese(struct ireq *iq, const char *sql, int sqlen, int block2_type)
{

    struct thr_handle *thr_self = thrman_self();

    if (iq->debug)
        reqprintf(iq, "%s received from node %s", __func__,
                  iq->sorese->target.host);

    thrman_wheref(thr_self, "%s [%s %s %llx]", req2a(iq->opcode),
                  breq2a(block2_type), iq->sorese->target.host,
                  iq->sorese->rqid);
}

extern int gbl_early_verify;
extern int gbl_osql_send_startgen;

/**
 *
 * All is set by now, since we need to be able to receive rows
 * before a block processor is actually associated with the request
 *
 */

/* Set to 1, check read-only transactions on the master. */
int gbl_serialize_reads_like_writes = 0;

static int rese_commit(struct sqlclntstate *clnt, struct sql_thread *thd,
                       char *tzname, int osqlreq_type, int is_distrib_tran)
{

    int sentops = 0;
    int bdberr = 0;
    int rc = 0;
    int usedb_only = 0;
    int force_master = gbl_serialize_reads_like_writes;

    if (gbl_early_verify && !clnt->early_retry && gbl_osql_send_startgen &&
        clnt->start_gen) {
        if (clnt->start_gen != bdb_get_rep_gen(thedb->bdb_env))
            clnt->early_retry = EARLY_ERR_GENCHANGE;
    }
    if (clnt->selectv_arr)
        currangearr_build_hash(clnt->selectv_arr);
    if (clnt->selectv_arr &&
        bdb_osql_serial_check(thedb->bdb_env, clnt->selectv_arr,
                              &(clnt->selectv_arr->file),
                              &(clnt->selectv_arr->offset), 0)) {
        clnt->osql.xerr.errval = ERR_CONSTR;
        errstat_cat_str(&(clnt->osql.xerr), "selectv constraints");
        rc = SQLITE_ABORT;
    } else if (clnt->early_retry == EARLY_ERR_VERIFY) {
        if (clnt->dbtran.mode == TRANLEVEL_SERIAL) {
            clnt->osql.xerr.errval = ERR_NOTSERIAL;
            errstat_cat_str(&(clnt->osql.xerr),
                            "transaction is not serializable");
        } else {
            clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
            errstat_cat_str(&(clnt->osql.xerr),
                            "unable to update record rc = 4");
        }
        rc = SQLITE_ABORT;
    } else if (clnt->early_retry == EARLY_ERR_SELECTV) {
        clnt->osql.xerr.errval = ERR_CONSTR;
        errstat_cat_str(&(clnt->osql.xerr), "constraints error, no genid");
        rc = SQLITE_ABORT;
    } else if (clnt->early_retry == EARLY_ERR_GENCHANGE) {
        clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
        errstat_cat_str(&(clnt->osql.xerr), "verify error on master swing");
        rc = SQLITE_ABORT;
    }
    if (rc) {
        clnt->early_retry = 0;
        rc = SQLITE_ABORT;
        goto goback;
    }

    /* optimization (will catch all transactions with no internal updates */
    if (!force_master && osql_shadtbl_empty(clnt)) {
        sql_debug_logf(clnt, __func__, __LINE__, "empty-shadtbl, returning\n");
        return 0;
    }

    usedb_only = osql_shadtbl_usedb_only(clnt);

    if (!force_master && usedb_only &&
        (!gbl_selectv_rangechk || !clnt->selectv_arr)) {
        sql_debug_logf(clnt, __func__, __LINE__, "empty-sv_arr, returning\n");
        return 0;
    }

    clnt->osql.timings.commit_prep = osql_log_time();

    /* start the block processor session */
    rc = osql_sock_start(clnt, osqlreq_type, is_distrib_tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to start sorese transaction rc=%d\n",
               __func__, rc);
        if (rc != SQLITE_ABORT) /* if abort, clnt->osql has the error */
            rc = SQLITE_CLIENT_CHANGENODE;
        goto goback;
    }

    int sent_readsets = 0;
    if (!clnt->osql.is_reorder_on) {
        if (clnt->arr) {
            rc = osql_serial_send_readset(clnt, NET_OSQL_SERIAL_RPL);
            sql_debug_logf(clnt, __func__, __LINE__, "returning rc=%d\n", rc);
        }
        if (clnt->selectv_arr) {
            rc = osql_serial_send_readset(clnt, NET_OSQL_SOCK_RPL);
            sql_debug_logf(clnt, __func__, __LINE__, "returning rc=%d\n", rc);
        }
        sent_readsets = 1;
    }

    /* process shadow tables */
    rc = osql_shadtbl_process(clnt, &sentops, &bdberr, 0);

    /* Preserve the sentops optimization */
    if (!sent_readsets && (force_master || sentops)) {
        if (clnt->arr) {
            rc = osql_serial_send_readset(clnt, NET_OSQL_SERIAL_RPL);
            sql_debug_logf(clnt, __func__, __LINE__, "returning rc=%d\n", rc);
        }

        if (clnt->selectv_arr) {
            rc = osql_serial_send_readset(clnt, NET_OSQL_SOCK_RPL);
            sql_debug_logf(clnt, __func__, __LINE__, "returning rc=%d\n", rc);
        }
    }

    if (rc && rc != -2) {
        int irc = 0;

        sql_debug_logf(clnt, __func__, __LINE__, "aborting\n");

        irc = osql_sock_abort(clnt, osqlreq_type);
        if (irc) {
            logmsg(LOGMSG_ERROR, "%s: failed to abort sorese transaction rc=%d\n",
                    __func__, rc);
            rc = SQLITE_ERROR;
            goto goback;
        }

        if (rc == -3) /* selectv skip optimization, not an error */
            rc = 0;

        clnt->osql.xerr.errval = rc;
    } else {

        /* close the block processor session and retrieve the result */
        sql_debug_logf(clnt, __func__, __LINE__, "committing\n");
        rc = osql_sock_commit(clnt, osqlreq_type, TRANS_CLNTCOMM_NORMAL);
        if (rc && rc != SQLITE_ABORT && rc != SQLITE_DEADLOCK &&
            rc != SQLITE_BUSY && rc != SQLITE_CLIENT_CHANGENODE) {
            // XXX HERE IS THE BUG .. SQLITE_ERROR IS 1 - THE SAME AS DUP
            logmsg(LOGMSG_ERROR, "%s line %d: rc is set to %d, changing rc to CLIENT_CHANGENODE\n", 
                    __func__, __LINE__, rc);
            rc = SQLITE_CLIENT_CHANGENODE;
            //rc = SQLITE_ERROR;
        }
        sql_debug_logf(clnt, __func__, __LINE__, "returning %d\n", rc);
    }

goback:

    /* if this is read committed and we just got a verify error,
       don't close the shadow tables since this will get retried */
    if (clnt->osql.xerr.errval == ERR_VERIFY &&
        clnt->dbtran.mode == TRANLEVEL_RECOM &&
        clnt->osql.replay != OSQL_RETRY_LAST) {
    } else {
        /* CLOSE the temporary tables */
        osql_shadtbl_close(clnt);
    }

    return rc;
}

static int sorese_abort(struct sqlclntstate *clnt, int osqlreq_type)
{
    int rc = 0;
    int bdberr = 0;

    /* CLOSE the shadow tables */
    osql_shadtbl_close(clnt);

    rc = trans_abort_shadow((void **)&clnt->dbtran.shadow_tran, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed abort rc=%d bdberr=%d\n", __func__, rc,
                bdberr);
    }

    return 0;
}

int recom_commit(struct sqlclntstate *clnt, struct sql_thread *thd,
                 char *tzname, int is_distributed_tran)
{
    int rc = 0;

    /* temp hook for sql transactions */
    if (clnt->dbtran.dtran) {
        rc = fdb_trans_commit(clnt, TRANS_CLNTCOMM_NORMAL);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s distributed failure rc=%d\n", __func__, rc);
            return rc;
        }
    }

    return rese_commit(clnt, thd, tzname, OSQL_RECOM_REQ, is_distributed_tran);
}

int recom_abort(struct sqlclntstate *clnt)
{
    int rc;

    /* temp hook for sql transactions */
    if (clnt->dbtran.dtran) {
        rc = fdb_trans_rollback(clnt);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s distributed failure rc=%d\n", __func__,
                   rc);
        }
    }

    return sorese_abort(clnt, OSQL_RECOM_REQ);
}

int snapisol_commit(struct sqlclntstate *clnt, struct sql_thread *thd,
                    char *tzname)
{

    return rese_commit(clnt, thd, tzname, OSQL_SNAPISOL_REQ, 0);
}

int snapisol_abort(struct sqlclntstate *clnt)
{

    return sorese_abort(clnt, OSQL_SNAPISOL_REQ);
}

int serial_commit(struct sqlclntstate *clnt, struct sql_thread *thd,
                  char *tzname)
{

    return rese_commit(clnt, thd, tzname, OSQL_SERIAL_REQ, 0);
}

int serial_abort(struct sqlclntstate *clnt)
{

    return sorese_abort(clnt, OSQL_SERIAL_REQ);
}

int selectv_range_commit(struct sqlclntstate *clnt)
{

    int rc = 0;

    if (!clnt->selectv_arr)
        return 0;

    if (clnt->selectv_arr->size == 0)
        return 0;

    currangearr_build_hash(clnt->selectv_arr);

    if (bdb_osql_serial_check(thedb->bdb_env, clnt->selectv_arr,
                              &(clnt->selectv_arr->file),
                              &(clnt->selectv_arr->offset), 0)) {
        rc = SQLITE_ABORT;
        clnt->osql.xerr.errval = ERR_CONSTR;
        errstat_cat_str(&(clnt->osql.xerr), "selectv constraints");
    }

    if (rc)
        return rc;

    if (!clnt->osql.sock_started) {
        rc = osql_sock_start(clnt, OSQL_SOCK_REQ, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: failed to start socksql transaction rc=%d\n", __func__,
                   rc);
            if (rc != SQLITE_ABORT)
                rc = SQLITE_CLIENT_CHANGENODE;
            return rc;
        }
        sql_debug_logf(clnt, __func__, __LINE__, "osql_sock_start returns %d\n",
                       rc);
    }

    rc = osql_serial_send_readset(clnt, NET_OSQL_SOCK_RPL);
    return rc;
}

int req2netreq(int reqtype)
{
    switch (reqtype) {
    case OSQL_SOCK_REQ:
        return NET_OSQL_SOCK_REQ;

    case OSQL_RECOM_REQ:
        return NET_OSQL_RECOM_REQ;

    case OSQL_SNAPISOL_REQ:
        return NET_OSQL_SNAPISOL_REQ;

    case OSQL_SERIAL_REQ:
        return NET_OSQL_SERIAL_REQ;
    }

    logmsg(LOGMSG_ERROR, "%s: unknown request type %d\n", __func__, reqtype);
    {
        int once = 0;
        if (!once) {
            cheap_stack_trace();
            once = 1;
        }
    }

    return -1;
}

int req2netrpl(int reqtype)
{
    switch (reqtype) {
    case OSQL_SOCK_REQ:
        return NET_OSQL_SOCK_RPL;

    case OSQL_RECOM_REQ:
        return NET_OSQL_RECOM_RPL;

    case OSQL_SNAPISOL_REQ:
        return NET_OSQL_SNAPISOL_RPL;

    case OSQL_SERIAL_REQ:
        return NET_OSQL_SERIAL_RPL;
    }

    logmsg(LOGMSG_ERROR, "%s: unknown request type %d\n", __func__, reqtype);
    {
        int once = 0;
        if (!once) {
            cheap_stack_trace();
            once = 1;
        }
    }

    return -1;
}

int tran2req(int dbtran)
{
    switch (dbtran) {
    case TRANLEVEL_SOSQL:
        return OSQL_SOCK_REQ;

    case TRANLEVEL_RECOM:
        return OSQL_RECOM_REQ;

    case TRANLEVEL_SNAPISOL:
        return OSQL_SNAPISOL_REQ;

    case TRANLEVEL_SERIAL:
        return OSQL_SERIAL_REQ;
    }

    logmsg(LOGMSG_ERROR, "%s: unknown transaction mode %d\n", __func__, dbtran);
    {
        int once = 0;
        if (!once) {
            cheap_stack_trace();
            once = 1;
        }
    }

    return OSQL_REQINV;
}

int osql_clean_sqlclntstate(struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    int bdberr = 0;
    enum ctrl_sqleng state = clnt->ctrl_sqlengine;

    /*
     * Warn of any invalid engine state.  Do it before srs_tran_destroy() as
     * clnt->sql will be pointing at free memory after that.
     */
    if (state != SQLENG_NORMAL_PROCESS /* most common case */
        && state != SQLENG_STRT_STATE  /* empty transactions */
        && state != SQLENG_FNSH_ABORTED_STATE /* aborted transactions */) {
        logmsg(LOGMSG_ERROR, "%p ctrl engine has wrong state %d %llx %p\n", clnt, state, clnt->osql.rqid,
               (void *)pthread_self());
        if (clnt->sql)
            logmsg(LOGMSG_ERROR, "%p sql is \"%s\"\n", clnt, clnt->sql);
    }

    /* TODO: once Dr. Hipp fixes the plan, this should be moved
       to sqlite3BtreeCloseCursor */
    clearClientSideRow(clnt);

    fdb_clear_sqlclntstate(clnt);

    if (osql->tablename)
        free(osql->tablename);
    if (!osql_shadtbl_empty(clnt))
        osql_shadtbl_close(clnt);
    if (osql->history)
        srs_tran_destroy(clnt);
    if (clnt->saved_errstr) {
        free(clnt->saved_errstr);
        clnt->saved_errstr = NULL;
    }

    if (clnt->dbtran.shadow_tran) {
        /* for some reason the clnt contains an unfinished
           shadow transaction, that could have allocated structures
           and temp tables
           Abort here to avoid leaking them */
        logmsg(LOGMSG_ERROR, "shadow_tran not cleared, aborting\n");
        rc = trans_abort_shadow((void **)&clnt->dbtran.shadow_tran, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s:%d: abort shadow failed rc=%d bdberr=%d\n",
                    __FILE__, __LINE__, rc, bdberr);
        }
    }

    if (osql_chkboard_sqlsession_exists(clnt->osql.rqid, clnt->osql.uuid)) {
        uuidstr_t us;
        logmsg(LOGMSG_ERROR, "%p [%llx %s] in USE! %p\n", clnt, clnt->osql.rqid, comdb2uuidstr(clnt->osql.uuid, us),
               (void *)pthread_self());
        /* XXX temporary debug code. */
        if (gbl_abort_on_clear_inuse_rqid)
            abort();
    }

    bzero(osql, sizeof(*osql));
    listc_init(&osql->shadtbls, offsetof(struct shad_tbl, linkv));

    sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_NORMAL_PROCESS);

    return 0;
}

static void osql_analyze_commit_callback(struct ireq *iq)
{
    int bdberr;
    if (iq->osql_flags & OSQL_FLAGS_ANALYZE) {
        bdb_llog_analyze(thedb->bdb_env, 1, &bdberr);
    }
}

static void osql_rowlocks_commit_callback(struct ireq *iq)
{
    int bdberr;
    if (iq->osql_flags & OSQL_FLAGS_ROWLOCKS) {
        bdb_llog_rowlocks(thedb->bdb_env,
                          iq->osql_rowlocks_enable ? rowlocks_on : rowlocks_off,
                          &bdberr);
    }
}

static void osql_genid48_commit_callback(struct ireq *iq)
{
    int bdberr;
    if (iq->osql_flags & OSQL_FLAGS_GENID48) {
        bdb_set_genid_format(iq->osql_genid48_enable ? LLMETA_GENID_48BIT
                                                     : LLMETA_GENID_ORIGINAL,
                             &bdberr);
        bdb_llog_genid_format(thedb->bdb_env,
                              iq->osql_genid48_enable ? genid48_enable
                                                      : genid48_disable,
                              &bdberr);
    }
}

extern int gbl_readonly_sc;
static void osql_scdone_commit_callback(struct ireq *iq)
{
    int bdberr = 0;
    int write_scdone =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_DONE_SAME_TRAN) ? 0 : 1;
    gbl_readonly_sc = 0;
    if (iq->osql_flags & OSQL_FLAGS_SCDONE) {
        struct schema_change_type *sc_next;
        iq->sc = iq->sc_pending;
        while (iq->sc != NULL) {
            int rc = 0;
            sc_next = iq->sc->sc_next;
            if (write_scdone) {
                if (iq->sc->done_type == invalid ||
                    (iq->sc->done_type != user_view && iq->sc->db == NULL)) {
                    logmsg(LOGMSG_ERROR, "%s: Skipping scdone for table %s\n",
                           __func__, iq->sc->tablename);
                } else {
                    rc = llog_scdone_rename_wrapper(thedb->bdb_env, iq->sc,
                                                    NULL, &bdberr);
                    if (rc || bdberr != BDBERR_NOERROR) {
                        /* We are here because we are running in R6 compatible
                         * mode. For R7 or later, use SC_DONE_SAME_TRAN.
                         *
                         * Don't quite know what to do here, the schema change
                         * is committed but one or more replicants dont get the
                         * scdone to reload tables. We really need to somehow
                         * bounce the replicants, but there's no way to do this.
                         */
                        logmsg(LOGMSG_ERROR,
                               "%s: Failed to log scdone for table %s\n",
                               __func__, iq->sc->tablename);
                    }
                }
            }

            broadcast_sc_end(iq->sc->tablename, iq->sc_seed);
            if (iq->sc->db) {
                int rc;
                tran_type *lock_trans = NULL;
                if ((rc = trans_start(iq, NULL, &lock_trans)) == 0) {
                    bdb_lock_tablename_read(thedb->bdb_env, iq->sc->tablename,
                                            lock_trans);
                    sc_del_unused_files_tran(iq->sc->db, lock_trans);
                    trans_abort(iq, lock_trans);
                } else {
                    logmsg(LOGMSG_ERROR,
                           "%s failed to start lock_trans, rc=%d\n", __func__,
                           rc);
                }
            }
            if (iq->sc->kind == SC_TRUNCATETABLE)
                autoanalyze_after_fastinit(iq->sc->tablename);
            free_schema_change_type(iq->sc);
            iq->sc = sc_next;
        }
        iq->sc_pending = NULL;
        iq->sc_seed = 0;
        iq->sc_should_abort = 0;
    }
    iq->tranddl = 0;
}

static void osql_scdone_abort_callback(struct ireq *iq)
{
    gbl_readonly_sc = 0;
    if (iq->osql_flags & OSQL_FLAGS_SCDONE) {
        iq->sc = iq->sc_pending;
        while (iq->sc != NULL) {
            struct schema_change_type *sc_next;
            scdone_abort_cleanup(iq);
            sc_next = iq->sc->sc_next;
            free_schema_change_type(iq->sc);
            iq->sc = sc_next;
        }
        iq->sc_pending = NULL;
        iq->sc_seed = 0;
        iq->sc_should_abort = 0;
    }
    iq->tranddl = 0;
}

void osql_postcommit_handle(struct ireq *iq)
{
    osql_analyze_commit_callback(iq);
    osql_rowlocks_commit_callback(iq);
    osql_genid48_commit_callback(iq);
    osql_scdone_commit_callback(iq);
}

void osql_postabort_handle(struct ireq *iq)
{
    osql_scdone_abort_callback(iq);
}

int osql_is_index_reorder_on(int osql_flags)
{
    return osql_flags & OSQL_FLAGS_REORDER_IDX_ON;
}

void osql_unset_index_reorder_bit(int *osql_flags)
{
    (*osql_flags) &= (~OSQL_FLAGS_REORDER_IDX_ON);
}
