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

#include "osqlrepository.h"
#include "osqlcheckboard.h"
#include "osqlcomm.h"
#include "osqlblockproc.h"
#include "osqlsqlthr.h"
#include "osqlshadtbl.h"
#include "osqlblkseq.h"
#include <net_types.h>

#include <logmsg.h>

#if 0
#define TEST_QSQL_REQ
#define TEST_OSQL
#define TEST_OSQL_DATA
#define TEST_BLOCKSOCK
#define TEST_RECOM
#endif

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

    rc = osql_blkseq_init();
    if (rc) {
        fprintf(stderr, "%s failing to init blocksql blockseq module\n",
                __func__);
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

char *osql_breq2a(int op)
{

    switch (op) {
    case OSQL_RPLINV:
        return "INVALID";
    case OSQL_DONE:
        return "OSQL_DONE";
    case OSQL_USEDB:
        return "OSQL_USEDB";
    case OSQL_DELREC:
        return "OSQL_DELREC";
    case OSQL_INSREC:
        return "OSQL_INSREC";
    case OSQL_CLRTBL:
        return "OSQL_CLRTBL";
    case OSQL_QBLOB:
        return "OSQL_QBLOB";
    case OSQL_UPDREC:
        return "OSQL_UPDREC";
    case OSQL_XERR:
        return "OSQL_XERR";
    case OSQL_UPDCOLS:
        return "OSQL_UPDCOLS";
    case OSQL_DONE_STATS:
        return "OSQL_DONE_STATS";
    case OSQL_SERIAL:
        return "OSQL_SERIAL";
    case OSQL_SELECTV:
        return "OSQL_SELECTV";
    case OSQL_DBGLOG:
        return "OSQL_DBGLOG";
    case OSQL_RECGENID:
        return "OSQL_RECGENID";
    case OSQL_UPDSTAT:
        return "OSQL_UPDSTAT";
    case OSQL_DBQ_CONSUME:
        return "OSQL_DBQ_CONSUME";
    case OSQL_INSERT:
        return "OSQL_INSERT";
    case OSQL_DELETE:
        return "OSQL_DELETE";
    case OSQL_UPDATE:
        return "OSQL_UPDATE";
    default:
        return "UNKNOWN";
    }
}

int block2_sorese(struct ireq *iq, const char *sql, int sqlen, int block2_type)
{

    struct thr_handle *thr_self = thrman_self();

    if (iq->debug)
        reqprintf(iq, "%s received from node %s", __func__, iq->sorese.host);

    thrman_wheref(thr_self, "%s [%s %s %llx]", req2a(iq->opcode),
                  breq2a(block2_type), iq->sorese.host, iq->sorese.rqid);

    return 0;
}

/**
 *
 * All is set by now, since we need to be able to receive rows
 * before a block processor is actually associated with the request
 *
 */

static int rese_commit(struct sqlclntstate *clnt, struct sql_thread *thd,
                       char *tzname, int osqlreq_type, int is_distrib_tran)
{

    int sentops = 0;
    int bdberr = 0;
    int rc = 0;
    int rc2 = 0;
    int usedb_only = 0;

    if (clnt->early_retry) {
        clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
        clnt->early_retry = 0;
        rc = SQLITE_ABORT;
        goto goback;
    }

    /* optimization (will catch all transactions with no internal updates */
    if (osql_shadtbl_empty(clnt)) {
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, "td=%u %s line %d empty-shadtbl, returning\n",
                   pthread_self(), __func__, __LINE__);
        }
        return 0;
    }

    usedb_only = osql_shadtbl_usedb_only(clnt);

    if (usedb_only && !clnt->selectv_arr && gbl_selectv_rangechk) {
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, "td=%u %s line %d empty-sv_arr, returning\n",
                   pthread_self(), __func__, __LINE__);
        }
        return 0;
    }

    if (clnt->selectv_arr)
        currangearr_build_hash(clnt->selectv_arr);
    if (clnt->selectv_arr &&
        bdb_osql_serial_check(thedb->bdb_env, clnt->selectv_arr,
                              &(clnt->selectv_arr->file),
                              &(clnt->selectv_arr->offset), 0)) {
        rc = SQLITE_ABORT;
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, "td=%u %s line %d returning SQLITE_ABORT\n", 
                    pthread_self(), __func__, __LINE__);
        }
        clnt->osql.xerr.errval = ERR_CONSTR;
        errstat_cat_str(&(clnt->osql.xerr), "selectv constraints");
        goto goback;
    }

    // This can incorrectly return serial-error on retry by conflicting 
    // against itself: cut-1 solution: disable this serial_check (we 
    // handle this case correctly on the master).  cut-2 solution might
    // be to add a blkseq check here & return the correct rcode.
#if 0
    if (!usedb_only) {
        if (clnt->arr)
            currangearr_build_hash(clnt->arr);

        if (clnt->arr &&
            bdb_osql_serial_check(thedb->bdb_env, clnt->arr, &(clnt->arr->file),
                                  &(clnt->arr->offset), 0)) {

            if (gbl_extended_sql_debug_trace) {
                fprintf(stderr, "td=%u %s line %d returning SQLITE_ABORT for serial_check failure\n", 
                        pthread_self(), __func__, __LINE__);
            }

            rc = SQLITE_ABORT;
            clnt->osql.xerr.errval = ERR_NOTSERIAL;
            errstat_cat_str(&(clnt->osql.xerr),
                            "transaction is not serializable");
            goto goback;
        }
    }
#endif

    clnt->osql.timings.commit_prep = osql_log_time();

    /* start the block processor session */
    rc = osql_sock_start(clnt, osqlreq_type, is_distrib_tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to start sorese transactin rc=%d\n",
                __func__, rc);
        if (rc != SQLITE_ABORT) /* if abort, clnt->osql has the error */
            rc = SQLITE_CLIENT_CHANGENODE;
        goto goback;
    }

    /* process shadow tables */
    rc = osql_shadtbl_process(clnt, &sentops, &bdberr);

    if (sentops && clnt->arr) {
        rc = osql_serial_send_readset(clnt, NET_OSQL_SERIAL_RPL);
        if (gbl_extended_sql_debug_trace && rc) {
            logmsg(LOGMSG_ERROR, "td=%u %s line %d returning %d\n", 
                    pthread_self(), __func__, __LINE__, rc);
        }
    }

    if (clnt->selectv_arr) {
        rc = osql_serial_send_readset(clnt, NET_OSQL_SOCK_RPL);
        if (gbl_extended_sql_debug_trace && rc) {
            logmsg(LOGMSG_ERROR, "td=%u %s line %d returning %d\n", 
                    pthread_self(), __func__, __LINE__, rc);
        }
    }

    if (rc && rc != -2) {
        int irc = 0;

        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, "td=%u %s line %d aborting\n", pthread_self(),
                   __func__, __LINE__);
        }

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
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, "td=%u %s line %d committing\n", pthread_self(),
                   __func__, __LINE__);
        }
        rc = osql_sock_commit(clnt, osqlreq_type);
        if (rc && rc != SQLITE_ABORT && rc != SQLITE_DEADLOCK &&
            rc != SQLITE_BUSY && rc != SQLITE_CLIENT_CHANGENODE) {
            // XXX HERE IS THE BUG .. SQLITE_ERROR IS 1 - THE SAME AS DUP
            logmsg(LOGMSG_ERROR, "%s line %d: rc is set to %d, changing rc to CLIENT_CHANGENODE\n", 
                    __func__, __LINE__, rc);
            rc = SQLITE_CLIENT_CHANGENODE;
            //rc = SQLITE_ERROR;
        }
        if (gbl_extended_sql_debug_trace && rc) {
            logmsg(LOGMSG_ERROR, "td=%u %s line %d returning %d\n", 
                    pthread_self(), __func__, __LINE__, rc);
        }
    }

goback:

    /* if this is read committed and we just got a verify error,
       don't close the shadow tables since this will get retried */
    if (clnt->osql.xerr.errval == ERR_VERIFY &&
        clnt->dbtran.mode == TRANLEVEL_RECOM &&
        clnt->osql.replay != OSQL_RETRY_LAST) {
        /*fprintf(stderr, "Received rc=%d=ERR_VERIFY\n", rc);*/
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
        rc = fdb_trans_commit(clnt);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s distributed failure rc=%d\n", __func__, rc);
            return rc;
        }
    }

    return rese_commit(clnt, thd, tzname, OSQL_RECOM_REQ, is_distributed_tran);
}

int recom_abort(struct sqlclntstate *clnt)
{

    return sorese_abort(clnt, OSQL_RECOM_REQ);
}

int block2_serial(struct ireq *iq, const char *sql, int sqlen)
{

    return block2_sorese(iq, sql, sqlen, BLOCK2_SERIAL);
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

    rc = osql_serial_send_readset(clnt, NET_OSQL_SOCK_RPL);
    return rc;
}

int req2netreq(int reqtype)
{
    switch (reqtype) {
    case OSQL_BLOCK_REQ:
        return NET_OSQL_BLOCK_REQ;

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
    case OSQL_BLOCK_REQ:
        return NET_OSQL_BLOCK_RPL;

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

    /* fields we don't control, make sure they are 0 */
    if (clnt->osql.sess_blocksock)
        logmsg(LOGMSG_ERROR, "sess_blocksock field is not cleared!\n");

    if (clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS &&
        clnt->ctrl_sqlengine != SQLENG_STRT_STATE) {
        logmsg(LOGMSG_ERROR, "%p ctrl engine has wrong state %d %llx %d\n", clnt,
                clnt->ctrl_sqlengine, clnt->osql.rqid, pthread_self());
        if (clnt->sql)
            logmsg(LOGMSG_ERROR, "%p sql is \"%s\"\n", clnt, clnt->sql);
    }

    if (osql_chkboard_sqlsession_exists(clnt->osql.rqid, clnt->osql.uuid, 1)) {
        logmsg(LOGMSG_ERROR, "%p rqid %llx in USE! %d\n", clnt, clnt->osql.rqid,
                pthread_self());
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
    if (btst(&iq->osql_flags, OSQL_FLAGS_ANALYZE)) {
        bdb_llog_analyze(thedb->bdb_env, 1, &bdberr);
    }
}

static void osql_rowlocks_commit_callback(struct ireq *iq)
{
    int bdberr;
    if (btst(&iq->osql_flags, OSQL_FLAGS_ROWLOCKS)) {
        bdb_llog_rowlocks(thedb->bdb_env,
                          iq->osql_rowlocks_enable ? rowlocks_on : rowlocks_off,
                          &bdberr);
    }
}

static void osql_genid48_commit_callback(struct ireq *iq)
{
    int bdberr;
    if (btst(&iq->osql_flags, OSQL_FLAGS_GENID48)) {
        bdb_set_genid_format(iq->osql_genid48_enable ? LLMETA_GENID_48BIT
                                                     : LLMETA_GENID_ORIGINAL,
                             &bdberr);
        bdb_llog_genid_format(thedb->bdb_env,
                              iq->osql_genid48_enable ? genid48_enable
                                                      : genid48_disable,
                              &bdberr);
    }
}

static void osql_scdone_commit_callback(struct ireq *iq)
{
    int bdberr;
    if (btst(&iq->osql_flags, OSQL_FLAGS_SCDONE)) {
        struct schema_change_type *sc_next;
        iq->sc = iq->sc_pending;
        while (iq->sc != NULL) {
            sc_next = iq->sc->sc_next;
            free_schema_change_type(iq->sc);
            iq->sc = sc_next;
        }
        iq->sc_pending = NULL;
        iq->sc_seed = 0;
    }
    if (iq->sc_locked) {
        unlock_schema_lk();
        iq->sc_locked = 0;
    }
}

static void osql_scdone_abort_callback(struct ireq *iq)
{
    if (btst(&iq->osql_flags, OSQL_FLAGS_SCDONE)) {
        iq->sc = iq->sc_pending;
        while (iq->sc != NULL) {
            int backout_schema_change(struct ireq * iq);
            struct schema_change_type *sc_next;
            backout_schema_change(iq);
            sc_next = iq->sc->sc_next;
            free_schema_change_type(iq->sc);
            iq->sc = sc_next;
        }
        iq->sc_pending = NULL;
        iq->sc_seed = 0;
    }
    if (iq->sc_locked) {
        unlock_schema_lk();
        iq->sc_locked = 0;
    }
}

typedef void (*osql_callback_t)(struct ireq *iq);
/* must matches OSQL_FLAGS_* enums order */
static osql_callback_t commit_callbacks[] = {
    NULL,                          /* OSQL_FLAGS_RECORD_COST */
    NULL,                          /* OSQL_FLAGS_AUTH */
    osql_analyze_commit_callback,  /* OSQL_FLAGS_ANALYZE */
    NULL,                          /* OSQL_FLAGS_CHECK_SELFLOCK */
    NULL,                          /* OSQL_FLAGS_USE_BLKSEQ */
    osql_rowlocks_commit_callback, /* OSQL_FLAGS_ROWLOCKS */
    osql_genid48_commit_callback,  /* OSQL_FLAGS_GENID48 */
    osql_scdone_commit_callback    /* OSQL_FLAGS_SCDONE */
};
static osql_callback_t abort_callbacks[] = {
    NULL,                      /* OSQL_FLAGS_RECORD_COST */
    NULL,                      /* OSQL_FLAGS_AUTH */
    NULL,                      /* OSQL_FLAGS_ANALYZE */
    NULL,                      /* OSQL_FLAGS_CHECK_SELFLOCK */
    NULL,                      /* OSQL_FLAGS_USE_BLKSEQ */
    NULL,                      /* OSQL_FLAGS_ROWLOCKS */
    NULL,                      /* OSQL_FLAGS_GENID48 */
    osql_scdone_abort_callback /* OSQL_FLAGS_SCDONE */
};

void osql_postcommit_handle(struct ireq *iq)
{
    commit_callbacks[OSQL_FLAGS_ANALYZE](iq);
    commit_callbacks[OSQL_FLAGS_ROWLOCKS](iq);
    commit_callbacks[OSQL_FLAGS_GENID48](iq);
    commit_callbacks[OSQL_FLAGS_SCDONE](iq);
}

void osql_postabort_handle(struct ireq *iq)
{
    abort_callbacks[OSQL_FLAGS_SCDONE](iq);
}
