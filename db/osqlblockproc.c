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

/**
 *
 * Osql Interface with Block Processor
 *
 * Each block processor used for osql keeps a local log of operations (bplog)
 * of the current transaction. The operations are sent by the sqlthread.
 *
 * In blocksql mode, the bp opens an osql session for each query part of the
 *current
 * transaction.
 * In socksql/recom/snapisol/serial mode, the bp has only one session.
 *
 * Block processor waits until all pending sessions are completed.  If any of
 *the sessions
 * completes with an error that could be masked (like deadlocks), the block
 *processor
 * will re-issue it.
 *
 * If all the sessions complete successfully, the bp scans through the log and
 *applies all the
 * changes by calling record.c functions.
 *
 * At the end, the return code is provided to the client (in the case of
 *blocksql)
 * or the remote socksql/recom/snapisol/serial thread.
 *
 */
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <limits.h>
#include <strings.h>
#include <poll.h>
#include <str0.h>
#include <epochlib.h>
#include <plhash.h>
#include <assert.h>

#include "comdb2.h"
#include "osqlblockproc.h"
#include "block_internal.h"
#include "osqlsession.h"
#include "osqlcomm.h"
#include "sqloffload.h"
#include "osqlrepository.h"
#include "comdb2uuid.h"
#include "bpfunc.h"
#include "logmsg.h"
#include "time_accounting.h"
#include <ctrace.h>
#include "intern_strings.h"

int g_osql_blocksql_parallel_max = 5;
int gbl_osql_check_replicant_numops = 1;
extern int gbl_blocksql_grace;


struct blocksql_tran {
    osql_sess_t *sess; /* pointer to the osql session */
    /* NOTE: we keep only block processor thread operating on this, so we don't
     * need a lock */
    pthread_mutex_t store_mtx; /* mutex for db access - those are non-env dbs */
    struct dbtable *last_db;
    struct temp_table *db_ins; /* keeps the list of INSERT ops for a session */
    struct temp_table *db;     /* keeps the list of all OTHER ops */

    pthread_mutex_t mtx; /* mutex and cond for notifying when any session
                           has completed */
    pthread_cond_t cond;
    int dowait; /* mark this when session completes to avoid loosing signal */
    int delayed;
    int rows;
    bool iscomplete;
};

typedef struct oplog_key {
    uint16_t tbl_idx;
    uint8_t stripe;
    unsigned long long genid;
    uint8_t is_rec; // 1 for record because it needs to go after blobs
    uint32_t seq;   // record and blob will share the same seq
} oplog_key_t;

static int apply_changes(struct ireq *iq, blocksql_tran_t *tran, void *iq_tran,
                         int *nops, struct block_err *err, SBUF2 *logsb,
                         int (*func)(struct ireq *, unsigned long long, uuid_t,
                                     void *, char **, int, int *, int **,
                                     blob_buffer_t blobs[MAXBLOBS], int,
                                     struct block_err *, int *, SBUF2 *));
static int req2blockop(int reqtype);
extern const char *get_tablename_from_rpl(unsigned long long rqid,
                                          const char *rpl, int *tableversion);

#define CMP_KEY_MEMBER(k1, k2, var)                                            \
    if (k1->var < k2->var) {                                                   \
        return -1;                                                             \
    }                                                                          \
    if (k1->var > k2->var) {                                                   \
        return 1;                                                              \
    }

/**
 * The bplog key-compare function - required because memcmp changes
 * the order of temp_table_next on little-endian machines.
 *
 * key will compare by rqid, uuid, table, stripe, genid, is_rec, then sequence
 */
int osql_bplog_key_cmp(void *usermem, int key1len, const void *key1,
                       int key2len, const void *key2)
{
    assert(sizeof(oplog_key_t) == key1len);
    assert(sizeof(oplog_key_t) == key2len);

#ifdef _SUN_SOURCE
    oplog_key_t t1, t2;
    memcpy(&t1, key1, key1len);
    memcpy(&t2, key2, key2len);
    oplog_key_t *k1 = &t1;
    oplog_key_t *k2 = &t2;
#else
    oplog_key_t *k1 = (oplog_key_t *)key1;
    oplog_key_t *k2 = (oplog_key_t *)key2;
#endif

    CMP_KEY_MEMBER(k1, k2, tbl_idx);
    CMP_KEY_MEMBER(k1, k2, stripe);

    // need to sort by genid correctly
    int cmp = bdb_cmp_genids(k1->genid, k2->genid);
    if (cmp)
        return cmp;

    CMP_KEY_MEMBER(k1, k2, is_rec);
    CMP_KEY_MEMBER(k1, k2, seq);

    return 0;
}

static int osql_bplog_instbl_key_cmp(void *usermem, int key1len,
                                     const void *key1, int key2len,
                                     const void *key2)
{
    assert(sizeof(oplog_key_t) == key1len);
    assert(sizeof(oplog_key_t) == key2len);

#ifdef _SUN_SOURCE
    oplog_key_t t1, t2;
    memcpy(&t1, key1, key1len);
    memcpy(&t2, key2, key2len);
    oplog_key_t *k1 = &t1;
    oplog_key_t *k2 = &t2;
#else
    oplog_key_t *k1 = (oplog_key_t *)key1;
    oplog_key_t *k2 = (oplog_key_t *)key2;
#endif

    CMP_KEY_MEMBER(k1, k2, tbl_idx);
    CMP_KEY_MEMBER(k1, k2, stripe);
    CMP_KEY_MEMBER(k1, k2, genid); // in this case genid is just a counter
    CMP_KEY_MEMBER(k1, k2, is_rec);
    CMP_KEY_MEMBER(k1, k2, seq);
    return 0;
}

/**
 * Adds the current session
 * If there is no bplog created, it creates one.
 * Returns 0 if success.
 *
 */
int osql_bplog_start(struct ireq *iq, osql_sess_t *sess)
{

    blocksql_tran_t *tran = NULL;
    int bdberr = 0;

    /*
       this stuff is LOCKLESS, since we have only block
       processor calling this

       if we need to have like "send ... thr" print these
       info, we'll need a lock around alloc/dealloc
     */

    if (iq->blocksql_tran)
        abort();

    tran = calloc(sizeof(blocksql_tran_t), 1);
    if (!tran) {
        logmsg(LOGMSG_ERROR, "%s: error allocating %zu bytes\n", __func__,
               sizeof(blocksql_tran_t));
        return -1;
    }

    Pthread_mutex_init(&tran->store_mtx, NULL);

    iq->blocksql_tran = tran; /* now blockproc knows about it */

    /* init temporary table and cursor */
    tran->db = bdb_temp_array_create(thedb->bdb_env, &bdberr);
    if (!tran->db || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to create temp table bdberr=%d\n",
               __func__, bdberr);
        free(tran);
        return -1;
    }

    bdb_temp_table_set_cmp_func(tran->db, osql_bplog_key_cmp);

    if (sess->is_reorder_on) {
        tran->db_ins = bdb_temp_array_create(thedb->bdb_env, &bdberr);
        if (!tran->db_ins) {
            // We can stll work without a INS table
            logmsg(LOGMSG_ERROR,
                   "%s: failed to create temp table for INS bdberr=%d\n",
                   __func__, bdberr);
        } else
            bdb_temp_table_set_cmp_func(tran->db_ins,
                                        osql_bplog_instbl_key_cmp);
    }

    tran->dowait = 1;

    iq->timings.req_received = osql_log_time();
    iq->tranddl = 0;
    /*printf("Set req_received=%llu\n", iq->timings.req_received);*/

    tran->sess = sess;
    return 0;
}

/* Wait for pending sessions to finish;
   If any request has failed because of deadlock, repeat */
int osql_bplog_finish_sql(struct ireq *iq, struct block_err *err)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    int error = 0;
    struct errstat generr = {0}, *xerr;
    int rc = 0;
    int stop_time = 0;
    iq->timings.req_alldone = osql_log_time();

    if (tran->iscomplete) // already complete this is a replicant replay??
        return 0;

    /* if the session finished with deadlock on replicant, or failed
       session, resubmit it */

    rc = osql_sess_test_complete(tran->sess, &xerr);
    switch (rc) {
    case SESS_DONE_OK:
        tran->iscomplete = 1;
        break;
    case SESS_DONE_ERROR_REPEATABLE:
        /* generate a new id for this session */
        if (iq->sorese.type) {
            /* this is socksql, recom, snapisol or serial; no retry here
             */
            generr.errval = ERR_INTERNAL;
            strncpy0(generr.errstr, "master cancelled transaction",
                     sizeof(generr.errstr));
            xerr = &generr;
            error = 1;
            break;
        }
        break;
    case SESS_DONE_ERROR:
        error = 1;
        break;
    case SESS_PENDING:
        rc = osql_sess_test_slow(tran->sess);
        if (rc)
            return rc;
        break;
    }

    /* please stop !!! */
    if (db_is_stopped()) {
        if (stop_time == 0) {
            stop_time = comdb2_time_epoch();
        } else {
            if (stop_time + gbl_blocksql_grace /*seconds grace time*/ <=
                comdb2_time_epoch()) {
                logmsg(LOGMSG_ERROR,
                       "blocksql session closing early, db stopped\n");
                return ERR_NOMASTER;
            }
        }
    }

    if (bdb_lock_desired(thedb->bdb_env)) {
        logmsg(LOGMSG_ERROR, "%lu %s:%d blocksql session closing early\n",
               pthread_self(), __FILE__, __LINE__);
        err->blockop_num = 0;
        err->errcode = ERR_NOMASTER;
        err->ixnum = 0;
        return ERR_NOMASTER;
    }

    if (error) {
        iq->errstat.errval = xerr->errval;
        errstat_cat_str(&iq->errstat, errstat_get_str(xerr));
        return xerr->errval;
    }

    return 0;
}

int sc_set_running(char *table, int running, uint64_t seed, const char *host,
                   time_t time);
void sc_set_downgrading(struct schema_change_type *s);

/**
 * Apply all schema changes and wait for them to finish
 */
int osql_bplog_schemachange(struct ireq *iq)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    int rc = 0;
    int nops = 0;
    struct block_err err;
    struct schema_change_type *sc;

    iq->sc_pending = NULL;
    iq->sc_seed = 0;
    iq->sc_host = 0;
    iq->sc_locked = 0;
    iq->sc_should_abort = 0;

    rc = apply_changes(iq, tran, NULL, &nops, &err, iq->sorese.osqllog,
                       osql_process_schemachange);

    if (rc)
        logmsg(LOGMSG_DEBUG, "apply_changes returns rc %d\n", rc);

    /* wait for all schema changes to finish */
    iq->sc = sc = iq->sc_pending;
    iq->sc_pending = NULL;
    while (sc != NULL) {
        Pthread_mutex_lock(&sc->mtx);
        sc->nothrevent = 1;
        Pthread_mutex_unlock(&sc->mtx);
        iq->sc = sc->sc_next;
        if (sc->sc_rc == SC_COMMIT_PENDING) {
            sc->sc_next = iq->sc_pending;
            iq->sc_pending = sc;
        } else if (sc->sc_rc == SC_MASTER_DOWNGRADE) {
            sc_set_running(sc->tablename, 0, iq->sc_seed, NULL, 0);
            free_schema_change_type(sc);
            rc = ERR_NOMASTER;
        } else if (sc->sc_rc == SC_PAUSED) {
            Pthread_mutex_lock(&sc->mtx);
            sc->sc_rc = SC_DETACHED;
            Pthread_mutex_unlock(&sc->mtx);
        } else if (sc->sc_rc == SC_PREEMPTED) {
            Pthread_mutex_lock(&sc->mtx);
            sc->sc_rc = SC_DETACHED;
            Pthread_mutex_unlock(&sc->mtx);
            rc = ERR_SC;
        } else if (sc->sc_rc != SC_DETACHED) {
            sc_set_running(sc->tablename, 0, iq->sc_seed, NULL, 0);
            if (sc->sc_rc)
                rc = ERR_SC;
            free_schema_change_type(sc);
        }
        sc = iq->sc;
    }
    if (rc)
        csc2_free_all();
    if (rc == ERR_NOMASTER) {
        /* free schema changes that have finished without marking schema change
         * over in llmeta so new master can resume properly */
        struct schema_change_type *next;
        sc = iq->sc_pending;
        while (sc != NULL) {
            next = sc->sc_next;
            if (sc->newdb && sc->newdb->handle) {
                int bdberr = 0;
                sc_set_downgrading(sc);
                bdb_close_only(sc->newdb->handle, &bdberr);
                freedb(sc->newdb);
                sc->newdb = NULL;
            }
            sc_set_running(sc->tablename, 0, iq->sc_seed, NULL, 0);
            free_schema_change_type(sc);
            sc = next;
        }
        iq->sc_pending = NULL;
    }
    logmsg(LOGMSG_INFO, ">>> DDL SCHEMA CHANGE RC %d <<<\n", rc);
    return rc;
}

typedef struct {
    struct ireq *iq;
    void *trans;
    struct block_err *err;
} ckgenid_state_t;

static int pselectv_callback(void *arg, const char *tablename, int tableversion,
                             unsigned long long genid)
{
    ckgenid_state_t *cgstate = (ckgenid_state_t *)arg;
    struct ireq *iq = cgstate->iq;
    void *trans = cgstate->trans;
    struct block_err *err = cgstate->err;
    int rc, bdberr = 0;

    if ((rc = bdb_lock_tablename_read(thedb->bdb_env, tablename, trans)) != 0) {
        if (rc == BDBERR_DEADLOCK) {
            if (iq->debug)
                reqprintf(iq, "LOCK TABLE READ DEADLOCK");
            return RC_INTERNAL_RETRY;
        } else if (rc) {
            if (iq->debug)
                reqprintf(iq, "LOCK TABLE READ ERROR: %d", rc);
            return ERR_INTERNAL;
        }
    }

    if ((rc = osql_set_usedb(iq, tablename, tableversion, 0, err)) != 0) {
        return rc;
    }

    if ((rc = ix_check_genid_wl(iq, trans, genid, &bdberr)) != 0) {
        if (rc != 1) {
            unsigned long long lclgenid = bdb_genid_to_host_order(genid);
            if ((bdberr == 0 && rc == 0) ||
                (bdberr == IX_PASTEOF && rc == -1)) {
                err->ixnum = -1;
                err->errcode = ERR_CONSTR;
                ctrace("constraints error, no genid %llx (%llu)\n", lclgenid,
                       lclgenid);
                reqerrstr(iq, COMDB2_CSTRT_RC_INVL_REC,
                          "constraints error, no genid");
                return ERR_CONSTR;
            }

            if (bdberr != RC_INTERNAL_RETRY) {
                reqerrstr(iq, COMDB2_DEL_RC_INVL_KEY,
                          "unable to find genid =%llx rc=%d", lclgenid, bdberr);
            }

            return bdberr;
        }
    }
    return 0;
}

/**
 * Wait for all pending osql sessions of this transaction to finish
 * Once all finished ok, we apply all the changes
 */
int osql_bplog_commit(struct ireq *iq, void *iq_trans, int *nops,
                      struct block_err *err)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    ckgenid_state_t cgstate = {.iq = iq, .trans = iq_trans, .err = err};
    int rc;

    /* Pre-process selectv's, getting a writelock on rows that are later updated
     */
    if ((rc = osql_process_selectv(((blocksql_tran_t *)iq->blocksql_tran)->sess,
                                   pselectv_callback, &cgstate)) != 0) {
        iq->timings.req_applied = osql_log_time();
        return rc;
    }

    /* apply changes */
    rc = apply_changes(iq, tran, iq_trans, nops, err, iq->sorese.osqllog,
                       osql_process_packet);

    iq->timings.req_applied = osql_log_time();

    return rc;
}

char *osql_sorese_type_to_str(int stype)
{
    char *rtn = "UNKNOWN";

    switch (stype) {
    case 0:
        rtn = "BLOCKSQL";
        break;
    case OSQL_SOCK_REQ:
        rtn = "SOCKSQL";
        break;
    case OSQL_RECOM_REQ:
        rtn = "RECOM";
        break;
    case OSQL_SNAPISOL_REQ:
        rtn = "SNAPISOL";
        break;
    case OSQL_SERIAL_REQ:
        rtn = "SERIAL";
        break;
    }

    return rtn;
}

/**
 * Prints summary for the current osql bp transaction
 *
 */
char *osql_get_tran_summary(struct ireq *iq)
{

    char *ret = NULL;
    char *nametype;

    /* format
       (BLOCKSQL OPS:[4] MIN:[8]msec MAX:[8]msec)
     */
    if (iq->blocksql_tran) {
        blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
        int sz = 128;
        int rtt = 0;
        int tottm = 0;
        int rtrs = 0;

        ret = (char *)malloc(sz);
        if (!ret) {
            logmsg(LOGMSG_ERROR, "%s: failed to allocated %d bytes\n", __func__, sz);
            return NULL;
        }

        if (tran->iscomplete) {
            osql_sess_getsummary(tran->sess, &tottm, &rtt, &rtrs);
        }

        nametype = osql_sorese_type_to_str(iq->sorese.type);

        snprintf(ret, sz, "%s tot=%u rtt=%u rtrs=%u", nametype, tottm, rtt,
                 rtrs);
        ret[sz - 1] = '\0';
    }

    return ret;
}

/**
 * Free all the sessions and free the bplog
 * HACKY: since we want to catch and report long requests in block
 * process, call this function only after reqlog_end_request is called
 * (sltdbt.c)
 */
static pthread_mutex_t kludgelk = PTHREAD_MUTEX_INITIALIZER;
void osql_bplog_free(struct ireq *iq, int are_sessions_linked, const char *func,
                     const char *callfunc, int line)
{
    int rc = 0;
    int bdberr = 0;
    Pthread_mutex_lock(&kludgelk);
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    iq->blocksql_tran = NULL;
    Pthread_mutex_unlock(&kludgelk);

    if (!tran)
        return;

    /* null this here, since the clients of sessions could still use
       sess->iq->blockproc to update the transaction bplog
       osql_close_session ensures there are no further clients
       of sess before removing it from the lookup hash
     */

    /* remove the sessions from repository and free them */
    osql_close_session(iq, &tran->sess, are_sessions_linked, func, callfunc,
                       line);

    /* destroy transaction */
    Pthread_mutex_destroy(&tran->store_mtx);

    rc = bdb_temp_table_close(thedb->bdb_env, tran->db, &bdberr);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: failed close table rc=%d bdberr=%d\n",
               __func__, rc, bdberr);
    } else {
        tran->db = NULL;
    }

    if (tran->db_ins) {
        rc = bdb_temp_table_close(thedb->bdb_env, tran->db_ins, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: failed close table rc=%d bdberr=%d\n",
                   __func__, rc, bdberr);
        }
    }

    free(tran);

    /* free the space for sql strings */
    if (iq->sqlhistory_ptr && iq->sqlhistory_ptr != &iq->sqlhistory[0]) {
        free(iq->sqlhistory_ptr);
        iq->sqlhistory_ptr = NULL;
    }
    iq->sqlhistory_len = 0;
}


const char *osql_reqtype_str(int type)
{   
    assert(0 < type && type < MAX_OSQL_TYPES);
    // copied from enum OSQL_RPL_TYPE
    static const char *typestr[] = {
        "RPLINV",
        "DONE",
        "USEDB",
        "DELREC",
        "INSREC",
        "CLRTBL",
        "QBLOB",
        "UPDREC",
        "XERR",
        "UPDCOLS",
        "DONE_STATS",
        "DBGLOG",
        "RECGENID",
        "UPDSTAT",
        "EXISTS",
        "SERIAL",
        "SELECTV",
        "DONE_SNAP",
        "SCHEMACHANGE",
        "BPFUNC",
        "DBQ_CONSUME",
        "DELETE",
        "INSERT",
        "UPDATE",
        "DELIDX",
        "INSIDX",
        "DBQ_CONSUME_UUID",
        "STARTGEN",
    };
    return typestr[type];
}

void setup_reorder_key(int type, osql_sess_t *sess, unsigned long long rqid,
                       struct ireq *iq, char *rpl, oplog_key_t *key)
{
    key->tbl_idx = USHRT_MAX;
    switch (type) {
    case OSQL_USEDB: {
        /* usedb is always called prior to any other osql event */
        const char *tablename = get_tablename_from_rpl(rqid, rpl, NULL);
        assert(tablename); // table or queue name
        if (tablename && !is_tablename_queue(tablename, strlen(tablename))) {
            strncpy0(sess->tablename, tablename, sizeof(sess->tablename));
            sess->tbl_idx = get_dbtable_idx_by_name(tablename) + 1;
            key->tbl_idx = sess->tbl_idx;

#if DEBUG_REORDER
            logmsg(LOGMSG_DEBUG, "REORDER: tablename='%s' idx=%d\n", tablename,
                   sess->tbl_idx);
#endif
        }
        break;
    }
    case OSQL_RECGENID:
    case OSQL_UPDATE:
    case OSQL_DELETE:
    case OSQL_UPDREC:
    case OSQL_DELREC:
    case OSQL_INSERT:
    case OSQL_INSREC: {
        enum { OSQLCOMM_UUID_RPL_TYPE_LEN = 4 + 4 + 16 };
        unsigned long long genid = 0;
        if (type == OSQL_INSERT || type == OSQL_INSREC) {
            genid = ++sess->ins_seq;
#if DEBUG_REORDER
            logmsg(LOGMSG_DEBUG, "REORDER: INS genid (seq) 0x%llx\n", genid);
#endif
        } else {
            const char *p_buf = rpl + OSQLCOMM_UUID_RPL_TYPE_LEN;
            buf_no_net_get(&genid, sizeof(genid), p_buf, p_buf + sizeof(genid));
#if DEBUG_REORDER
            logmsg(LOGMSG_DEBUG, "REORDER: Received genid 0x%llx\n", genid);
#endif
        }
        sess->last_genid = genid;
        key->is_rec = 1;
    } /* FALL THROUGH TO NEXT CASE */
    case OSQL_QBLOB:
    case OSQL_DELIDX:
    case OSQL_INSIDX:
    case OSQL_UPDCOLS: {
        key->tbl_idx = sess->tbl_idx;
        key->genid = sess->last_genid;
        /* NB: this stripe is only used for ordering, NOT for inserting */
        key->stripe = get_dtafile_from_genid(key->genid);
        assert(key->stripe >= 0);
        break;
    }
    /* This doesn't touch btrees and should be processed first */
    case OSQL_SERIAL:
    case OSQL_SELECTV:
        key->tbl_idx = 0;
        key->genid = 0;
        key->stripe = 0;
        key->is_rec = 0;
        break;
    default:
        break;
    }

    switch (type) {
    case OSQL_QBLOB:
    case OSQL_DELIDX:
    case OSQL_INSIDX:
    case OSQL_UPDCOLS:
        break; /* keep last_is_ins as is */
    case OSQL_UPDATE:
    case OSQL_DELETE:
    case OSQL_UPDREC:
    case OSQL_DELREC:
        sess->last_is_ins = 0;
        break;
    case OSQL_INSERT:
    case OSQL_INSREC:
        sess->last_is_ins = 1;
        break;
    default:
        sess->last_is_ins = 0;
        break;
    }
}

static void send_error_to_replicant(int rqid, const char *host, int errval,
                                    const char *errstr)
{
    sorese_info_t sorese_info = {0};
    struct errstat generr = {0};

    logmsg(LOGMSG_ERROR, "%s: %s\n", __func__, errstr);

    sorese_info.rqid = rqid;
    sorese_info.host = host;
    sorese_info.type = -1; /* I don't need it */

    generr.errval = errval;
    strncpy0(generr.errstr, errstr, sizeof(generr.errstr));

    int rc =
        osql_comm_signal_sqlthr_rc(&sorese_info, &generr, RC_INTERNAL_RETRY);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Failed to signal replicant rc=%d\n", rc);
    }
}

#if DEBUG_REORDER
#define DEBUG_PRINT_TMPBL_SAVING()                                             \
    uuidstr_t mus;                                                             \
    comdb2uuidstr(uuid, mus);                                                  \
    logmsg(                                                                    \
        LOGMSG_DEBUG,                                                          \
        "%p:%s: rqid=%llx uuid=%s REORDER: SAVING %s tp=%d(%s), tbl_idx=%d,"   \
        "stripe=%d, genid=0x%llx, seq=%d, is_rec=%d\n",                        \
        (void *)pthread_self(), __func__, rqid, mus,                           \
        (tmptbl == tran->db_ins ? "(INS) " : " "), type,                       \
        osql_reqtype_str(type), key.tbl_idx, key.stripe, key.genid, key.seq,   \
        key.is_rec);

#else
#define DEBUG_PRINT_TMPBL_SAVING()
#endif

/**
 * Inserts the op in the iq oplog
 * If sql processing is local, this is called by sqlthread
 * If sql processing is remote, this is called by reader_thread for the offload
 *node
 * Returns 0 if success
 *
 */
int osql_bplog_saveop(osql_sess_t *sess, char *rpl, int rplen,
                      unsigned long long rqid, uuid_t uuid, int type)
{
#if DEBUG_REORDER
    logmsg(LOGMSG_DEBUG, "REORDER: saving for sess %p\n", sess);
#endif
    blocksql_tran_t *tran = (blocksql_tran_t *)osql_sess_getbptran(sess);
    if (!tran || !tran->db) {
        /* something has gone wrong dispatching the socksql request, ignoring
           when the bplog creation failed, the caller is responsible for
           notifying the source that the session has gone awry
         */
        return 0;
    }

    struct ireq *iq = osql_session_get_ireq(sess);
    int rc = 0, rc_op = 0;
    oplog_key_t key = {0};
    int bdberr;
    int debug = 0;

    if (type == OSQL_SCHEMACHANGE)
        iq->tranddl++;

    assert(sess->rqid == rqid);
    key.seq = sess->seq;
#if DEBUG_REORDER
    uuidstr_t us;
    comdb2uuidstr(uuid, us);
    DEBUGMSG("uuid=%s type=%d (%s) seq=%lld\n", us, type,
             osql_reqtype_str(type), sess->seq);
#endif

    /* add the op into the temporary table */
    Pthread_mutex_lock(&tran->store_mtx);

    /* make sure that the temp table is empty since commit
     * retries will use same rqid */
    if (sess->seq == 0 && tran->rows > 0) {
        logmsg(LOGMSG_FATAL,
               "Malformed transaction, received duplicated first row");
        abort();
    }

    if (type == OSQL_USEDB &&
        (sess->selectv_writelock_on_update || sess->is_reorder_on)) {
        int tableversion = 0;
        const char *tablename =
            get_tablename_from_rpl(rqid, rpl, &tableversion);
        sess->table = intern(tablename);
        sess->tableversion = tableversion;
    }

    if (sess->selectv_writelock_on_update)
        osql_cache_selectv(type, sess, rqid, rpl);

    struct temp_table *tmptbl = tran->db;
    if (sess->is_reorder_on) {
        setup_reorder_key(type, sess, rqid, iq, rpl, &key);
        if (sess->last_is_ins && tran->db_ins) { // insert into ins temp table
            tmptbl = tran->db_ins;
        }
    }

    DEBUG_PRINT_TMPBL_SAVING();

    ACCUMULATE_TIMING(CHR_TMPSVOP,
                      rc_op = bdb_temp_table_put(thedb->bdb_env, tmptbl, &key,
                                                 sizeof(key), rpl, rplen, NULL,
                                                 &bdberr););

    if (rc_op) {
        logmsg(LOGMSG_ERROR, "%s: fail to put oplog seq=%llu rc=%d bdberr=%d\n",
               __func__, sess->seq, rc_op, bdberr);
    } else if (gbl_osqlpfault_threads) {
        osql_page_prefault(rpl, rplen, &(tran->last_db),
                           &(osql_session_get_ireq(sess)->osql_step_ix), rqid,
                           uuid, sess->seq);
    }

    tran->rows++;

    Pthread_mutex_unlock(&tran->store_mtx);

    if (rc_op)
        return rc_op;

    struct errstat *xerr;
    /* check if type is done */
    rc = osql_comm_is_done(type, rpl, rplen, rqid == OSQL_RQID_USE_UUID, &xerr,
                           osql_session_get_ireq(sess));
    if (rc == 0)
        return 0;

    // only OSQL_DONE_SNAP, OSQL_DONE, OSQL_DONE_STATS, and OSQL_XERR
    // are processed beyond this point

    if (type != OSQL_XERR) { // if tran not aborted
        int numops = osql_get_replicant_numops(rpl, rqid == OSQL_RQID_USE_UUID);
#ifndef NDEBUG
        uuidstr_t us;
        comdb2uuidstr(uuid, us);
        DEBUGMSG("uuid=%s type %s numops=%d, seq=%lld %s\n", us,
                 osql_reqtype_str(type), numops, sess->seq,
                 (numops != sess->seq + 1 ? "NO match" : ""));
#endif

        if (gbl_osql_check_replicant_numops && numops != sess->seq + 1) {
            send_error_to_replicant(
                rqid, sess->offhost, RC_INTERNAL_RETRY,
                "Master received inconsistent number of opcodes");

            logmsg(LOGMSG_ERROR,
                   "%s: Replicant sent %d opcodes, master received %lld\n",
                   __func__, numops, sess->seq + 1);

            // TODO: terminate session so replicant can retry
            // or mark session bad so don't process this session from toblock
            // osql_sess_try_terminate(sess);
            // return 0;
            abort(); // for now abort to catch failure cases
        }
    }

    /* TODO: check the generation and fail early if it does not match */

    osql_sess_set_complete(rqid, uuid, sess, xerr);

    /* if we received a too early, check the coherency and mark blackout node */
    if (xerr && xerr->errval == OSQL_TOOEARLY) {
        osql_comm_blkout_node(sess->offhost);
    }

    debug = debug_this_request(gbl_debug_until);
    if (gbl_who > 0 && gbl_debug) {
        debug = 1;
    }

    if (osql_session_is_sorese(sess)) {
        osql_sess_lock(sess);
        osql_sess_lock_complete(sess);
        if (!osql_sess_dispatched(sess) && !osql_sess_is_terminated(sess)) {
            osql_session_set_ireq(sess, NULL);
            osql_sess_set_dispatched(sess, 1);
            rc = handle_buf_sorese(thedb, iq, debug);
        }
        osql_sess_unlock_complete(sess);
        osql_sess_unlock(sess);

        return rc;
    }

    return 0;
}


/**
 * Construct a blockprocessor transaction buffer containing
 * a sock sql /recom /snapisol /serial transaction
 *
 * Note: sqlqret is the location where we saved the query
 *       This will allow let us point directly to the buffer
 *
 */
static int wordoff(int byteoff) { return (byteoff + 3) / 4; }
int osql_bplog_build_sorese_req(uint8_t *p_buf_start,
                                const uint8_t **pp_buf_end, const char *sqlq,
                                int sqlqlen, const char *tzname, int reqtype,
                                char **sqlqret, int *sqlqlenret,
                                unsigned long long rqid, uuid_t uuid)
{
    struct dbtable *db;

    struct req_hdr req_hdr;
    struct block_req req;
    struct packedreq_hdr op_hdr;
    struct packedreq_usekl usekl;
    struct packedreq_tzset tzset;
    struct packedreq_sql sql;
    struct packedreq_seq seq;
    struct packedreq_seq2 seq2;
    int *p_rqid = (int *)&rqid;
    int seed;

    uint8_t *p_buf;

    uint8_t *p_buf_req_start;
    const uint8_t *p_buf_req_end;

    uint8_t *p_buf_op_hdr_start;
    const uint8_t *p_buf_op_hdr_end;

    p_buf = p_buf_start;

    /*
     * req hdr (prccom hdr)
     */

    /* build prccom header */
    bzero(&req_hdr, sizeof(req_hdr));
    req_hdr.opcode = OP_BLOCK;

    /* pack prccom header */
    if (!(p_buf = req_hdr_put(&req_hdr, p_buf, *pp_buf_end)))
        return -1;

    /*
     * req
     */

    /* save room in the buffer for req, we need to write it last after we know
     * the proper overall offset */
    if (BLOCK_REQ_LEN > (*pp_buf_end - p_buf))
        return -1;
    p_buf_req_start = p_buf;
    p_buf += BLOCK_REQ_LEN;
    p_buf_req_end = p_buf;

    req.num_reqs = 0; /* will be incremented as we go */

    /*
     * usekl
     */

    /* save room for usekl op header */
    if (PACKEDREQ_HDR_LEN > (*pp_buf_end - p_buf))
        return -1;
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    p_buf_op_hdr_end = p_buf;

    /* provide db[0], it doesn't matter anyway */
    db = &thedb->static_table;
    usekl.dbnum = db->dbnum;
    usekl.taglen = strlen(db->tablename) + 1 /*NUL byte*/;

    /* pack usekl */
    if (!(p_buf = packedreq_usekl_put(&usekl, p_buf, *pp_buf_end)))
        return -1;
    if (!(p_buf =
              buf_no_net_put(db->tablename, usekl.taglen, p_buf, *pp_buf_end)))
        return -1;

    /* build usekl op hdr */
    ++req.num_reqs;
    op_hdr.opcode = BLOCK2_USE;
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    /* pad to the next word */
    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, *pp_buf_end)))
        return -1;

    /* pack usekl op hdr in the space we saved */
    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return -1;

    /*
     * tzset
     */

    /* save room for tzset op header */
    if (PACKEDREQ_HDR_LEN > (*pp_buf_end - p_buf))
        return -1;
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    p_buf_op_hdr_end = p_buf;

    /* build tzset */
    tzset.tznamelen = strlen(tzname) + 1 /*NUL byte*/;

    /* pack tzset */
    if (!(p_buf = packedreq_tzset_put(&tzset, p_buf, *pp_buf_end)))
        return -1;
    if (!(p_buf = buf_no_net_put(tzname, tzset.tznamelen, p_buf, *pp_buf_end)))
        return -1;

    /* build tzset op hdr */
    ++req.num_reqs;
    op_hdr.opcode = BLOCK2_TZ;
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    /* pad to the next word */
    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, *pp_buf_end)))
        return -1;

    /* pack usekl op hdr in the space we saved */
    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return -1;

    /*
     * sql
     */

    /* save room for sql op header */
    if (PACKEDREQ_HDR_LEN > (*pp_buf_end - p_buf))
        return -1;
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    p_buf_op_hdr_end = p_buf;

    /* bulid sql */
    sql.sqlqlen = sqlqlen;
    /* include all sql if requested for longreq logging */
    if (sqlqlen > 256 && !gbl_enable_osql_longreq_logging)
        sql.sqlqlen = 256;
    /* must make sure the packed sqlq is NUL terminated later on */

    /* pack sql */
    if (!(p_buf = packedreq_sql_put(&sql, p_buf, *pp_buf_end)))
        return -1;
    *sqlqret =
        (char *)p_buf; /* save location of the sql string in the buffer */
    *sqlqlenret = sql.sqlqlen;
    if (!(p_buf = buf_no_net_put(sqlq, sql.sqlqlen, p_buf, *pp_buf_end)))
        return -1;

    /* build sql op hdr */
    ++req.num_reqs;
    op_hdr.opcode = req2blockop(reqtype);
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    /* TODO NUL terminate sql here if it was truncated earlier */
    /* pad to the next word */
    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, *pp_buf_end)))
        return -1;

    /* pack sql op hdr in the space we saved */
    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return -1;

    /*
     * blkseq
     */
    p_buf_op_hdr_start = p_buf;
    p_buf += PACKEDREQ_HDR_LEN;
    if (rqid == OSQL_RQID_USE_UUID) {
        if (PACKEDREQ_SEQ2_LEN > (*pp_buf_end - p_buf))
            return -1;
    } else {
        if (PACKEDREQ_SEQ_LEN > (*pp_buf_end - p_buf))
            return -1;
    }
    p_buf_op_hdr_end = p_buf;

    ++req.num_reqs;
    if (rqid == OSQL_RQID_USE_UUID) {
        comdb2uuidcpy(seq2.seq, uuid);

        if (!(p_buf = packedreq_seq2_put(&seq2, p_buf, *pp_buf_end)))
            return -1;

        op_hdr.opcode = BLOCK2_SEQV2;
    } else {
        seq.seq1 = p_rqid[0];
        seq.seq2 = p_rqid[1];
        seed = seq.seq1 ^ seq.seq2;
        seq.seq3 = rand_r((unsigned int *)&seed);

        if (!(p_buf = packedreq_seq_put(&seq, p_buf, *pp_buf_end)))
            return -1;

        op_hdr.opcode = BLOCK_SEQ;
    }

    /* blkseq header */
    op_hdr.nxt = one_based_word_offset_from_ptr(p_buf_start, p_buf);

    /* pad to the next word */
    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(p_buf_start, op_hdr.nxt) - p_buf,
              p_buf, *pp_buf_end)))
        return -1;

    /* pack seq op hdr in the space we saved */
    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return -1;

    /*
     * req
     */

    /* build req */
    req.flags = BLKF_ERRSTAT; /* we want error stat */
    req.offset = op_hdr.nxt;  /* overall offset = next offset of last op */

    /* pack req in the space we saved at the start */
    if (block_req_put(&req, p_buf_req_start, p_buf_req_end) != p_buf_req_end)
        return -1;

    *pp_buf_end = p_buf;
    return 0;
}

/**
 * Set parallelism threshold
 *
 */
void osql_bplog_setlimit(int limit) { g_osql_blocksql_parallel_max = limit; }

/************************* INTERNALS
 * ***************************************************/


/* initialize the ins tmp table pointer to the first item if any
 */
static inline int init_ins_tbl(struct reqlogger *reqlogger,
                               struct temp_cursor *dbc_ins,
                               oplog_key_t **opkey_ins, uint8_t *add_stripe_p,
                               int *bdberr)
{
    if (!dbc_ins)
        return 0;

    int rc_ins = bdb_temp_table_first(thedb->bdb_env, dbc_ins, bdberr);
    if (rc_ins && rc_ins != IX_EMPTY && rc_ins != IX_NOTFND) {
        reqlog_set_error(reqlogger, "bdb_temp_table_first failed", rc_ins);
        logmsg(LOGMSG_ERROR,
               "%s: bdb_temp_table_first failed rc_ins=%d bdberr=%d\n",
               __func__, rc_ins, *bdberr);
        return rc_ins;
    }
    if (rc_ins == 0)
        *opkey_ins = (oplog_key_t *)bdb_temp_table_key(dbc_ins);

    // active stripe is set from toblock_outer, that's where the adds will go
    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_ROUND_ROBIN_STRIPES))
        *add_stripe_p = bdb_attr_get(
            thedb->bdb_attr, BDB_ATTR_DTASTRIPE); // add after last stripe
    else
        *add_stripe_p = bdb_get_active_stripe(thedb->bdb_env);

    return 0;
}

/* Fetch the data from the appropriate temp table
 * based on drain_adds variable which is initally set to 0
 */
static inline void get_tmptbl_data_and_len(struct temp_cursor *dbc,
                                           struct temp_cursor *dbc_ins,
                                           bool drain_adds, char **data_p,
                                           int *datalen_p)
{
    if (drain_adds) {
        *data_p = bdb_temp_table_data(dbc_ins);
        *datalen_p = bdb_temp_table_datasize(dbc_ins);
    } else {
        *data_p = bdb_temp_table_data(dbc);
        *datalen_p = bdb_temp_table_datasize(dbc);
    }
}

/* utility function to determine if row from ins tmp tbl sorts less than
 * row from normal temp tbl:
 * if key_ins tbl_idx < key tbl_idx return true
 * if tbl_idx is same, return true if add_stripe < opkey->stripe
 * return false otherwise
 */
static inline bool ins_is_less(oplog_key_t *opkey_ins, oplog_key_t *opkey,
                               int add_stripe)
{
    if (!opkey_ins)
        return false;

    return opkey_ins->tbl_idx < opkey->tbl_idx ||
           (opkey_ins->tbl_idx == opkey->tbl_idx && add_stripe < opkey->stripe);
}

/* Get the next record from either the ins tmp table or from the 
 * generic one, depending on from where we read previous record,
 * and on which entry is smaller (ins tmp tbl, or normal tmp tbl). 
 * Drain adds in add_stripe only after doing the upd/dels for that stripe
 */
static inline int
get_next_merge_tmps(struct temp_cursor *dbc, struct temp_cursor *dbc_ins,
                    oplog_key_t **opkey, oplog_key_t **opkey_ins,
                    bool *drain_adds_p, int *bdberr, int add_stripe)
{
    if (*drain_adds_p) {
        int rc = bdb_temp_table_next(thedb->bdb_env, dbc_ins, bdberr);
        if (rc != 0) { // ins tbl contains no more records
            *drain_adds_p = false;
            *opkey_ins = NULL;
            return 0;
        }

        *opkey_ins = (oplog_key_t *)bdb_temp_table_key(dbc_ins);
        if (!ins_is_less(*opkey_ins, *opkey, add_stripe))
            *drain_adds_p = false;
    } else {
        int rc = bdb_temp_table_next(thedb->bdb_env, dbc, bdberr);
        if (rc)
            return rc;

        *opkey = (oplog_key_t *)bdb_temp_table_key(dbc);
        /* if cursor valid for dbc_ins, and if we changed table/stripe and
         * prev table/strip match dbc_ins table/stripe then process adds */
        if (ins_is_less(*opkey_ins, *opkey, add_stripe))
            *drain_adds_p = true;
    }
    return 0;
}

#if DEBUG_REORDER
#define DEBUG_PRINT_TMPBL_READ()                                               \
    uuidstr_t mus;                                                             \
    comdb2uuidstr(uuid, mus);                                                  \
    uint8_t *p_buf = (uint8_t *)data;                                          \
    int type = 0;                                                              \
    buf_get(&type, sizeof(type), p_buf, p_buf + sizeof(type));                 \
    oplog_key_t *k_ptr = opkey;                                                \
    if (drain_adds) {                                                          \
        k_ptr = opkey_ins;                                                     \
    }                                                                          \
    logmsg(LOGMSG_DEBUG,                                                       \
           "%p:%s: rqid=%llx uuid=%s REORDER: READ %s tp=%d(%s), tbl_idx=%d, " \
           "stripe=%d, "                                                       \
           "genid=0x%llx, seq=%d, is_rec=%d\n",                                \
           (void *)pthread_self(), __func__, rqid, mus,                        \
           (k_ptr == opkey_ins ? "(ins)" : " "), type, osql_reqtype_str(type), \
           k_ptr->tbl_idx, k_ptr->stripe, k_ptr->genid, k_ptr->seq,            \
           k_ptr->is_rec);

#else
#define DEBUG_PRINT_TMPBL_READ()
#endif

static int process_this_session(
    struct ireq *iq, void *iq_tran, osql_sess_t *sess, int *bdberr, int *nops,
    struct block_err *err, SBUF2 *logsb, struct temp_cursor *dbc,
    struct temp_cursor *dbc_ins,
    int (*func)(struct ireq *, unsigned long long, uuid_t, void *, char **, int,
                int *, int **, blob_buffer_t blobs[MAXBLOBS], int,
                struct block_err *, int *, SBUF2 *))
{
    unsigned long long rqid = osql_sess_getrqid(sess);
    int countops = 0;
    int lastrcv = 0;
    int rc = 0, rc_out = 0;
    int *updCols = NULL;

    /* session info */
    blob_buffer_t blobs[MAXBLOBS] = {{0}};
    int step = 0;
    int receivedrows = 0;
    int flags = 0;
    uuid_t uuid;

    iq->queryid = osql_sess_queryid(sess);

    osql_sess_getuuid(sess, uuid);

    if (rqid != OSQL_RQID_USE_UUID)
        reqlog_set_rqid(iq->reqlogger, &rqid, sizeof(unsigned long long));
    else
        reqlog_set_rqid(iq->reqlogger, uuid, sizeof(uuid));
    reqlog_set_event(iq->reqlogger, "txn");

#if DEBUG_REORDER
    // if needed to check content of socksql temp table, dump with:
    void bdb_temp_table_debug_dump(bdb_state_type * bdb_state,
                                   tmpcursor_t * cur);
    bdb_temp_table_debug_dump(thedb->bdb_env, dbc);
    if (dbc_ins)
        bdb_temp_table_debug_dump(thedb->bdb_env, dbc_ins);
#endif

    /* go through each record */
    rc = bdb_temp_table_first(thedb->bdb_env, dbc, bdberr);
    if (rc && rc != IX_EMPTY && rc != IX_NOTFND) {
        reqlog_set_error(iq->reqlogger, "bdb_temp_table_first failed", rc);
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_first failed rc=%d bdberr=%d\n",
                __func__, rc, *bdberr);
        return rc;
    }

    if (rc == IX_NOTFND) {
        uuidstr_t us;
        comdb2uuidstr(uuid, us);
        logmsg(LOGMSG_ERROR, "%s: session %llx %s has no update rows?\n", __func__,
                rqid, us);
    }

    oplog_key_t *opkey = (oplog_key_t *)bdb_temp_table_key(dbc);
    oplog_key_t *opkey_ins = NULL;
    uint8_t add_stripe = 0;
    bool drain_adds = false; // we always start by reading normal tmp tbl
    rc = init_ins_tbl(iq->reqlogger, dbc_ins, &opkey_ins, &add_stripe, bdberr);
    if (rc)
        return rc;

    while (!rc && !rc_out) {
        char *data = NULL;
        int datalen = 0;
        // fetch the data from the appropriate temp table -- based on drain_adds
        get_tmptbl_data_and_len(dbc, dbc_ins, drain_adds, &data, &datalen);
        /* Reset temp cursor data - it will be freed after the callback. */
        bdb_temp_table_reset_datapointers(drain_adds ? dbc_ins : dbc);
        DEBUG_PRINT_TMPBL_READ();

        if (bdb_lock_desired(thedb->bdb_env)) {
            logmsg(LOGMSG_ERROR, "%lu %s:%d blocksql session closing early\n",
                   pthread_self(), __FILE__, __LINE__);
            err->blockop_num = 0;
            err->errcode = ERR_NOMASTER;
            err->ixnum = 0;
            reqlog_set_error(iq->reqlogger, "ERR_NOMASTER", ERR_NOMASTER);
            return ERR_NOMASTER /*OSQL_FAILDISPATCH*/;
        }

        lastrcv = receivedrows;

        /* this locks pages */
        rc_out = func(iq, rqid, uuid, iq_tran, &data, datalen, &flags, &updCols,
                      blobs, step, err, &receivedrows, logsb);
        free(data);

        if (rc_out != 0 && rc_out != OSQL_RC_DONE) {
            reqlog_set_error(iq->reqlogger, "Error processing", rc_out);
            /* error processing, can be a verify error or deadlock */
            break;
        }

        if (lastrcv != receivedrows && is_rowlocks_transaction(iq_tran)) {
            rowlocks_check_commit_physical(thedb->bdb_env, iq_tran, ++countops);
        }

        step++;
        rc = get_next_merge_tmps(dbc, dbc_ins, &opkey, &opkey_ins, &drain_adds,
                                 bdberr, add_stripe);
    }

    if (iq->osql_step_ix)
        gbl_osqlpf_step[*(iq->osql_step_ix)].step = opkey->seq << 7;

    /* if for some reason the session has not completed correctly,
       this will free the eventually allocated buffers */
    free_blob_buffers(blobs, MAXBLOBS);

    if (updCols)
        free(updCols);

    // should never have both of them set
    assert(rc == 0 || rc_out == 0 || rc_out == OSQL_RC_DONE);

    if (rc != 0 && rc != IX_PASTEOF && rc != IX_EMPTY) {
        reqlog_set_error(iq->reqlogger, "Internal Error", rc);
        logmsg(LOGMSG_ERROR, "%s:%d bdb_temp_table_next failed rc=%d bdberr=%d\n",
                __func__, __LINE__, rc, *bdberr);
        rc_out = ERR_INTERNAL;
        /* fall-through */
    }

    if (rc_out == OSQL_RC_DONE) {
        *nops += receivedrows;
        rc_out = 0;
    }

    return rc_out;
}

/**
 * Log the strings for each completed blocksql request for the
 * reqlog
 */
int osql_bplog_reqlog_queries(struct ireq *iq)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    if (tran->iscomplete)
        osql_sess_reqlogquery(tran->sess, iq->reqlogger);
    return 0;
}

static int apply_changes(struct ireq *iq, blocksql_tran_t *tran, void *iq_tran,
                         int *nops, struct block_err *err, SBUF2 *logsb,
                         int (*func)(struct ireq *, unsigned long long, uuid_t,
                                     void *, char **, int, int *, int **,
                                     blob_buffer_t blobs[MAXBLOBS], int,
                                     struct block_err *, int *, SBUF2 *))
{

    int rc = 0;
    int out_rc = 0;
    int bdberr = 0;
    struct temp_cursor *dbc = NULL;
    struct temp_cursor *dbc_ins = NULL;

    /* lock the table (it should get no more access anway) */
    Pthread_mutex_lock(&tran->store_mtx);

    *nops = 0;

    /* if we've already had a few verify-failures, add extended checking now */
    if (!iq->vfy_genid_track &&
        iq->sorese.verify_retries >= gbl_osql_verify_ext_chk) {
        iq->vfy_genid_track = 1;
        iq->vfy_genid_hash = hash_init(sizeof(unsigned long long));
        iq->vfy_genid_pool =
            pool_setalloc_init(sizeof(unsigned long long), 0, malloc, free);
    }

    /* clear everything- we are under store_mtx */
    if (iq->vfy_genid_track) {
        hash_clear(iq->vfy_genid_hash);
        pool_clear(iq->vfy_genid_pool);
    }

    /* create a cursor */
    dbc = bdb_temp_table_cursor(thedb->bdb_env, tran->db, NULL, &bdberr);
    if (!dbc || bdberr) {
        Pthread_mutex_unlock(&tran->store_mtx);
        logmsg(LOGMSG_ERROR, "%s: failed to create cursor bdberr = %d\n", __func__,
                bdberr);
        return ERR_INTERNAL;
    }

    if (tran->db_ins) {
        dbc_ins =
            bdb_temp_table_cursor(thedb->bdb_env, tran->db_ins, NULL, &bdberr);
        if (!dbc_ins || bdberr) {
            Pthread_mutex_unlock(&tran->store_mtx);
            logmsg(LOGMSG_ERROR, "%s: failed to create cursor bdberr = %d\n",
                   __func__, bdberr);
            return ERR_INTERNAL;
        }
    }

    listc_init(&iq->bpfunc_lst, offsetof(bpfunc_lstnode_t, linkct));

    /* go through the complete list and apply all the changes */
    if (tran->iscomplete) {
        out_rc = process_this_session(iq, iq_tran, tran->sess, &bdberr, nops,
                                      err, logsb, dbc, dbc_ins, func);
    }

    Pthread_mutex_unlock(&tran->store_mtx);

    /* close the cursor */
    rc = bdb_temp_table_close_cursor(thedb->bdb_env, dbc, &bdberr);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: failed close cursor rc=%d bdberr=%d\n",
               __func__, rc, bdberr);
    }

    if (dbc_ins) {
        rc = bdb_temp_table_close_cursor(thedb->bdb_env, dbc_ins, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: failed close cursor rc=%d bdberr=%d\n",
                   __func__, rc, bdberr);
        }
    }

    return out_rc;
}

static int req2blockop(int reqtype)
{
    switch (reqtype) {
    case OSQL_BLOCK_REQ:
        return BLOCK2_SQL;

    case OSQL_SOCK_REQ:
        return BLOCK2_SOCK_SQL;

    case OSQL_RECOM_REQ:
        return BLOCK2_RECOM;

    case OSQL_SNAPISOL_REQ:
        return BLOCK2_SNAPISOL;

    case OSQL_SERIAL_REQ:
        return BLOCK2_SERIAL;
    }

    logmsg(LOGMSG_ERROR, "%s: unknown request type %d\n", __func__, reqtype);
    {
        int once = 0;
        if (!once) {
            cheap_stack_trace();
            once = 1;
        }
    }

    return OSQL_REQINV;
}

void osql_bplog_time_done(struct ireq *iq)
{
    blocksql_tran_t *tran = (blocksql_tran_t *)iq->blocksql_tran;
    osql_bp_timings_t *tms = &iq->timings;
    char msg[4096];
    int tottm = 0;
    int rtt = 0;
    int rtrs = 0;
    int len;

    if (!gbl_time_osql)
        return;

    if (tran) {
        if (tms->req_sentrc == 0)
            tms->req_sentrc = tms->req_applied;

        snprintf0(
            msg, sizeof(msg),
            "Total %llu (sql=%llu upd=%llu repl=%llu signal=%llu retries=%u) [",
            tms->req_finished - tms->req_received, /* total time */
            tms->req_alldone -
                tms->req_received, /* time to get sql processing done */
            tms->req_applied - tms->req_alldone,    /* time to apply updates */
            tms->req_sentrc - tms->replication_end, /* time to sent rc back to
                                                       sql (non-relevant for
                                                       blocksql*/
            tms->replication_end -
                tms->replication_start, /* time to replicate */
            tms->retries - 1);          /* how many time bplog was retried */
        len = strlen(msg);

        /* these are failed */
        osql_sess_getsummary(tran->sess, &tottm, &rtt, &rtrs);
        snprintf0(msg + len, sizeof(msg) - len,
                  " %s(rqid=%llu time=%u lastrtt=%u retries=%u)",
                  (tran->iscomplete ? "C" : "F"), osql_sess_getrqid(tran->sess),
                  tottm, rtt, rtrs);
        len = strlen(msg);
    }
    logmsg(LOGMSG_USER, "%s]\n", msg);
}

void osql_set_delayed(struct ireq *iq)
{
    if (iq) {
        blocksql_tran_t *t = (blocksql_tran_t *)iq->blocksql_tran;
        if (t)
            t->delayed = 1;
    }
}

int osql_get_delayed(struct ireq *iq)
{
    if (iq) {
        blocksql_tran_t *t = (blocksql_tran_t *)iq->blocksql_tran;
        if (t)
            return t->delayed;
    }
    return 1;
}

/**
 * Throw bplog to /dev/null, sql does not need this
 *
 */
int sql_cancelled_transaction(struct ireq *iq)
{
    int rc = 0;

    logmsg(LOGMSG_DEBUG, "%s: cancelled transaction\n", __func__);
    osql_bplog_free(iq, 1, __func__, NULL, 0);

    if (iq->p_buf_orig) {
        /* nothing changed this siince init_ireq */
        free(iq->p_buf_orig);
        iq->p_buf_orig = NULL;
    }

    destroy_ireq(thedb, iq);

    return rc;
}

int backout_schema_changes(struct ireq *iq, tran_type *tran);
void *osql_commit_timepart_resuming_sc(void *p)
{
    struct ireq iq;
    struct schema_change_type *sc_pending = (struct schema_change_type *)p;
    struct schema_change_type *sc;
    tran_type *parent_trans = NULL;
    int error = 0;
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
    init_fake_ireq(thedb, &iq);
    iq.tranddl = 1;
    iq.sc = sc = sc_pending;
    sc_pending = NULL;
    while (sc != NULL) {
        Pthread_mutex_lock(&sc->mtx);
        sc->nothrevent = 1;
        Pthread_mutex_unlock(&sc->mtx);
        iq.sc = sc->sc_next;
        if (sc->sc_rc == SC_COMMIT_PENDING) {
            sc->sc_next = sc_pending;
            sc_pending = sc;
        } else {
            logmsg(LOGMSG_ERROR, "%s: shard '%s', rc %d\n", __func__,
                   sc->tablename, sc->sc_rc);
            sc_set_running(sc->tablename, 0, 0, NULL, 0);
            free_schema_change_type(sc);
            error = 1;
        }
        sc = iq.sc;
    }
    iq.sc_pending = sc_pending;
    iq.sc_locked = 0;
    iq.osql_flags |= OSQL_FLAGS_SCDONE;
    iq.sc_tran = NULL;
    iq.sc_logical_tran = NULL;

    if (error) {
        logmsg(LOGMSG_ERROR, "%s: Aborting schema change because of errors\n",
               __func__);
        goto abort_sc;
    }

    if (trans_start_logical_sc(&iq, &(iq.sc_logical_tran))) {
        logmsg(LOGMSG_ERROR,
               "%s:%d failed to start schema change transaction\n", __func__,
               __LINE__);
        goto abort_sc;
    }

    if ((parent_trans = bdb_get_physical_tran(iq.sc_logical_tran)) == NULL) {
        logmsg(LOGMSG_ERROR,
               "%s:%d failed to start schema change transaction\n", __func__,
               __LINE__);
        goto abort_sc;
    }

    if (trans_start(&iq, parent_trans, &(iq.sc_tran))) {
        logmsg(LOGMSG_ERROR,
               "%s:%d failed to start schema change transaction\n", __func__,
               __LINE__);
        goto abort_sc;
    }

    iq.sc = iq.sc_pending;
    while (iq.sc != NULL) {
        if (!iq.sc_locked) {
            /* Lock schema from now on before we finalize any schema changes
             * and hold on to the lock until the transaction commits/aborts.
             */
            wrlock_schema_lk();
            iq.sc_locked = 1;
        }
        if (iq.sc->db)
            iq.usedb = iq.sc->db;
        if (finalize_schema_change(&iq, iq.sc_tran)) {
            logmsg(LOGMSG_ERROR, "%s: failed to finalize '%s'\n", __func__,
                   iq.sc->tablename);
            goto abort_sc;
        }
        iq.usedb = NULL;
        iq.sc = iq.sc->sc_next;
    }

    if (iq.sc_pending) {
        create_sqlmaster_records(iq.sc_tran);
        create_sqlite_master();
    }

    if (trans_commit(&iq, iq.sc_tran, gbl_mynode)) {
        logmsg(LOGMSG_FATAL, "%s:%d failed to commit schema change\n", __func__,
               __LINE__);
        abort();
    }

    unlock_schema_lk();

    if (trans_commit_logical(&iq, iq.sc_logical_tran, gbl_mynode, 0, 1, NULL, 0,
                             NULL, 0)) {
        logmsg(LOGMSG_FATAL, "%s:%d failed to commit schema change\n", __func__,
               __LINE__);
        abort();
    }

    osql_postcommit_handle(&iq);

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

    return NULL;

abort_sc:
    logmsg(LOGMSG_ERROR, "%s: aborting schema change\n", __func__);
    if (iq.sc_tran)
        trans_abort(&iq, iq.sc_tran);
    backout_schema_changes(&iq, parent_trans);
    if (iq.sc_locked) {
        unlock_schema_lk();
        iq.sc_locked = 0;
    }
    if (parent_trans)
        trans_abort(&iq, parent_trans);
    if (iq.sc_logical_tran)
        trans_abort_logical(&iq, iq.sc_logical_tran, NULL, 0, NULL, 0);
    osql_postabort_handle(&iq);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);
    return NULL;
}
