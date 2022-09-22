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
 * Each sql transaction creates a logical list of writes, a.k.a. bplog,
 * that is send and accumulated on the master in a session
 * Once the last bplog message is received, the bplog can be passed to
 * a block processor that runs the actual transaction
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
#include <unistd.h>

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
#include "reqlog.h"
#include "time_accounting.h"
#include "ctrace.h"
#include "intern_strings.h"
#include "sc_global.h"
#include "schemachange.h"
#include "gettimeofday_ms.h"

extern int gbl_reorder_idx_writes;
extern uint32_t gbl_max_time_per_txn_ms;


struct blocksql_tran {
    pthread_mutex_t store_mtx; /* mutex for db access - those are non-env dbs */
    struct temp_table *db_ins; /* keeps the list of INSERT ops for a session */
    struct temp_table *db;     /* keeps the list of all OTHER ops */

    pthread_mutex_t mtx;

    int seq; /* counting ops saved */
    int is_uuid;

    /* selectv caches */
    int is_selectv_wl_upd;
    hash_t *selectv_genids;
    int tableversion;

    /* reorder */
    int is_reorder_on;
    char *tablename; /* remember tablename in saveop for reordering */
    uint16_t tbl_idx;
    unsigned long long last_genid; /* remember updrec/insrec genid for qblobs */
    unsigned long long
        ins_seq;      /* remember key seq for inserts into ins tmptbl */
    int last_is_ins; /* 1 if processing INSERT, 0 for any other oql type */

    /* prefetch */
    struct dbtable *last_db;
};

typedef struct oplog_key {
    uint16_t tbl_idx;
    uint8_t stripe;
    unsigned long long genid;
    uint8_t is_rec; // 1 for record because it needs to go after blobs
    uint32_t seq;   // record and blob will share the same seq
} oplog_key_t;

typedef struct {
    char *tablename;
    unsigned long long genid;
    int tableversion;
    int get_writelock;
} selectv_genid_t;

int gbl_selectv_writelock_on_update = 1;

static int apply_changes(struct ireq *iq, blocksql_tran_t *tran, void *iq_tran,
                         int *nops, struct block_err *err,
                         int (*func)(struct ireq *, unsigned long long, uuid_t,
                                     void *, char **, int, int *, int **,
                                     blob_buffer_t blobs[MAXBLOBS], int,
                                     struct block_err *, int *));
static int req2blockop(int reqtype);
extern const char *get_tablename_from_rpl(int is_uuid, const char *rpl,
                                          int *tableversion);
extern void live_sc_off(struct dbtable * db);

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
blocksql_tran_t *osql_bplog_create(int is_uuid, int is_reorder)
{

    blocksql_tran_t *tran = NULL;
    int bdberr = 0;

    tran = calloc(sizeof(blocksql_tran_t), 1);
    if (!tran) {
        logmsg(LOGMSG_ERROR, "%s: error allocating %zu bytes\n", __func__,
               sizeof(blocksql_tran_t));
        return NULL;
    }

    tran->is_uuid = is_uuid;
    Pthread_mutex_init(&tran->store_mtx, NULL);

    /* init temporary table and cursor */
    tran->db = bdb_temp_array_create(thedb->bdb_env, &bdberr);
    if (!tran->db || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to create temp table bdberr=%d\n",
               __func__, bdberr);
        free(tran);
        return NULL;
    }

    bdb_temp_table_set_cmp_func(tran->db, osql_bplog_key_cmp);

    tran->is_reorder_on = is_reorder;
    if (tran->is_reorder_on) {
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

    tran->is_selectv_wl_upd = gbl_selectv_writelock_on_update;
    if (tran->is_selectv_wl_upd)
        tran->selectv_genids =
            hash_init(offsetof(selectv_genid_t, get_writelock));

    return tran;
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

typedef struct {
    int (*wr_sv)(void *, const char *tablename, int tableversion,
                 unsigned long long genid);
    void *arg;
} sv_hf_args;

static int process_selectv(void *obj, void *arg)
{
    sv_hf_args *hf_args = (sv_hf_args *)arg;
    selectv_genid_t *sgenid = (selectv_genid_t *)obj;
    if (sgenid->get_writelock) {
        return (*hf_args->wr_sv)(hf_args->arg, sgenid->tablename,
                                 sgenid->tableversion, sgenid->genid);
    }
    return 0;
}

static int osql_process_selectv(blocksql_tran_t *tran,
                                int (*wr_sv)(void *arg, const char *tablename,
                                             int tableversion,
                                             unsigned long long genid),
                                void *wr_selv_arg)
{
    sv_hf_args hf_args = {.wr_sv = wr_sv, .arg = wr_selv_arg};
    if (!tran->is_selectv_wl_upd)
        return 0;
    return hash_for(tran->selectv_genids, process_selectv, &hf_args);
}

/**
 * Wait for all pending osql sessions of this transaction to finish
 * Once all finished ok, we apply all the changes
 */
int osql_bplog_commit(struct ireq *iq, void *iq_trans, int *nops,
                      struct block_err *err)
{
    blocksql_tran_t *tran = iq->sorese->tran;
    ckgenid_state_t cgstate = {.iq = iq, .trans = iq_trans, .err = err};
    int rc;

    /* Pre-process selectv's, getting a writelock on rows that are later updated
     */
    if ((rc = osql_process_selectv(tran, pselectv_callback, &cgstate)) != 0) {
        iq->timings.req_applied = osql_log_time();
        return rc;
    }

    /* apply changes */
    rc = apply_changes(iq, tran, iq_trans, nops, err, osql_process_packet);

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

static int free_selectv_genids(void *obj, void *arg)
{
    free(obj);
    return 0;
}

/**
 * Free the bplog
 */
void osql_bplog_close(blocksql_tran_t **ptran)
{
    blocksql_tran_t *tran = *ptran;
    int rc = 0;
    int bdberr = 0;

    *ptran = NULL;

    /* code ensures that only one thread will run this code
    (reader 1, terminator 2, or blockproc 3)*/
    if (tran->is_selectv_wl_upd) {
        hash_for(tran->selectv_genids, free_selectv_genids, NULL);
        hash_clear(tran->selectv_genids);
        hash_free(tran->selectv_genids);
    }

    Pthread_mutex_destroy(&tran->store_mtx);

    rc = bdb_temp_table_close(thedb->bdb_env, tran->db, &bdberr);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: failed close table rc=%d bdberr=%d\n",
               __func__, rc, bdberr);
    }

    if (tran->db_ins) {
        rc = bdb_temp_table_close(thedb->bdb_env, tran->db_ins, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: failed close ins table rc=%d bdberr=%d\n",
                   __func__, rc, bdberr);
        }
    }

    free(tran);
}

static void setup_reorder_key(blocksql_tran_t *tran, int type,
                              osql_sess_t *sess, unsigned long long rqid,
                              char *rpl, oplog_key_t *key)
{
    char *tablename = tran->tablename;

    key->tbl_idx = USHRT_MAX;
    switch (type) {
    case OSQL_USEDB: {
        /* usedb is always called prior to any other osql event */
        if (tablename && !is_tablename_queue(tablename)) {
            tran->tbl_idx = get_dbtable_idx_by_name(tablename) + 1;
            key->tbl_idx = tran->tbl_idx;

#if DEBUG_REORDER
            logmsg(LOGMSG_DEBUG, "REORDER: tablename='%s' idx=%d\n",
                   sess->tablename, tran->tbl_idx);
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
            genid = ++tran->ins_seq;
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
        tran->last_genid = genid;
        key->is_rec = 1;
    } /* FALL THROUGH TO NEXT CASE */
    case OSQL_QBLOB:
    case OSQL_DELIDX:
    case OSQL_INSIDX:
    case OSQL_UPDCOLS: {
        key->tbl_idx = tran->tbl_idx;
        key->genid = tran->last_genid;
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
        tran->last_is_ins = 0;
        sess->tran_rows++;
        break;
    case OSQL_INSERT:
    case OSQL_INSREC:
        tran->last_is_ins = 1;
        sess->tran_rows++;
        break;
    default:
        tran->last_is_ins = 0;
        break;
    }
}

static void osql_cache_selectv(blocksql_tran_t *tran, char *rpl, int type);

static void _pre_process_saveop(osql_sess_t *sess, blocksql_tran_t *tran,
                                char *rpl, int rplen, int type)
{
    switch (type) {
    case OSQL_SCHEMACHANGE:
        sess->is_tranddl++;
        break;
    case OSQL_USEDB:
        if (tran->is_selectv_wl_upd || tran->is_reorder_on) {
            int tableversion = 0;
            const char *tblname =
                get_tablename_from_rpl(tran->is_uuid, rpl, &tableversion);
            assert(tblname); // table or queue name
            tran->tablename = intern(tblname);
            tran->tableversion = tableversion;
        }
        break;
    case OSQL_BPFUNC:
        if (need_views_lock(rpl, rplen, tran->is_uuid) == 1) {
            sess->is_tptlock = 1;
        }
        break;
    }

    if (tran->is_selectv_wl_upd)
        osql_cache_selectv(tran, rpl, type);
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
int osql_bplog_saveop(osql_sess_t *sess, blocksql_tran_t *tran, char *rpl,
                      int rplen, int type)
{
    int rc = 0;
    oplog_key_t key = {0};
    int bdberr;

#if DEBUG_REORDER
    logmsg(LOGMSG_DEBUG, "REORDER: saving for sess %p\n", sess);
    uuidstr_t us;
    comdb2uuidstr(sess->uuid, us);
    DEBUGMSG("uuid=%s type=%d (%s) seq=%lld\n", us, type, osql_reqtype_str(type), tran->seq);
#endif

    _pre_process_saveop(sess, tran, rpl, rplen, type);

    key.seq = tran->seq;

    /* add the op into the temporary table */
    Pthread_mutex_lock(&tran->store_mtx);

    struct temp_table *tmptbl = tran->db;
    if (tran->is_reorder_on) {
        setup_reorder_key(tran, type, sess, sess->rqid, rpl, &key);
        if (tran->last_is_ins && tran->db_ins) { // insert into ins temp table
            tmptbl = tran->db_ins;
        }
    }

    DEBUG_PRINT_TMPBL_SAVING();

    ACCUMULATE_TIMING(CHR_TMPSVOP,
                      rc = bdb_temp_table_put(thedb->bdb_env, tmptbl, &key,
                                              sizeof(key), rpl, rplen, NULL,
                                              &bdberr););

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: fail to put oplog seq=%u rc=%d bdberr=%d\n",
               __func__, tran->seq, rc, bdberr);
    } else {
        tran->seq++;
        if (gbl_osqlpfault_threads) {
            osql_page_prefault(rpl, rplen, &(tran->last_db),
                               &sess->iq->osql_step_ix, sess->rqid, sess->uuid,
                               tran->seq);
        }
    }

    Pthread_mutex_unlock(&tran->store_mtx);

    return rc;
}

/**
 * Set proper blkseq from session to iq
 * NOTE: We don't need to create buffers _SEQ, _SEQV2 for it
 *
 */
void osql_bplog_set_blkseq(osql_sess_t *sess, struct ireq *iq)
{
    iq->have_blkseq = 1;
    if (sess->rqid == OSQL_RQID_USE_UUID) {
        memcpy(iq->seq, sess->uuid, sizeof(uuid_t));
        iq->seqlen = sizeof(uuid_t);
    } else {
        struct packedreq_seq seq;
        int *p_rqid = (int *)&sess->rqid;
        int seed;
        seq.seq1 = p_rqid[0];
        seq.seq2 = p_rqid[1];
        seed = seq.seq1 ^ seq.seq2;
        seq.seq3 = rand_r((unsigned int *)&seed);

        memcpy(iq->seq, &seq, 3 * sizeof(int));
        iq->seqlen = 3 * sizeof(int);
    }
}

/**
 * Construct a blockprocessor transaction buffer containing
 * a sock sql /recom /snapisol /serial transaction
 *
 * Note: sqlqret is the location where we saved the query
 *       This will allow let us point directly to the buffer
 *
 */
int osql_bplog_build_sorese_req(uint8_t **pp_buf_start,
                                const uint8_t **pp_buf_end, const char *sqlq,
                                int sqlqlen, const char *tzname, int reqtype,
                                unsigned long long rqid, uuid_t uuid)
{
    struct req_hdr req_hdr;
    struct block_req req;
    struct packedreq_hdr op_hdr;
    struct packedreq_sql sql;

    uint8_t *p_buf;

    uint8_t *p_buf_req_start;
    const uint8_t *p_buf_req_end;

    uint8_t *p_buf_op_hdr_start;
    const uint8_t *p_buf_op_hdr_end;

    /* FORMAT:
       op_block + req_flags + block2_sorese */
    int buflen = REQ_HDR_LEN /* op_block */ + BLOCK_REQ_LEN +
                 3 /* req flags */ + PACKEDREQ_HDR_LEN + 3 + PACKEDREQ_SQL_LEN +
                 sqlqlen + 3;

    *pp_buf_start = calloc(1, buflen);
    if (!(*pp_buf_start))
        return -1;
    p_buf = *pp_buf_start;
    *pp_buf_end = (*pp_buf_start) + buflen;

    /*
     * req hdr (prccom hdr)
     * we need this to dispatch to mark this as a write transaction
     * and to arrive in toblock()
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
    /* save room in the buffer for req, until we have the overall offset */
    if (BLOCK_REQ_LEN > (*pp_buf_end - p_buf))
        return -1;
    p_buf_req_start = p_buf;
    p_buf += BLOCK_REQ_LEN;
    p_buf_req_end = p_buf;

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
    if (!(p_buf = buf_no_net_put(sqlq, sql.sqlqlen, p_buf, *pp_buf_end)))
        return -1;

    /* build sql op hdr */
    op_hdr.opcode = req2blockop(reqtype);
    op_hdr.nxt = one_based_word_offset_from_ptr(*pp_buf_start, p_buf);

    /* pad to the next word */
    if (!(p_buf = buf_zero_put(
              ptr_from_one_based_word_offset(*pp_buf_start, op_hdr.nxt) - p_buf,
              p_buf, *pp_buf_end)))
        return -1;

    /* pack sql op hdr in the space we saved */
    if (packedreq_hdr_put(&op_hdr, p_buf_op_hdr_start, p_buf_op_hdr_end) !=
        p_buf_op_hdr_end)
        return -1;

    /*
     * req, written in the original saved space (after OP_BLOCK header)
     */

    /* build req */
    req.flags = BLKF_ERRSTAT; /* we want error stat */
    req.num_reqs = 1;
    req.offset = op_hdr.nxt;  /* overall offset = next offset of last op */

    /* pack req in the space we saved at the start */
    if (block_req_put(&req, p_buf_req_start, p_buf_req_end) != p_buf_req_end)
        return -1;

    *pp_buf_end = p_buf;
    return 0;
}

/************************* INTERNALS ****************************************/

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
                                           int drain_adds, char **data_p,
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
static inline int ins_is_less(oplog_key_t *opkey_ins, oplog_key_t *opkey,
                               int add_stripe)
{
    if (!opkey_ins)
        return 0;

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
                    int *drain_adds_p, int *bdberr, int add_stripe)
{
    if (*drain_adds_p) {
        int rc = bdb_temp_table_next(thedb->bdb_env, dbc_ins, bdberr);
        if (rc != 0) { // ins tbl contains no more records
            *drain_adds_p = 0;
            *opkey_ins = NULL;
            return 0;
        }

        *opkey_ins = (oplog_key_t *)bdb_temp_table_key(dbc_ins);
        if (!ins_is_less(*opkey_ins, *opkey, add_stripe))
            *drain_adds_p = 0;
    } else {
        int rc = bdb_temp_table_next(thedb->bdb_env, dbc, bdberr);
        if (rc)
            return rc;

        *opkey = (oplog_key_t *)bdb_temp_table_key(dbc);
        /* if cursor valid for dbc_ins, and if we changed table/stripe and
         * prev table/strip match dbc_ins table/stripe then process adds */
        if (ins_is_less(*opkey_ins, *opkey, add_stripe))
            *drain_adds_p = 1;
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
    struct block_err *err, struct temp_cursor *dbc, struct temp_cursor *dbc_ins,
    int (*func)(struct ireq *, unsigned long long, uuid_t, void *, char **, int,
                int *, int **, blob_buffer_t blobs[MAXBLOBS], int,
                struct block_err *, int *))
{
    int countops = 0;
    int lastrcv = 0;
    int rc = 0, rc_out = 0;
    int *updCols = NULL;

    /* session info */
    blob_buffer_t blobs[MAXBLOBS] = {{0}};
    int step = 0;
    int receivedrows = 0;
    int flags = 0;

    iq->queryid = osql_sess_queryid(sess);
    if (gbl_max_time_per_txn_ms)
        iq->txn_ttl_ms = gettimeofday_ms() + gbl_max_time_per_txn_ms; 

    if (sess->rqid != OSQL_RQID_USE_UUID)
        reqlog_set_rqid(iq->reqlogger, &sess->rqid, sizeof(unsigned long long));
    else
        reqlog_set_rqid(iq->reqlogger, sess->uuid, sizeof(sess->uuid));
    reqlog_set_event(iq->reqlogger, EV_TXN);

#if DEBUG_REORDER
    logmsg(LOGMSG_DEBUG, "OSQL ");
    // if needed to check content of socksql temp table, dump with:
    void bdb_temp_table_debug_dump(bdb_state_type * bdb_state,
                                   tmpcursor_t * cur, int);
    bdb_temp_table_debug_dump(thedb->bdb_env, dbc, LOGMSG_DEBUG);
    if (dbc_ins) {
        logmsg(LOGMSG_DEBUG, "INS ");
        bdb_temp_table_debug_dump(thedb->bdb_env, dbc_ins, LOGMSG_DEBUG);
    }
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
        comdb2uuidstr(sess->uuid, us);
        logmsg(LOGMSG_ERROR, "%s: session %llx %s has no update rows?\n",
               __func__, sess->rqid, us);
    }

    oplog_key_t *opkey = (oplog_key_t *)bdb_temp_table_key(dbc);
    oplog_key_t *opkey_ins = NULL;
    uint8_t add_stripe = 0;
    int drain_adds = 0; // we always start by reading normal tmp tbl
    rc = init_ins_tbl(iq->reqlogger, dbc_ins, &opkey_ins, &add_stripe, bdberr);
    if (rc)
        return rc;

    /* only reorder indices if more than one row add/upd/dels
     * NB: the idea is that single row transactions can not deadlock but
     * update can have a del/ins index component and can deadlock -- in future 
     * consider reordering for single upd stmts (only if performance 
     * improves so this requires a solid test). */
    if (sess->tran_rows > 1 && gbl_reorder_idx_writes)
        iq->osql_flags |= OSQL_FLAGS_REORDER_IDX_ON;

    while (!rc && !rc_out) {
        char *data = NULL;
        int datalen = 0;
        // fetch the data from the appropriate temp table -- based on drain_adds
        get_tmptbl_data_and_len(dbc, dbc_ins, drain_adds, &data, &datalen);
        /* Reset temp cursor data - it will be freed after the callback. */
        bdb_temp_table_reset_datapointers(drain_adds ? dbc_ins : dbc);
        DEBUG_PRINT_TMPBL_READ();

        if (bdb_lock_desired(thedb->bdb_env)) {
            logmsg(LOGMSG_ERROR, "%p %s:%d blocksql session closing early\n", (void *)pthread_self(), __FILE__,
                   __LINE__);
            err->blockop_num = 0;
            err->errcode = ERR_NOMASTER;
            err->ixnum = 0;
            reqlog_set_error(iq->reqlogger, "ERR_NOMASTER", ERR_NOMASTER);
            return ERR_NOMASTER /*OSQL_FAILDISPATCH*/;
        }

        lastrcv = receivedrows;

        /* This call locks pages:
         * func is osql_process_packet or osql_process_schemachange */
        rc_out = func(iq, sess->rqid, sess->uuid, iq_tran, &data, datalen,
                      &flags, &updCols, blobs, step, err, &receivedrows);
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

static int apply_changes(struct ireq *iq, blocksql_tran_t *tran, void *iq_tran,
                         int *nops, struct block_err *err,
                         int (*func)(struct ireq *, unsigned long long, uuid_t,
                                     void *, char **, int, int *, int **,
                                     blob_buffer_t blobs[MAXBLOBS], int,
                                     struct block_err *, int *))
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
        iq->sorese->verify_retries >= gbl_osql_verify_ext_chk) {
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
    out_rc = process_this_session(iq, iq_tran, iq->sorese, &bdberr, nops, err,
                                  dbc, dbc_ins, func);

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

void osql_bplog_time_done(osql_bp_timings_t *tms)
{
    char msg[4096];

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
        tms->replication_end - tms->replication_start, /* time to replicate */
        tms->retries - 1); /* how many time bplog was retried */
    logmsg(LOGMSG_USER, "%s]\n", msg);
}

static void osql_cache_selectv(blocksql_tran_t *tran, char *rpl, int type)
{
    char *p_buf;
    selectv_genid_t *sgenid, fnd = {0};
    enum {
        OSQLCOMM_UUID_RPL_TYPE_LEN = 4 + 4 + 16,
        OSQLCOMM_RPL_TYPE_LEN = 4 + 4 + 8
    };
    switch (type) {
    case OSQL_UPDATE:
    case OSQL_DELETE:
    case OSQL_UPDREC:
    case OSQL_DELREC:
        p_buf = rpl + (tran->is_uuid ? OSQLCOMM_UUID_RPL_TYPE_LEN
                                     : OSQLCOMM_RPL_TYPE_LEN);
        buf_no_net_get(&fnd.genid, sizeof(fnd.genid), p_buf,
                       p_buf + sizeof(fnd.genid));
        assert(tran->tablename);
        fnd.tablename = tran->tablename;
        fnd.tableversion = tran->tableversion;
        if ((sgenid = hash_find(tran->selectv_genids, &fnd)) != NULL)
            sgenid->get_writelock = 1;
        break;
    case OSQL_RECGENID:
        p_buf = rpl + (tran->is_uuid ? OSQLCOMM_UUID_RPL_TYPE_LEN
                                     : OSQLCOMM_RPL_TYPE_LEN);
        buf_no_net_get(&fnd.genid, sizeof(fnd.genid), p_buf,
                       p_buf + sizeof(fnd.genid));
        assert(tran->tablename);
        fnd.tablename = tran->tablename;
        if (hash_find(tran->selectv_genids, &fnd) == NULL) {
            sgenid = (selectv_genid_t *)calloc(sizeof(*sgenid), 1);
            sgenid->genid = fnd.genid;
            sgenid->tablename = tran->tablename;
            sgenid->tableversion = tran->tableversion;
            sgenid->get_writelock = 0;
            hash_add(tran->selectv_genids, sgenid);
        }
        break;
    }
}

/**
 * Apply all schema changes and wait for them to finish
 */
int bplog_schemachange(struct ireq *iq, blocksql_tran_t *tran, void *err)
{
    int rc = 0;
    int nops = 0;
    struct schema_change_type *sc;

    iq->sc_pending = NULL;
    iq->sc_seed = 0;
    iq->sc_host = 0;
    iq->sc_locked = 0;
    iq->sc_should_abort = 0;

    rc = apply_changes(iq, tran, NULL, &nops, err, osql_process_schemachange);

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
            sc->sc_next = iq->sc_pending;
            iq->sc_pending = sc;
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
            sc->sc_next = iq->sc_pending;
            iq->sc_pending = sc;
            if (sc->sc_rc) {
                rc = ERR_SC;
            }
        }
        sc = iq->sc;
    }
    if (rc)
        csc2_free_all();

    if (rc) {
        /* free schema changes that have finished without marking schema change
         * over in llmeta so new master can resume properly */
        struct schema_change_type *next;
        sc = iq->sc_pending;
        while (sc != NULL) {
            next = sc->sc_next;
            if (sc->newdb && sc->newdb->handle) {
                int bdberr = 0;
                live_sc_off(sc->db);
                while (sc->logical_livesc) {
                    usleep(200);
                }
                if (sc->db->sc_live_logical) {
                    bdb_clear_logical_live_sc(sc->db->handle, 1);
                    sc->db->sc_live_logical = 0;
                }
                if (rc == ERR_NOMASTER)
                    sc_set_downgrading(sc);
                bdb_close_only(sc->newdb->handle, &bdberr);
                freedb(sc->newdb);
                sc->newdb = NULL;
            }
            sc_set_running(iq, sc, sc->tablename, 0, NULL, 0, 0, __func__,
                           __LINE__);
            free_schema_change_type(sc);
            sc = next;
        }
        iq->sc_pending = NULL;
    }
    logmsg(LOGMSG_INFO, ">>> DDL SCHEMA CHANGE RC %d <<<\n", rc);

    assert(!iq->sc_locked);
    iq->sc_locked = 1;
    wrlock_schema_lk();

    return rc;
}

void *bplog_commit_timepart_resuming_sc(void *p)
{
    comdb2_name_thread(__func__);
    struct ireq iq;
    struct schema_change_type *sc_pending = (struct schema_change_type *)p;
    struct schema_change_type *sc;
    tran_type *parent_trans = NULL;
    int error = 0;
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START_RDWR);
    init_fake_ireq(thedb, &iq);
    iq.tranddl = 1;

    /* iq sc_running count */
    sc = sc_pending;
    while (sc != NULL) {
        if (sc->set_running)
            iq.sc_running++;
        sc = sc->sc_next;
    }
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
            sc_set_running(&iq, sc, sc->tablename, 0, NULL, 0, 0, __func__,
                           __LINE__);
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

    if (trans_commit(&iq, iq.sc_tran, gbl_myhostname)) {
        logmsg(LOGMSG_FATAL, "%s:%d failed to commit schema change\n", __func__,
               __LINE__);
        abort();
    }

    unlock_schema_lk();

    if (trans_commit_logical(&iq, iq.sc_logical_tran, gbl_myhostname, 0, 1,
                             NULL, 0, NULL, 0)) {
        logmsg(LOGMSG_FATAL, "%s:%d failed to commit schema change\n", __func__,
               __LINE__);
        abort();
    }
    iq.sc_logical_tran = NULL;

    osql_postcommit_handle(&iq);

    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);

    return NULL;

abort_sc:
    logmsg(LOGMSG_ERROR, "%s: aborting schema change\n", __func__);
    if (iq.sc_tran) {
        trans_abort(&iq, iq.sc_tran);
        iq.sc_tran = NULL;
    }
    backout_schema_changes(&iq, parent_trans);
    if (iq.sc_locked) {
        unlock_schema_lk();
        iq.sc_locked = 0;
    }
    if (parent_trans)
        trans_abort(&iq, parent_trans);
    if (iq.sc_logical_tran) {
        trans_abort_logical(&iq, iq.sc_logical_tran, NULL, 0, NULL, 0);
        iq.sc_logical_tran = NULL;
    }
    osql_postabort_handle(&iq);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE_RDWR);
    return NULL;
}
