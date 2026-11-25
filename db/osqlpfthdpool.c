/*
   Copyright 2020, Bloomberg Finance L.P.

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

#include "comdb2.h"
#include "comdb2_atomic.h"
#include "sc_util.h"

extern const char *get_tablename_from_rpl(int is_uuid, const uint8_t *rpl, int rpllen, int *tableversion);

struct thdpool *gbl_osqlpfault_thdpool = NULL;

osqlpf_step *gbl_osqlpf_step = NULL;

queue_type *gbl_osqlpf_stepq = NULL;

pthread_mutex_t osqlpf_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct osqlpf_rq {
    short type;
    struct dbtable *db;
    unsigned long long genid;
    int index;
    unsigned char key[MAXKEYLEN + 1];
    void *record;
    unsigned short len; /* if its a key, the len of the key.  if its a dta rec,
                           the len of the record */
    int i;
    unsigned long long rqid;
    unsigned long long seq;
    uuid_t uuid;
    /* the table as it was when the request was queued: a schema change can
       drop it or replace its handle before a worker runs the request */
    char tablename[MAXTABLELEN];
    void *handle;
    unsigned long long tableversion;
} osqlpf_rq_t;

static void osqlpf_set_table(osqlpf_rq_t *req, struct dbtable *db)
{
    req->db = db;
    strncpy0(req->tablename, db->tablename, sizeof(req->tablename));
    req->handle = db->handle;
    req->tableversion = db->tableversion;
}

/* From the osql reader thread, which holds no schema lock: only the name and
 * version the replica sent; the worker looks the table up under the lock */
static void osqlpf_set_table_name(osqlpf_rq_t *req, const struct osqlpf_tbl *tbl)
{
    req->db = NULL;
    strncpy0(req->tablename, tbl->name, sizeof(req->tablename));
    req->handle = NULL;
    req->tableversion = tbl->version;
}

/* osql request io prefault, code stolen from prefault.c */

/* The key of an index on expressions is computed by an internal SQL query
 * (indexes_expressions_data): far too costly for a prefetch, skip it */
static int ix_has_expr(struct dbtable *db, int ix)
{
    struct schema *sc;
    int i;

    if (!db->ix_expr || !db->ixschema || !(sc = db->ixschema[ix]))
        return 0;
    for (i = 0; i < sc->nmembers; i++)
        if (sc->member[i].isExpr)
            return 1;
    return 0;
}

int osqlpfthdpool_init(void)
{
    int i = 0;
    gbl_osqlpfault_thdpool = thdpool_create("osqlpfaultpool", 0);

    if (!gbl_exit_on_pthread_create_fail)
        thdpool_unset_exit(gbl_osqlpfault_thdpool);

    thdpool_set_minthds(gbl_osqlpfault_thdpool, 0);
    thdpool_set_maxthds(gbl_osqlpfault_thdpool, gbl_osqlpfault_threads);
    thdpool_set_maxqueue(gbl_osqlpfault_thdpool, 1000);
    thdpool_set_linger(gbl_osqlpfault_thdpool, 10);
    thdpool_set_longwaitms(gbl_osqlpfault_thdpool, 10000);

    gbl_osqlpf_step = (osqlpf_step *)calloc(1000, sizeof(osqlpf_step));
    if (gbl_osqlpf_step == NULL)
        return 1;
    gbl_osqlpf_stepq = queue_new();
    if (gbl_osqlpf_stepq == NULL)
        return 1;
    for (i = 0; i < 1000; i++) {
        int *ii = (int *)malloc(sizeof(int));
        *ii = i;
        gbl_osqlpf_step[i].rqid = 0;
        gbl_osqlpf_step[i].step = 0;
        queue_add(gbl_osqlpf_stepq, ii);
    }
    return 0;
}

static int is_bad_rc(int rc)
{
    if (rc == 0)
        return 0;
    if (rc == 1)
        return 0;

    return 1;
}

static void osqlpfault_do_work_pp(struct thdpool *pool, void *work,
                                  void *thddata, int op);

/* Lookahead progress of a transaction, in its slot: lookahead_pending counts
 * its requests queued or running; lookahead_state is LA_STARTED once one was
 * queued, plus LA_INCOMPLETE once one was dropped before doing its work.
 * Started, complete and nothing pending: the lookahead has read every key of
 * the transaction, so the block processor need not prefetch them again. */
enum { LA_STARTED = 1, LA_INCOMPLETE = 2 };

/* the request belongs to the slot's current transaction: a stale request must
 * not touch the count of the slot's next owner */
static int osqlpf_la_mine(osqlpf_rq_t *req)
{
    osqlpf_step *s = &gbl_osqlpf_step[req->i];
    return req->rqid == ATOMIC_LOAD64(s->rqid) &&
           (req->rqid != OSQL_RQID_USE_UUID || !comdb2uuidcmp(req->uuid, s->uuid));
}

static void osqlpf_la_begin(osqlpf_rq_t *req)
{
    int zero = 0;
    osqlpf_step *s = &gbl_osqlpf_step[req->i];
    ATOMIC_ADD32(s->lookahead_pending, 1);
    if (!osqlpf_la_mine(req)) { /* the transaction is gone: undo */
        ATOMIC_ADD32(s->lookahead_pending, -1);
        return;
    }
    if (ATOMIC_LOAD32(s->lookahead_state) == 0)
        CAS32(s->lookahead_state, zero, LA_STARTED);
}

static void osqlpf_la_end(osqlpf_rq_t *req, int dropped)
{
    osqlpf_step *s;
    if (req->i < 0 || !osqlpf_la_mine(req))
        return;
    s = &gbl_osqlpf_step[req->i];
    if (dropped) /* before the decrement: whoever sees the count drop sees this */
        XCHANGE32(s->lookahead_state, LA_STARTED | LA_INCOMPLETE);
    ATOMIC_ADD32(s->lookahead_pending, -1);
}

/* a request that could not be built: the lookahead is incomplete */
static int osqlpf_la_drop(int i)
{
    if (i >= 0)
        XCHANGE32(gbl_osqlpf_step[i].lookahead_state, LA_STARTED | LA_INCOMPLETE);
    return -1;
}

static int osqlpf_enqueue(osqlpf_rq_t *req)
{
    int rc;
    if (req->i >= 0)
        osqlpf_la_begin(req);
    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, req, 0, NULL, 0);
    if (rc != 0)
        osqlpf_la_end(req, 1);
    return rc;
}

/* given a table, key   : enqueue a fault for the a single ix record */
int enque_osqlpfault_oldkey(struct dbtable *db, void *key, int keylen, int ixnum, int i, unsigned long long rqid,
                            uuid_t uuid, unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL)
        return osqlpf_la_drop(i); /* advisory: no prefault for this op */

    qdata->type = OSQLPFRQ_OLDKEY;
    qdata->len = keylen;
    qdata->index = ixnum;
    osqlpf_set_table(qdata, db);
    qdata->genid = -1;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    if ((keylen > 0) && (keylen < MAXKEYLEN + 1))
        memcpy(qdata->key, key, keylen);

    rc = osqlpf_enqueue(qdata);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, key   : enqueue a fault for the a single ix record */
int enque_osqlpfault_newkey(struct dbtable *db, void *key, int keylen, int ixnum, int i, unsigned long long rqid,
                            uuid_t uuid, unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL)
        return osqlpf_la_drop(i); /* advisory: no prefault for this op */

    qdata->type = OSQLPFRQ_NEWKEY;
    qdata->len = keylen;
    qdata->index = ixnum;
    osqlpf_set_table(qdata, db);
    qdata->genid = -1;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    if ((keylen > 0) && (keylen < MAXKEYLEN + 1))
        memcpy(qdata->key, key, keylen);

    rc = osqlpf_enqueue(qdata);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, genid   : enqueue an op that faults in the dta record by
                            genid then forms all keys from that record and
                            enqueues n ops to fault in each key.
                            */
int enque_osqlpfault_olddata_oldkeys(const struct osqlpf_tbl *tbl, unsigned long long genid, int i,
                                     unsigned long long rqid, uuid_t uuid, unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL)
        return osqlpf_la_drop(i); /* advisory: no prefault for this op */

    qdata->type = OSQLPFRQ_OLDDATA_OLDKEYS;
    qdata->index = -1;
    osqlpf_set_table_name(qdata, tbl);
    qdata->genid = genid;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    rc = osqlpf_enqueue(qdata);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, record : enqueue an op that faults in the dta record by
                            genid then forms all keys from that record and
                            enqueues n ops to fault in each key.
                            */
int enque_osqlpfault_newdata_newkeys(const struct osqlpf_tbl *tbl, void *record, int reclen, int i,
                                     unsigned long long rqid, uuid_t uuid, unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL)
        return osqlpf_la_drop(i); /* advisory: no prefault for this op */

    qdata->type = OSQLPFRQ_NEWDATA_NEWKEYS;
    qdata->index = -1;
    osqlpf_set_table_name(qdata, tbl);
    qdata->genid = -1;
    qdata->record = malloc(reclen);
    if (qdata->record == NULL) {
        free(qdata);
        return osqlpf_la_drop(i);
    }
    memcpy(qdata->record, record, reclen);
    qdata->len = reclen;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    rc = osqlpf_enqueue(qdata);

    if (rc != 0) {
        free(qdata->record);
        free(qdata);
    }
    return rc;
}

/* given a                      : enqueue a an op that
     table,genid,                 1) faults in dta by tbl/genid
     tag,record,reclen            2) forms all keys
                                  3) enqueues n ops to fault in each key
                                  4) forms new record by taking found record +
                                     tag/record/reclen
                                  5) forms all keys from new record.
                                  6) enqueues n ops to fault in each key.
                                  */
int enque_osqlpfault_olddata_oldkeys_newkeys(const struct osqlpf_tbl *tbl, unsigned long long genid, void *record,
                                             int reclen, int i, unsigned long long rqid, uuid_t uuid,
                                             unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL)
        return osqlpf_la_drop(i); /* advisory: no prefault for this op */

    qdata->type = OSQLPFRQ_OLDDATA_OLDKEYS_NEWKEYS;
    qdata->index = -1;
    osqlpf_set_table_name(qdata, tbl);
    qdata->genid = genid;
    qdata->record = malloc(reclen);
    if (qdata->record == NULL) {
        free(qdata);
        return osqlpf_la_drop(i);
    }
    memcpy(qdata->record, record, reclen);
    qdata->len = reclen;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    rc = osqlpf_enqueue(qdata);

    if (rc != 0) {
        free(qdata->record);
        free(qdata);
    }
    return rc;
}

static void osqlpfault_do_work(struct thdpool *pool, void *work, void *thddata)
{
    int rc = 0;
    struct ireq iq;
    unsigned long long step;
    osqlpf_rq_t *req = (osqlpf_rq_t *)work;
    int schema_locked = 0;
    int done_ok = 0; /* the request did its lookup (and queued its key requests) */
    init_fake_ireq(thedb, &iq);
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_START);
    if (gbl_prefault_udp)
        send_prefault_udp = 2;

    if (!gbl_osqlpfault_threads)
        goto done;

    /* i < 0: issued by the block processor itself (osqlpf_bp_keys), no session checks */
    /* the slot fields are read without a lock while their owner writes them (atomics); a request
       that is stale by the time it runs only costs one lookup */
    if (req->i >= 0 && req->rqid != ATOMIC_LOAD64(gbl_osqlpf_step[req->i].rqid)) {
        goto done;
    }
    if (req->i >= 0 && req->rqid == OSQL_RQID_USE_UUID && comdb2uuidcmp(req->uuid, gbl_osqlpf_step[req->i].uuid))
        goto done;

    /* A schema change closes or replaces table handles under the schema
       write lock: skip the work while one runs, and use the table only if it
       is still the one the request was queued for */
    if (get_schema_change_in_progress(__func__, __LINE__) || tryrdlock_schema_lk() != 0)
        goto done;
    schema_locked = 1;
    struct dbtable *db = get_dbtable_by_name(req->tablename);
    if (db == NULL || db->tableversion != req->tableversion ||
        (req->db && (db != req->db || db->handle != req->handle)))
        goto done;
    req->db = db; /* requests queued by the reader thread carry only the name */

    step = req->seq << 7;

    switch (req->type) {
    case OSQLPFRQ_OLDDATA: {
        int fndlen;
        int od_len;
        unsigned char *fnddta = malloc(32768 * sizeof(unsigned char));
        iq.usedb = req->db;

        step += 1;
        if (step <= ATOMIC_LOAD32(gbl_osqlpf_step[req->i].step)) {
            if (fnddta)
                free(fnddta);
            break;
        }

        od_len = getdatsize(iq.usedb);
        if (fnddta == NULL)
            break;
        rc = ix_find_by_rrn_and_genid_prefault(&iq, 2, req->genid, fnddta,
                                               &fndlen, od_len);
        if (fnddta)
            free(fnddta);
        done_ok = 1;
    } break;
    case OSQLPFRQ_OLDKEY: {
        int fndrrn = 0;
        char fndkey[MAXKEYLEN + 1];
        unsigned long long genid = 0;
        if ((req->index < 0) || (req->index > 49)) {
            logmsg(LOGMSG_ERROR, "PFRQ_OLDKEY ix %d out of bounds\n",
                   req->index);
            break;
        }

        step += ((1 + (unsigned long long)req->index) << 1);
        if (req->i >= 0 && step <= ATOMIC_LOAD32(gbl_osqlpf_step[req->i].step)) {
            break;
        }

        iq.usedb = req->db;
        rc = ix_find_prefault(&iq, req->index, req->key, req->len, fndkey,
                              &fndrrn, &genid, NULL, NULL, 0);
        done_ok = 1;
    } break;
    case OSQLPFRQ_NEWKEY: {
        int fndrrn = 0;
        char fndkey[MAXKEYLEN + 1];
        unsigned long long genid = 0;

        if ((req->index < 0) || (req->index > 49)) {
            logmsg(LOGMSG_ERROR, "PFRQ_OLDKEY ix %d out of bounds\n",
                   req->index);
            break;
        }

        step += 1 + ((1 + (unsigned long long)req->index) << 1);
        if (req->i >= 0 && step <= ATOMIC_LOAD32(gbl_osqlpf_step[req->i].step)) {
            break;
        }

        iq.usedb = req->db;
        rc = ix_find_prefault(&iq, req->index, req->key, req->len, fndkey,
                              &fndrrn, &genid, NULL, NULL, 0);
        done_ok = 1;
    } break;
    case OSQLPFRQ_OLDDATA_OLDKEYS: {
        size_t od_len;
        int od_len_int;
        int fndlen = 0;
        int ixnum = 0;
        unsigned char *fnddta = malloc(32768 * sizeof(unsigned char));

        iq.usedb = req->db;

        od_len_int = getdatsize(iq.usedb);
        if (od_len_int <= 0) {
            if (fnddta)
                free(fnddta);
            break;
        }
        od_len = (size_t)od_len_int;

        step += 1;
        if (step <= ATOMIC_LOAD32(gbl_osqlpf_step[req->i].step)) {
            if (fnddta)
                free(fnddta);
            break;
        }

        if (fnddta == NULL)
            break;

        rc = ix_find_by_rrn_and_genid_prefault(&iq, 2, req->genid, fnddta,
                                               &fndlen, od_len);

        if ((is_bad_rc(rc)) || (od_len != fndlen)) {
            if (fnddta)
                free(fnddta);
            break;
        }

        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char key[MAXKEYLEN + 1];
            int keysz = 0;
            if (ix_has_expr(iq.usedb, ixnum))
                continue;
            keysz = getkeysize(iq.usedb, ixnum);
            if (keysz < 0) {
                logmsg(LOGMSG_ERROR,
                       "osqlpfault_do_work:cannot get key size"
                       " tbl %s. idx %d\n",
                       iq.usedb->tablename, ixnum);
                break;
            }
            rc = stag_ondisk_to_ix(iq.usedb, ixnum, (char *)fnddta, key);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR,
                       "osqlpfault_do_work:cannot convert .ONDISK to IDX"
                       " %d of TBL %s\n",
                       ixnum, iq.usedb->tablename);
                break;
            }

            rc = enque_osqlpfault_oldkey(iq.usedb, key, keysz, ixnum, req->i, req->rqid, req->uuid, req->seq);
        }
        if (fnddta)
            free(fnddta);
        done_ok = 1;
    } break;
    case OSQLPFRQ_NEWDATA_NEWKEYS: {
        int ixnum = 0;

        iq.usedb = req->db;

        /* enqueue faults for new keys */
        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char key[MAXKEYLEN + 1];
            int keysz = 0;
            if (ix_has_expr(iq.usedb, ixnum))
                continue;
            keysz = getkeysize(iq.usedb, ixnum);
            if (keysz < 0) {
                logmsg(LOGMSG_ERROR,
                       "osqlpfault_do_work:cannot get key size"
                       " tbl %s. idx %d\n",
                       iq.usedb->tablename, ixnum);
                continue;
            }
            rc = stag_ondisk_to_ix(iq.usedb, ixnum, (char *)req->record, key);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR,
                       "osqlpfault_do_work:cannot convert .ONDISK to IDX"
                       " %d of TBL %s\n",
                       ixnum, iq.usedb->tablename);
                continue;
            }

            rc = enque_osqlpfault_newkey(iq.usedb, key, keysz, ixnum, req->i, req->rqid, req->uuid, req->seq);
        }
        done_ok = 1;
    } break;
    case OSQLPFRQ_OLDDATA_OLDKEYS_NEWKEYS: {
        size_t od_len = 0;
        int od_len_int;
        int fndlen = 0;
        int ixnum = 0;
        unsigned char *fnddta = malloc(32768 * sizeof(unsigned char));

        if (fnddta == NULL)
            break;
        iq.usedb = req->db;

        od_len_int = getdatsize(iq.usedb);
        if (od_len_int <= 0) {
            free(fnddta);
            break;
        }
        od_len = (size_t)od_len_int;

        step += 1;
        if (step <= ATOMIC_LOAD32(gbl_osqlpf_step[req->i].step)) {
            free(fnddta);
            break;
        }

        rc = ix_find_by_rrn_and_genid_prefault(&iq, 2, req->genid, fnddta,
                                               &fndlen, od_len);

        if ((is_bad_rc(rc)) || (od_len != fndlen)) {
            free(fnddta);
            break;
        }

        /* enqueue faults for old keys */
        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char key[MAXKEYLEN + 1];
            int keysz = 0;
            if (ix_has_expr(iq.usedb, ixnum))
                continue;
            keysz = getkeysize(iq.usedb, ixnum);
            if (keysz < 0) {
                logmsg(LOGMSG_ERROR,
                       "osqlpfault_do_work:cannot get key size"
                       " tbl %s. idx %d\n",
                       iq.usedb->tablename, ixnum);
                continue;
            }
            rc = stag_ondisk_to_ix(iq.usedb, ixnum, (char *)fnddta, key);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR,
                       "osqlpfault_do_work:cannot convert .ONDISK to IDX"
                       " %d of TBL %s\n",
                       ixnum, iq.usedb->tablename);
                continue;
            }

            rc = enque_osqlpfault_oldkey(iq.usedb, key, keysz, ixnum, req->i, req->rqid, req->uuid, req->seq);
        }

        free(fnddta);

        /* enqueue faults for new keys */
        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char key[MAXKEYLEN + 1];
            int keysz = 0;
            if (ix_has_expr(iq.usedb, ixnum))
                continue;
            keysz = getkeysize(iq.usedb, ixnum);
            if (keysz < 0) {
                logmsg(LOGMSG_ERROR,
                       "osqlpfault_do_work:cannot get key size"
                       " tbl %s. idx %d\n",
                       iq.usedb->tablename, ixnum);
                continue;
            }
            rc = stag_ondisk_to_ix(iq.usedb, ixnum, (char *)req->record, key);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR,
                       "osqlpfault_do_work:cannot convert .ONDISK to IDX"
                       " %d of TBL %s\n",
                       ixnum, iq.usedb->tablename);
                continue;
            }

            rc = enque_osqlpfault_newkey(iq.usedb, key, keysz, ixnum, req->i, req->rqid, req->uuid, req->seq);
        }
        done_ok = 1;
    } break;
    }

done:
    /* after the key requests were queued (they hold the count up) */
    osqlpf_la_end(req, !done_ok);
    if (schema_locked)
        unlock_schema_lk();
    bdb_thread_event(thedb->bdb_env, BDBTHR_EVENT_DONE);
    send_prefault_udp = 0;
}

static void osqlpfault_do_work_pp(struct thdpool *pool, void *work,
                                  void *thddata, int op)
{
    osqlpf_rq_t *req = (osqlpf_rq_t *)work;
    switch (op) {
    case THD_RUN:
        osqlpfault_do_work(pool, work, thddata);
        break;
    default:
        osqlpf_la_end(req, 1); /* never ran */
        break;
    }
    if (req->i <= -2) /* a block processor request of slot -2 - i is done */
        ATOMIC_ADD32(gbl_osqlpf_step[-2 - req->i].bp_pending, -1);
    free(req->record);
    free(req);
}

int osql_page_prefault(char *rpl, int rplen, struct osqlpf_tbl *tbl, int **iq_step_ix, unsigned long long rqid,
                       uuid_t uuid, unsigned long long seq)
{
    int last_step_idex;
    int *ii;
    osql_rpl_t rpl_op;
    uint8_t *p_buf = (uint8_t *)rpl;
    uint8_t *p_buf_end = p_buf + rplen;
    osqlcomm_rpl_type_get(&rpl_op, p_buf, p_buf_end);

    /* a stopped pool refuses (and logs) every request: do not queue any */
    if (thdpool_get_status(gbl_osqlpfault_thdpool))
        return 0;

    if (*iq_step_ix == NULL) {
        Pthread_mutex_lock(&osqlpf_mutex);
        ii = queue_next(gbl_osqlpf_stepq);
        Pthread_mutex_unlock(&osqlpf_mutex);
        if (ii == NULL) /* all slots in use: no prefault for this transaction */
            return 0;
        XCHANGE32(gbl_osqlpf_step[*ii].step, 0);
        XCHANGE32(gbl_osqlpf_step[*ii].bp_pending, 0);
        gbl_osqlpf_step[*ii].bp_last = 0;
        XCHANGE32(gbl_osqlpf_step[*ii].lookahead_pending, 0);
        XCHANGE32(gbl_osqlpf_step[*ii].lookahead_state, 0);
        comdb2uuidcpy(gbl_osqlpf_step[*ii].uuid, uuid);
        XCHANGE64(gbl_osqlpf_step[*ii].rqid, rqid); /* last: whoever sees this rqid sees this uuid */
        *iq_step_ix = ii;
    }
    last_step_idex = **iq_step_ix;

    /* payload follows the rqid or the (larger) uuid header, as in the block processor */
    uint8_t *p_dt = (uint8_t *)rpl + (rqid == OSQL_RQID_USE_UUID ? sizeof(osql_uuid_rpl_t) : sizeof(osql_rpl_t));

    switch (rpl_op.type) {
    case OSQL_USEDB: {
        int version = 0;
        const char *tablename = get_tablename_from_rpl(rqid == OSQL_RQID_USE_UUID, (uint8_t *)rpl, rplen, &version);

        /* the ops that follow are for this table; a queue or a bad name: none */
        tbl->name[0] = '\0';
        if (tablename && strlen(tablename) < sizeof(tbl->name) && !is_tablename_queue(tablename)) {
            strcpy(tbl->name, tablename);
            tbl->version = version;
        }
    } break;
    case OSQL_DELREC:
    case OSQL_DELETE: {
        osql_del_t dt = {0};
        p_buf = p_dt;
        p_buf = (uint8_t *)osqlcomm_del_type_get(&dt, p_buf, p_buf_end,
                                                 rpl_op.type == OSQL_DELETE);
        if (p_buf == NULL || tbl->name[0] == '\0')
            break;
        enque_osqlpfault_olddata_oldkeys(tbl, dt.genid, last_step_idex, rqid, uuid, seq);
    } break;
    case OSQL_INSREC:
    case OSQL_INSERT: {
        osql_ins_t dt;
        unsigned char *pData = NULL;
        uint8_t *p_buf = p_dt;
        pData = (uint8_t *)osqlcomm_ins_type_get(&dt, p_buf, p_buf_end,
                                                 rpl_op.type == OSQL_INSREC);
        if (pData == NULL || tbl->name[0] == '\0' || dt.nData <= 0 || dt.nData > p_buf_end - pData)
            break;
        enque_osqlpfault_newdata_newkeys(tbl, pData, dt.nData, last_step_idex, rqid, uuid, seq);
    } break;
    case OSQL_UPDREC:
    case OSQL_UPDATE: {
        osql_upd_t dt;
        uint8_t *p_buf = p_dt;
        unsigned char *pData;
        pData = (uint8_t *)osqlcomm_upd_type_get(&dt, p_buf, p_buf_end,
                                                 rpl_op.type == OSQL_UPDATE);
        if (pData == NULL || tbl->name[0] == '\0' || dt.nData <= 0 || dt.nData > p_buf_end - pData)
            break;
        enque_osqlpfault_olddata_oldkeys_newkeys(tbl, dt.genid, pData, dt.nData, last_step_idex, rqid, uuid, seq);
    } break;
    default:
        return 0;
    }
    return 0;
}

/* Block processor (tunable osqlpf_bp_keys): before it changes a row's index
 * entries one after another, look up its old and new keys in the prefault pool
 * so that their leaf pages are read in parallel.  Index 0 is left out: the
 * block processor works on it first. */
int gbl_osqlpf_bp_keys = 1;
void osqlpf_bp_keys(struct ireq *iq, const void *oldrec, const void *newrec)
{
    uuid_t nouuid = {0};
    char ko[MAXKEYLEN + 1], kn[MAXKEYLEN + 1];
    struct dbtable *db = iq->usedb;
    int *slot = iq->osql_step_ix;
    int ix, ksz, o, n, bpi, nq;

    if (!gbl_osqlpf_bp_keys || !gbl_osqlpfault_threads || !gbl_osqlpfault_thdpool || !db ||
        thdpool_get_status(gbl_osqlpfault_thdpool))
        return;
    /* A lookup of a page this transaction already changed waits for its
       commit. While lookups queued before the previous row still run, queue
       no more (in a large transaction they would hold every worker until the
       commit). Without a slot to count them, only the first row; schema
       change rows are not counted as written rows, so they never qualify. */
    if (slot ? ATOMIC_LOAD32(gbl_osqlpf_step[*slot].bp_pending) > gbl_osqlpf_step[*slot].bp_last
             : iq->written_row_count != 1)
        return;
    bpi = slot ? -2 - *slot : -1; /* request index < 0: issued by the block processor */
    /* the transaction's lookahead already read these keys (count first: see osqlpf_la_end) */
    if (slot && ATOMIC_LOAD32(gbl_osqlpf_step[*slot].lookahead_pending) == 0 &&
        ATOMIC_LOAD32(gbl_osqlpf_step[*slot].lookahead_state) == LA_STARTED)
        return;
    nq = 0;
    /* only with spare capacity: behind a backlog the lookups would come after the block processor's own reads */
    if (thdpool_get_queue_depth(gbl_osqlpfault_thdpool) > 0)
        return;
    for (ix = 1; ix < db->nix; ix++) {
        if (ix_has_expr(db, ix))
            continue;
        ksz = getkeysize(db, ix);
        if (ksz <= 0 || ksz > MAXKEYLEN)
            continue;
        o = oldrec && stag_ondisk_to_ix(db, ix, (char *)oldrec, ko) != -1;
        n = newrec && stag_ondisk_to_ix(db, ix, (char *)newrec, kn) != -1;
        if (o) {
            if (slot)
                ATOMIC_ADD32(gbl_osqlpf_step[*slot].bp_pending, 1);
            if (enque_osqlpfault_oldkey(db, ko, ksz, ix, bpi, 0, nouuid, 0) != 0) {
                if (slot)
                    ATOMIC_ADD32(gbl_osqlpf_step[*slot].bp_pending, -1);
            } else
                nq++;
        }
        if (n && !(o && memcmp(ko, kn, ksz) == 0)) {
            if (slot)
                ATOMIC_ADD32(gbl_osqlpf_step[*slot].bp_pending, 1);
            if (enque_osqlpfault_newkey(db, kn, ksz, ix, bpi, 0, nouuid, 0) != 0) {
                if (slot)
                    ATOMIC_ADD32(gbl_osqlpf_step[*slot].bp_pending, -1);
            } else
                nq++;
        }
    }
    if (slot) /* the block processor's own field: only it reads it */
        gbl_osqlpf_step[*slot].bp_last = nq;
}
