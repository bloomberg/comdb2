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

struct thdpool *gbl_osqlpfault_thdpool = NULL;

osqlpf_step *gbl_osqlpf_step = NULL;

queue_type *gbl_osqlpf_stepq = NULL;

pthread_mutex_t osqlpf_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct osqlpf_rq {
    short type;
    struct dbtable *db;
    unsigned long long genid;
    int index;
    unsigned char key[MAXKEYLEN];
    void *record;
    unsigned short len; /* if its a key, the len of the key.  if its a dta rec,
                           the len of the record */
    int i;
    unsigned long long rqid;
    unsigned long long seq;
    uuid_t uuid;
} osqlpf_rq_t;

/* osql request io prefault, code stolen from prefault.c */

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

/* given a table, key   : enqueue a fault for the a single ix record */
int enque_osqlpfault_oldkey(struct dbtable *db, void *key, int keylen,
                            int ixnum, int i, unsigned long long rqid,
                            unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc osql prefault request\n");
        exit(1);
    }

    qdata->type = OSQLPFRQ_OLDKEY;
    qdata->len = keylen;
    qdata->index = ixnum;
    qdata->db = db;
    qdata->genid = -1;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;

    if ((keylen > 0) && (keylen < MAXKEYLEN))
        memcpy(qdata->key, key, keylen);

    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, qdata,
                         0, NULL, 0);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, key   : enqueue a fault for the a single ix record */
int enque_osqlpfault_newkey(struct dbtable *db, void *key, int keylen,
                            int ixnum, int i, unsigned long long rqid,
                            unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc osql prefault request\n");
        exit(1);
    }

    qdata->type = OSQLPFRQ_NEWKEY;
    qdata->len = keylen;
    qdata->index = ixnum;
    qdata->db = db;
    qdata->genid = -1;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;

    if ((keylen > 0) && (keylen < MAXKEYLEN))
        memcpy(qdata->key, key, keylen);

    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, qdata,
                         0, NULL, 0);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, genid   : enqueue an op that faults in the dta record by
                            genid then forms all keys from that record and
                            enqueues n ops to fault in each key.
                            */
int enque_osqlpfault_olddata_oldkeys(struct dbtable *db,
                                     unsigned long long genid, int i,
                                     unsigned long long rqid, uuid_t uuid,
                                     unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc osql prefault request\n");
        exit(1);
    }

    qdata->type = OSQLPFRQ_OLDDATA_OLDKEYS;
    qdata->index = -1;
    qdata->db = db;
    qdata->genid = genid;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, qdata,
                         0, NULL, 0);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

/* given a table, record : enqueue an op that faults in the dta record by
                            genid then forms all keys from that record and
                            enqueues n ops to fault in each key.
                            */
int enque_osqlpfault_newdata_newkeys(struct dbtable *db, void *record,
                                     int reclen, int i, unsigned long long rqid,
                                     uuid_t uuid, unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc osql prefault request\n");
        exit(1);
    }

    qdata->type = OSQLPFRQ_NEWDATA_NEWKEYS;
    qdata->index = -1;
    qdata->db = db;
    qdata->genid = -1;
    qdata->record = malloc(reclen);
    memcpy(qdata->record, record, reclen);
    qdata->len = reclen;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, qdata,
                         0, NULL, 0);

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
int enque_osqlpfault_olddata_oldkeys_newkeys(
    struct dbtable *db, unsigned long long genid, void *record, int reclen,
    int i, unsigned long long rqid, uuid_t uuid, unsigned long long seq)
{
    osqlpf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(osqlpf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc osql prefault request\n");
        exit(1);
    }

    qdata->type = OSQLPFRQ_OLDDATA_OLDKEYS_NEWKEYS;
    qdata->index = -1;
    qdata->db = db;
    qdata->genid = genid;
    qdata->record = malloc(reclen);
    memcpy(qdata->record, record, reclen);
    qdata->len = reclen;
    qdata->i = i;
    qdata->seq = seq;
    qdata->rqid = rqid;
    comdb2uuidcpy(qdata->uuid, uuid);

    rc = thdpool_enqueue(gbl_osqlpfault_thdpool, osqlpfault_do_work_pp, qdata,
                         0, NULL, 0);

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
    init_fake_ireq(thedb, &iq);
    bdb_thread_event(thedb->bdb_env, 1);
    if (gbl_prefault_udp)
        send_prefault_udp = 2;

    if (!gbl_osqlpfault_threads)
        goto done;

    if (req->rqid != gbl_osqlpf_step[req->i].rqid) {
        goto done;
    }
    if (req->rqid == OSQL_RQID_USE_UUID &&
        comdb2uuidcmp(req->uuid, gbl_osqlpf_step[req->i].uuid))
        goto done;

    step = req->seq << 7;

    switch (req->type) {
    case OSQLPFRQ_OLDDATA: {
        int fndlen;
        int od_len;
        unsigned char *fnddta = malloc(32768 * sizeof(unsigned char));
        iq.usedb = req->db;

        step += 1;
        if (step <= gbl_osqlpf_step[req->i].step) {
            if (fnddta)
                free(fnddta);
            break;
        }

        od_len = getdatsize(iq.usedb);
        if (fnddta == NULL) {
            logmsg(LOGMSG_FATAL, "osqlpfault_do_work: malloc %u failed\n",
                   od_len);
            exit(1);
        }
        rc = ix_find_by_rrn_and_genid_prefault(&iq, 2, req->genid, fnddta,
                                               &fndlen, od_len);
        if (fnddta)
            free(fnddta);
    } break;
    case OSQLPFRQ_OLDKEY: {
        int fndrrn = 0;
        char fndkey[MAXKEYLEN];
        unsigned long long genid = 0;
        if ((req->index < 0) || (req->index > 49)) {
            logmsg(LOGMSG_ERROR, "PFRQ_OLDKEY ix %d out of bounds\n",
                   req->index);
            break;
        }

        step += ((1 + (unsigned long long)req->index) << 1);
        if (step <= gbl_osqlpf_step[req->i].step) {
            break;
        }

        iq.usedb = req->db;
        rc = ix_find_prefault(&iq, req->index, req->key, req->len, fndkey,
                              &fndrrn, &genid, NULL, NULL, 0);
    } break;
    case OSQLPFRQ_NEWKEY: {
        int fndrrn = 0;
        char fndkey[MAXKEYLEN];
        unsigned long long genid = 0;

        if ((req->index < 0) || (req->index > 49)) {
            logmsg(LOGMSG_ERROR, "PFRQ_OLDKEY ix %d out of bounds\n",
                   req->index);
            break;
        }

        step += 1 + ((1 + (unsigned long long)req->index) << 1);
        if (step <= gbl_osqlpf_step[req->i].step) {
            break;
        }

        iq.usedb = req->db;
        rc = ix_find_prefault(&iq, req->index, req->key, req->len, fndkey,
                              &fndrrn, &genid, NULL, NULL, 0);
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
        if (step <= gbl_osqlpf_step[req->i].step) {
            if (fnddta)
                free(fnddta);
            break;
        }

        if (fnddta == NULL) {
            logmsg(LOGMSG_FATAL, "osqlpfault_do_work: malloc %zu failed\n",
                   od_len);
            exit(1);
        }

        rc = ix_find_by_rrn_and_genid_prefault(&iq, 2, req->genid, fnddta,
                                               &fndlen, od_len);

        if ((is_bad_rc(rc)) || (od_len != fndlen)) {
            if (fnddta)
                free(fnddta);
            break;
        }

        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char key[MAXKEYLEN];
            int keysz = 0;
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

            rc = enque_osqlpfault_oldkey(iq.usedb, key, keysz, ixnum, req->i,
                                         req->rqid, req->seq);
        }
        if (fnddta)
            free(fnddta);
    } break;
    case OSQLPFRQ_NEWDATA_NEWKEYS: {
        int ixnum = 0;

        iq.usedb = req->db;

        /* enqueue faults for new keys */
        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char key[MAXKEYLEN];
            int keysz = 0;
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

            rc = enque_osqlpfault_newkey(iq.usedb, key, keysz, ixnum, req->i,
                                         req->rqid, req->seq);
        }
    } break;
    case OSQLPFRQ_OLDDATA_OLDKEYS_NEWKEYS: {
        size_t od_len = 0;
        int od_len_int;
        int fndlen = 0;
        int ixnum = 0;
        unsigned char *fnddta = malloc(32768 * sizeof(unsigned char));

        if (fnddta == NULL) {
            logmsg(LOGMSG_FATAL, "osqlpfault_do_work: malloc %zu failed\n",
                   od_len);
            exit(1);
        }
        iq.usedb = req->db;

        od_len_int = getdatsize(iq.usedb);
        if (od_len_int <= 0) {
            free(fnddta);
            break;
        }
        od_len = (size_t)od_len_int;

        step += 1;
        if (step <= gbl_osqlpf_step[req->i].step) {
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
            char key[MAXKEYLEN];
            int keysz = 0;
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

            rc = enque_osqlpfault_oldkey(iq.usedb, key, keysz, ixnum, req->i,
                                         req->rqid, req->seq);
        }

        free(fnddta);

        /* enqueue faults for new keys */
        for (ixnum = 0; ixnum < iq.usedb->nix; ixnum++) {
            char key[MAXKEYLEN];
            int keysz = 0;
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

            rc = enque_osqlpfault_newkey(iq.usedb, key, keysz, ixnum, req->i,
                                         req->rqid, req->seq);
        }
    } break;
    }

done:
    bdb_thread_event(thedb->bdb_env, 0);
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
    }
    free(req->record);
    free(req);
}

int osql_page_prefault(char *rpl, int rplen, struct dbtable **last_db,
                       int **iq_step_ix, unsigned long long rqid, uuid_t uuid,
                       unsigned long long seq)
{
    static int last_step_idex = 0;
    int *ii;
    osql_rpl_t rpl_op;
    uint8_t *p_buf = (uint8_t *)rpl;
    uint8_t *p_buf_end = p_buf + rplen;
    osqlcomm_rpl_type_get(&rpl_op, p_buf, p_buf_end);

    if (seq == 0) {
        Pthread_mutex_lock(&osqlpf_mutex);
        ii = queue_next(gbl_osqlpf_stepq);
        Pthread_mutex_unlock(&osqlpf_mutex);
        if (ii == NULL) {
            logmsg(LOGMSG_ERROR, "osql io prefault got a BUG!\n");
            exit(1);
        }
        last_step_idex = *ii;
        *iq_step_ix = ii;
        gbl_osqlpf_step[last_step_idex].rqid = rqid;
        comdb2uuidcpy(gbl_osqlpf_step[last_step_idex].uuid, uuid);
    }

    switch (rpl_op.type) {
    case OSQL_USEDB: {
        osql_usedb_t dt = {0};
        p_buf = (uint8_t *)&((osql_usedb_rpl_t *)rpl)->dt;
        const char *tablename;
        struct dbtable *db;

        tablename =
            (const char *)osqlcomm_usedb_type_get(&dt, p_buf, p_buf_end);

        db = get_dbtable_by_name(tablename);
        if (db == NULL) {
            logmsg(LOGMSG_ERROR, "%s: unable to get usedb for table %.*s\n",
                   __func__, dt.tablenamelen, tablename);
        } else {
            *last_db = db;
        }
    } break;
    case OSQL_DELREC:
    case OSQL_DELETE: {
        osql_del_t dt = {0};
        p_buf = (uint8_t *)&((osql_del_rpl_t *)rpl)->dt;
        p_buf = (uint8_t *)osqlcomm_del_type_get(&dt, p_buf, p_buf_end,
                                                 rpl_op.type == OSQL_DELETE);
        enque_osqlpfault_olddata_oldkeys(*last_db, dt.genid, last_step_idex,
                                         rqid, uuid, seq);
    } break;
    case OSQL_INSREC:
    case OSQL_INSERT: {
        osql_ins_t dt;
        unsigned char *pData = NULL;
        uint8_t *p_buf = (uint8_t *)&((osql_ins_rpl_t *)rpl)->dt;
        pData = (uint8_t *)osqlcomm_ins_type_get(&dt, p_buf, p_buf_end,
                                                 rpl_op.type == OSQL_INSREC);
        enque_osqlpfault_newdata_newkeys(*last_db, pData, dt.nData,
                                         last_step_idex, rqid, uuid, seq);
    } break;
    case OSQL_UPDREC:
    case OSQL_UPDATE: {
        osql_upd_t dt;
        uint8_t *p_buf = (uint8_t *)&((osql_upd_rpl_t *)rpl)->dt;
        unsigned char *pData;
        pData = (uint8_t *)osqlcomm_upd_type_get(&dt, p_buf, p_buf_end,
                                                 rpl_op.type == OSQL_UPDATE);
        enque_osqlpfault_olddata_oldkeys_newkeys(*last_db, dt.genid, pData,
                                                 dt.nData, last_step_idex, rqid,
                                                 uuid, seq);
    } break;
    default:
        return 0;
    }
    return 0;
}
