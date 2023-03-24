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

#include <unistd.h>
#include "comdb2.h"
#include "bdb_verify.h"
#include <sql.h>

struct thdpool *gbl_verify_thdpool;
static pthread_once_t once = PTHREAD_ONCE_INIT;

struct verify_thd_state {
    struct thr_handle *thr_self;
};

static void verify_thd_start(struct thdpool *pool, void *thddata)
{
    struct verify_thd_state *state = thddata;
    state->thr_self = thrman_register(THRTYPE_VERIFY);
}

void init_verify_thdpool(void)
{
    assert(!gbl_verify_thdpool);

    gbl_verify_thdpool =
        thdpool_create("verify_pool", sizeof(struct verify_thd_state));
    assert(gbl_verify_thdpool);

    if (!gbl_exit_on_pthread_create_fail)
        thdpool_unset_exit(gbl_verify_thdpool);

    // thdpool_set_stack_size(gbl_verify_thdpool, bdb_attr_get(thedb->bdb_attr,
    // BDB_ATTR_VERIFY_THREAD_STACKSZ));
    thdpool_set_init_fn(gbl_verify_thdpool, verify_thd_start);
    thdpool_set_minthds(gbl_verify_thdpool, 0);
    thdpool_set_maxthds(
        gbl_verify_thdpool,
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_VERIFY_POOL_MAXT));
    thdpool_set_linger(gbl_verify_thdpool, 1);
    thdpool_set_longwaitms(gbl_verify_thdpool, 1000000);
    thdpool_set_maxqueue(gbl_verify_thdpool, 100);
    thdpool_set_mem_size(gbl_verify_thdpool, 4 * 1024);
}

void dump_record_by_rrn_genid(struct dbtable *db, int rrn, unsigned long long genid)
{
    int rc;
    struct ireq iq;
    char *dta;
    char *key = NULL;
    int dtasz;
    int fndlen;
    int ix;
    char tag[MAXTAGLEN];

    init_fake_ireq(db->dbenv, &iq);
    iq.usedb = db;

    dtasz = getdatsize(db);
    dta = malloc(dtasz);
    if (dta == NULL) {
        logmsg(LOGMSG_INFO, "dump_record_by_rrn_genid: malloc failed\n");
        return;
    }

    rc = ix_find_by_rrn_and_genid(&iq, 2, genid, dta, &fndlen, dtasz);
    if (rc) {
        logmsg(LOGMSG_INFO, "rrn %d genid 0x%llx not found\n", rrn, genid);
        return;
    }
    logmsg(LOGMSG_INFO, "rrn %d genid 0x%016llx\n", rrn, genid);
    dump_tagged_buf(db->tablename, ".ONDISK", (unsigned char *)dta);
    for (ix = 0; ix < db->nix; ix++) {
        key = malloc(getkeysize(db, ix));
        if (key == NULL) {
            logmsg(LOGMSG_INFO, "dump_record_by_rrn_genid: malloc failed\n");
            free(dta);
            return;
        }
        snprintf(tag, sizeof(tag), ".ONDISK_IX_%d", ix);
        rc = stag_to_stag_buf(db->tablename, ".ONDISK", dta, tag, key, NULL);
        if (rc) {
            logmsg(LOGMSG_INFO,
                   "dump_record_by_rrn:stag_to_stag_buf rrn %d genid %016llx "
                   "failed\n",
                   rrn, genid);
            free(key);
            break;
        }
        logmsg(LOGMSG_INFO, "ix %d:\n", ix);
        dump_tagged_buf(db->tablename, tag, (unsigned char *)key);
        free(key);
    }
    free(dta);
}

void purge_by_genid(struct dbtable *db, unsigned long long *genid)
{
    int bdberr;
    /* purge all records, index entries and blobs containing a
       given genid. Presumably these will previously have been found
       through verify.

       This is transactional, verify is not.
     */

    void *tran;
    int retries = 0;
    int rc;
    struct ireq iq = {0};
    iq.usedb = db;
    iq.is_fake = 1;

    /* genid can be NULL in which case we do an auto purge */
    if (genid)
        logmsg(LOGMSG_INFO, "Purging genid %016llx from table %s\n", *genid,
               db->tablename);
retry:
    tran = bdb_tran_begin(db->handle, NULL, &bdberr);

    if (!tran) {
        logmsg(LOGMSG_ERROR, "purge_by_genid: bdb_trans_start failed - err %d\n"
                             "Please try again.\n",
               bdberr);
        return;
    }

    {
        char fndkey[MAXKEYLEN];
        char key[MAXKEYLEN];
        int rrn;
        int maxsz = getdatsize(db);
        char *dta = malloc(maxsz);
        int fndlen;
        unsigned long long fnd_genid;

        bzero(fndkey, MAXKEYLEN);
        bzero(dta, maxsz);
        bzero(key, MAXKEYLEN);

        /* try to find it */
        if (genid) {
            rc = ix_find_by_rrn_and_genid_tran(&iq, 0, *genid, dta, &fndlen,
                                               maxsz, tran);
            if (rc != 0 || maxsz != fndlen) {
                printf("Unable to find data record for this genid.\n");

            } else {

                /* remove data (skip blobs) */
                rc = dat_del_auxdb(AUXDB_NONE, &iq, tran, 0, *genid, 0);
                printf(" dat_del rc: %d \n", rc);
            }
        } else {
            int i;
            printf("Corrupting database...\n");
            for (i = 0; i < db->nix; i++) {
                bzero(fndkey, MAXKEYLEN);
                bzero(dta, maxsz);
                bzero(key, MAXKEYLEN);

                rc = ix_find(&iq, i, key, 0, fndkey, &rrn, &fnd_genid, dta,
                             &fndlen, maxsz);
                /* delete a data record */
                if (rc == 0 || rc == 1) {
                    printf("Genid: 0x%016llx\n", fnd_genid);
                    rc = dat_del(&iq, tran, 0, fnd_genid);
                    printf("dat_del rc: %d \n", rc);
                }
            }
        }
    }

    rc = bdb_tran_commit(db->handle, tran, &bdberr);
    if (rc != 0) {
        rc = bdb_tran_abort(db->handle, tran, &bdberr);
        if (bdberr == BDBERR_DEADLOCK && ++retries < gbl_maxretries) {
            n_retries++;
            printf("Retrying purge transaction...\n");
            goto retry;
        }
        fprintf(stderr, "purge_by_genid: trans_commit failed %d\n", bdberr);
    }
}

static int verify_blobsizes_callback(const dbtable *tbl, void *dta,
                                     int blobsizes[16], int offset[16],
                                     int *nblobs)
{
    int i;
    struct schema *s;
    int blobix = 0;
    int rc;
    int isnull;
    int outsz;
    int sz;

    *nblobs = 0;

    for (i = 0; i < tbl->schema->nmembers; i++) {
        s = tbl->schema;
        if (s->member[i].type == SERVER_BLOB ||
            s->member[i].type == SERVER_VUTF8 ||
            s->member[i].type == SERVER_BLOB2) {
            char *p = ((char *)dta) + s->member[i].offset;
            if (stype_is_null(p)) {
                blobsizes[blobix] = -1;
            } else {
                rc = SERVER_UINT_to_CLIENT_UINT(
                    ((char *)dta) + s->member[i].offset, 5, NULL, NULL, &sz,
                    sizeof(int), &isnull, &outsz, NULL, NULL);
                if (rc)
                    return rc;
                /* tell bdb that the record fits in the inline portion
                 * and that it should make sure that the blob DOESN'T exist */
                blobsizes[blobix] = ntohl(sz);
                if (s->member[i].type == SERVER_VUTF8 ||
                    s->member[i].type == SERVER_BLOB2) {
                    if (blobsizes[blobix] <= s->member[i].len - 5)
                        blobsizes[blobix] = -2;
                }
            }
            offset[blobix] = s->member[i].offset;
            blobix++;
        }
        /* TODO: vutf8 */
    }
    *nblobs = blobix;
    assert(blobix == tbl->schema->numblobs);
    return 0;
}

static int verify_formkey_callback(const dbtable *tbl, void *dta,
                                   void *blob_parm, int ix, void *keyout,
                                   int *keysz)
{
    *keysz = get_size_of_schema(tbl->ixschema[ix]);
    return create_key_from_schema_simple(tbl, NULL, ix, dta, keyout, blob_parm, MAXBLOBS);
}

static int convert_to_partial_datacopy(const struct dbtable *tbl, const int pd_ix, const void *inbuf, void *outbuf)
{
    return stag_to_stag_buf_schemas(tbl->schema, tbl->schema->ix[pd_ix]->partial_datacopy, inbuf, outbuf, NULL);
}

static int verify_add_blob_buffer_callback(void *parm, void *dta, int dtasz,
                                           int blobno)
{
    blob_buffer_t *blobs = (blob_buffer_t *)parm;
    blobs[blobno].exists = 1;
    blobs[blobno].data = malloc(dtasz);
    if (blobs[blobno].data == NULL) {
        fprintf(stderr, "%s: failed to malloc size %d\n", __func__, dtasz);
        return -1;
    }
    memcpy(blobs[blobno].data, dta, dtasz);
    blobs[blobno].length = dtasz;
    blobs[blobno].collected = dtasz;
    return 0;
}

static void verify_free_blob_buffer_callback(void *parm)
{
    free_blob_buffers((blob_buffer_t *)parm, MAXBLOBS);
}

static unsigned long long verify_indexes_callback(void *parm, void *dta,
                                                  void *blob_parm)
{
    return verify_indexes(parm, dta, blob_parm, MAXBLOBS, 0);
}

// call this with schema lock
static int get_tbl_and_lock_in_tran(verify_common_t *par, const char *table,
                                    struct dbtable **db, tran_type **tran)
{
    int bdberr;
    struct dbtable *locdb = get_dbtable_by_name(table);
    if (locdb == NULL) {
        char err[LINE_MAX];
        snprintf(err, sizeof(err), "?Unknown table name '%s'", table);
        logmsg(LOGMSG_ERROR, "%s\n", err + 1);
        par->verify_response(err, par->arg);
        return -1;
    }
    void *loctran = bdb_tran_begin(thedb->bdb_env, NULL, &bdberr);
    if (!loctran) {
        char err[LINE_MAX];
        snprintf(err, sizeof(err), "?Error starting transaction rc:%d table:%s", bdberr, table);
        logmsg(LOGMSG_ERROR, "%s\n", err + 1);
        par->verify_response(err, par->arg);
        return -1;
    }
    *db = locdb;
    *tran = loctran;
    int rc;
    if ((rc = bdb_lock_tablename_read(thedb->bdb_env, locdb->tablename,
                                      loctran)) != 0) {
        char err[LINE_MAX];
        snprintf(err, sizeof(err), "?Readlock rc:%d table:%s", rc, table);
        logmsg(LOGMSG_ERROR, "%s\n", err + 1);
        par->verify_response(err, par->arg);
        return -1;
    }
    return 0;
}

/* verify table main entry point called both by lua/syssp.c
 * and by verify_table() which is called by bb plugins
 */
int verify_table(const char *table, int progress_report_seconds,
                 int attempt_fix, verify_mode_t mode,
                 verify_peer_check_func *peer_check,
                 verify_response_func *response, void *arg)
{
    pthread_once(&once, init_verify_thdpool);
    verify_common_t par = {
        .tablename = table,
        .partial_datacopy_callback = convert_to_partial_datacopy,
        .formkey_callback = verify_formkey_callback,
        .get_blob_sizes_callback = verify_blobsizes_callback,
        .vtag_callback = (int (*)(void *, void *, int *, uint8_t))vtag_to_ondisk_vermap,
        .add_blob_buffer_callback = verify_add_blob_buffer_callback,
        .free_blob_buffer_callback = verify_free_blob_buffer_callback,
        .verify_indexes_callback = verify_indexes_callback,
        .progress_report_seconds = progress_report_seconds,
        .attempt_fix = attempt_fix,
        .verify_mode = mode,
        .peer_check = peer_check,
        .verify_response = response,
        .arg = arg,
    };
    tran_type *tran = NULL;
    struct dbtable *db = NULL;
    rdlock_schema_lk();
    int rc = get_tbl_and_lock_in_tran(&par, table, &db, &tran);
    unlock_schema_lk();
    if (rc) {
        goto done;
    }
    par.bdb_state = db->handle;
    par.db_table = db;
    td_processing_info_t info = {.common_params = &par};
    bdb_verify_enqueue(&info, gbl_verify_thdpool);

    while (par.threads_spawned > par.threads_completed) {
        if (!par.client_dropped_connection && par.peer_check(par.arg)){
            logmsg(LOGMSG_WARN, "%s: client connection closed, stopped verify\n", __func__);
            par.client_dropped_connection = 1;
        }
        sleep(1);
    }
    rc = par.verify_status;
done:
    if (tran) {
        int bdberr;
        bdb_tran_abort(thedb->bdb_env, tran, &bdberr);
    }
    return rc;
}
