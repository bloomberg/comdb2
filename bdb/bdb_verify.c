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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <strings.h>
#include <alloca.h>
#include <sys/poll.h>
#include <unistd.h>

#include <sbuf2.h>

#include <build/db.h>

#include "bdb_int.h"
#include "locks.h"
#include "endian_core.h"

#include "genid.h"
#include "logmsg.h"

/* NOTE: This is from "comdb2.h". */
extern int ix_isnullk(void *db_table, void *key, int ixnum);

/* print to sb if available lua callback otherwise */
static int locprint(SBUF2 *sb, int (*lua_callback)(void *, const char *), 
        void *lua_params, char *fmt, ...)
{
    char lbuf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(lbuf, sizeof(lbuf), fmt, ap);
    va_end(ap);

    if(sb) 
        return sbuf2printf(sb, lbuf);
    else if(lua_callback)
        return lua_callback(lua_params, lbuf);
    return -1;
}

static int bdb_verify_ll(
    SBUF2 *sb, bdb_state_type *bdb_state, void *db_table,
    int (*formkey_callback)(void *parm, void *dta, void *blob_parm, int ix,
                            void *keyout, int *keysz),
    int (*get_blob_sizes_callback)(void *parm, void *dta, int blobs[16],
                                   int bloboffs[16], int *nblobs),
    int (*vtag_callback)(void *parm, void *dta, int *dtasz, uint8_t ver),
    int (*add_blob_buffer_callback)(void *parm, void *dta, int dtasz,
                                    int blobno),
    void (*free_blob_buffer_callback)(void *parm),
    unsigned long long (*verify_indexes_callback)(void *parm, void *dta,
                                                  void *blob_parm),
    void *callback_parm, 
    int (*lua_callback)(void *, const char *), void *lua_params, 
    void *callback_blob_buf, int progress_report_seconds,
    int attempt_fix, unsigned int lid);

int bdb_verify(
    SBUF2 *sb, bdb_state_type *bdb_state, void *db_table,
    int (*formkey_callback)(void *parm, void *dta, void *blob_parm, int ix,
                            void *keyout, int *keysz),
    int (*get_blob_sizes_callback)(void *parm, void *dta, int blobs[16],
                                   int bloboffs[16], int *nblobs),
    int (*vtag_callback)(void *parm, void *dta, int *dtasz, uint8_t ver),
    int (*add_blob_buffer_callback)(void *parm, void *dta, int dtasz,
                                    int blobno),
    void (*free_blob_buffer_callback)(void *parm),
    unsigned long long (*verify_indexes_callback)(void *parm, void *dta,
                                                  void *blob_parm),
    void *callback_parm, 
    int (*lua_callback)(void *, const char *), void *lua_params, 
    void *callback_blob_buf, int progress_report_seconds,
    int attempt_fix)
{
    int rc;
    DB_LOCKREQ rq = {0};
    unsigned int flags = DB_LOCK_ID_READONLY;
    unsigned int lid;

    BDB_READLOCK("bdb_verify");

    if ((rc = bdb_state->dbenv->lock_id_flags(bdb_state->dbenv, &lid,
                                              DB_LOCK_ID_READONLY)) != 0) {
        BDB_RELLOCK();
        logmsg(LOGMSG_ERROR, "%s: error getting a lockid, %d\n", __func__, rc);
        return rc;
    }

    rc = bdb_verify_ll(sb, bdb_state, db_table, formkey_callback, get_blob_sizes_callback,
                       vtag_callback, add_blob_buffer_callback,
                       free_blob_buffer_callback, verify_indexes_callback,
                       callback_parm, 
                       lua_callback, lua_params,
                       callback_blob_buf,
                       progress_report_seconds, attempt_fix, lid);

    rq.op = DB_LOCK_PUT_ALL;
    bdb_state->dbenv->lock_vec(bdb_state->dbenv, lid, 0, &rq, 1, NULL);
    bdb_state->dbenv->lock_id_free(bdb_state->dbenv, lid);

    BDB_RELLOCK();

    return rc;
}

static int dropped_connection(SBUF2 *sb)
{
    struct pollfd p;
    int rc;

    p.fd = sbuf2fileno(sb);
    p.events = POLLIN;
    rc = poll(&p, 1, 0);
    if (rc == 1)
        return 1;
    return 0;
}

void set_null_func(void *p, int len);
void set_data_func(void *to, const void *from, int sz);

extern void fsnapf(FILE *, void *, int);

static int restore_cursor_at_genid(DB *db, DBC **cdata,
                                   unsigned long long genid, unsigned int lid)
{
    DBC *c;
    int rc;
    DBT key, dta;

    rc = db->paired_cursor_from_lid(db, lid, &c, 0);
    if (rc)
        return rc;
    bzero(&key, sizeof(DBT));
    bzero(&dta, sizeof(DBT));
    dta.flags = DB_DBT_REALLOC;

    key.data = &genid;
    key.size = sizeof(unsigned long long);
    rc = c->c_get(c, &key, &dta, DB_SET_RANGE);
    if (rc)
        return rc;

    /* back up to something <= the original - it's ok for us to go over the same
     * records twice */
    while (rc == 0 && memcmp(&genid, key.data, sizeof(unsigned long long)) < 0)
        rc = c->c_get(c, &key, &dta, DB_PREV);
    if (rc == DB_NOTFOUND)
        rc = 0;
    free(dta.data);
    *cdata = c;
    return 0;
}

static int fix_blobs(bdb_state_type *bdb_state, DB *db, DBC **cdata,
                     unsigned long long genid, int nblobs, int *bloboffs,
                     int *bloblen, unsigned int lid)
{
    int i;
    DBC *c = NULL;
    int rc, crc;
    tran_type *t = NULL;
    int bdberr;
    DBT key;
    DBT dta;
    int len;
    uint8_t ver;

    /* The caller already had a cursor on this record, but it wasn't
     * transactional.
     * So we need to close it and open a transactional cursor suitable for
     * writes. When
     * done, we restore the cursor to the same genid. */

    t = bdb_tran_begin(bdb_state, NULL, &bdberr);
    if (t == NULL)
        goto done;

    rc = (*cdata)->c_close(*cdata);
    if (rc)
        goto done;
    *cdata = NULL;

    rc = db->cursor(db, t->tid, &c, 0);
    if (rc)
        goto done;

    bzero(&key, sizeof(DBT));
    bzero(&dta, sizeof(DBT));
    key.data = &genid;
    key.size = sizeof(unsigned long long);
    dta.flags = DB_DBT_MALLOC;

    rc = bdb_cget_unpack(bdb_state, c, &key, &dta, &ver, DB_SET);
    if (rc)
        goto done;

    /* found the record, patch it */
    /* This is a kludge - this rouine should take a callback.  We should not
     * know internals of data formats at this layer, nor should we be calling
     * up to db. */
    for (i = 0; i < nblobs; i++) {
        logmsg(LOGMSG_USER, "blob %d, was:\n", i);
        fsnapf(stdout, (uint8_t *)dta.data + bloboffs[i], 5);
        if (bloblen[i] == -1)
            set_null_func((uint8_t *)dta.data + bloboffs[i], 5);
        else {
            len = htonl(bloblen[i]);
            set_data_func((uint8_t *)dta.data + bloboffs[i], &len, 5);
        }
        logmsg(LOGMSG_USER, "now:\n");
        fsnapf(stdout, (uint8_t *)dta.data + bloboffs[i], 5);
    }

    /* write it back */
    rc = bdb_cput_pack(bdb_state, 0, c, &key, &dta, DB_CURRENT);
    if (rc)
        goto done;

done:
    if (dta.data)
        free(dta.data);
    if (c) {
        crc = c->c_close(c);
        if (crc)
            rc = crc;
    }
    if (t) {
        if (rc == 0) {
            seqnum_type seqnum;
            rc = bdb_tran_commit_with_seqnum(bdb_state, t, &seqnum, &bdberr);
            if (rc)
                goto ret;
            rc = bdb_wait_for_seqnum_from_all(bdb_state, &seqnum);
        } else {
            rc = bdb_tran_abort(bdb_state, t, &bdberr);
        }
    }

    /* We need to get back to a non-transactional cursor */
    crc = restore_cursor_at_genid(db, cdata, genid, lid);
    if (crc)
        rc = crc;

ret:
    if (rc)
        logmsg(LOGMSG_ERROR, "%s rc %d bdberr %d\n", __func__, rc, bdberr);
    return rc;
}

static void printhex(SBUF2 *sb, int (*lua_callback)(void *, const char *),
        void *lua_params, uint8_t *hex, int sz)
{
    const char hexbytes[] = "0123456789abcdef";
    for (int i = 0; i < sz; i++)
        locprint(sb, lua_callback, lua_params, "%c%c", 
                hexbytes[(hex[i] & 0xf0) >> 4], hexbytes[hex[i] & 0xf]);
}

extern int gbl_expressions_indexes;
int is_comdb2_index_expression(const char *dbname);
/* TODO: handle deadlock, get rowlocks if db in rowlocks mode */
static int bdb_verify_ll(
    SBUF2 *sb, bdb_state_type *bdb_state, void *db_table,
    int (*formkey_callback)(void *parm, void *dta, void *blob_parm, int ix,
                            void *keyout, int *keysz),
    int (*get_blob_sizes_callback)(void *parm, void *dta, int blobs[16],
                                   int bloboffs[16], int *nblobs),
    int (*vtag_callback)(void *parm, void *dta, int *dtasz, uint8_t ver),
    int (*add_blob_buffer_callback)(void *parm, void *dta, int dtasz,
                                    int blobno),
    void (*free_blob_buffer_callback)(void *parm),
    unsigned long long (*verify_indexes_callback)(void *parm, void *dta,
                                                  void *blob_parm),
    void *callback_parm, 
    int (*lua_callback)(void *, const char *), void *lua_params, 
    void *callback_blob_buf, int progress_report_seconds,
    int attempt_fix, unsigned int lid)
{
    DBC *cdata = NULL;
    DBC *ckey = NULL;
    DB *db;
    DBC *cblob;
    unsigned char databuf[17 * 1024];
    unsigned char keybuf[18 * 1024];
    unsigned char expected_keybuf[18 * 1024];
    unsigned char verify_keybuf[18 * 1024];
    int dtastripe;
    DBT dbt_data = {0};
    DBT dbt_key = {0};
    DBT dbt_blob_key = {0}, dbt_blob_data = {0};
    DBT dbt_dta_check_key = {0}, dbt_dta_check_data = {0};
    int rc;
    int ix;
    int keylen, expected_keylen;
    unsigned long long has_keys;
    unsigned long long genid, verify_genid;
    int blobsizes[16];
    int bloboffs[16];
    int nblobs = 0;
    int blobno;
    char dumbuf;
    uint8_t ver;
    int now, last;
    int64_t nrecs = 0;
    int nrecs_progress = 0;
    unsigned long long genid_flipped;
    int ret = 0;

    bzero(&dbt_data, sizeof(DBT));
    bzero(&dbt_key, sizeof(DBT));

    dbt_data.flags = DB_DBT_USERMEM;
    dbt_data.ulen = sizeof(databuf);
    dbt_data.data = databuf;
    dbt_key.flags = DB_DBT_USERMEM;
    dbt_key.ulen = sizeof(keybuf);
    dbt_key.data = keybuf;

    bzero(&dbt_blob_key, sizeof(DBT));
    bzero(&dbt_blob_data, sizeof(DBT));

    now = last = comdb2_time_epochms();

    /* scan 1 - run through data, verify all the keys and blobs */
    for (dtastripe = 0; dtastripe < bdb_state->attr->dtastripe; dtastripe++) {
        nrecs = 0;
        nrecs_progress = 0;
        dbt_data.flags = DB_DBT_USERMEM;
        dbt_data.ulen = sizeof(databuf);
        dbt_data.data = databuf;
        dbt_key.flags = DB_DBT_USERMEM;
        dbt_key.ulen = sizeof(keybuf);
        dbt_key.data = keybuf;

        db = bdb_state->dbp_data[0][dtastripe];
        rc = db->paired_cursor_from_lid(db, lid, &cdata, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "dtastripe %d cursor rc %d\n", dtastripe, rc);
            return rc;
        }
        rc = bdb_cget_unpack(bdb_state, cdata, &dbt_key, &dbt_data, &ver,
                             DB_FIRST);
        if (rc == DB_NOTFOUND) {
            cdata->c_close(cdata);
            continue;
        }

        while (rc == 0) {
            nrecs++;
            nrecs_progress++;

            now = comdb2_time_epochms();

            /* check if comdb2sc is killed */
            if ((now - last) > 1000) {
                if (dropped_connection(sb)) {
                    cdata->c_close(cdata);
                    logmsg(LOGMSG_WARN, "condb2sc connection closed, stopped verify\n");
                    return rc;
                }
            }

            if (progress_report_seconds &&
                ((now - last) >= (progress_report_seconds * 1000))) {
                rc = locprint(sb, lua_callback, lua_params, "!verifying dtastripe %d, did %lld records, %d "
                                "per second\n",
                            dtastripe, nrecs,
                            nrecs_progress / progress_report_seconds);
                if(rc) return ret; //dropped connection
                last = now;
                nrecs_progress = 0;
                sbuf2flush(sb);
            }

            /* is it the right size? */
            if (dbt_key.size != sizeof(genid)) {
                ret = 1;
                locprint(sb, lua_callback, lua_params, "!bad genid sz %d\n", dbt_key.size);
                goto next_record;
            }
            memcpy(&genid, dbt_key.data, sizeof(genid));

/* why do we open a cursor for each record/blob?
   1) cursors are cheap - berkeley opens one for every cursor
      operation
   2) we don't want to keep an active cursor to prevent
      locking up db operations
   */
#ifdef _LINUX_SOURCE
            buf_put(&genid, sizeof(unsigned long long),
                    (uint8_t *)&genid_flipped,
                    (uint8_t *)&genid_flipped + sizeof(unsigned long long));
#else
            genid_flipped = genid;
#endif
            vtag_callback(callback_parm, dbt_data.data, (int *)&dbt_data.size,
                          ver);

            rc = get_blob_sizes_callback(callback_parm, dbt_data.data,
                                         blobsizes, bloboffs, &nblobs);
            if (rc) {
                ret = 1;
                locprint(sb, lua_callback, lua_params, "!%016llx blob size rc %d\n", genid, rc);
            } else {
                /* verify blobs */
                int realblobsz[16];
                int had_errors, had_irrecoverable_errors;

                had_errors = 0;
                had_irrecoverable_errors = 0;
                for (blobno = 0; blobno < nblobs; blobno++) {
                    DBC *cblob;
                    DB *blobdb;
                    unsigned long long blob_genid = genid;
                    int dtafile;

                    realblobsz[blobno] = -1;
                    had_irrecoverable_errors = 0;
                    had_errors = 0;

                    dtafile = get_dtafile_from_genid(genid);
                    if (dtafile < 0) {
                        ret = 1;
                        locprint(sb, lua_callback, lua_params, "!%016llx unknown dtafile\n",
                                    genid_flipped);
                        continue;
                    }
                    blobdb =
                        get_dbp_from_genid(bdb_state, blobno + 1, genid, NULL);

                    rc = blobdb->paired_cursor_from_lid(blobdb, lid, &cblob, 0);
                    if (rc) {
                        ret = 1;
                        locprint(sb, lua_callback, lua_params, "!%016llx cursor on blob %d rc %d\n",
                                    genid_flipped, blobno, rc);
                        continue;
                    }

                    /* Note: we have to fetch the whole blob here because with
                       ondisk headers + compression
                       the size of the blob will not match what's stored in the
                       record so a partial find
                       won't do.  I guess we could optimize for the more common
                       case of no headers/compression. */
                    dbt_blob_key.data = &blob_genid;
                    dbt_blob_key.size = sizeof(unsigned long long);
                    dbt_blob_data.flags = DB_DBT_MALLOC;
                    dbt_blob_data.data = NULL;

                    rc = bdb_cget_unpack_blob(bdb_state, cblob, &dbt_blob_key,
                                              &dbt_blob_data, &ver, DB_SET);
                    if (rc == DB_NOTFOUND) {
                        realblobsz[blobno] = -1;
                        if (blobsizes[blobno] != -1 &&
                            blobsizes[blobno] != -2) {
                            had_errors = 1;
                            ret = 1;
                            locprint(sb, lua_callback, lua_params,
                                "!%016llx no blob %d found expected sz %d\n",
                                genid_flipped, blobno, blobsizes[blobno]);
                        }
                    } else if (rc) {
                        had_irrecoverable_errors = 1;
                        ret = 1;
                        locprint(sb, lua_callback, lua_params, "!%016llx blob %d rc %d\n",
                                    genid_flipped, blobno, rc);
                        had_errors = 1;
                    }

                    if (rc == 0) {
                        realblobsz[blobno] = dbt_blob_data.size;
                        if (blobsizes[blobno] == -1 && rc != DB_NOTFOUND) {
                            ret = 1;
                            locprint(sb, lua_callback, lua_params,
                                "!%016llx blob %d null but found blob\n",
                                genid_flipped, blobno);
                        } else if (blobsizes[blobno] == -2) {
                            ret = 1;
                            locprint(sb, lua_callback, lua_params, "!%016llx blob %d size %d expected "
                                            "none (inline vutf8)\n",
                                        genid_flipped, blobno,
                                        realblobsz[blobno]);
                        } else if (blobsizes[blobno] != -1 &&
                                   dbt_blob_data.size != blobsizes[blobno]) {
                            ret = 1;
                            locprint(sb, lua_callback, lua_params, "!%016llx blob %d size mismatch "
                                            "got %d expected %d\n",
                                        genid_flipped, blobno,
                                        dbt_blob_data.size, blobsizes[blobno]);
                            had_errors = 1;
                        }

                        if (blobsizes[blobno] >= 0 && realblobsz[blobno] >= 0) {
                            rc = add_blob_buffer_callback(
                                callback_blob_buf, dbt_blob_data.data,
                                dbt_blob_data.size, blobno);
                            if (rc)
                                return rc;
                        }

                        if (dbt_blob_data.data && had_errors == 0)
                            free(dbt_blob_data.data);
                    }
                    cblob->c_close(cblob);
                }
                if (attempt_fix && had_errors && !had_irrecoverable_errors) {
                    rc = fix_blobs(bdb_state, db, &cdata, genid, nblobs,
                                   bloboffs, realblobsz, lid);
                    if (rc) {
                        logmsg(LOGMSG_ERROR, "fix_blobs rc %d\n", rc);
                        /* close? */
                        free_blob_buffer_callback(callback_blob_buf);
                        return rc;
                    }
                }
            }

            has_keys = verify_indexes_callback(callback_parm, dbt_data.data,
                                               callback_blob_buf);
            for (ix = 0; ix < bdb_state->numix; ix++) {
                rc = bdb_state->dbp_ix[ix]->paired_cursor_from_lid(
                    bdb_state->dbp_ix[ix], lid, &ckey, 0);
                if (rc) {
                    ckey = NULL;
                    free_blob_buffer_callback(callback_blob_buf);
                    logmsg(LOGMSG_ERROR, "unexpected rc opening cursor for ix %d: %d\n", ix,
                           rc);
                    return rc;
                }

                rc = formkey_callback(callback_parm, databuf, callback_blob_buf,
                                      ix, expected_keybuf, &keylen);
                if (rc) {
                    ret = 1;
                    locprint(sb, lua_callback, lua_params, "!%016llx ix %d formkey rc %d\n",
                                genid_flipped, rc);
                    ckey->c_close(ckey);
                    continue;
                }

                /* set up key */

                memcpy(dbt_key.data, expected_keybuf, keylen);
                dbt_key.size = keylen;
                if (bdb_keycontainsgenid(bdb_state, ix)) {
                    unsigned long long masked_genid =
                        get_search_genid(bdb_state, genid);

                    /* use 0 as the genid if no null values to keep it unique */
                    if (bdb_state->ixnulls[ix] && !ix_isnullk(db_table, dbt_key.data, ix))
                        masked_genid = 0;

                    memcpy((char *)dbt_key.data + keylen, &masked_genid,
                           sizeof(unsigned long long));
                    dbt_key.size += sizeof(unsigned long long);
                }

                /* just fetch the genid portion, we'll verify dtacopy in the key
                 * passes */
                verify_genid = 0;
                dbt_data.data = &verify_genid;
                dbt_data.size = sizeof(unsigned long long);
                dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
                dbt_data.ulen = sizeof(unsigned long long);
                dbt_data.doff = 0;
                dbt_data.dlen = sizeof(unsigned long long);

                rc = ckey->c_get(ckey, &dbt_key, &dbt_data, DB_SET);
                if (!(has_keys & (1ULL << ix))) {
                    if (!rc) {
                        ret = 1;
                        locprint(
                            sb, lua_callback, lua_params,
                            "!%016llx ix %d expect notfound but got an index\n",
                            genid_flipped, ix);
                    }
                } else if (rc == DB_NOTFOUND) {
                    ret = 1;
                    locprint(sb,  lua_callback, lua_params,
                            "!%016llx ix %d missing key\n", genid_flipped, ix);
                }
                else if (rc) {
                    ret = 1;
                    locprint(sb, lua_callback, lua_params, "!%016llx ix %d fetch rc %d\n",
                                genid_flipped, ix, rc);
                }
                else if (genid != verify_genid) {
                    ret = 1;
                    locprint(sb, lua_callback, lua_params, "!%016llx ix %d genid mismatch %016llx\n",
                                genid_flipped, ix, verify_genid);
                }

                ckey->c_close(ckey);
            }
            free_blob_buffer_callback(callback_blob_buf);

            sbuf2flush(sb);
        next_record:

            dbt_data.flags = DB_DBT_USERMEM;
            dbt_data.ulen = sizeof(databuf);
            dbt_data.data = databuf;
            dbt_key.flags = DB_DBT_USERMEM;
            dbt_key.ulen = sizeof(keybuf);
            dbt_key.data = keybuf;

            rc = bdb_cget_unpack(bdb_state, cdata, &dbt_key, &dbt_data, &ver,
                                 DB_NEXT);
        }
        if (rc != DB_NOTFOUND) {
            cdata->c_close(cdata);
            ret = 1;
            locprint(sb, lua_callback, lua_params, "!dtastripe %d c_get unexpected rc %d\n", dtastripe,
                        rc);
            return rc;
        }
        cdata->c_close(cdata);
    }

    bzero(&dbt_key, sizeof(DBT));
    bzero(&dbt_data, sizeof(DBT));

    dbt_key.data = keybuf;
    dbt_key.ulen = sizeof(keybuf);
    dbt_key.flags = DB_DBT_USERMEM;

    dbt_data.data = databuf;
    dbt_data.ulen = sizeof(databuf);
    dbt_data.flags = DB_DBT_USERMEM;

    dbt_dta_check_key.data = &genid;
    dbt_dta_check_key.ulen = sizeof(unsigned long long);
    dbt_dta_check_key.size = sizeof(unsigned long long);
    dbt_dta_check_key.flags = DB_DBT_USERMEM;

    dbt_dta_check_data.data = &verify_keybuf;
    dbt_dta_check_data.ulen = sizeof(verify_keybuf);
    dbt_dta_check_data.flags = DB_DBT_USERMEM;

    /* scan 2: scan each key, verify data exists */
    for (ix = 0; ix < bdb_state->numix; ix++) {
        nrecs = 0;
        nrecs_progress = 0;
        rc = bdb_state->dbp_ix[ix]->paired_cursor_from_lid(
            bdb_state->dbp_ix[ix], lid, &ckey, 0);
        if (rc) {
            ret = 1;
            locprint(sb, lua_callback, lua_params, "!ix %d cursor rc %d\n", ix, rc);
            continue;
        }
        rc = ckey->c_get(ckey, &dbt_key, &dbt_data, DB_FIRST);
        if (rc && rc != DB_NOTFOUND) {
            ret = 1;
            locprint(sb, lua_callback, lua_params, "!ix %d first rc %d\n", ix, rc);
        }
        while (rc == 0) {
            nrecs++;
            nrecs_progress++;

            now = comdb2_time_epochms();

            /* check if comdb2sc is killed */
            if ((now - last) > 1000) {
                if (dropped_connection(sb)) {
                    cdata->c_close(cdata);
                    logmsg(LOGMSG_WARN, "condb2sc connection closed, stopped verify\n");
                    return rc;
                }
            }

            if (progress_report_seconds &&
                ((now - last) >= (progress_report_seconds * 1000))) {
                locprint(sb, lua_callback, lua_params,
                    "!verifying index %d, did %lld records, %d per second\n",
                    ix, nrecs, (int)(nrecs_progress / progress_report_seconds));
                last = now;
                nrecs_progress = 0;
                sbuf2flush(sb);
            }

            if (dbt_data.size < sizeof(unsigned long long)) {
                ret = 1;
                locprint(sb, lua_callback, lua_params,
                         "!ix %d unexpected length %d\n", dbt_data.size);
                goto next_key;
            }
            memcpy(&genid, dbt_data.data, sizeof(unsigned long long));

#ifdef _LINUX_SOURCE
            buf_put(&genid, sizeof(unsigned long long),
                    (uint8_t *)&genid_flipped,
                    (uint8_t *)&genid_flipped + sizeof(unsigned long long));
#else
            genid_flipped = genid;
#endif

            /* make sure the data entry exists: */
            db = get_dbp_from_genid(bdb_state, 0, genid, NULL);
            rc = db->paired_cursor_from_lid(db, lid, &cdata, 0);
            if (rc) {
                ret = 1;
                locprint(sb, lua_callback, lua_params, "!%016llx ix %d rc %d\n", genid_flipped, ix,
                            rc);
                goto next_key;
            }
            rc = bdb_cget_unpack(bdb_state, cdata, &dbt_dta_check_key,
                                 &dbt_dta_check_data, &ver, DB_SET);
            if (rc == DB_NOTFOUND) {
                ret = 1;
                locprint(sb, lua_callback, lua_params, "!%016llx ix %d orphaned ", genid_flipped, ix);
                printhex(sb, lua_callback, lua_params, dbt_key.data, dbt_key.size);
                locprint(sb, lua_callback, lua_params, "\n");

                goto next_key;
            } else if (rc) {
                ret = 1;
                locprint(sb, lua_callback, lua_params, "!%016llx ix %d dta rc %d\n", genid_flipped, ix,
                            rc);
                goto next_key;
            }
            cdata->c_close(cdata);

            vtag_callback(callback_parm, dbt_dta_check_data.data, &keylen, ver);
            if (gbl_expressions_indexes &&
                is_comdb2_index_expression(bdb_state->name)) {
                /* indexes expressions may need blobs */
                rc = get_blob_sizes_callback(callback_parm,
                                             dbt_dta_check_data.data, blobsizes,
                                             bloboffs, &nblobs);
                if (rc) {
                    sbuf2printf(sb, "!%016llx blob size rc %d\n", genid, rc);
                } else {
                    /* verify blobs */
                    int realblobsz[16];
                    int had_errors, had_irrecoverable_errors;

                    had_errors = 0;
                    had_irrecoverable_errors = 0;
                    for (blobno = 0; blobno < nblobs; blobno++) {
                        DBC *cblob;
                        DB *blobdb;
                        unsigned long long blob_genid = genid;
                        int dtafile;

                        realblobsz[blobno] = -1;
                        had_irrecoverable_errors = 0;
                        had_errors = 0;

                        dtafile = get_dtafile_from_genid(genid);
                        if (dtafile < 0) {
                            sbuf2printf(sb, "!%016llx unknown dtafile\n",
                                        genid_flipped);
                            continue;
                        }
                        blobdb = get_dbp_from_genid(bdb_state, blobno + 1,
                                                    genid, NULL);

                        rc = blobdb->paired_cursor_from_lid(blobdb, lid, &cblob,
                                                            0);
                        if (rc) {
                            sbuf2printf(sb,
                                        "!%016llx cursor on blob %d rc %d\n",
                                        genid_flipped, blobno, rc);
                            continue;
                        }

                        /* Note: we have to fetch the whole blob here because
                           with ondisk headers + compression
                           the size of the blob will not match what's stored in
                           the record so a partial find
                           won't do.  I guess we could optimize for the more
                           common case of no headers/compression. */
                        dbt_blob_key.data = &blob_genid;
                        dbt_blob_key.size = sizeof(unsigned long long);
                        dbt_blob_data.flags = DB_DBT_MALLOC;
                        dbt_blob_data.data = NULL;

                        rc = bdb_cget_unpack_blob(bdb_state, cblob,
                                                  &dbt_blob_key, &dbt_blob_data,
                                                  &ver, DB_SET);
                        if (rc == DB_NOTFOUND) {
                            realblobsz[blobno] = -1;
                            if (blobsizes[blobno] != -1 &&
                                blobsizes[blobno] != -2) {
                                had_errors = 1;
                                sbuf2printf(sb, "!%016llx no blob %d found "
                                                "expected sz %d\n",
                                            genid_flipped, blobno,
                                            blobsizes[blobno]);
                            }
                        } else if (rc) {
                            had_irrecoverable_errors = 1;
                            sbuf2printf(sb, "!%016llx blob %d rc %d\n",
                                        genid_flipped, blobno, rc);
                            had_errors = 1;
                        }

                        if (rc == 0) {
                            realblobsz[blobno] = dbt_blob_data.size;
                            if (blobsizes[blobno] == -1 && rc != DB_NOTFOUND) {
                                sbuf2printf(
                                    sb,
                                    "!%016llx blob %d null but found blob\n",
                                    genid_flipped, blobno);
                            } else if (blobsizes[blobno] == -2) {
                                sbuf2printf(
                                    sb, "!%016llx blob %d size %d expected "
                                        "none (inline vutf8)\n",
                                    genid_flipped, blobno, realblobsz[blobno]);
                            } else if (blobsizes[blobno] != -1 &&
                                       dbt_blob_data.size !=
                                           blobsizes[blobno]) {
                                sbuf2printf(sb, "!%016llx blob %d size "
                                                "mismatch got %d expected %d\n",
                                            genid_flipped, blobno,
                                            dbt_blob_data.size,
                                            blobsizes[blobno]);
                                had_errors = 1;
                            }

                            if (blobsizes[blobno] >= 0 &&
                                realblobsz[blobno] >= 0) {
                                rc = add_blob_buffer_callback(
                                    callback_blob_buf, dbt_blob_data.data,
                                    dbt_blob_data.size, blobno);
                                if (rc)
                                    return rc;
                            }

                            if (dbt_blob_data.data && had_errors == 0)
                                free(dbt_blob_data.data);
                        }
                        cblob->c_close(cblob);
                    }
                }
            }

            rc = formkey_callback(callback_parm, dbt_dta_check_data.data,
                                  callback_blob_buf, ix, expected_keybuf,
                                  &keylen);
            free_blob_buffer_callback(callback_blob_buf);

            if (dbt_key.size < keylen) {
                ret = 1;
                locprint(sb, lua_callback, lua_params, "!%016llx ix %d key size %d < formed key %d\n",
                            genid_flipped, ix, dbt_key.size, keylen);
                goto next_key;
            }

            if (memcmp(expected_keybuf, dbt_key.data, keylen)) {
                ret = 1;
                locprint(sb, lua_callback, lua_params, "!%016llx ix %d key mismatch\n", genid_flipped,
                            ix);
                goto next_key;
            }

            if (bdb_keycontainsgenid(bdb_state, ix))
                keylen += sizeof(unsigned long long);

            if (keylen != dbt_key.size) {
                ret = 1;
                locprint(sb, lua_callback, lua_params,
                    "!%016llx ix %d key size mismatch expected %d got %d\n",
                    genid_flipped, ix, keylen, dbt_key.size);
                goto next_key;
            }

            unsigned long long genid_left, genid_right, masked_genid;

            if (bdb_state->ixdta[ix]) {
                /*  if dtacopy, does data payload in the key match the data
                 * payload in the dta file? */
                int expected_size;
                uint8_t *expected_data;
                uint8_t datacopy_buffer[bdb_state->lrl];
                if (bdb_state->datacopy_odh) {
                    int odhlen;
                    unpack_index_odh(bdb_state, &dbt_data, &genid_right,
                                     datacopy_buffer, sizeof(datacopy_buffer),
                                     &odhlen, &ver);
                    expected_size = odhlen;
                    vtag_callback(callback_parm, datacopy_buffer,
                                  &expected_size, ver);
                    expected_data = datacopy_buffer;
                } else {
                    expected_size = dbt_data.size - sizeof(genid);
                    expected_data = (uint8_t *)dbt_data.data + sizeof(genid);
                    memcpy(&genid_right, (uint8_t *)dbt_data.data,
                           sizeof(genid));
                }

                if (expected_size != bdb_state->lrl) {
                    ret = 1;
                    locprint(sb, lua_callback, lua_params, "!%016llx ix %d dtacpy payload wrong size "
                                    "expected %d got %d\n",
                                genid_flipped, ix, bdb_state->lrl,
                                expected_size);
                    goto next_key;
                }

                if (memcmp(expected_data, dbt_dta_check_data.data,
                           bdb_state->lrl)) {
                    ret = 1;
                    locprint(sb, lua_callback, lua_params, "!%016llx ix %d dtacpy data mismatch\n",
                                genid_flipped, ix);
                    goto next_key;
                }

            } else if (bdb_state->ixcollattr[ix]) {
                if (dbt_data.size != (sizeof(unsigned long long) +
                                      4 * bdb_state->ixcollattr[ix])) {
                    ret = 1;
                    locprint(sb, lua_callback, lua_params,
                             "!%016llx ix %d decimal payload wrong size "
                             "expected %d got %d\n",
                             genid_flipped, ix,
                             sizeof(unsigned long long) +
                                 4 * bdb_state->ixcollattr[ix],
                             dbt_data.size);
                    goto next_key;
                }
                memcpy(&genid_right, (uint8_t *)dbt_data.data, sizeof(genid));
            } else {
                if (dbt_data.size != sizeof(unsigned long long)) {
                    ret = 1;
                    locprint(sb, lua_callback, lua_params,
                        "!%016llx ix %d payload wrong size expected 8 got %d\n",
                        genid_flipped, ix, dbt_data.size);
                    goto next_key;
                }
                memcpy(&genid_right, (uint8_t *)dbt_data.data, sizeof(genid));
            }

            if (bdb_state->ixdups[ix]) {
                memcpy(&genid_left, (uint8_t *)dbt_key.data + keylen - 8,
                       sizeof(genid_left));
                masked_genid = get_search_genid(bdb_state, genid);
                if (memcmp(&genid_left, &masked_genid, sizeof(genid))) {
                    ret = 1;
                    locprint(sb, lua_callback, lua_params, "!%016llx ix %d dupe key genid != dta "
                                    "genid %016llx (%016llx)\n",
                                genid_left, ix, masked_genid, genid);
                }
            }

            if (memcmp(&genid_right, &genid, sizeof(genid))) {
                ret = 1;
                locprint(sb, lua_callback, lua_params,
                    "!%016llx ix %d dupe key genid != dta genid %016llx\n",
                    genid_right, ix, genid);
            }

        next_key:
            rc = ckey->c_get(ckey, &dbt_key, &dbt_data, DB_NEXT);
        }
        if (rc && rc != DB_NOTFOUND) {
            ret = 1;
            locprint(sb, lua_callback, lua_params, "!ix %d first rc %d\n", ix, rc);
        }
        rc = ckey->c_close(ckey);
        if (rc) {
            ret = 1;
            locprint(sb, lua_callback, lua_params, "!%016llx ix %d close cursor rc %d\n", genid, ix,
                        rc);
        }
    }

    /* scan 3: scan each blob, verify data exists */
    for (blobno = 0; blobno < nblobs; blobno++) {
        for (dtastripe = 0; dtastripe < bdb_state->attr->blobstripe;
             dtastripe++) {
            db = bdb_state->dbp_data[blobno + 1][dtastripe];

            if (!db) {
                ret = 1;
                locprint(sb, lua_callback, lua_params, "incorrect number of blobs? blob index %d "
                                "stripe %d has no DB\n",
                            blobno, dtastripe);
                continue;
            }

            rc = db->paired_cursor_from_lid(db, lid, &cblob, 0);
            if (rc) {
                logmsg(LOGMSG_ERROR, "dtastripe %d blobno %d cursor rc %d\n", dtastripe,
                       blobno, rc);
                continue;
            }

            dbt_key.ulen = dbt_key.size = sizeof(unsigned long long);
            dbt_key.data = &genid;
            dbt_key.flags = DB_DBT_USERMEM;
            dbt_data.data = &dumbuf;
            dbt_data.ulen = 1;
            dbt_data.doff = 0;
            dbt_data.dlen = 0;
            dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

            dbt_dta_check_key.ulen = sizeof(int);
            dbt_dta_check_key.data = &genid;
            dbt_dta_check_key.flags = DB_DBT_USERMEM;
            dbt_dta_check_data.data = &dumbuf;
            dbt_dta_check_data.ulen = 1;
            dbt_dta_check_data.doff = 0;
            dbt_dta_check_data.dlen = 0;
            dbt_dta_check_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

            rc = cblob->c_get(cblob, &dbt_key, &dbt_data, DB_FIRST);
            while (rc == 0) {
                int stripe;
                unsigned long long genid_flipped;

#ifdef _LINUX_SOURCE
                buf_put(&genid, sizeof(unsigned long long),
                        (uint8_t *)&genid_flipped,
                        (uint8_t *)&genid_flipped + sizeof(unsigned long long));
#else
                genid_flipped = genid;
#endif

                stripe = get_dtafile_from_genid(genid);

                if (!bdb_state->blobstripe_convert_genid ||
                    bdb_check_genid_is_newer(
                        bdb_state, genid,
                        bdb_state->blobstripe_convert_genid)) {
                    /* verify blobstripe and datastripe is the same */
                    if (dtastripe != stripe)
                        locprint(sb, lua_callback, lua_params,
                                 "!%016llx blobstripe %d != datastripe %d\n",
                                 genid_flipped, dtastripe, stripe);
                }

                rc = bdb_state->dbp_data[0][stripe]->paired_cursor_from_lid(
                    bdb_state->dbp_data[0][stripe], lid, &cdata, 0);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "dtastripe %d genid %016llx cursor rc %d\n", stripe,
                           genid_flipped, rc);
                    rc = cblob->c_get(cblob, &dbt_key, &dbt_data, DB_NEXT);
                    continue;
                }
                rc = cdata->c_get(cdata, &dbt_dta_check_key,
                                  &dbt_dta_check_data, DB_SET);
                if (rc == DB_NOTFOUND) {
                    ret = 1;
                    locprint(sb, lua_callback, lua_params, "!%016llx orphaned blob\n", genid_flipped);
                }
                else if (rc) {
                    ret = 1;
                    locprint(sb, lua_callback, lua_params, "!%016llx get rc %d\n", genid_flipped, rc);
                }

                rc = cdata->c_close(cdata);
                if (rc)
                    logmsg(LOGMSG_ERROR, "close rc %d\n", rc);

                rc = cblob->c_get(cblob, &dbt_key, &dbt_data, DB_NEXT);
            }
            if (rc != DB_NOTFOUND)
                logmsg(LOGMSG_ERROR, "fetch blob rc %d\n", rc);

            cblob->c_close(cblob);
        }
    }

    return ret;
}
