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
#include <bdb_api.h>
#include <bdb_verify.h>

#include "sbuf2.h"
#include "bdb_int.h"
#include "locks.h"
#include "endian_core.h"

#include "logmsg.h"
#include "tohex.h"
#include "blob_buffer.h"
#include "comdb2_atomic.h"
#include "constraints.h"

/* NOTE: This is from "comdb2.h". */
extern int gbl_expressions_indexes;
extern int get_numblobs(const dbtable *tbl);
extern int ix_isnullk(const dbtable *db_table, void *key, int ixnum);
extern int is_comdb2_index_expression(const char *dbname);
extern void set_null_func(void *p, int len);
extern void set_data_func(void *to, const void *from, int sz);
extern void fsnapf(FILE *, void *, int);


/* print to sb if available lua callback otherwise */
static int locprint(SBUF2 *sb, int (*lua_callback)(void *, const char *), 
        void *lua_params, char *fmt, ...)
{
    char lbuf[1024];
    va_list ap;
    va_start(ap, fmt);
    int wrote = vsnprintf(lbuf, sizeof(lbuf), fmt, ap);
    va_end(ap);

    if (sb) {
        if (wrote < sizeof(lbuf) - 1)
            strcat(lbuf, "\n");
        return sbuf2printf(sb, lbuf) >= 0 ? 0 : -1;
    } else if (lua_callback)
        return lua_callback(lua_params, lbuf);
    return -1;
}

int bdb_dropped_connection(SBUF2 *sb)
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

static int restore_cursor_at_genid(DB *db, DBC **cdata,
                                   unsigned long long genid, unsigned int lid)
{
    DBC *c;
    int rc;
    DBT key = {0};
    DBT dta = {0};

    rc = db->paired_cursor_from_lid(db, lid, &c, 0);
    if (rc)
        return rc;
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
    int rc =0, crc;
    tran_type *t = NULL;
    int bdberr;
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

    DBT key = {0};
    DBT dta = {0};
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
            rc = bdb_tran_commit_with_seqnum_size(bdb_state, t, &seqnum, NULL,
                                                  &bdberr);
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

static inline int print_verify_progress(verify_common_t *par, int now)
{
    if (!par->progress_report_seconds)
        goto out;

    int last = par->last_reported; // get a copy of the last timestamp

    // do the comparison with now
    if (((now - last) < (par->progress_report_seconds * 1000)))
        goto out;

    // enough time has passed, attempt to update
    int res = CAS32(par->last_reported, last, now);
    if (!res)
        goto out; // someonelse updated, get out

    if (bdb_dropped_connection(par->sb)) {
        logmsg(LOGMSG_WARN, "client connection closed, stopped verify\n");
        par->client_dropped_connection = 1;
        goto out;
    }

    int rc;
    if (par->verify_mode == VERIFY_SERIAL) {
        rc = locprint(par->sb, par->lua_callback, par->lua_params,
                      "!%s, did %d records, %d per second", par->header,
                      par->nrecs_progress,
                      par->nrecs_progress / par->progress_report_seconds);
        par->nrecs_progress = 0;
    } else {
        unsigned long long delta = par->items_processed - par->saved_progress;
        rc = locprint(par->sb, par->lua_callback, par->lua_params,
                      "!verify: processed %lld items, %lld per second",
                      par->items_processed,
                      delta / par->progress_report_seconds);
        par->saved_progress = par->items_processed;
    }

    if (rc) {
        par->client_dropped_connection = 1;
        goto out;
    }
    sbuf2flush(par->sb);
out:
    return par->client_dropped_connection;
}

/* TODO: handle deadlock, get rowlocks if db in rowlocks mode */
static int bdb_verify_data_stripe(verify_common_t *par, int dtastripe,
                                  unsigned int lid)
{
    DBC *cdata = NULL;
    DBC *ckey = NULL;
    DB *db;
    DBC *cblob = NULL;
    unsigned char databuf[17 * 1024];
    unsigned char keybuf[18 * 1024];
    unsigned char expected_keybuf[18 * 1024];
    int rc = 0;
    int blobsizes[16];
    int bloboffs[16];
    int nblobs = 0;
    blob_buffer_t blob_buf[MAXBLOBS] = {{0}};

    bdb_state_type *bdb_state = par->bdb_state;

    DBT dbt_data = {
        .flags = DB_DBT_USERMEM, .ulen = sizeof(databuf), .data = databuf};

    DBT dbt_key = {
        .flags = DB_DBT_USERMEM, .ulen = sizeof(keybuf), .data = keybuf};

    db = bdb_state->dbp_data[0][dtastripe];
    rc = db->paired_cursor_from_lid(db, lid, &cdata, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "dtastripe %d cursor rc %d\n", dtastripe, rc);
        return rc;
    }
    uint8_t ver;
    rc = bdb_cget_unpack(bdb_state, cdata, &dbt_key, &dbt_data, &ver, DB_FIRST);
    int atstart = comdb2_time_epochms();
    int now = atstart;
    logmsg(LOGMSG_DEBUG, "%p:%s Entering stripe=%d\n", (void *)pthread_self(),
           __func__, dtastripe);

    while (rc == 0 && !par->client_dropped_connection) {
        ATOMIC_ADD64(par->items_processed, 1);
        par->records_processed++;
        par->nrecs_progress++;

        now = comdb2_time_epochms();
        /* check existence of client and print progress every 1000ms */
        if (print_verify_progress(par, now))
            break;

        unsigned long long genid;
        /* is it the right size? */
        if (dbt_key.size != sizeof(genid)) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!bad genid sz %d", dbt_key.size);
            goto next_record;
        }
        memcpy(&genid, dbt_key.data, sizeof(genid));

        /* why do we open a cursor for each record/blob?
        1) cursors are cheap - berkeley opens one for every cursor operation
        2) we don't want to keep an active cursor to prevent
          locking up db operations
        */
        unsigned long long genid_flipped;

#ifdef _LINUX_SOURCE
        buf_put(&genid, sizeof(unsigned long long), (uint8_t *)&genid_flipped,
                (uint8_t *)&genid_flipped + sizeof(unsigned long long));
#else
        genid_flipped = genid;
#endif
        par->vtag_callback(par->db_table, dbt_data.data, (int *)&dbt_data.size,
                           ver);
        rc = par->get_blob_sizes_callback(par->db_table, dbt_data.data,
                                          blobsizes, bloboffs, &nblobs);
        if (rc) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!%016llx blob size rc %d", genid, rc);
        } else {
            /* verify blobs */
            int realblobsz[16];
            int had_errors, had_irrecoverable_errors;

            had_errors = 0;
            had_irrecoverable_errors = 0;
            for (int blobno = 0; blobno < nblobs; blobno++) {
                DB *blobdb;
                unsigned long long blob_genid = genid;
                int dtafile;

                realblobsz[blobno] = -1;
                had_irrecoverable_errors = 0;
                had_errors = 0;

                dtafile = get_dtafile_from_genid(genid);
                if (dtafile < 0) {
                    par->verify_status = 1;
                    locprint(par->sb, par->lua_callback, par->lua_params,
                             "!%016llx unknown dtafile", genid_flipped);
                    rc = 0;
                    goto next_record;
                }
                blobdb = get_dbp_from_genid(bdb_state, blobno + 1, genid, NULL);

                rc = blobdb->paired_cursor_from_lid(blobdb, lid, &cblob, 0);
                if (rc) {
                    par->verify_status = 1;
                    locprint(par->sb, par->lua_callback, par->lua_params,
                             "!%016llx cursor on blob %d rc %d", genid_flipped,
                             blobno, rc);
                    rc = 0;
                    goto next_record;
                }

                /* Note: we have to fetch the whole blob here because with
                   ondisk headers + compression
                   the size of the blob will not match what's stored in the
                   record so a partial find
                   won't do.  I guess we could optimize for the more common
                   case of no headers/compression. */
                DBT dbt_blob_key = {.data = &blob_genid,
                                    .size = sizeof(unsigned long long)};

                DBT dbt_blob_data = {.flags = DB_DBT_MALLOC, .data = NULL};

                rc = bdb_cget_unpack_blob(bdb_state, cblob, &dbt_blob_key,
                                          &dbt_blob_data, &ver, DB_SET);
                if (rc == DB_NOTFOUND) {
                    realblobsz[blobno] = -1;
                    if (blobsizes[blobno] != -1 && blobsizes[blobno] != -2) {
                        had_errors = 1;
                        par->verify_status = 1;
                        locprint(par->sb, par->lua_callback, par->lua_params,
                                 "!%016llx no blob %d found expected sz %d",
                                 genid_flipped, blobno, blobsizes[blobno]);
                    }
                } else if (rc) {
                    had_irrecoverable_errors = 1;
                    par->verify_status = 1;
                    locprint(par->sb, par->lua_callback, par->lua_params,
                             "!%016llx blob %d rc %d", genid_flipped, blobno,
                             rc);
                    had_errors = 1;
                }

                if (rc == 0) {
                    realblobsz[blobno] = dbt_blob_data.size;
                    if (blobsizes[blobno] == -1) {
                        par->verify_status = 1;
                        locprint(par->sb, par->lua_callback, par->lua_params,
                                 "!%016llx blob %d null but found blob",
                                 genid_flipped, blobno);
                        had_errors = 1;
                    } else if (blobsizes[blobno] == -2) {
                        par->verify_status = 1;
                        locprint(par->sb, par->lua_callback, par->lua_params,
                                 "!%016llx blob %d size %d expected "
                                 "none (inline vutf8)",
                                 genid_flipped, blobno, realblobsz[blobno]);
                        had_errors = 1;
                    } else if (blobsizes[blobno] != -1 &&
                               dbt_blob_data.size != blobsizes[blobno]) {
                        par->verify_status = 1;
                        locprint(par->sb, par->lua_callback, par->lua_params,
                                 "!%016llx blob %d size mismatch "
                                 "got %d expected %d",
                                 genid_flipped, blobno, dbt_blob_data.size,
                                 blobsizes[blobno]);
                        had_errors = 1;
                    }

                    if (blobsizes[blobno] >= 0 && realblobsz[blobno] >= 0) {
                        rc = par->add_blob_buffer_callback(
                            blob_buf, dbt_blob_data.data, dbt_blob_data.size,
                            blobno);
                        if (rc)
                            goto err;
                    }

                    free(dbt_blob_data.data);
                }
                cblob->c_close(cblob);
                cblob = NULL;
            }
            if (par->attempt_fix && had_errors && !had_irrecoverable_errors) {
                rc = fix_blobs(bdb_state, db, &cdata, genid, nblobs, bloboffs,
                               realblobsz, lid);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "fix_blobs rc %d\n", rc);
                    /* close? */
                    par->free_blob_buffer_callback(blob_buf);
                    goto err;
                }
            }
        }

        unsigned long long has_keys = par->verify_indexes_callback(
            par->db_table, dbt_data.data, blob_buf);

        for (int ix = 0; ix < bdb_state->numix; ix++) {
            rc = bdb_state->dbp_ix[ix]->paired_cursor_from_lid(
                bdb_state->dbp_ix[ix], lid, &ckey, 0);
            if (rc) {
                par->verify_status = 1;
                par->free_blob_buffer_callback(blob_buf);
                logmsg(LOGMSG_ERROR,
                       "unexpected rc opening cursor for ix %d: %d\n", ix, rc);
                goto err;
            }

            int keylen;
            rc = par->formkey_callback(par->db_table, databuf, blob_buf, ix,
                                       expected_keybuf, &keylen);
            if (rc) {
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix %d formkey rc %d", genid_flipped, ix, rc);
                ckey->c_close(ckey);
                ckey = NULL;
                rc = 0;
                goto next_record; /* ? */
            }

            /* set up key */

            // AZ why not eliminate expected_keybuf totally, write directly data
            memcpy(dbt_key.data, expected_keybuf, keylen);
            dbt_key.size = keylen;
            if (bdb_keycontainsgenid(bdb_state, ix)) {
                unsigned long long masked_genid =
                    get_search_genid(bdb_state, genid);

                /* use 0 as the genid if no null values to keep it unique */
                if (bdb_state->ixnulls[ix] &&
                    !ix_isnullk(par->db_table, dbt_key.data, ix))
                    masked_genid = 0;

                memcpy((char *)dbt_key.data + keylen, &masked_genid,
                       sizeof(unsigned long long));
                dbt_key.size += sizeof(unsigned long long);
            }

            /* just fetch the genid portion, we'll verify dtacopy in the key
             * passes */
            unsigned long long verify_genid = 0;
            dbt_data.data = &verify_genid;
            dbt_data.size = sizeof(unsigned long long);
            dbt_data.ulen = sizeof(unsigned long long);
            dbt_data.dlen = sizeof(unsigned long long);
            dbt_data.doff = 0;
            dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

            rc = ckey->c_get(ckey, &dbt_key, &dbt_data, DB_SET);
            if (!(has_keys & (1ULL << ix))) {
                if (!rc && (bdb_state->ixdups[ix] || genid == verify_genid)) {
                    par->verify_status = 1;
                    locprint(par->sb, par->lua_callback, par->lua_params,
                             "!%016llx ix %d expect notfound but got an index",
                             genid_flipped, ix);
                }
            } else if (rc == DB_NOTFOUND) {
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix %d missing key", genid_flipped, ix);
            } else if (rc) {
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix %d fetch rc %d", genid_flipped, ix, rc);
            } else if (genid != verify_genid) {
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix %d genid mismatch %016llx", genid_flipped,
                         ix, verify_genid);
            }

            ckey->c_close(ckey);
            ckey = NULL;
        }
        par->free_blob_buffer_callback(blob_buf);
        sbuf2flush(par->sb);
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
        par->verify_status = 1;
        locprint(par->sb, par->lua_callback, par->lua_params,
                 "!dtastripe %d c_get unexpected rc %d", dtastripe, rc);
    } else
        rc = 0;
err:
    if (cblob)
        cblob->c_close(cblob);
    if (ckey)
        ckey->c_close(ckey);
    if (cdata)
        cdata->c_close(cdata);
    logmsg(LOGMSG_DEBUG, "%p:%s Exiting stripe=%d, delta=%dms\n",
           (void *)pthread_self(), __func__, dtastripe, now - atstart);
    return rc;
}

/* similar to check_single_key_constraint but uses a paired cursor/cget
 * so we can release the lock at the end of this function
 */
static int verify_foreign_key_constraint(constraint_t *ct, char *lcl_tag,
                                         char *lcl_key, char *tblname, int lid,
                                         int *remote_ri)
{
    int rc = 0;
    if (remote_ri)
        *remote_ri = 0;
    char rkey[BDB_RECORD_MAX + sizeof(unsigned long long)];
    DBT dbt_key = {.flags = DB_DBT_USERMEM, .ulen = sizeof(rkey), .data = rkey};
    DBC *ckey = NULL;

    for (int ri = 0; ri < ct->nrules; ri++) {
        int ridx;
        int rixlen;
        bdb_state_type *r_state;
        int skip = 0;
        rc = convert_key_to_foreign_key(ct, lcl_tag, lcl_key, tblname, &r_state,
                                        &ridx, &rixlen, rkey, &skip, ri);
        if (rc)
            return rc;
        
        if (!skip) {
            DB *ix_state = r_state->dbp_ix[ridx];
            rc = ix_state->paired_cursor_from_lid(ix_state, lid, &ckey, 0);
            if (rc) {
                logmsg(LOGMSG_ERROR, "unexpected rc get cursor for ix %d: %d\n",
                       ridx, rc);
                continue; // so we continue to next
            }

            /* fetch the genid portion to verify existence */
            unsigned long long verify_genid = 0;
            DBT dbt_data = {.data = &verify_genid,
                            .dlen = sizeof(verify_genid),
                            .ulen = sizeof(verify_genid),
                            .size = sizeof(verify_genid),
                            .doff = 0,
                            .flags = DB_DBT_USERMEM | DB_DBT_PARTIAL};
            dbt_key.size = rixlen;

            rc = ckey->c_get(ckey, &dbt_key, &dbt_data, DB_SET_RANGE);
            ckey->c_close(ckey);
        }

        if (rc != IX_FND && rc != IX_FNDMORE) {
            if (remote_ri)
                *remote_ri = ri;
            goto done;
        }
    }
done:
    return rc;
}

static int bdb_verify_key(verify_common_t *par, int ix, unsigned int lid)
{
    DBC *cdata = NULL;
    DBC *ckey = NULL;
    unsigned char databuf[17 * 1024];
    unsigned char keybuf[18 * 1024];
    unsigned char expected_keybuf[18 * 1024];
    unsigned char verify_keybuf[18 * 1024];
    int rc = 0;
    int blobsizes[16];
    int bloboffs[16];
    int nblobs = 0;
    blob_buffer_t blob_buf[MAXBLOBS] = {{0}};

    bdb_state_type *bdb_state = par->bdb_state;

    DBT dbt_key = {
        .data = keybuf, .ulen = sizeof(keybuf), .flags = DB_DBT_USERMEM};

    DBT dbt_data = {
        .data = databuf, .ulen = sizeof(databuf), .flags = DB_DBT_USERMEM};

    unsigned long long genid;
    DBT dbt_dta_check_key = {.ulen = sizeof(unsigned long long),
                             .size = sizeof(unsigned long long),
                             .data = &genid,
                             .flags = DB_DBT_USERMEM};

    DBT dbt_dta_check_data = {.data = &verify_keybuf,
                              .flags = DB_DBT_USERMEM,
                              .ulen = sizeof(verify_keybuf)};

    int atstart = comdb2_time_epochms();
    int now = atstart;
    logmsg(LOGMSG_DEBUG, "%p:%s Entering ix=%d\n", (void *)pthread_self(),
           __func__, ix);

    rc = bdb_state->dbp_ix[ix]->paired_cursor_from_lid(bdb_state->dbp_ix[ix],
                                                       lid, &ckey, 0);
    if (rc) {
        par->verify_status = 1;
        locprint(par->sb, par->lua_callback, par->lua_params,
                 "!ix %d cursor rc %d", ix, rc);
        return 0;
    }

    char ix_tag[MAXTAGLEN];
    constraint_t *ix_constraint = get_constraint_for_ix(par->db_table, ix);
    if (ix_constraint) {
        snprintf(ix_tag, MAXTAGLEN, ".ONDISK_IX_%d", ix);
    }

    rc = ckey->c_get(ckey, &dbt_key, &dbt_data, DB_FIRST);
    if (rc && rc != DB_NOTFOUND) {
        par->verify_status = 1;
        locprint(par->sb, par->lua_callback, par->lua_params,
                 "!ix %d first rc %d", ix, rc);
    }
    while (rc == 0 && !par->client_dropped_connection) {
        ATOMIC_ADD64(par->items_processed, 1);
        par->records_processed++;
        par->nrecs_progress++;

        now = comdb2_time_epochms();
        /* check existence of client and print progress every 1000ms */
        if (print_verify_progress(par, now))
            break;

        if (dbt_data.size < sizeof(unsigned long long)) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!ix %d unexpected length %d", ix, dbt_data.size);
            goto next_key;
        }
        memcpy(&genid, dbt_data.data, sizeof(unsigned long long));
        unsigned long long genid_flipped;

#ifdef _LINUX_SOURCE
        buf_put(&genid, sizeof(unsigned long long), (uint8_t *)&genid_flipped,
                (uint8_t *)&genid_flipped + sizeof(unsigned long long));
#else
        genid_flipped = genid;
#endif

        /* make sure the data entry exists: */
        DB *db = get_dbp_from_genid(bdb_state, 0, genid, NULL);
        rc = db->paired_cursor_from_lid(db, lid, &cdata, 0);
        if (rc) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!%016llx ix %d rc %d", genid_flipped, ix, rc);
            goto next_key;
        }
        uint8_t ver;
        rc = bdb_cget_unpack(bdb_state, cdata, &dbt_dta_check_key,
                             &dbt_dta_check_data, &ver, DB_SET);
        if (rc == DB_NOTFOUND) {
            par->verify_status = 1;
            char *hexstr = alloca(dbt_key.size * 2 + 1);
            util_tohex(hexstr, dbt_key.data, dbt_key.size);
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!%016llx ix %d orphaned %s", genid_flipped, ix, hexstr);
            goto next_key;
        } else if (rc) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!%016llx ix %d dta rc %d", genid_flipped, ix, rc);
            goto next_key;
        }
        cdata->c_close(cdata);
        cdata = NULL;

        int keylen;
        par->vtag_callback(par->db_table, dbt_dta_check_data.data, &keylen,
                           ver);
        if (gbl_expressions_indexes &&
            is_comdb2_index_expression(bdb_state->name)) {
            /* indexes expressions may need blobs */
            rc = par->get_blob_sizes_callback(par->db_table,
                                              dbt_dta_check_data.data,
                                              blobsizes, bloboffs, &nblobs);
            if (rc) {
                sbuf2printf(par->sb, "!%016llx blob size rc %d", genid, rc);
            } else {
                /* verify blobs */
                int realblobsz[16];

                for (int blobno = 0; blobno < nblobs; blobno++) {
                    DBC *cblob;
                    DB *blobdb;
                    unsigned long long blob_genid = genid;
                    int dtafile;

                    realblobsz[blobno] = -1;

                    dtafile = get_dtafile_from_genid(genid);
                    if (dtafile < 0) {
                        sbuf2printf(par->sb, "!%016llx unknown dtafile",
                                    genid_flipped);
                        goto next_key;
                    }
                    blobdb =
                        get_dbp_from_genid(bdb_state, blobno + 1, genid, NULL);

                    rc = blobdb->paired_cursor_from_lid(blobdb, lid, &cblob, 0);
                    if (rc) {
                        sbuf2printf(par->sb, "!%016llx cursor on blob %d rc %d",
                                    genid_flipped, blobno, rc);
                        goto next_key;
                    }

                    /* Note: we have to fetch the whole blob here because
                       with ondisk headers + compression
                       the size of the blob will not match what's stored in
                       the record so a partial find
                       won't do.  I guess we could optimize for the more
                       common case of no headers/compression. */
                    DBT dbt_blob_key = {0};
                    dbt_blob_key.data = &blob_genid;
                    dbt_blob_key.size = sizeof(unsigned long long);

                    DBT dbt_blob_data = {0};
                    dbt_blob_data.flags = DB_DBT_MALLOC;
                    dbt_blob_data.data = NULL;

                    rc = bdb_cget_unpack_blob(bdb_state, cblob, &dbt_blob_key,
                                              &dbt_blob_data, &ver, DB_SET);
                    if (rc == DB_NOTFOUND) {
                        realblobsz[blobno] = -1;
                        if (blobsizes[blobno] != -1 &&
                            blobsizes[blobno] != -2) {
                            sbuf2printf(par->sb,
                                        "!%016llx no blob %d found "
                                        "expected sz %d",
                                        genid_flipped, blobno,
                                        blobsizes[blobno]);
                        }
                    } else if (rc) {
                        sbuf2printf(par->sb, "!%016llx blob %d rc %d",
                                    genid_flipped, blobno, rc);
                    }

                    if (rc == 0) {
                        realblobsz[blobno] = dbt_blob_data.size;
                        if (blobsizes[blobno] == -1) {
                            sbuf2printf(par->sb,
                                        "!%016llx blob %d null but found blob",
                                        genid_flipped, blobno);
                        } else if (blobsizes[blobno] == -2) {
                            sbuf2printf(par->sb,
                                        "!%016llx blob %d size %d expected "
                                        "none (inline vutf8)",
                                        genid_flipped, blobno,
                                        realblobsz[blobno]);
                        } else if (blobsizes[blobno] != -1 &&
                                   dbt_blob_data.size != blobsizes[blobno]) {
                            sbuf2printf(par->sb,
                                        "!%016llx blob %d size "
                                        "mismatch got %d expected %d",
                                        genid_flipped, blobno,
                                        dbt_blob_data.size, blobsizes[blobno]);
                        }

                        if (blobsizes[blobno] >= 0 && realblobsz[blobno] >= 0) {
                            rc = par->add_blob_buffer_callback(
                                blob_buf, dbt_blob_data.data,
                                dbt_blob_data.size, blobno);
                            if (rc)
                                goto done;
                        }

                        free(dbt_blob_data.data);
                    }
                    cblob->c_close(cblob);
                    cblob = NULL;
                }
            }
        }

        rc = par->formkey_callback(par->db_table, dbt_dta_check_data.data,
                                   blob_buf, ix, expected_keybuf, &keylen);
        par->free_blob_buffer_callback(blob_buf);

        if (dbt_key.size < keylen) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!%016llx ix %d key size %d < formed key %d",
                     genid_flipped, ix, dbt_key.size, keylen);
            goto next_key;
        }

        if (memcmp(expected_keybuf, dbt_key.data, keylen)) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!%016llx ix %d key mismatch", genid_flipped, ix);
            goto next_key;
        }

        if (bdb_keycontainsgenid(bdb_state, ix))
            keylen += sizeof(unsigned long long);

        if (keylen != dbt_key.size) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!%016llx ix %d key size mismatch expected %d got %d",
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
                par->vtag_callback(par->db_table, datacopy_buffer,
                                   &expected_size, ver);
                expected_data = datacopy_buffer;
            } else {
                expected_size = dbt_data.size - sizeof(genid);
                expected_data = (uint8_t *)dbt_data.data + sizeof(genid);
                memcpy(&genid_right, (uint8_t *)dbt_data.data, sizeof(genid));
            }

            if (expected_size != bdb_state->lrl) {
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix %d dtacpy payload wrong size "
                         "expected %d got %d",
                         genid_flipped, ix, bdb_state->lrl, expected_size);
                goto next_key;
            }

            if (memcmp(expected_data, dbt_dta_check_data.data,
                       bdb_state->lrl)) {
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix %d dtacpy data mismatch", genid_flipped,
                         ix);
                goto next_key;
            }

        } else if (bdb_state->ixcollattr[ix]) {
            if (dbt_data.size !=
                (sizeof(unsigned long long) + 4 * bdb_state->ixcollattr[ix])) {
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix %d decimal payload wrong size "
                         "expected %zu got %d",
                         genid_flipped, ix,
                         sizeof(unsigned long long) +
                             4 * bdb_state->ixcollattr[ix],
                         dbt_data.size);
                goto next_key;
            }
            memcpy(&genid_right, (uint8_t *)dbt_data.data, sizeof(genid));
        } else {
            if (dbt_data.size != sizeof(unsigned long long)) {
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix %d payload wrong size expected 8 got %d",
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
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix %d dupe key genid != dta "
                         "genid %016llx (%016llx)",
                         genid_left, ix, masked_genid, genid);
            }
        }

        if (memcmp(&genid_right, &genid, sizeof(genid))) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!%016llx ix %d dupe key genid != dta genid %016llx",
                     genid_right, ix, genid);
        }

        if (ix_constraint) {
            int ridx;
            rc = verify_foreign_key_constraint(ix_constraint, ix_tag,
                                               dbt_key.data, bdb_state->name,
                                               lid, &ridx);
            if (rc == DB_NOTFOUND) {
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix '%d' key '%s': foreign key table '%s' "
                         "key '%s' not found\n",
                         genid, ix, ix_constraint->lclkeyname,
                         ix_constraint->table[ridx],
                         ix_constraint->keynm[ridx]);
            } else if (rc) {
                par->verify_status = 1;
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx ix '%d' key '%s' foreign key table '%s' key "
                         "'%s' error loading rc = %d\n",
                         genid, ix, ix_constraint->lclkeyname,
                         ix_constraint->table[ridx], ix_constraint->keynm[ridx],
                         rc);
            }
        }

    next_key:
        rc = ckey->c_get(ckey, &dbt_key, &dbt_data, DB_NEXT);
    }
    if (rc && rc != DB_NOTFOUND) {
        par->verify_status = 1;
        locprint(par->sb, par->lua_callback, par->lua_params,
                 "!ix %d first rc %d", ix, rc);
    }
    rc = ckey->c_close(ckey);
    if (rc) {
        par->verify_status = 1;
        locprint(par->sb, par->lua_callback, par->lua_params,
                 "!%016llx ix %d close cursor rc %d", genid, ix, rc);
    }

    logmsg(LOGMSG_DEBUG, "%p:%s Exiting ix=%d, delta=%dms\n",
           (void *)pthread_self(), __func__, ix, now - atstart);
done:

    return rc;
}

static void bdb_verify_blob(verify_common_t *par, int blobno, int dtastripe,
                            unsigned int lid)
{
    DBC *cblob;
    int rc = 0;

    bdb_state_type *bdb_state = par->bdb_state;
    DB *db = bdb_state->dbp_data[blobno + 1][dtastripe];

    if (!db) {
        par->verify_status = 1;
        locprint(par->sb, par->lua_callback, par->lua_params,
                 "incorrect number of blobs? blob index %d "
                 "stripe %d has no DB",
                 blobno, dtastripe);
        return;
    }

    rc = db->paired_cursor_from_lid(db, lid, &cblob, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "dtastripe %d blobno %d cursor rc %d\n", dtastripe,
               blobno, rc);
        return;
    }

    char dumbuf;
    unsigned long long genid;

    DBT dbt_key = {.ulen = sizeof(unsigned long long),
                   .size = sizeof(unsigned long long),
                   .data = &genid,
                   .flags = DB_DBT_USERMEM};

    DBT dbt_data = {
        .data = &dumbuf, .ulen = 1, .flags = DB_DBT_USERMEM | DB_DBT_PARTIAL};

    DBT dbt_dta_check_key = {.size = sizeof(unsigned long long),
                             .ulen = sizeof(int), // TODO: why sizeof int?
                             .data = &genid,
                             .flags = DB_DBT_USERMEM};

    DBT dbt_dta_check_data = {
        .data = &dumbuf, .ulen = 1, .flags = DB_DBT_USERMEM | DB_DBT_PARTIAL};

    int atstart = comdb2_time_epochms();
    int now = atstart;
    logmsg(LOGMSG_DEBUG, "%p:%s Entering blobno=%d, stripe=%d\n",
           (void *)pthread_self(), __func__, blobno, dtastripe);

    rc = cblob->c_get(cblob, &dbt_key, &dbt_data, DB_FIRST);
    while (rc == 0 && !par->client_dropped_connection) {
        ATOMIC_ADD64(par->items_processed, 1);
        par->records_processed++;
        par->nrecs_progress++;
        unsigned long long genid_flipped;

        now = comdb2_time_epochms();
        /* check existence of client and print progress every 1000ms */
        if (print_verify_progress(par, now))
            break;

#ifdef _LINUX_SOURCE
        buf_put(&genid, sizeof(unsigned long long), (uint8_t *)&genid_flipped,
                (uint8_t *)&genid_flipped + sizeof(unsigned long long));
        genid_flipped = bdb_genid_to_host_order(genid);
#else
        genid_flipped = genid;
#endif

        int stripe = get_dtafile_from_genid(genid);

        if (!bdb_state->blobstripe_convert_genid ||
            bdb_check_genid_is_newer(bdb_state, genid,
                                     bdb_state->blobstripe_convert_genid)) {
            /* verify blobstripe and datastripe is the same */
            if (dtastripe != stripe)
                locprint(par->sb, par->lua_callback, par->lua_params,
                         "!%016llx blobstripe %d != datastripe %d",
                         genid_flipped, dtastripe, stripe);
        }

        DBC *cdata;
        rc = bdb_state->dbp_data[0][stripe]->paired_cursor_from_lid(
            bdb_state->dbp_data[0][stripe], lid, &cdata, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "dtastripe %d genid %016llx cursor rc %d\n",
                   stripe, genid_flipped, rc);
            rc = cblob->c_get(cblob, &dbt_key, &dbt_data, DB_NEXT);
            return;
        }

        rc = cdata->c_get(cdata, &dbt_dta_check_key, &dbt_dta_check_data,
                          DB_SET);
        if (rc == DB_NOTFOUND) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!%016llx orphaned blob %d", genid_flipped, blobno);
        } else if (rc) {
            par->verify_status = 1;
            locprint(par->sb, par->lua_callback, par->lua_params,
                     "!%016llx get rc %d", genid_flipped, rc);
        }

        rc = cdata->c_close(cdata);
        if (rc)
            logmsg(LOGMSG_ERROR, "close rc %d\n", rc);

        rc = cblob->c_get(cblob, &dbt_key, &dbt_data, DB_NEXT);
    }
    if (rc != DB_NOTFOUND)
        logmsg(LOGMSG_ERROR, "fetch blob rc %d\n", rc);

    cblob->c_close(cblob);
    logmsg(LOGMSG_DEBUG, "%p:%s Exiting blobno=%d, stripe=%d, delta=%dms\n",
           (void *)pthread_self(), __func__, blobno, dtastripe, now - atstart);
}

/* sequential processing of the stripes, keys, blobs
 */
static int bdb_verify_sequential(verify_common_t *par, unsigned int lid)
{
    int rc = 0;
    /* scan 1 - run through data, verify all the keys and blobs */
    for (int dtastripe = 0; dtastripe < par->bdb_state->attr->dtastripe;
         dtastripe++) {
        char header[256];
        snprintf(header, sizeof(header), "verifying dtastripe %d", dtastripe);
        par->header = header;
        par->records_processed = 0;
        par->nrecs_progress = 0;
        rc = bdb_verify_data_stripe(par, dtastripe, lid);
        if (rc)
            goto done;
    }

    /* scan 2: scan each key, verify data exists */
    for (int ix = 0; ix < par->bdb_state->numix; ix++) {
        par->records_processed = 0;
        par->nrecs_progress = 0;
        char header[256];
        snprintf(header, sizeof(header), "verifying index %d", ix);
        par->header = header;
        rc = bdb_verify_key(par, ix, lid);
        if (rc)
            goto done;
    }

    /* scan 3: scan each blob, verify data exists */
    int nblobs = get_numblobs(par->db_table);
    for (int blobno = 0; blobno < nblobs; blobno++) {
        par->records_processed = 0;
        par->nrecs_progress = 0;
        for (int dtastripe = 0; dtastripe < par->bdb_state->attr->blobstripe;
             dtastripe++) {
            char header[256];
            snprintf(header, sizeof(header), "verifying blob %d stripe %d",
                     blobno, dtastripe);
            par->header = header;
            bdb_verify_blob(par, blobno, dtastripe, lid);
        }
    }

done:
    return par->verify_status;
}

/* This function serves as a wrapper around calling individual functions
 * to verify data, key, and blob.
 * It gets a separate locker id under which it processes the appropriate verify.
 */
void bdb_verify_handler(td_processing_info_t *info)
{
    verify_common_t *par = info->common_params;
    bdb_state_type *bdb_state = par->bdb_state;
    int rc;
    unsigned int lid;
    BDB_READLOCK("bdb_verify");

    if ((rc = bdb_state->dbenv->lock_id_flags(bdb_state->dbenv, &lid,
                                              DB_LOCK_ID_READONLY)) != 0) {
        logmsg(LOGMSG_ERROR, "%s: error getting a lockid, %d\n", __func__, rc);
        par->verify_status = 1;
        return;
    }

    switch (info->type) {
    case PROCESS_DATA:
        bdb_verify_data_stripe(par, info->dtastripe, lid);
        break;
    case PROCESS_KEY:
        bdb_verify_key(par, info->index, lid);
        break;
    case PROCESS_BLOB:
        bdb_verify_blob(par, info->blobno, info->dtastripe, lid);
        break;
    }

    BDB_RELLOCK();
    ATOMIC_ADD32(par->threads_completed, 1);
}

static void bdb_verify_handler_work_pp(struct thdpool *pool, void *work,
                                       void *thddata, int op)
{
    td_processing_info_t *info = work;
    bdb_state_type *bdb_state = info->common_params->bdb_state;
    bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDONLY);
    bdb_verify_handler(info);
    bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDONLY);
}

/* Enqueue work object onto verify_thdpool
 * If verify_thdpool is NULL then processing occurs sequentially.
 */
static inline void enqueue_work(td_processing_info_t *work,
                                thdpool *verify_thdpool)
{
    // this function is called sequentially, no need for atomics
    work->common_params->threads_spawned++;

    if (verify_thdpool) {
        int rc = thdpool_enqueue(verify_thdpool, bdb_verify_handler_work_pp,
                                 work, 0, NULL, THDPOOL_FORCE_QUEUE,
                                 PRIORITY_T_DEFAULT);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:thdpool_enqueue error, proceeding sequentially\n",
                   __func__);
            verify_thdpool = NULL;
        }
    }
    if (!verify_thdpool) {
        bdb_verify_handler(work);
        free(work);
    }
}

/* Enqueue onto verify_thdpool for processing all data stripes,
 * all keys, and all blobs.
 * If verify_thdpool is null, processing will be performed serially.
 */
int bdb_verify_enqueue(td_processing_info_t *info, thdpool *verify_thdpool)
{
    verify_common_t *par = info->common_params;
    verify_mode_t v_mode = par->verify_mode;
#ifndef NDEBUG
    const char *tp = "";
    switch (v_mode) {
    case VERIFY_PARALLEL:
        break;
    case VERIFY_DATA:
        tp = "DATA";
        break;
    case VERIFY_INDICES:
        tp = "INDICES";
        break;
    case VERIFY_BLOBS:
        tp = "BLOBS";
        break;
    default:
        abort();
    };
    logmsg(LOGMSG_DEBUG, "%s: Verify %s in parallel mode\n", __func__, tp);
#endif
    par->last_reported = comdb2_time_epochms(); // initialize

    if (v_mode == VERIFY_PARALLEL || v_mode == VERIFY_DATA) {
        /* scan 1 - run through data, verify all the keys and blobs */
        for (int dtastripe = 0; dtastripe < par->bdb_state->attr->dtastripe;
             dtastripe++) {
            td_processing_info_t *work = malloc(sizeof(*work));
            memcpy(work, info, sizeof(*work));
            work->type = PROCESS_DATA;
            work->dtastripe = dtastripe;
            enqueue_work(work, verify_thdpool);
        }
    }

    if (v_mode == VERIFY_PARALLEL || v_mode == VERIFY_INDICES) {
        /* scan 2: scan each key, verify data exists */
        for (int ix = 0; ix < par->bdb_state->numix; ix++) {
            td_processing_info_t *work = malloc(sizeof(*work));
            memcpy(work, info, sizeof(*work));
            work->type = PROCESS_KEY;
            work->index = ix;
            enqueue_work(work, verify_thdpool);
        }
    }

    if (v_mode == VERIFY_PARALLEL || v_mode == VERIFY_BLOBS) {
        /* scan 3: scan each blob, verify data exists */
        int nblobs = get_numblobs(par->db_table);
        for (int blobno = 0; blobno < nblobs; blobno++) {
            for (int dtastripe = 0;
                 dtastripe < par->bdb_state->attr->blobstripe; dtastripe++) {
                td_processing_info_t *work = malloc(sizeof(*work));
                memcpy(work, info, sizeof(*work));
                work->type = PROCESS_BLOB;
                work->blobno = blobno;
                work->dtastripe = dtastripe;
                enqueue_work(work, verify_thdpool);
            }
        }
    }

    return par->verify_status;
}

/* this is the sequential verify version
 */
int bdb_verify(verify_common_t *par)
{
    int rc;
    unsigned int lid;
    bdb_state_type *bdb_state = par->bdb_state;

    BDB_READLOCK("bdb_verify");

    if ((rc = bdb_state->dbenv->lock_id_flags(bdb_state->dbenv, &lid,
                                              DB_LOCK_ID_READONLY)) != 0) {
        BDB_RELLOCK();
        logmsg(LOGMSG_ERROR, "%s: error getting a lockid, %d\n", __func__, rc);
        return rc;
    }

    par->last_reported = comdb2_time_epochms(); // initialize

    rc = bdb_verify_sequential(par, lid);

    DB_LOCKREQ rq = {0};
    rq.op = DB_LOCK_PUT_ALL;
    bdb_state->dbenv->lock_vec(bdb_state->dbenv, lid, 0, &rq, 1, NULL);
    bdb_state->dbenv->lock_id_free(bdb_state->dbenv, lid);

    BDB_RELLOCK();

    return rc;
}
