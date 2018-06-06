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
 * This module provides the highest level abstraction of "a record".
 * It contains the functions to use to add, update and delete records
 * (maybe later on to find records too).  Ideally this should be the
 * *only* module in the application that contains a function to add a
 * record.
 *
 * Entry points:
 *  - block processor
 *  - Java stored procedures
 *  - bulk load (in the future)
 *  - schema change
 *  - SQL, via block SQL
 *
 * These routines can be passed dynamic schema specifications.  taglen should
 * always be specified correctly - taglen of zero causes it to use the
 * .DEFAULT tag.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <netinet/in.h>
#include <poll.h>

#include <memory_sync.h>

#include "comdb2.h"
#include "tag.h"
#include "types.h"
#include "block_internal.h"
#include "translistener.h"
#include "prefault.h"
#include "localrep.h"
#include "osqlcomm.h"

#include <locks.h>
#include "debug_switches.h"

#include <cdb2api.h>

#include <genid.h>

#include <bdb_api.h>
#include <unistd.h>
#include "logmsg.h"

extern int gbl_partial_indexes;
extern int gbl_expressions_indexes;

static int check_blob_buffers(struct ireq *iq, blob_buffer_t *blobs,
                              size_t maxblobs, const char *tblname,
                              const char *tagname, struct schema *sc,
                              void *record, const void *nulls);

static int check_blob_sizes(struct ireq *iq, blob_buffer_t *blobs,
                            int maxblobs);

void free_cached_idx(uint8_t * *cached_idx);

/*
 * Add a record:
 *  - check arguments
 *  - add data
 *  - add blobs
 *  - add keys (or defer add for constraints check)
 *  - trigger stored procedures
 */

#define ERR                                                                    \
    if (gbl_verbose_toblock_backouts)                                          \
        logmsg(LOGMSG_USER, "err line %d rc %d retrc %d\n", __LINE__, rc, retrc);           \
    goto err;
#define VERIFY_TABLE_VERSION                                                   \
    if (iq->usedb->tableversion != iq->usedbtablevers) {                       \
        if (iq->debug)                                                         \
            reqprintf(iq, "Stale buffer: usedb version %d vs curr ver %d\n",   \
                      iq->usedbtablevers, iq->usedb->tableversion);            \
        poll(NULL, 0, BDB_ATTR_GET(thedb->bdb_attr, SC_DELAY_VERIFY_ERROR));   \
        *opfailcode = OP_FAILED_VERIFY;                                        \
        retrc = ERR_VERIFY;                                                    \
        ERR;                                                                   \
    }

int gbl_max_wr_rows_per_txn = 0;

int add_record(struct ireq *iq, void *trans, const uint8_t *p_buf_tag_name,
               const uint8_t *p_buf_tag_name_end, uint8_t *p_buf_rec,
               const uint8_t *p_buf_rec_end, const unsigned char fldnullmap[32],
               blob_buffer_t *blobs, size_t maxblobs, int *opfailcode,
               int *ixfailnum, int *rrn, unsigned long long *genid,
               unsigned long long ins_keys, int opcode, int blkpos, int flags)
{
    char tag[MAXTAGLEN + 1];
    int is_od_tag;
    int rc = 0;
    int retrc = 0;
    int expected_dat_len;
    struct schema *dynschema = NULL;
    void *od_dta;
    size_t od_len;
    size_t blobno;
    int prefixes = 0;
    unsigned char lclnulls[64];
    const char *ondisktag;
    int using_myblobs = 0;
    int conv_flags = 0;
    blob_buffer_t myblobs[MAXBLOBS];
    const char *tagdescr = (const char *)p_buf_tag_name;
    size_t taglen = p_buf_tag_name_end - p_buf_tag_name;
    void *record = p_buf_rec;
    size_t reclen = p_buf_rec_end - p_buf_rec;
    char *od_dta_tail = NULL;
    int od_len_tail;

    *ixfailnum = -1;

    if (!blobs) {
        bzero(myblobs, sizeof(myblobs));
        maxblobs = MAXBLOBS;
        blobs = myblobs;
        using_myblobs = 1;
    }

    if (flags & RECFLAGS_NEW_SCHEMA)
        ondisktag = ".NEW..ONDISK";
    else
        ondisktag = ".ONDISK";

    if (iq->debug) {
        reqpushprefixf(iq, "add_record: ");
        prefixes++;
    }

    if (!iq->usedb) {
        if (iq->debug)
            reqprintf(iq, "NO USEDB SET");
        retrc = ERR_BADREQ;
        ERR;
    }

    if (iq->debug) {
        reqpushprefixf(iq, "TBL %s ", iq->usedb->tablename);
        prefixes++;
    }

    if (!(flags & RECFLAGS_NEW_SCHEMA)) {
        if (gbl_max_wr_rows_per_txn &&
            ((++iq->written_row_count) > gbl_max_wr_rows_per_txn)) {
            reqerrstr(iq, COMDB2_CSTRT_RC_TRN_TOO_BIG,
                      "Transaction exceeds max rows");
            retrc = ERR_TRAN_TOO_BIG;
            ERR;
        }
    }

    if ((flags & RECFLAGS_NEW_SCHEMA) &&
        ((gbl_partial_indexes && iq->usedb->ix_partial) ||
         (gbl_expressions_indexes && iq->usedb->ix_expr))) {
        int ixnum;
        int rebuild_keys = 0;
        if (!gbl_use_plan || !iq->usedb->plan)
            rebuild_keys = 1;
        else {
            for (ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
                if (iq->usedb->plan->ix_plan[ixnum] == -1) {
                    rebuild_keys = 1;
                    break;
                }
            }
        }
        if (rebuild_keys) {
            if (iq->idxInsert || iq->idxDelete) {
                free_cached_idx(iq->idxInsert);
                free_cached_idx(iq->idxDelete);
                free(iq->idxInsert);
                free(iq->idxDelete);
                iq->idxInsert = iq->idxDelete = NULL;
            }
            ins_keys = -1ULL;
        }
    }


    if (!(flags & RECFLAGS_NEW_SCHEMA)) { // dont lock if adding from SC

        int d_ms = BDB_ATTR_GET(thedb->bdb_attr, DELAY_LOCK_TABLE_RECORD_C);
        if (d_ms) {
            if (iq->debug)
                reqprintf(iq, "Sleeping for %d ms", d_ms);
            usleep(1000 * d_ms);
        }

        rc = bdb_lock_table_read(iq->usedb->handle, trans);
        if (rc == BDBERR_DEADLOCK) {
            if (iq->debug)
                reqprintf(iq, "LOCK TABLE READ DEADLOCK");
            retrc = RC_INTERNAL_RETRY;
            ERR;
        } else if (rc) {
            if (iq->debug)
                reqprintf(iq, "LOCK TABLE READ ERROR: %d", rc);
            *opfailcode = OP_FAILED_INTERNAL;
            retrc = ERR_INTERNAL;
            ERR;
        }

        VERIFY_TABLE_VERSION;
    }

    rc = resolve_tag_name(iq, tagdescr, taglen, &dynschema, tag, sizeof(tag));
    if (rc != 0) {
        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TAG,
                  "invalid tag description '%.*s'", taglen, tagdescr);
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        ERR;
    }

    if ((!dynschema && (flags & RECFLAGS_DYNSCHEMA_NULLS_ONLY)) ||
        (!fldnullmap)) {
        bzero(lclnulls, sizeof(lclnulls));
        fldnullmap = lclnulls;
    }

    /* Tweak blob-descriptors for static tags. */
    if (gbl_disallow_null_blobs && !dynschema &&
        (flags & RECFLAGS_DYNSCHEMA_NULLS_ONLY)) {
        static_tag_blob_conversion(iq->usedb->tablename, tag, record, blobs,
                                   maxblobs);
    }

    if (iq->debug) {
        reqpushprefixf(iq, "TAG %s ", tag);
        prefixes++;
    }

    struct schema *dbname_schema = find_tag_schema(iq->usedb->tablename, tag);
    if (dbname_schema == NULL) {
        if (iq->debug)
            reqprintf(iq, "UNKNOWN TAG %s TABLE %s\n", tag,
                      iq->usedb->tablename);
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        ERR;
    }

    expected_dat_len = get_size_of_schema(dbname_schema);
    if ((size_t)expected_dat_len > reclen) {
        /* Another check.  we don't care about padding, but we need to make
           sure the user-supplied struct has enough room to contain the last
           field. */
        struct field *f;
        int mismatched_size = 1;

        if (gbl_allow_mismatched_tag_size) {
            f = &dbname_schema->member[dbname_schema->nmembers - 1];
            if (f->offset + f->len <= reclen)
                mismatched_size = 0;
        }

        if (mismatched_size) {
            if (iq->debug)
                reqprintf(iq, "BAD DTA LEN %u TAG %s EXPECTS DTALEN %u\n",
                          reclen, tag, expected_dat_len);
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_ADD_RC_INVL_DTA,
                      "bad data length %u tag '%s' expects data length %u\n",
                      reclen, tag, expected_dat_len);
            *opfailcode = OP_FAILED_BAD_REQUEST;
            retrc = ERR_BADREQ;
            ERR;
        }
    }

    reclen = expected_dat_len;

    if (!(flags & RECFLAGS_NO_BLOBS) &&
        check_blob_buffers(iq, blobs, maxblobs, iq->usedb->tablename, tag,
                           dbname_schema, record, fldnullmap) != 0) {
        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_ADD_RC_INVL_BLOB,
                  "no blobs flags with blob buffers");
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        ERR;
    }

    /* Also check blob sizes */
    if (!(flags & RECFLAGS_NO_BLOBS)) {
        if (check_blob_sizes(iq, blobs, maxblobs)) {
            retrc = ERR_BLOB_TOO_LARGE;
            ERR;
        }
    }

    struct schema *ondisktagsc; // schema for .ONDISK
    int tag_same_as_ondisktag = (strcmp(tag, ondisktag) == 0);

    if (tag_same_as_ondisktag) {
        /* we have the ondisk data already, no conversion needed */
        od_dta = record;
        od_len = reclen;
        is_od_tag = 1;

        ondisktagsc = dbname_schema;
    } else {
        int od_len_int;
        struct convert_failure reason;

        /* we need to convert the record to ondisk format */
        is_od_tag = 0;
        od_len_int = getdatsize(iq->usedb);
        if (od_len_int <= 0) {
            if (iq->debug)
                reqprintf(iq, "BAD ONDISK SIZE");
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_ADD_RC_INVL_DTA, "bad ondisk size");
            *opfailcode = OP_FAILED_BAD_REQUEST;
            retrc = ERR_BADREQ;
            ERR;
        }

        od_len = (size_t)od_len_int;
        void *allocced_memory = alloca(od_len);
        if (!allocced_memory) {
            logmsg(LOGMSG_ERROR,
                   "add_record: malloc %u failed! (table %s tag %s)\n",
                   (unsigned)od_len, iq->usedb->tablename, tag);
            *opfailcode = OP_FAILED_INTERNAL;
            retrc = ERR_INTERNAL;
            ERR;
        }
        od_dta = allocced_memory;

        if (iq->have_client_endian &&
            TAGGED_API_LITTLE_ENDIAN == iq->client_endian) {
            conv_flags |= CONVERT_LITTLE_ENDIAN_CLIENT;
        }

        rc = ctag_to_stag_blobs_tz(iq->usedb->tablename, tag, record,
                                   WHOLE_BUFFER, fldnullmap, ondisktag, od_dta,
                                   conv_flags, &reason /*fail reason*/, blobs,
                                   maxblobs, iq->tzname);
        if (rc == -1) {
            char str[128];
            convert_failure_reason_str(&reason, iq->usedb->tablename, tag,
                                       ondisktag, str, sizeof(str));
            if (iq->debug) {
                reqprintf(iq, "ERR CONVERT DTA %s->%s '%s'", tag, ondisktag, str);
            }
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_ADD_RC_CNVT_DTA,
                      "error convert data %s->.ONDISK '%s'", tag, str);
            *opfailcode = OP_FAILED_CONVERSION;
            retrc = ERR_CONVERT_DTA;
            ERR;
        }

        ondisktagsc = find_tag_schema(iq->usedb->tablename, ondisktag);
    }

    struct convert_failure reason;
    rc = validate_server_record(od_dta, od_len, ondisktagsc, &reason);
    if (rc == -1) {
        char str[128];
        convert_failure_reason_str(&reason, iq->usedb->tablename, tag,
                                   ondisktag, str, sizeof(str));
        if (iq->debug) {
            reqprintf(iq, "ERR VERIFY DTA %s->.ONDISK '%s'", tag, str);
        }
        reqerrstrhdr(
            iq, "Null constraint violation for column '%s' on table '%s'. ",
            reason.target_schema->member[reason.target_field_idx].name,
            iq->usedb->tablename);
        reqerrstr(iq, COMDB2_ADD_RC_CNVT_DTA,
                  "null constraint error data %s->.ONDISK '%s'", tag, str);
        *opfailcode = ERR_NULL_CONSTRAINT;
        rc = retrc = ERR_NULL_CONSTRAINT;
        ERR;
    }

    /*
     * Add the data record
     */
    if (!gbl_use_plan || !iq->usedb->plan || iq->usedb->plan->dta_plan == -1) {
        if (flags & RECFLAGS_KEEP_GENID)
            rc = dat_set(iq, trans, od_dta, od_len, *rrn, *genid);
        else
            rc = dat_add(iq, trans, od_dta, od_len, genid, rrn);

        if (iq->debug) {
            reqprintf(iq, "dat_add RRN %d GENID 0x%llx DTALEN %u RC %d DATA ",
                      *rrn, *genid, od_len, rc);
            reqdumphex(iq, od_dta, od_len);
        }
        if (rc != 0) {
            *opfailcode = OP_FAILED_INTERNAL + ERR_ADD_RRN;
            retrc = rc;
            ERR;
        }
    }

    /*
     * Add all the blobs.  ctag_to_stag_blobs reordered the blob array
     * as appropriate for ondisk tag.
     */
    for (blobno = 0; blobno < maxblobs; blobno++) {
        blob_buffer_t *blob = &blobs[blobno];
        if (blob->exists && (!gbl_use_plan || !iq->usedb->plan ||
                             iq->usedb->plan->blob_plan[blobno] == -1)) {
            rc = blob_add(iq, trans, blobno, blob->data, blob->length, *rrn,
                          *genid);
            if (iq->debug) {
                reqprintf(iq, "blob_add LEN %u RC %d DATA ", blob->length, rc);
                reqdumphex(iq, blob->data, blob->length);
            }
            if (rc != 0) {
                retrc = rc;
                *opfailcode = OP_FAILED_INTERNAL + ERR_ADD_BLOB;
                ERR;
            }
        }
    }

    if (gbl_partial_indexes && iq->usedb->ix_partial && ins_keys == -1ULL) {
        ins_keys = verify_indexes(iq->usedb, od_dta, blobs, maxblobs, 0);
        if (ins_keys == -1ULL) {
            fprintf(stderr, "%s: failed to verify_indexes\n", __func__);
            *opfailcode = OP_FAILED_INTERNAL;
            retrc = ERR_INTERNAL;
            ERR;
        }
    }

    /*
     * Form and add all the keys.
     * If there are constraints, do the add to indices defered.
     */
    if (!(flags & RECFLAGS_NO_CONSTRAINTS)) /* if NOT no constraints */
    {
        if (!(flags & RECFLAGS_NEW_SCHEMA)) {
            /* enqueue the add of the key for constaint checking purposes */
            rc = insert_add_op(iq, iq->blkstate, iq->usedb, NULL, NULL, opcode,
                               *rrn, -1, *genid, ins_keys, blkpos);
            if (rc != 0) {
                if (iq->debug)
                    reqprintf(iq, "FAILED TO PUSH KEYOP");
                *opfailcode = OP_FAILED_INTERNAL;
                retrc = ERR_INTERNAL;
                ERR;
            }
        } else {
            /* if rec adding to NEW SCHEMA and this has constraints,
             * handle idx in live_sc_*
             */
        }
    } else {
        int ixnum;
        od_dta_tail = NULL;
        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step += 1;
        for (ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
            int ixkeylen;
            char ixtag[MAXTAGLEN];
            char key[MAXKEYLEN];
            char mangled_key[MAXKEYLEN];

            if (gbl_use_plan && iq->usedb->plan &&
                iq->usedb->plan->ix_plan[ixnum] != -1)
                continue;

            /* only add keys when told */
            if (gbl_partial_indexes && iq->usedb->ix_partial &&
                !(ins_keys & (1ULL << ixnum)))
                continue;

            ixkeylen = getkeysize(iq->usedb, ixnum);
            if (ixkeylen < 0) {
                if (iq->debug)
                    reqprintf(iq, "BAD INDEX %d OR KEYLENGTH %d", ixnum,
                              ixkeylen);
                reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
                reqerrstr(iq, COMDB2_ADD_RC_INVL_KEY,
                          "bad index %d or keylength %d", ixnum, ixkeylen);
                *ixfailnum = ixnum;
                *opfailcode = OP_FAILED_BAD_REQUEST;
                retrc = ERR_BADREQ;
                ERR;
            }

            snprintf(ixtag, sizeof(ixtag), "%s_IX_%d", ondisktag, ixnum);

            if (iq->idxInsert)
                rc = create_key_from_ireq(iq, ixnum, 0, &od_dta_tail,
                                          &od_len_tail, mangled_key, od_dta,
                                          od_len, key);
            else
                rc = create_key_from_ondisk_sch_blobs(
                    iq->usedb, ondisktagsc, ixnum, &od_dta_tail, &od_len_tail,
                    mangled_key, ondisktag, od_dta, od_len, ixtag, key, NULL,
                    blobs, maxblobs, iq->tzname);
            if (rc == -1) {
                if (iq->debug)
                    reqprintf(iq, "CAN'T FORM INDEX %d", ixnum);
                reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
                reqerrstr(iq, COMDB2_ADD_RC_INVL_IDX, "cannot form index %d",
                          ixnum);
                *ixfailnum = ixnum;
                *opfailcode = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                retrc = rc;
                ERR;
            }

            /* light the prefault kill bit for this subop - newkeys */
            prefault_kill_bits(iq, ixnum, PFRQ_NEWKEY);
            if (iq->osql_step_ix)
                gbl_osqlpf_step[*(iq->osql_step_ix)].step += 2;

            /* add the key */
            rc = ix_addk(iq, trans, key, ixnum, *genid, *rrn, od_dta_tail,
                         od_len_tail, ix_isnullk(iq->usedb, key, ixnum));
            if (iq->debug) {
                reqprintf(iq, "ix_addk IX %d LEN %u KEY ", ixnum, ixkeylen);
                reqdumphex(iq, key, ixkeylen);
                reqmoref(iq, " RC %d", rc);
            }

            if (rc == RC_INTERNAL_RETRY) {
                retrc = rc;
                ERR;
            } else if (rc != 0) {
                retrc = rc;
                *ixfailnum = ixnum;
                /* If following changes, update OSQL_INSREC in osqlcomm.c */
                *opfailcode = OP_FAILED_UNIQ; /* really? */

                ERR;
            }
        }
    }

    /*
     * Trigger stored procedures (JAVASP_TRANS_LISTEN_AFTER_ADD)
     */
    if (!(flags & RECFLAGS_NO_TRIGGERS) &&
        javasp_trans_care_about(iq->jsph, JAVASP_TRANS_LISTEN_AFTER_ADD)) {
        struct javasp_rec *jrec;
        jrec = javasp_alloc_rec(od_dta, od_len, iq->usedb->tablename);
        if (!jrec) {
            *opfailcode = OP_FAILED_INTERNAL;
            retrc = ERR_INTERNAL;
            ERR;
        }
        javasp_rec_set_trans(jrec, iq->jsph, *rrn, *genid);
        /* If we have blobs then make the blob information available
         * to the Java record.  Any blobs not specified in the tag must
         * be null, which is the default anyway. */
        for (blobno = 0; blobno < maxblobs; blobno++) {
            if (blobs[blobno].exists) {
                blob_buffer_t *blob = &blobs[blobno];
                javasp_rec_have_blob(jrec, blobno, blob->data, 0, blob->length);
            }
        }
        rc =
            javasp_trans_tagged_trigger(iq->jsph, JAVASP_TRANS_LISTEN_AFTER_ADD,
                                        NULL, jrec, iq->usedb->tablename);
        javasp_dealloc_rec(jrec);
        if (iq->debug)
            reqprintf(iq, "JAVASP_TRANS_LISTEN_AFTER_ADD RC %d", rc);
        if (rc != 0) {
            retrc = rc;
            *opfailcode = ERR_JAVASP_ABORT_OP;
            ERR;
        }
    }

    /* Save the op to replay later locally */
    if (gbl_replicate_local &&
        (strcasecmp(iq->usedb->tablename, "comdb2_oplog") != 0 &&
         (strcasecmp(iq->usedb->tablename, "comdb2_commit_log")) != 0 &&
         strncasecmp(iq->usedb->tablename, "sqlite_stat", 11) != 0) &&
        !(flags & RECFLAGS_NEW_SCHEMA)) {
        retrc = local_replicant_log_add(iq, trans, od_dta, blobs, opfailcode);
        if (retrc)
            ERR;
    }

    if (!(flags & RECFLAGS_NEW_SCHEMA)) {
        iq->usedb->write_count[RECORD_WRITE_INS]++;
        gbl_sc_last_writer_time = comdb2_time_epoch();

        /* For live schema change */
        rc = live_sc_post_add(iq, trans, *genid, od_dta, ins_keys, 
                blobs, maxblobs, flags, rrn);

        if (rc != 0) {
            retrc = rc;
            ERR;
        }
    }

    dbglog_record_db_write(iq, "insert");
    if (iq->__limits.maxcost && iq->cost > iq->__limits.maxcost)
        retrc = ERR_LIMIT;

    if (debug_switch_alternate_verify_fail()) {
        static int flipon = 0;
        if (flipon) {
            flipon = 0;
        } else {
            flipon = 1;
            *opfailcode = OP_FAILED_VERIFY;
            retrc = ERR_VERIFY;
            ERR;
        }
    }

err:
    if (iq->debug)
        reqpopprefixes(iq, prefixes);
    if (dynschema)
        free_dynamic_schema(iq->usedb->tablename, dynschema);
    if (using_myblobs)
        free_blob_buffers(myblobs, MAXBLOBS);
    if (iq->is_block2positionmode) {
        iq->last_genid = *genid;
    }
    return retrc;
}

/*
 * Add an individual key.  The operation
 * is defered until the end of the block op (we call insert_add_op).
 *
 * Only call this from outside this module for UNTAGGED databases.
 */
static int add_key(struct ireq *iq, void *trans, int ixnum,
                   unsigned long long ins_keys, int rrn,
                   unsigned long long genid, void *od_dta, size_t od_len,
                   int opcode, int blkpos, int *opfailcode, char *newkey,
                   char *od_dta_tail, int od_tail_len, int do_inline)
{
    int rc;

    if (!do_inline) {
        if ((iq->usedb->ix_disabled[ixnum] & INDEX_WRITE_DISABLED)) {
            if (iq->debug)
                reqprintf(iq, "%s: ix %d write disabled", __func__, ixnum);
            return 0;
        }
        const uint8_t *p_buf_req_start = NULL;
        const uint8_t *p_buf_req_end = NULL;
        rc = insert_add_op(iq, iq->blkstate, iq->usedb, p_buf_req_start,
                           p_buf_req_end, opcode, rrn, ixnum, genid, ins_keys,
                           blkpos);
        if (iq->debug)
            reqprintf(iq, "insert_add_op IX %d RRN %d RC %d", ixnum, rrn, rc);
        if (rc != 0) {
            *opfailcode = OP_FAILED_INTERNAL;
            rc = ERR_INTERNAL;
        }
    } else /* cascading update case or dup, dont defer add, do immediately */
    {
        if (!od_dta) {
            logmsg(LOGMSG_ERROR, "%s: no key or ondisk data\n", __func__);
            return ERR_INTERNAL;
        }

        rc = ix_addk(iq, trans, newkey, ixnum, genid, rrn, od_dta_tail,
                     od_tail_len, ix_isnullk(iq->usedb, newkey, ixnum));
        if (iq->debug) {
            reqprintf(iq, "ix_addk IX %d RRN %d KEY ", ixnum, rrn);
            reqdumphex(iq, newkey, getkeysize(iq->usedb, ixnum));
            reqmoref(iq, " RC %d", rc);
        }
        if (rc == IX_DUP)
            *opfailcode = OP_FAILED_UNIQ;
        else if (rc != 0)
            *opfailcode = OP_FAILED_INTERNAL;
    }

    return rc;
}

/*
 * Upgrade an existing record to ondisk format
 * without changing genid.
 */
int upgrade_record(struct ireq *iq, void *trans, unsigned long long vgenid,
                   uint8_t *p_buf_rec, const uint8_t *p_buf_rec_end,
                   int *opfailcode, int *ixfailnum, int opcode, int blkpos)
{
    static const char ondisktag[] = ".ONDISK";

    uint8_t *p_tagname_buf = (uint8_t *)ondisktag;
    uint8_t *p_tagname_buf_end = p_tagname_buf + strlen(ondisktag);

    unsigned long long dummy_genid;

    return upd_record(iq, trans, NULL, 2, vgenid, p_tagname_buf,
                      p_tagname_buf_end, p_buf_rec, p_buf_rec_end, NULL, NULL,
                      NULL, NULL, /*updcols*/
                      NULL,       /*blobs*/
                      0,          /*maxblobs*/
                      &dummy_genid, -1ULL, -1ULL, opfailcode, ixfailnum, opcode,
                      blkpos, RECFLAGS_UPGRADE_RECORD);
}

/* We used to return conversion error (113) for
   not-null constraint violations on updates.
   Switch it on to keep the old behavior.
   Switch it off to return null constraint error (4). */
int gbl_upd_null_cstr_return_conv_err = 0;

/*
 * Update an existing record.
 *
 * Verification is through either a snapshot (weak) or by genid (strong).
 * Snapshot verification is used if vrecord!=NULL, else vgenid is used.
 *
 * If vrecord is passed in but primkey==NULL then we will form primary key
 * from vrecord and use it to find the record to update.
 *
 * New genid is written to *genid
 */
int upd_record(struct ireq *iq, void *trans, void *primkey, int rrn,
               unsigned long long vgenid, const uint8_t *p_buf_tag_name,
               const uint8_t *p_buf_tag_name_end, uint8_t *p_buf_rec,
               const uint8_t *p_buf_rec_end, uint8_t *p_buf_vrec,
               const uint8_t *p_buf_vrec_end,
               const unsigned char fldnullmap[32], int *updCols,
               blob_buffer_t *blobs, size_t maxblobs, unsigned long long *genid,
               unsigned long long ins_keys, unsigned long long del_keys,
               int *opfailcode, int *ixfailnum, int opcode, int blkpos,
               int flags)
{
    int rc;
    int retrc = 0;
    int prefixes = 0;
    int conv_flags = 0;
    int expected_dat_len;
    blob_status_t oldblobs[MAXBLOBS];
    struct schema *dynschema = NULL;
    char *allocced_memory = NULL;
    size_t mallocced_bytes;
    size_t od_len;
    int od_len_int;
    int myupdatecols[MAXCOLUMNS + 1];
    int using_myupdatecols = 0;
    void *od_dta = NULL;
    void *odv_dta = NULL;
    void *old_dta = NULL;
    char tag[MAXTAGLEN + 1];
    int fndlen;
    int blobno, num_cblobs;
    int ixnum;
    char lclprimkey[MAXKEYLEN];
    unsigned char lclnulls[64];
    struct convert_failure reason;
    int using_myblobs = 0;
    blob_buffer_t myblobs[MAXBLOBS];
    const char *tagdescr = (const char *)p_buf_tag_name;
    size_t taglen = p_buf_tag_name_end - p_buf_tag_name;
    void *record = p_buf_rec;
    void *vrecord = p_buf_vrec;
    size_t reclen = p_buf_rec_end - p_buf_rec;
    char *od_dta_tail = NULL;
    int od_tail_len;
    char *od_olddta_tail = NULL;
    int od_oldtail_len;
    int got_oldblobs = 0;
    blob_buffer_t add_blobs_buf[MAXBLOBS];
    blob_buffer_t del_blobs_buf[MAXBLOBS];
    blob_buffer_t *add_idx_blobs = NULL;
    blob_buffer_t *del_idx_blobs = NULL;

    if (p_buf_vrec && (p_buf_vrec_end - p_buf_vrec) != reclen) {
        if (iq->debug)
            reqprintf(iq, "REC LEN %u DOES NOT EQUAL VREC LEN %u", reclen,
                      p_buf_vrec_end - p_buf_vrec);
        retrc = ERR_BADREQ;
        goto err;
    }

    *ixfailnum = -1;

    if (!(flags & RECFLAGS_NEW_SCHEMA)) {
        if (gbl_max_wr_rows_per_txn &&
            ((++iq->written_row_count) > gbl_max_wr_rows_per_txn)) {
            reqerrstr(iq, COMDB2_CSTRT_RC_TRN_TOO_BIG,
                      "Transaction exceeds max rows");
            retrc = ERR_TRAN_TOO_BIG;
            goto err;
        }
    }

    bzero(oldblobs, sizeof(oldblobs));
    bzero(add_blobs_buf, sizeof(add_blobs_buf));
    bzero(del_blobs_buf, sizeof(del_blobs_buf));

    /* must have blobs in case any byte arrays in .DEFAULT convert to blobs */
    if (!blobs) {
        bzero(myblobs, sizeof(myblobs));
        maxblobs = MAXBLOBS;
        blobs = myblobs;
        using_myblobs = 1;
    }

    if (iq->debug) {
        reqpushprefixf(iq, "upd_record: ");
        prefixes++;
    }

    if (!iq->usedb) {
        if (iq->debug)
            reqprintf(iq, "NO USEDB SET");
        retrc = ERR_BADREQ;
        goto err;
    }

    if (iq->debug) {
        reqpushprefixf(iq, "TBL %s ", iq->usedb->tablename);
        prefixes++;
    }

    int d_ms = BDB_ATTR_GET(thedb->bdb_attr, DELAY_LOCK_TABLE_RECORD_C);
    if (d_ms) {
        if (iq->debug)
            reqprintf(iq, "Sleeping for %d ms", d_ms);
        usleep(1000 * d_ms);
    }

    rc = bdb_lock_table_read(iq->usedb->handle, trans);
    if (rc == BDBERR_DEADLOCK) {
        if (iq->debug)
            reqprintf(iq, "LOCK TABLE READ DEADLOCK");
        retrc = RC_INTERNAL_RETRY;
        goto err;
    } else if (rc) {
        if (iq->debug)
            reqprintf(iq, "LOCK TABLE READ ERROR: %d", rc);
        *opfailcode = OP_FAILED_INTERNAL;
        retrc = ERR_INTERNAL;
        goto err;
    }

    VERIFY_TABLE_VERSION;

    rc = resolve_tag_name(iq, tagdescr, taglen, &dynschema, tag, sizeof(tag));
    if (rc != 0) {
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        goto err;
    }

    if ((!dynschema && (flags & RECFLAGS_DYNSCHEMA_NULLS_ONLY)) ||
        (!fldnullmap)) {
        bzero(lclnulls, sizeof(lclnulls));
        fldnullmap = lclnulls;
    }

    /* Tweak blob-descriptors for static tags. */
    if (gbl_disallow_null_blobs && !dynschema &&
        (flags & RECFLAGS_DYNSCHEMA_NULLS_ONLY)) {
        static_tag_blob_conversion(iq->usedb->tablename, tag, record, blobs,
                                   maxblobs);
    }

    struct schema *dbname_schema = find_tag_schema(iq->usedb->tablename, tag);
    if (dbname_schema == NULL) {
        if (iq->debug)
            if (iq->debug)
                reqprintf(iq, "UNKNOWN TAG %s TABLE %s\n", tag,
                          iq->usedb->tablename);
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        ERR;
    }

    expected_dat_len = get_size_of_schema(dbname_schema);
    if ((size_t)expected_dat_len > reclen) {
        /* same check as in add_record */
        struct field *f;
        int mismatched_size = 1;

        if (gbl_allow_mismatched_tag_size) {
            f = &dbname_schema->member[dbname_schema->nmembers - 1];
            if (f->offset + f->len > reclen)
                mismatched_size = 0;
        }

        if (mismatched_size) {
            if (iq->debug)
                reqprintf(iq, "BAD DTA LEN %u TAG %s EXPECTS DTALEN %u\n",
                          reclen, tag, expected_dat_len);
            reqerrstr(iq, COMDB2_UPD_RC_INVL_DTA,
                      "bad data length %u tag '%s' expects data length %u\n",
                      reclen, tag, expected_dat_len);
            *opfailcode = OP_FAILED_BAD_REQUEST;
            retrc = ERR_BADREQ;
            goto err;
        }
    }

    reclen = expected_dat_len;

    if (!(flags & RECFLAGS_NO_BLOBS) &&
        check_blob_buffers(iq, blobs, maxblobs, iq->usedb->tablename, tag,
                           dbname_schema, record, fldnullmap) != 0) {
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        goto err;
    }

    /* Also check blob sizes */
    if (!(flags & RECFLAGS_NO_BLOBS)) {
        if (check_blob_sizes(iq, blobs, maxblobs)) {
            retrc = ERR_BLOB_TOO_LARGE;
            ERR;
        }
    }

    /*
     * We need memory for the ondisk data and for the old record, and maybe for
     * a verification buffer.
     */
    od_len_int = getdatsize(iq->usedb);
    if (od_len_int <= 0) {
        if (iq->debug)
            reqprintf(iq, "BAD ONDISK SIZE");
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        goto err;
    }
    od_len = od_len_int;

    mallocced_bytes = od_len * 2;
    if (vrecord)
        mallocced_bytes += od_len;
    allocced_memory = alloca(mallocced_bytes);
    if (!allocced_memory) {
        logmsg(LOGMSG_ERROR,
               "upd_record: malloc %u failed! (table %s tag %s)\n",
               (unsigned)mallocced_bytes, iq->usedb->tablename, tag);
        *opfailcode = OP_FAILED_INTERNAL;
        retrc = ERR_INTERNAL;
        goto err;
    }
    od_dta = allocced_memory;
    /* This is the current image at it exists in the db */
    old_dta = allocced_memory + od_len;
    if (vrecord)
        odv_dta = allocced_memory + od_len * 2;

    if (iq->have_client_endian &&
        TAGGED_API_LITTLE_ENDIAN == iq->client_endian) {
        conv_flags |= CONVERT_LITTLE_ENDIAN_CLIENT;
    }

    /*
     * If we have a vrecord but no primkey then form primkey so we can do the
     * search.
     */
    if (vrecord && !primkey) {
        static unsigned char nullnulls[32] = {0};
        rc = ctag_to_stag_buf_tz(iq->usedb->tablename, tag, vrecord, reclen,
                                 nullnulls, ".ONDISK_IX_0", lclprimkey,
                                 conv_flags, NULL, iq->tzname);
        if (rc < 0) {
            if (iq->debug)
                reqprintf(iq, "ERR FORMING PRIMARY KEY");
            reqerrstr(iq, COMDB2_UPD_RC_INVL_PK, "error forming primary key");
            *opfailcode = OP_FAILED_CONVERSION;
            retrc = ERR_CONVERT_IX;
            goto err;
        }
        primkey = lclprimkey;
    }

    /* light the prefault kill bit for this subop - olddta */
    prefault_kill_bits(iq, -1, PFRQ_OLDDATA);
    if (iq->osql_step_ix)
        gbl_osqlpf_step[*(iq->osql_step_ix)].step += 1;

    /*
     * Find the old record using either rrn+genid or primkey.  The old record
     * will be placed into "old_record."
     * Handle deadlock correctly!
     */
    if (primkey) {
        int fndrrn;
        char fndkey[MAXKEYLEN];
        int primkeysz = getkeysize(iq->usedb, 0);
        rc = ix_find_by_primkey_tran(iq, primkey, primkeysz, fndkey, &fndrrn,
                                     &vgenid, old_dta, &fndlen, od_len, trans);
        if (iq->debug) {
            reqprintf(iq, "ix_find_by_primkey_tran RRN %d FND RRN %d "
                          "GENID 0x%llx DTALEN %u FNDLEN %u PRIMKEY ",
                      rrn, fndrrn, vgenid, od_len, fndlen);
            reqdumphex(iq, primkey, primkeysz);
            reqmoref(iq, " RC %d", rc);
        }
        if (rc == 0 && rrn != fndrrn) {
            *opfailcode = OP_FAILED_VERIFY;
            retrc = ERR_VERIFY;
            goto err;
        }
    } else if (flags == RECFLAGS_UPGRADE_RECORD) {
        if (record != NULL) {
            // this is a record upgrade and caller specifies data buffer.
            // make old_dta point to record
            old_dta = record;
            fndlen = reclen;
        } else {
            // this is a record upgrade and no data buffer specified.
            // find data by genid, and then write to where old_dta points to
            int ver;
            rc = ix_find_ver_by_rrn_and_genid_tran(
                iq, rrn, vgenid, old_dta, &fndlen, od_len, trans, &ver);
            if (iq->debug)
                reqprintf(
                    iq, "ix_find_ver_by_rrn_and_genid_tran RRN %d GENID 0x%llx "
                        "DTALEN %u FNDLEN %u VER %d RC %d",
                    rrn, vgenid, od_len, fndlen, ver, rc);

            if (rc == 0 && ver == iq->usedb->version) {
                // record is at ondisk version, return
                retrc = rc;
                goto err;
            }
        }

        // od_dta and old_dta are necessarily identical. so point one to the
        // other
        // instead of relatively expensive memcpy()
        od_dta = old_dta;
    } else {
        rc = ix_find_by_rrn_and_genid_tran(iq, rrn, vgenid, old_dta, &fndlen,
                                           od_len, trans);
        if (iq->debug)
            reqprintf(iq, "ix_find_by_rrn_and_genid_tran RRN %d GENID 0x%llx "
                          "DTALEN %u FNDLEN %u RC %d",
                      rrn, vgenid, od_len, fndlen, rc);
    }
    if (rc != 0 || od_len != fndlen) {
        if (iq->debug)
            reqprintf(iq, "FIND OLD RECORD FAILED od_len %u fndlen %u RC %d",
                      od_len, fndlen, rc);
        reqerrstr(iq, COMDB2_UPD_RC_UNKN_REC, "find old record failed");
        *opfailcode = OP_FAILED_VERIFY;
        if (rc == RC_INTERNAL_RETRY)
            retrc = rc;
        else
            retrc = ERR_VERIFY;
        goto err;
    }

    /*
     * If we have to verify data:
     *   Form the ondisk verification record in odv_dta by taking the union of
     *   the old record on disk and the snapshot, as what they passed in could
     *   be a subset of the full ONDISK tag.
     */
    if (vrecord) {
        memcpy(odv_dta, old_dta, od_len);
        if (strncasecmp(tag, ".ONDISK", 7) == 0) {
            /* the input record is .ONDISK or a .ONDISK_IX_ (which would be the
             * case for a cascaded update) */
            rc = stag_to_stag_buf_update_tz(iq->usedb->tablename, tag, vrecord,
                                            ".ONDISK", odv_dta, NULL,
                                            iq->tzname);
        } else {
            rc = ctag_to_stag_buf_tz(iq->usedb->tablename, tag, vrecord,
                                     WHOLE_BUFFER, fldnullmap, ".ONDISK",
                                     odv_dta, (conv_flags | CONVERT_UPDATE),
                                     NULL, iq->tzname);
        }
        if (rc < 0) {
            if (iq->debug)
                reqprintf(iq, "VRECORD CONVERSION FAILED RC %d", rc);
            reqerrstr(iq, COMDB2_UPD_RC_CNVT_VREC,
                      "VRECORD CONVERSION FAILED RC %d", rc);
            *opfailcode = OP_FAILED_CONVERSION;
            retrc = ERR_CONVERT_DTA;
            goto err;
        }
    }

    /*
     * If required, remember the old blobs ready for the update trigger.
     * Handle deadlock correctly.
     */
    if (!(flags & RECFLAGS_NO_TRIGGERS) &&
        javasp_trans_care_about(iq->jsph, JAVASP_TRANS_LISTEN_SAVE_BLOBS_UPD)) {
        rc = save_old_blobs(iq, trans, ".ONDISK", old_dta, rrn, vgenid,
                            oldblobs);
        if (rc != 0) {
            *opfailcode = OP_FAILED_INTERNAL + ERR_SAVE_BLOBS;
            if (rc == RC_INTERNAL_RETRY)
                retrc = rc;
            else
                retrc = ERR_INTERNAL;
            goto err;
        }
        got_oldblobs = 1;
    }

    /*
     * Form the new record in od_dta by taking the union of the old record
     * and the changes.
     */
    if (flags != RECFLAGS_UPGRADE_RECORD) {
        memcpy(od_dta, old_dta, od_len);
        if (strncasecmp(tag, ".ONDISK", 7) == 0) {
            /* the input record is .ONDISK or a .ONDISK_IX_ (which would be the
             * case for a cascaded update) */
            rc = stag_to_stag_buf_update_tz(iq->usedb->tablename, tag, record,
                                            ".ONDISK", od_dta, &reason,
                                            iq->tzname);
        } else {
            rc = ctag_to_stag_blobs_tz(iq->usedb->tablename, tag, record,
                                       WHOLE_BUFFER, fldnullmap, ".ONDISK",
                                       od_dta, (conv_flags | CONVERT_UPDATE),
                                       &reason, blobs, maxblobs, iq->tzname);
        }

        /* used for schema-change */
        if (record != NULL && (NULL == updCols) &&
            (0 == describe_update_columns(iq->usedb->tablename, tag,
                                          myupdatecols))) {
            using_myupdatecols = 1;
            updCols = myupdatecols;
        }

        if (rc < 0) {
            char str[128];
            convert_failure_reason_str(&reason, iq->usedb->tablename, tag,
                                       ".ONDISK", str, sizeof(str));
            if (iq->debug) {
                reqprintf(iq, "ERR CONVERT DTA %s->.ONDISK '%s'", tag, str);
            }

            if (reason.reason == CONVERT_FAILED_NULL_CONSTRAINT_VIOLATION) {
                if (gbl_upd_null_cstr_return_conv_err) {
                    *opfailcode = OP_FAILED_CONVERSION;
                    retrc = ERR_CONVERT_DTA;
                } else {
                    *opfailcode = ERR_NULL_CONSTRAINT;
                    retrc = ERR_NULL_CONSTRAINT;
                }
                reqerrstrhdr(
                    iq,
                    "Null constraint violation for column '%s' on table '%s'. ",
                    reason.target_schema->member[reason.target_field_idx].name,
                    iq->usedb->tablename);
            } else {
                *opfailcode = OP_FAILED_CONVERSION;
                retrc = ERR_CONVERT_DTA;
            }
            reqerrstr(iq, COMDB2_UPD_RC_CNVT_DTA,
                      "error convert data %s->.ONDISK '%s'", tag, str);
            goto err;
        }
    }

    if (iq->usedb->ix_blob ||
        (iq->usedb->sc_from == iq->usedb && iq->usedb->sc_to->ix_blob)) {
        if (!got_oldblobs) {
            rc = save_old_blobs(iq, trans, ".ONDISK", old_dta, rrn, vgenid,
                                oldblobs);
            if (rc != 0) {
                if (rc == RC_INTERNAL_RETRY)
                    retrc = rc;
                else
                    retrc = ERR_INTERNAL;
                goto err;
            }
            blob_status_to_blob_buffer(oldblobs, del_blobs_buf);
            blob_status_to_blob_buffer(oldblobs, add_blobs_buf);
        }
        for (blobno = 0;
             blobno < maxblobs && blobno < iq->usedb->schema->numblobs;
             blobno++) {
            blob_buffer_t *blob;
            blob = &blobs[blobno];
            if (blob->collected && !using_myupdatecols && updCols) {
                int idx;
                int ncols;

                idx = get_schema_blob_field_idx((char *)iq->usedb->tablename,
                                                ".ONDISK", blobno);
                ncols = updCols[0];

                if ((idx >= 0) && (idx < ncols) && (-1 == updCols[idx + 1]))
                    continue;
            }
            if (!(blob->collected) && !(flags & RECFLAGS_DONT_SKIP_BLOBS))
                continue;

            add_blobs_buf[blobno] = *blob;
        }
        if (gbl_partial_indexes && iq->usedb->ix_partial) {
            if (del_keys == -1ULL)
                del_keys = verify_indexes(iq->usedb, old_dta, del_blobs_buf,
                                          MAXBLOBS, 0);
            if (ins_keys == -1ULL)
                ins_keys = verify_indexes(iq->usedb, od_dta, add_blobs_buf,
                                          MAXBLOBS, 0);
            if (ins_keys == -1ULL || del_keys == -1ULL) {
                fprintf(stderr, "%s: failed to verify_indexes\n", __func__);
                *opfailcode = OP_FAILED_INTERNAL;
                retrc = ERR_INTERNAL;
                goto err;
            }
        }
        del_idx_blobs = del_blobs_buf;
        add_idx_blobs = add_blobs_buf;
    }

    if (!(flags & RECFLAGS_NO_CONSTRAINTS)) {
        rc = check_update_constraints(iq, trans, iq->blkstate, opcode, old_dta,
                                      od_dta, del_keys, opfailcode);
        if (rc != 0) {
            if (iq->debug)
                reqprintf(iq, "FAILED TO VERIFY CONSTRAINTS");
            retrc = *opfailcode;
            goto err;
        }
    }

    if (gbl_replicate_local &&
        (strcasecmp(iq->usedb->tablename, "comdb2_oplog") != 0 &&
         (strcasecmp(iq->usedb->tablename, "comdb2_commit_log")) != 0 &&
         strncasecmp(iq->usedb->tablename, "sqlite_stat", 11) != 0) &&
        !(flags & RECFLAGS_NEW_SCHEMA)) {

        rc = local_replicant_log_delete_for_update(iq, trans, rrn, vgenid,
                                                   opfailcode);
        if (rc) {
            retrc = rc;
            ERR;
        }
    }

    /*
     * Update the data record using the correct verification technique.
     * bdblib will give us a new genid (if tagged) and will update the genid
     * of all our blobs too.
     */
    if (vrecord) {
        if (iq->debug) {
            reqprintf(iq, "old_dta = ");
            reqdumphex(iq, old_dta, od_len);
            reqprintf(iq, "odv_dta = ");
            reqdumphex(iq, odv_dta, od_len);
        }

        /* do verified update */

        /* pass in the genid for striping purposes - need to figure out
           which dta file to use from the genid, but force dta verification
           as verifying the genid is bogus - we just found it right here! */
        rc = dat_upv(iq, trans,
                     0, /*offset to verify from, only zero is supported*/
                     odv_dta, od_len, vgenid, od_dta, od_len, rrn, genid, 1,
                     iq->blkstate->modnum); /* verifydta */
    } else {
        if (flags == RECFLAGS_UPGRADE_RECORD) {
            rc = dat_upgrade(iq, trans, od_dta, od_len, vgenid);
            *genid = vgenid;
        } else {
            rc = dat_upv(iq, trans, 0, /*vptr*/
                         NULL,         /*vdta*/
                         0,            /*vlen*/
                         vgenid, od_dta, od_len, rrn, genid, 0,
                         iq->blkstate->modnum);
        }
    }

    if (iq->debug) {
        if (flags & RECFLAGS_KEEP_GENID)
            reqprintf(iq, "dat_upgrade RRN %d VGENID 0x%llx RC %d", rrn, vgenid,
                      rc);
        else
            reqprintf(iq, "dat_upv RRN %d VGENID 0x%llx GENID 0x%llx RC %d",
                      rrn, vgenid, *genid, rc);
    }

    if (rc != 0) {
        *opfailcode = OP_FAILED_VERIFY;
        retrc = rc;
        goto err;
    }

    od_dta_tail = NULL;
    od_olddta_tail = NULL;

    // if even one ix is done deferred, we want to do the post_update deferred
    int deferredAdd = 0;
    int same_genid_with_upd =
        bdb_inplace_cmp_genids(iq->usedb->handle, *genid, vgenid) == 0;

    /* update the indexes as required */
    for (ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        if (flags == RECFLAGS_UPGRADE_RECORD &&
            iq->usedb->ix_datacopy[ixnum] == 0)
            // skip non-datacopy indexes if it is a record upgrade
            continue;

        char keytag[MAXTAGLEN];
        char oldkey[MAXKEYLEN];
        char newkey[MAXKEYLEN];
        char mangled_oldkey[MAXKEYLEN];
        char mangled_newkey[MAXKEYLEN];
        int keysize;

        /* index doesnt change */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(ins_keys & (1ULL << ixnum)) && !(del_keys & (1ULL << ixnum)))
            continue;

        keysize = getkeysize(iq->usedb, ixnum);

        /* light the prefault kill bit for this subop - oldkeys */
        prefault_kill_bits(iq, ixnum, PFRQ_OLDKEY);
        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step += 1;

        /* form the old key from old_dta into "oldkey" */
        snprintf(keytag, sizeof(keytag), ".ONDISK_IX_%d", ixnum);

        if (iq->idxDelete) {
            /* only create key if we need it */
            if (!gbl_partial_indexes || !iq->usedb->ix_partial ||
                (del_keys & (1ULL << ixnum)))
                rc = create_key_from_ireq(iq, ixnum, 1, &od_olddta_tail,
                                          &od_oldtail_len, mangled_oldkey,
                                          old_dta, od_len, oldkey);
        } else
            rc = create_key_from_ondisk_blobs(
                iq->usedb, ixnum, &od_olddta_tail, &od_oldtail_len,
                mangled_oldkey, ".ONDISK", old_dta, od_len, keytag, oldkey,
                NULL, del_idx_blobs, del_idx_blobs ? MAXBLOBS : 0, NULL);
        /*
                rc = stag_to_stag_buf(iq->usedb->tablename, ".ONDISK", old_dta,
                        keytag, oldkey, NULL);
                        */
        if (rc < 0) {
            if (iq->debug)
                reqprintf(iq, "CAN'T FORM OLD KEY IX %d", ixnum);
            reqerrstr(iq, COMDB2_UPD_RC_INVL_KEY,
                      "cannot form old key index %d", ixnum);
            *ixfailnum = ixnum;
            *opfailcode = OP_FAILED_CONVERSION;
            retrc = ERR_CONVERT_IX;
            goto err;
        }

        if (iq->idxInsert) {
            /* only create key if we need it */
            if (!gbl_partial_indexes || !iq->usedb->ix_partial ||
                (ins_keys & (1ULL << ixnum)))
                rc = create_key_from_ireq(iq, ixnum, 0, &od_dta_tail,
                                          &od_tail_len, mangled_newkey, od_dta,
                                          od_len, newkey);
        } else /* form the new key from "od_dta" into "newkey" */
            rc = create_key_from_ondisk_blobs(
                iq->usedb, ixnum, &od_dta_tail, &od_tail_len, mangled_newkey,
                ".ONDISK", od_dta, od_len, keytag, newkey, NULL, add_idx_blobs,
                add_idx_blobs ? MAXBLOBS : 0, NULL);
        /*
       rc = stag_to_stag_buf(iq->usedb->tablename, ".ONDISK", od_dta,
               keytag, newkey, NULL);
               */
        if (rc < 0) {
            if (iq->debug)
                reqprintf(iq, "CAN'T FORM NEW KEY IX %d", ixnum);
            reqerrstr(iq, COMDB2_UPD_RC_INVL_KEY,
                      "cannot form new key index %d", ixnum);
            *ixfailnum = ixnum;
            *opfailcode = OP_FAILED_CONVERSION;
            retrc = ERR_CONVERT_IX;
            goto err;
        }

        /*
          determine if the key to be added is the same as the key to be
          deleted.  if so, attempt an update, not a delete/add
          - if the key doesnt allow dups (it doesnt contain a genid) then we
            can always do an in place key update if the key didnt change,
            ie, poke in the new genid to the dta portion of the key.
          - if the key allows dups (has a genid on the right side of the key)
            then we can only do the in place update if the genid (minus the
            updateid portion) didnt change, ie if an in place dta update
            happened here. */
        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step += 1;

        int key_unique = (iq->usedb->ix_dupes[ixnum] == 0);
        int same_key = (memcmp(newkey, oldkey, keysize) == 0);
        if (gbl_key_updates && (key_unique || same_genid_with_upd) &&
            same_key &&
            (!gbl_partial_indexes || !iq->usedb->ix_partial ||
             ((ins_keys & (1ULL << ixnum)) &&
              (del_keys & (1ULL << ixnum))))) { /* in place key update */

            /*fprintf(stderr, "IX %d didnt change, poking genid 0x%016llx\n",
              ixnum, *genid);*/

            gbl_upd_key++;

            rc = ix_upd_key(iq, trans, newkey, iq->usedb->ix_keylen[ixnum],
                            ixnum, vgenid, *genid, od_dta_tail, od_tail_len,
                            ix_isnullk(iq->usedb, newkey, ixnum));
            if (iq->debug)
                reqprintf(iq, "upd_key IX %d GENID 0x%016llx RC %d", ixnum,
                          *genid, rc);

            if (rc != 0) {
                *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                *ixfailnum = ixnum;
                retrc = rc;
                goto err;
            }

            /* need to do this here as we're not adding the new key so we
               dont have the luxury of letting the constraint engine catch it
               later */
            verify_schema_change_constraint(iq, iq->usedb, trans, od_dta,
                                            ins_keys);
        } else /* delete / add the key */
        {
            /*
              fprintf(stderr, "IX %d changed, deleting key at genid 0x%016llx "
              "adding key at genid 0x%016llx\n",
              ixnum, vgenid, *genid);
            */

            /* only delete keys when told */
            if (!gbl_partial_indexes || !iq->usedb->ix_partial ||
                (del_keys & (1ULL << ixnum))) {
                rc = ix_delk(iq, trans, oldkey, ixnum, rrn, vgenid, ix_isnullk(iq->usedb, oldkey, ixnum));

                if (iq->debug)
                    reqprintf(iq, "ix_delk IX %d RRN %d RC %d", ixnum, rrn, rc);

                if (rc != 0) {
                    *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                    *ixfailnum = ixnum;
                    retrc = rc;
                    goto err;
                }
            }

            int do_inline;
            if (!gbl_partial_indexes || !iq->usedb->ix_partial ||
                ((del_keys & (1ULL << ixnum)) &&
                 (ins_keys & (1ULL << ixnum)))) {
                do_inline = (flags & UPDFLAGS_CASCADE) ||
                            (iq->usedb->ix_dupes[ixnum] &&
                             iq->usedb->n_constraints == 0);
            } else {
                do_inline = 1;
            }
            deferredAdd |= (!do_inline);

            if (!gbl_partial_indexes || !iq->usedb->ix_partial ||
                (ins_keys & (1ULL << ixnum))) {
                rc = add_key(iq, trans, ixnum, ins_keys, rrn, *genid, od_dta,
                             od_len, opcode, blkpos, opfailcode, newkey,
                             od_dta_tail, od_tail_len, do_inline);

                if (iq->debug)
                    reqprintf(iq, "add_key IX %d RRN %d RC %d", ixnum, rrn, rc);

                if (rc != 0) {
                    *ixfailnum = ixnum;
                    retrc = rc;
                    goto err;
                }
            }
        }
    }

    /*
     * Now we need to change the blobs for this tag.  For each blob
     * in the user tag, get the ondisk blob number and delete/update
     * it accordingly.  And handle deadlock correctly!
     */
    for (blobno = 0; blobno < maxblobs && blobno < iq->usedb->schema->numblobs;
         blobno++) {
        blob_buffer_t *blob;
        blob = &blobs[blobno];
        int upgenid = 0;

        /*
         * If this blob was collected- check the updcols array to determine if
         * we need to update the genid only if updCols was passed into this:
         * The codepath which doesn't use updCols already optimizes by not
         * marking the blob as collected.
         *
         * TODO:
         * We can avoid putting the blob on the network in the osql layer if it
         * hasn't been updated.
         */
        if (blob->collected && !using_myupdatecols && updCols) {
            int idx;
            int ncols;

            idx = get_schema_blob_field_idx((char *)iq->usedb->tablename,
                                            ".ONDISK", blobno);
            ncols = updCols[0];

            if ((idx >= 0) && (idx < ncols) && (-1 == updCols[idx + 1])) {
                upgenid = 1;
            }
        }

        /*
         * If RECFLAGS_DONT_SKIP_BLOBS is set, delete an uncollected blob
         * rather than update its genid.  An uncollected blob in this case
         * means that it should change to NULL.
         */

        if (!(blob->collected)) {
            if (flags & RECFLAGS_DONT_SKIP_BLOBS) {
                /* flags tell me to delete this uncollected record */
            } else {
                /* I will update the genid for this uncollected record */
                upgenid = 1;
            }
        }

        if (upgenid) {
            if (blobno < iq->usedb->numblobs) {
                if (gbl_inplace_blobs && gbl_inplace_blob_optimization &&
                    same_genid_with_upd) {
                    if (iq->debug)
                        reqprintf(iq, "blob_upd_genid SKIP BLOBNO %d BLOB "
                                      "OPTIMIZATION RC %d",
                                  blobno, rc);
                    gbl_untouched_blob_cnt++;
                    continue;
                }
                rc = blob_upd_genid(iq, trans, blobno, rrn, vgenid, *genid);
                if (iq->debug)
                    reqprintf(iq, "blob_upd_genid BLOBNO %d RC %d", blobno, rc);
                if (rc != 0) {
                    *opfailcode = OP_FAILED_INTERNAL + ERR_UPD_GENIDS;
                    retrc = rc;
                    goto err;
                }
                gbl_update_genid_blob_cnt++;
            }
            if (iq->debug)
                reqprintf(iq, "skipping ondisk blob %d", blobno);
            continue;
        }

        /* Attempt to update blobs in-place if that's enabled. */
        if (gbl_inplace_blobs) {
            if (!blob->exists) {
                rc = blob_del(iq, trans, rrn, vgenid, blobno);
                if (iq->debug)
                    reqprintf(iq, "blob_del BLOBNO %d RC %d", blobno, rc);
                if (rc != IX_NOTFND && rc != 0) {
                    *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_BLOB;
                    retrc = rc;
                    goto err;
                }
                if (rc != IX_NOTFND) {
                    gbl_delupd_blob_cnt++;
                }
            } else {
                /* Add/Update case. */
                rc = blob_upv(iq, trans, 0, vgenid, blob->data, blob->length,
                              blobno, rrn, *genid);
                if (iq->debug)
                    reqprintf(iq, "blob_upv BLOBNO %d RC %d", blobno, rc);
                if (rc != 0) {
                    *opfailcode = OP_FAILED_INTERNAL + ERR_UPD_BLOB;
                    retrc = rc;
                    goto err;
                }
                gbl_inplace_blob_cnt++;
            }
        } else {
            /* delete old blob, if there was one, using the old genid */
            rc = blob_del(iq, trans, rrn, vgenid, blobno);
            if (iq->debug)
                reqprintf(iq, "blob_del BLOBNO %d RC %d", blobno, rc);
            if (rc != IX_NOTFND && rc != 0) {
                *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_BLOB;
                retrc = rc;
                goto err;
            }
            if (rc != IX_NOTFND) {
                gbl_delupd_blob_cnt++;
            }
            /* add the new blob data if it's not NULL. */
            if (blob->exists) {
                rc = blob_add(iq, trans, blobno, blob->data, blob->length, rrn,
                              *genid);
                if (iq->debug)
                    reqprintf(iq, "blob_add BLOBNO %d RC %d", blobno, rc);
                if (rc != 0) {
                    *opfailcode = OP_FAILED_INTERNAL + ERR_ADD_BLOB;
                    retrc = rc;
                    goto err;
                }
                gbl_addupd_blob_cnt++;
            }
        }
    }

    /* Update the genids of any remaining blobs */
    for (; blobno < iq->usedb->numblobs; blobno++) {
        if (gbl_inplace_blobs && gbl_inplace_blob_optimization &&
            same_genid_with_upd) {
            if (iq->debug)
                reqprintf(
                    iq, "blob_upd_genid SKIP BLOBNO %d BLOB OPTIMIZATION RC %d",
                    blobno, rc);
            gbl_untouched_blob_cnt++;
            continue;
        }

        rc = blob_upd_genid(iq, trans, blobno, rrn, vgenid, *genid);
        if (iq->debug)
            reqprintf(iq, "blob_upd_genid BLOBNO %d RC %d", blobno, rc);
        if (rc != 0) {
            *opfailcode = OP_FAILED_INTERNAL + ERR_UPD_GENIDS;
            retrc = rc;
            goto err;
        }
        gbl_update_genid_blob_cnt++;
    }

    /* TODO: largely copy and paste from the add case, with some complexities.
     * functionalize the bastard */
    if (gbl_replicate_local &&
        (strcasecmp(iq->usedb->tablename, "comdb2_oplog") != 0 &&
         (strcasecmp(iq->usedb->tablename, "comdb2_commit_log")) != 0 &&
         strncasecmp(iq->usedb->tablename, "sqlite_stat", 11) != 0) &&
        !(flags & RECFLAGS_NEW_SCHEMA)) {
        rc = local_replicant_log_add_for_update(iq, trans, rrn, *genid,
                                                opfailcode);
        if (rc) {
            retrc = rc;
            ERR;
        }
    }

    /*
     * Trigger JAVASP_TRANS_LISTEN_AFTER_UPD.
     */
    if (!(flags & RECFLAGS_NO_TRIGGERS) &&
        javasp_trans_care_about(iq->jsph, JAVASP_TRANS_LISTEN_AFTER_UPD)) {
        struct javasp_rec *joldrec;
        struct javasp_rec *jnewrec;
        blob_status_t new_rec_blobs[MAXBLOBS] = {0};

        /* old record no longer exists - don't set trans or rrn */
        joldrec = javasp_alloc_rec(old_dta, od_len, iq->usedb->tablename);
        javasp_rec_set_blobs(joldrec, oldblobs);
        javasp_rec_set_trans(joldrec, iq->jsph, rrn, vgenid);

        /* new record now exists on disk */
        jnewrec = javasp_alloc_rec(od_dta, od_len, iq->usedb->tablename);

        /* we also need to pass down blobs.  not all of them are necessarily
           specified in the 'blobs' variable (eg: static tag that omits a blob)
           */
        save_old_blobs(iq, trans, ".ONDISK", od_dta, rrn, *genid,
                       new_rec_blobs);
        javasp_rec_set_blobs(jnewrec, new_rec_blobs);
        javasp_rec_set_trans(jnewrec, iq->jsph, rrn, vgenid);
        rc =
            javasp_trans_tagged_trigger(iq->jsph, JAVASP_TRANS_LISTEN_AFTER_UPD,
                                        joldrec, jnewrec, iq->usedb->tablename);
        javasp_dealloc_rec(joldrec);
        javasp_dealloc_rec(jnewrec);
        free_blob_status_data(new_rec_blobs);
        if (iq->debug)
            reqprintf(iq, "JAVASP_TRANS_LISTEN_AFTER_UPD %d", rc);
        if (rc != 0) {
            *opfailcode = OP_FAILED_INTERNAL + ERR_JAVASP_ABORT_OP;
            retrc = rc;
            goto err;
        }
    }

    /* For live schema change */
    rc = live_sc_post_update(iq, trans, vgenid, old_dta, *genid, od_dta,
                             ins_keys, del_keys, od_len, updCols, blobs, 
                             maxblobs, flags, rrn, deferredAdd, del_idx_blobs,
                             add_idx_blobs);
    if (rc != 0) {
        retrc = rc;
        goto err;
    }

    iq->usedb->write_count[RECORD_WRITE_UPD]++;
    gbl_sc_last_writer_time = comdb2_time_epoch();

    dbglog_record_db_write(iq, "update");
    if (iq->__limits.maxcost && iq->cost > iq->__limits.maxcost)
        retrc = ERR_LIMIT;

    if (debug_switch_alternate_verify_fail()) {
        static int flipon = 0;
        if (flipon) {
            gbl_maxretries = 500;
            flipon = 0;
        } else {
            gbl_maxretries = 2;
            flipon = 1;
            *opfailcode = OP_FAILED_VERIFY;
            retrc = ERR_VERIFY;
            ERR;
        }
    }

err:
    free_blob_status_data(oldblobs);
    if (iq->debug)
        reqpopprefixes(iq, prefixes);
    if (dynschema)
        free_dynamic_schema(iq->usedb->tablename, dynschema);
    if (using_myblobs)
        free_blob_buffers(myblobs, MAXBLOBS);
    if (iq->is_block2positionmode) {
        iq->last_genid = *genid;
    }
    return retrc;
}

/*
 * Delete a single record.
 *
 * If primary key is provided then it must be in ondisk format and it is used
 * in preference to rrn/genid for finding and deleting the record.
 */
int del_record(struct ireq *iq, void *trans, void *primkey, int rrn,
               unsigned long long genid, unsigned long long del_keys,
               int *opfailcode, int *ixfailnum, int opcode, int flags)
{
    int retrc = 0;
    int prefixes = 0;
    void *allocced_memory = NULL;
    blob_status_t oldblobs[MAXBLOBS];
    void *od_dta;
    size_t od_len;
    int od_len_int;
    int fndlen;
    int rc;
    int ixnum;
    int got_oldblobs = 0;
    blob_buffer_t blobs_buf[MAXBLOBS];
    blob_buffer_t *del_idx_blobs = NULL;

    *ixfailnum = -1;
    bzero(oldblobs, sizeof(oldblobs));
    bzero(blobs_buf, sizeof(blobs_buf));

    if (iq->debug) {
        reqpushprefixf(iq, "del_record: ");
        prefixes++;
    }

    if (!iq->usedb) {
        if (iq->debug)
            reqprintf(iq, "NO USEDB SET");
        retrc = ERR_BADREQ;
        goto err;
    }

    if (!(flags & RECFLAGS_NEW_SCHEMA)) {
        if (gbl_max_wr_rows_per_txn &&
            ((++iq->written_row_count) > gbl_max_wr_rows_per_txn)) {
            reqerrstr(iq, COMDB2_CSTRT_RC_TRN_TOO_BIG,
                      "Transaction exceeds max rows");
            retrc = ERR_TRAN_TOO_BIG;
            goto err;
        }
    }

    if (iq->debug) {
        reqpushprefixf(iq, "TBL %s ", iq->usedb->tablename);
        prefixes++;
    }

    od_len_int = getdatsize(iq->usedb);
    if (od_len_int <= 0) {
        if (iq->debug)
            reqprintf(iq, "BAD ONDISK SIZE");
        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_DEL_RC_INVL_DTA, "bad ondisk size");
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        goto err;
    }
    od_len = (size_t)od_len_int;
    allocced_memory = alloca(od_len);
    if (!allocced_memory) {
        logmsg(LOGMSG_ERROR, "del_record: malloc %u failed\n", (unsigned)od_len);
        *opfailcode = OP_FAILED_INTERNAL;
        retrc = ERR_INTERNAL;
        goto err;
    }
    od_dta = allocced_memory;

    /* light the prefault kill bit for this subop - olddta */
    prefault_kill_bits(iq, -1, PFRQ_OLDDATA);
    if (iq->osql_step_ix)
        gbl_osqlpf_step[*(iq->osql_step_ix)].step += 1;

    int d_ms = BDB_ATTR_GET(thedb->bdb_attr, DELAY_LOCK_TABLE_RECORD_C);
    if (d_ms) {
        if (iq->debug)
            reqprintf(iq, "Sleeping for %d ms", d_ms);
        usleep(1000 * d_ms);
    }

    rc = bdb_lock_table_read(iq->usedb->handle, trans);
    if (rc == BDBERR_DEADLOCK) {
        if (iq->debug)
            reqprintf(iq, "LOCK TABLE READ DEADLOCK");
        retrc = RC_INTERNAL_RETRY;
        goto err;
    } else if (rc) {
        if (iq->debug)
            reqprintf(iq, "LOCK TABLE READ ERROR: %d", rc);
        *opfailcode = OP_FAILED_INTERNAL;
        retrc = ERR_INTERNAL;
        goto err;
    }

    VERIFY_TABLE_VERSION;

    if (primkey) {
        int fndrrn;
        unsigned long long fndgenid;
        char fndkey[MAXKEYLEN];
        rc = ix_find_by_primkey_tran(iq, primkey, getkeysize(iq->usedb, 0),
                                     fndkey, &fndrrn, &fndgenid, od_dta,
                                     &fndlen, od_len, trans);
        if (iq->debug)
            reqprintf(iq, "ix_find_by_primkey_tran RRN %d FND RRN %d "
                          "GENID 0x%llx DTALEN %u FNDLEN %u RC %d",
                      rrn, fndrrn, fndgenid, od_len, fndlen, rc);
        if (rc == 0 && rrn != fndrrn) {
            *opfailcode = OP_FAILED_VERIFY;
            retrc = ERR_VERIFY;
            goto err;
        }
        genid = fndgenid;
    } else {
        rc = ix_find_by_rrn_and_genid_tran(iq, rrn, genid, od_dta, &fndlen,
                                           od_len, trans);
        if (iq->debug)
            reqprintf(iq, "ix_find_by_rrn_and_genid_tran RRN %d GENID 0x%llx "
                          "DTALEN %u FNDLEN %u RC %d",
                      rrn, genid, od_len, fndlen, rc);
    }

    /* Must handle deadlock correctly */
    if (rc != 0 || od_len != fndlen) {
        if (iq->debug)
            reqprintf(iq, "FIND OLD RECORD FAILED od_len %u fndlen %u RC %d",
                      od_len, fndlen, rc);
        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_DEL_RC_UNKN_REC, "find old record failed");
        *opfailcode = OP_FAILED_VERIFY;
        if (rc == RC_INTERNAL_RETRY)
            retrc = rc;
        else
            retrc = ERR_VERIFY;
        goto err;
    }

    if (iq->usedb->ix_blob ||
        (iq->usedb->sc_from == iq->usedb && iq->usedb->sc_to->ix_blob)) {
        if (!got_oldblobs) {
            rc = save_old_blobs(iq, trans, ".ONDISK", od_dta, rrn, genid,
                                oldblobs);
            if (rc != 0) {
                if (rc == RC_INTERNAL_RETRY)
                    retrc = rc;
                else
                    retrc = ERR_INTERNAL;
                goto err;
            }
            blob_status_to_blob_buffer(oldblobs, blobs_buf);
            got_oldblobs = 1;
        }
        if (gbl_partial_indexes && iq->usedb->ix_partial && del_keys == -1ULL) {
            del_keys =
                verify_indexes(iq->usedb, od_dta, blobs_buf, MAXBLOBS, 0);
            if (del_keys == -1ULL) {
                fprintf(stderr, "%s: failed to verify_indexes\n", __func__);
                *opfailcode = OP_FAILED_INTERNAL;
                retrc = ERR_INTERNAL;
                goto err;
            }
        }
        del_idx_blobs = blobs_buf;
    }

    if (!(flags & RECFLAGS_NO_CONSTRAINTS)) {
        rc = check_delete_constraints(iq, trans, iq->blkstate, opcode, od_dta,
                                      del_keys, opfailcode);
        if (rc != 0) {
            if (iq->debug)
                reqprintf(iq, "FAILED TO VERIFY CONSTRAINTS");
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_DEL_RC_VFY_CSTRT,
                      "failed to verify constraints");
            retrc = *opfailcode;
            goto err;
        }
    }

    if (gbl_replicate_local &&
        (strcasecmp(iq->usedb->tablename, "comdb2_oplog") != 0 &&
         (strcasecmp(iq->usedb->tablename, "comdb2_commit_log")) != 0 &&
         strncasecmp(iq->usedb->tablename, "sqlite_stat", 11) != 0) &&
        !(flags & RECFLAGS_NEW_SCHEMA)) {
        rc = local_replicant_log_delete(iq, trans, od_dta, opfailcode);
        if (rc) {
            retrc = rc;
            ERR;
        }
    }

    /*
     * If we want to remember the old blobs for the delete trigger then find
     * them all here.  Handle deadlock correctly.
     */
    if (!got_oldblobs && ((!(flags & RECFLAGS_NO_TRIGGERS) &&
        javasp_trans_care_about(iq->jsph, JAVASP_TRANS_LISTEN_SAVE_BLOBS_DEL)))) {
        rc = save_old_blobs(iq, trans, ".ONDISK", od_dta, rrn, genid, oldblobs);
        if (rc != 0) {
            *opfailcode = OP_FAILED_INTERNAL + ERR_SAVE_BLOBS;
            if (rc == RC_INTERNAL_RETRY)
                retrc = rc;
            else
                retrc = *opfailcode;
            goto err;
        }
        got_oldblobs = 1;
    }

    /* Delete data record.
       Bdblib automatically deletes associated blobs too. */
    rc = dat_del(iq, trans, rrn, genid);
    if (iq->debug)
        reqprintf(iq, "DEL RRN %d GENID 0x%llx RC %d", rrn, genid, rc);
    if (rc != 0) {
        *opfailcode = (rc == ERR_VERIFY) ? OP_FAILED_VERIFY
                                         : OP_FAILED_INTERNAL + ERR_DEL_DTA;
        retrc = rc;
        goto err;
    }

    /* Form and delete all keys. */
    for (ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char keytag[MAXTAGLEN];
        char key[MAXKEYLEN];

        /* only delete keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(del_keys & (1ULL << ixnum)))
            continue;

        if (iq->idxDelete)
            memcpy(key, iq->idxDelete[ixnum], iq->usedb->ix_keylen[ixnum]);
        else {
            if (flags & RECFLAGS_NEW_SCHEMA) {
                snprintf(keytag, sizeof(keytag), ".NEW..ONDISK_IX_%d", ixnum);
                rc = stag_to_stag_buf_blobs(
                    iq->usedb->tablename, ".NEW..ONDISK", od_dta, keytag, key,
                    NULL, del_idx_blobs, del_idx_blobs ? MAXBLOBS : 0, 0);
            } else {
                snprintf(keytag, sizeof(keytag), ".ONDISK_IX_%d", ixnum);
                rc = stag_to_stag_buf_blobs(
                    iq->usedb->tablename, ".ONDISK", od_dta, keytag, key, NULL,
                    del_idx_blobs, del_idx_blobs ? MAXBLOBS : 0, 0);
            }
            if (rc == -1) {
                if (iq->debug)
                    reqprintf(iq, "CAN'T FORM INDEX %d", ixnum);
                reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
                reqerrstr(iq, COMDB2_DEL_RC_INVL_IDX, "cannot form index %d",
                          ixnum);
                *ixfailnum = ixnum;
                *opfailcode = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                retrc = rc;
                goto err;
            }
        }

        /* handle the key special datacopy options */
        if (iq->usedb->ix_collattr[ixnum]) {
            /* handle key tails */
            rc = extract_decimal_quantum(iq->usedb, ixnum, key, NULL, 0, NULL);
            if (rc) {
                *ixfailnum = ixnum;
                *opfailcode = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                retrc = rc;
                goto err;
            }
        }

        /* light the prefault kill bit for this subop - oldkeys */
        prefault_kill_bits(iq, ixnum, PFRQ_OLDKEY);
        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step += 2;

        /* delete the key */
        rc = ix_delk(iq, trans, key, ixnum, rrn, genid, ix_isnullk(iq->usedb, key, ixnum));
        if (iq->debug) {
            reqprintf(iq, "ix_delk IX %d KEY ", ixnum);
            reqdumphex(iq, key, getkeysize(iq->usedb, ixnum));
            reqmoref(iq, " RC %d", rc);
        }
        if (rc != 0) {
            if (rc == IX_NOTFND) {
                reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
                reqerrstr(iq, COMDB2_DEL_RC_INVL_KEY,
                          "key not found on index %d", ixnum);
            }
            *ixfailnum = ixnum;
            *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_KEY;
            retrc = rc;
            goto err;
        }
    }

    /*
     * Trigger JAVASP_TRANS_LISTEN_AFTER_DEL
     */
    if (!(flags & RECFLAGS_NO_TRIGGERS) &&
        javasp_trans_care_about(iq->jsph, JAVASP_TRANS_LISTEN_AFTER_DEL)) {
        struct javasp_rec *jrec;
        jrec = javasp_alloc_rec(od_dta, od_len, iq->usedb->tablename);
        javasp_rec_set_trans(jrec, iq->jsph, rrn, genid);
        javasp_rec_set_blobs(jrec, oldblobs);
        rc =
            javasp_trans_tagged_trigger(iq->jsph, JAVASP_TRANS_LISTEN_AFTER_DEL,
                                        jrec, NULL, iq->usedb->tablename);
        javasp_dealloc_rec(jrec);
        if (iq->debug)
            reqprintf(iq, "JAVASP_TRANS_LISTEN_AFTER_DEL %d", rc);
        if (rc != 0) {
            *opfailcode = OP_FAILED_INTERNAL + ERR_JAVASP_ABORT_OP;
            retrc = rc;
            goto err;
        }
    }

    /* For live schema change */
    rc = live_sc_post_delete(iq, trans, genid, od_dta, del_keys, del_idx_blobs);
    if (rc != 0) {
        retrc = rc;
        goto err;
    }

    iq->usedb->write_count[RECORD_WRITE_DEL]++;
    gbl_sc_last_writer_time = comdb2_time_epoch();

err:
    dbglog_record_db_write(iq, "delete");
    if (iq->__limits.maxcost && iq->cost > iq->__limits.maxcost)
        retrc = ERR_LIMIT;

    free_blob_status_data(oldblobs);
    if (iq->debug)
        reqpopprefixes(iq, prefixes);
    return retrc;
}

/*
 * Update a single record in the new table as part of a live schema
 * change.  This code is called to add to indices only, adding to
 * the dta files should be already done earlier.
 */
int upd_new_record_add2indices(struct ireq *iq, void *trans,
                               unsigned long long newgenid, const void *new_dta,
                               int nd_len, unsigned long long ins_keys,
                               int use_new_tag, blob_buffer_t *blobs)
{
    int prefixes = 0;
    int rc = 0;
#ifdef DEBUG
    fprintf(stderr, "upd_new_record_add2indices: genid %llx\n", newgenid);
#endif

    if (!iq->usedb)
        return ERR_BADREQ;

    /* Add all keys */
    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char keytag[MAXTAGLEN];
        char key[MAXKEYLEN];
        char mangled_key[MAXKEYLEN];
        char *od_dta_tail = NULL;
        int od_tail_len = 0;

        /* are we supposed to convert this ix -- if no skip work */
        if (gbl_use_plan && iq->usedb->plan &&
            iq->usedb->plan->ix_plan[ixnum] != -1)
            continue;

        /* only add  keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(ins_keys & (1ULL << ixnum)))
            continue;

        snprintf(keytag, sizeof(keytag), ".NEW..ONDISK_IX_%d", ixnum);

        /* form new index */
        if (iq->idxInsert)
            rc =
                create_key_from_ireq(iq, ixnum, 0, &od_dta_tail, &od_tail_len,
                                     mangled_key, (char *)new_dta, nd_len, key);
        else
            rc = create_key_from_ondisk_blobs(
                iq->usedb, ixnum, &od_dta_tail, &od_tail_len, mangled_key,
                use_new_tag ? ".NEW..ONDISK" : ".ONDISK", (char *)new_dta,
                nd_len, keytag, key, NULL, blobs, blobs ? MAXBLOBS : 0, NULL);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "upd_new_record_add2indices: %s newgenid 0x%llx "
                   "conversions -> ix%d failed (use_new_tag %d) rc=%d\n",
                   (iq->idxInsert ? "create_key_from_ireq"
                                  : "create_key_from_ondisk_blobs"),
                   newgenid, ixnum, use_new_tag, rc);
            break;
        }

        rc = ix_addk(iq, trans, key, ixnum, newgenid, 2, (void *)od_dta_tail,
                     od_tail_len, ix_isnullk(iq->usedb, key, ixnum));
        if (iq->debug) {
            reqprintf(iq, "ix_addk IX %d KEY ", ixnum);
            reqdumphex(iq, key, getkeysize(iq->usedb, ixnum));
            reqmoref(iq, " RC %d", rc);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "upd_new_record_add2indices: ix_addk "
                                 "newgenid 0x%llx ix_addk  ix%d rc=%d\n",
                   newgenid, ixnum, rc);
            fsnapf(stderr, key, getkeysize(iq->usedb, ixnum));
            break;
        }
    }

    return rc;
}

/*
 * Update a single record in the new table as part of a live schema
 * change.  This code is called when you update a record in-place
 * behind the cursor.  This code will only be called if in-place updates
 * have been enabled.
 *
 * If deferredAdd is set, we want to defer adding new keys to indices
 * (which will be done from constraints.c:delayed_key_adds()) because 
 * adding the keys here can result in SC aborting when it shouldn't 
 * (in the case when update causes a conflict in one of the keys--transaction
 * should abort rather, and that will be caught by constraints.c). 
 *
 * Note that we can't call upd_new_record() from delayed_key_adds() because 
 * there we lack the old_dta record. So to update happens partially in this
 * function (delete old data and idxs, adding new data0), and the rest in
 * upd_new_record_add2indices() which will finally add to the indices.
 */

int upd_new_record(struct ireq *iq, void *trans, unsigned long long oldgenid,
                   const void *old_dta, unsigned long long newgenid,
                   const void *new_dta, unsigned long long ins_keys,
                   unsigned long long del_keys, int nd_len, const int *updCols,
                   blob_buffer_t *blobs, int deferredAdd,
                   blob_buffer_t *del_idx_blobs, blob_buffer_t *add_idx_blobs)
{
    int retrc = 0;
    int prefixes = 0;
    int rc;
    int newrec_len;
    int ixnum;
    int blobn;
    int myupdatecols[MAXCOLUMNS + 1];
    unsigned long long newgenidcpy = newgenid;

    void *sc_old= NULL;
    void *sc_new = NULL;
    int use_new_tag = 0;

    if (iq->debug) {
        reqpushprefixf(iq, "upd_new_record: ");
        prefixes++;
    }

    if (!iq->usedb) {
        if (iq->debug)
            reqprintf(iq, "NO USEDB SET");
        logmsg(LOGMSG_ERROR, "upd_new_record oldgenid 0x%llx no usedb \n", oldgenid);
        retrc = ERR_BADREQ;
        goto err;
    }

    if (iq->debug) {
        reqpushprefixf(iq, "TBL %s ", iq->usedb->tablename);
        prefixes++;
    }

    if ((gbl_partial_indexes && iq->usedb->ix_partial) ||
         (gbl_expressions_indexes && iq->usedb->ix_expr)) {
        int ixnum;
        int rebuild_keys = 0;
        if (!gbl_use_plan || !iq->usedb->plan)
            rebuild_keys = 1;
        else {
            for (ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
                if (iq->usedb->plan->ix_plan[ixnum] == -1) {
                    rebuild_keys = 1;
                    break;
                }
            }
        }
        if (rebuild_keys) {
            if (iq->idxInsert || iq->idxDelete) {
                free_cached_idx(iq->idxInsert);
                free_cached_idx(iq->idxDelete);
                free(iq->idxInsert);
                free(iq->idxDelete);
                iq->idxInsert = iq->idxDelete = NULL;
            }
            del_keys = -1ULL;
            ins_keys = -1ULL;
        }
    }

    /* Remap the incoming updCols to new schema's updCols */
    rc = remap_update_columns(iq->usedb->tablename, ".ONDISK", updCols,
                              ".NEW..ONDISK", myupdatecols);
    if (iq->debug) {
        reqprintf(iq, "upd_new_record returns %d", rc);
    }

    if (0 != rc) {
        logmsg(LOGMSG_ERROR, 
                "upd_new_record oldgenid 0x%llx remap_update_columns -> "
                "rc %d failed\n",
                oldgenid, rc);
        retrc = ERR_BADREQ;
        goto err;
    }

    struct schema *fromsch = find_tag_schema(iq->usedb->tablename, ".ONDISK");

    if (!gbl_use_plan || !iq->usedb->plan || iq->usedb->plan->dta_plan == -1) {
        newrec_len = getdatsize(iq->usedb);
        sc_new = malloc(newrec_len);
        if (!sc_new) {
            logmsg(LOGMSG_ERROR, "upd_new_record: malloc %u failed\n", newrec_len);
            goto err;
        }

        rc = stag_to_stag_buf_tz(fromsch, iq->usedb->tablename, ".ONDISK",
                                 (char *)new_dta, ".NEW..ONDISK",
                                 (char *)sc_new, NULL, iq->tzname);

        if (rc == -1) {
            logmsg(LOGMSG_ERROR, 
                    "upd_new_record: newgenid 0x%llx conversion error\n",
                    newgenid);
            if (iq->debug)
                reqprintf(iq, "CAN'T FORM NEW UPDATE RECORD\n");
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_UPD_RC_INVL_DTA, "cannot form new record");
            retrc = rc;
            goto err;
        }

        /* dat_upv_sc requests the bdb layer to use newgenid argument */
        rc = dat_upv_sc(iq, trans,
                        0, /*offset to verify from, only zero is supported*/
                        NULL, 0, oldgenid, (void *)sc_new, newrec_len, 2,
                        &newgenidcpy, 0, iq->blkstate->modnum); /* verifydta */

        if (newgenid != newgenidcpy) {
            logmsg(LOGMSG_ERROR, "upd_new_record: dat_upv_sc generated genid!! newgenid "
                   "arg=%llx generated newgenid=%llx",
                   newgenid, newgenidcpy);
            retrc = -1;
            goto err;
        }

        if (iq->debug) {
            reqprintf(iq, "dat_upv - newgenid arg=%llx generated newgenid=%llx",
                      newgenid, newgenidcpy);
            reqmoref(iq, " RC %d", rc);
        }

        /* workaround a bug in current schema change; if we somehow
           fail to find the row in the new btree, try again */
        if (rc == ERR_VERIFY)
            rc = RC_INTERNAL_RETRY;

        if (rc != 0) {
            logmsg(LOGMSG_ERROR, 
                    "upd_new_record oldgenid 0x%llx dat_upv_sc -> rc %d failed\n",
                    oldgenid, rc);
            retrc = rc;
            goto err;
        }
        free(sc_new);
        sc_new = NULL;
    }

    if (iq->usedb->has_datacopy_ix ||
        (gbl_partial_indexes && iq->usedb->ix_partial && del_keys == -1ULL) ||
        (gbl_expressions_indexes && iq->usedb->ix_expr && !iq->idxDelete)) {
        /* save new blobs being deleted */
        sc_old = malloc(iq->usedb->lrl);
        if (sc_old == NULL) {
            logmsg(LOGMSG_ERROR, "%s malloc failed\n", __func__);
            retrc = ERR_INTERNAL;
            goto err;
        }
        /* convert old_dta and oldblobs to ".NEW..ONDISK" */
        rc = stag_to_stag_buf_blobs(iq->usedb->tablename, ".ONDISK", old_dta,
                                    ".NEW..ONDISK", sc_old, NULL, del_idx_blobs,
                                    del_idx_blobs ? MAXBLOBS : 0, 1);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to convert to new schema\n", __func__);
            retrc = rc;
            goto err;
        }

        sc_new = malloc(iq->usedb->lrl);
        if (sc_new == NULL) {
            logmsg(LOGMSG_ERROR, "%s malloc failed\n", __func__);
            retrc = ERR_INTERNAL;
            goto err;
        }
        /* convert new_dta and newblobs to ".NEW..ONDISK" */
        rc = stag_to_stag_buf_blobs(iq->usedb->tablename, ".ONDISK", new_dta,
                                    ".NEW..ONDISK", sc_new, NULL, add_idx_blobs,
                                    add_idx_blobs ? MAXBLOBS : 0, 1);

        /* re-verify keys on new table using ".NEW..ONDISK" */
        if (iq->usedb->ix_partial) {
            del_keys =
                verify_indexes(iq->usedb, sc_old, del_idx_blobs,
                               del_idx_blobs ? MAXBLOBS : 0, 0);
            ins_keys =
                verify_indexes(iq->usedb, sc_new, add_idx_blobs,
                               add_idx_blobs ? MAXBLOBS : 0, 0);
            if (ins_keys == -1ULL || del_keys == -1ULL) {
                fprintf(stderr, "%s: failed to verify_indexes\n", __func__);
                retrc = ERR_INTERNAL;
                goto err;
            }
        }

        /* use ".NEW..ONDISK" to form keys */
        use_new_tag = 1;
    }

    /*
     * Update blob records
     */

    for (blobn = 0; blobn < iq->usedb->numblobs; blobn++) {
        int doblob = 0;

        /* try to ignore */
        if (!gbl_use_plan || !iq->usedb->plan ||
            iq->usedb->plan->blob_plan[blobn] == -1) {
            doblob = 1;
        }

        /* optimization: try to update the genid only */
        if (doblob) {
            blob_buffer_t *blob;
            int oldcol, oldblobidx, idx;

            idx = get_schema_blob_field_idx((char *)iq->usedb->tablename,
                                            ".NEW..ONDISK", blobn);
            if (iq->debug) {
                reqprintf(iq,
                          "get_schema_blob_field_idx returns %d for blobno %d",
                          idx, blobn);
                reqmoref(iq, "myupdatecols[0] = %d", myupdatecols[0]);
            }

            if (idx < 0 || idx >= myupdatecols[0]) {
                logmsg(LOGMSG_ERROR, 
                        "upd_new_record newgenid 0x%llx get_schema_blob_field_idx "
                        "-> idx %d failed\n",
                    newgenid, idx);
                retrc = ERR_BADREQ;
                goto err;
            }

            /* can we update in place? */
            if (-1 == myupdatecols[idx + 1]) {
                rc = blob_upd_genid(iq, trans, blobn, 2, oldgenid, newgenid);
                if (iq->debug) {
                    reqprintf(iq, "blob_upd_genid blobno %d rc %d", blobn, rc);
                }
                if (0 != rc) {
                    logmsg(LOGMSG_ERROR, 
                            "upd_new_record newgenid 0x%llx blob_upd_genid "
                            "-> blobn %d failed\n",
                            newgenid, blobn);
                    retrc = rc;
                    goto err;
                }
                continue;
            }

            /* delete */
            rc = blob_del(iq, trans, 2, oldgenid, blobn);
            if (iq->debug) {
                reqprintf(iq, "blob_del genid 0x%llx rc %d", blobn, rc);
            }

            if (rc != IX_NOTFND && rc != 0) /* like in upd_record() */
            {
                logmsg(LOGMSG_ERROR, "upd_new_record oldgenid 0x%llx blob_del -> "
                                "blobn %d failed\n",
                        oldgenid, blobn);
                retrc = rc;
                goto err;
            }

            /* Use the column of the old blob to map to an old blob index */
            oldcol = myupdatecols[idx + 1];
            oldblobidx = get_schema_field_blob_idx((char *)iq->usedb->tablename,
                                                   ".ONDISK", oldcol);
            if (iq->debug) {
                reqprintf(iq, "get_schema_field_blob_idx returns %d for blobno "
                              "%d oldcol %d",
                          oldblobidx, blobn, oldcol);
            }

            /* check blob range */
            if (oldblobidx < 0 || oldblobidx >= MAXBLOBS) {
                logmsg(LOGMSG_ERROR, "upd_new_record newgenid 0x%llx blobrange -> "
                                "oldblobidx %d failed\n",
                        newgenid, oldblobidx);
                retrc = ERR_BADREQ;
                goto err;
            }

            /* add this only if it exists - if it doesn't exist it will change
             * to NULL */
            blob = &blobs[oldblobidx];
            if (blob->exists) {
                rc = blob_add(iq, trans, blobn, blob->data, blob->length, 2,
                              newgenid);
                if (iq->debug) {
                    reqprintf(iq, "blob_add blobno %d rc %d\n", blobn, rc);
                }
                if (rc != 0) {
                    logmsg(LOGMSG_ERROR, "upd_new_record newgenid 0x%llx blob_add ->"
                                    "blobn %d failed\n",
                            newgenid, blobn);
                    retrc = OP_FAILED_INTERNAL + ERR_ADD_BLOB;
                    goto err;
                }
            }
        }
    }

    /* First delete all keys */
    for (ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char keytag[MAXTAGLEN];
        char key[MAXKEYLEN];
        char mangled_key[MAXKEYLEN];
        char *od_dta_tail = NULL;
        int od_tail_len = 0;

        if (gbl_use_plan && iq->usedb->plan &&
            iq->usedb->plan->ix_plan[ixnum] != -1)
            continue;

        /* only delete keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(del_keys & (1ULL << ixnum)))
            continue;

        snprintf(keytag, sizeof(keytag), ".NEW..ONDISK_IX_%d", ixnum);

        if (iq->idxDelete) {
            memcpy(key, iq->idxDelete[ixnum], iq->usedb->ix_keylen[ixnum]);
            rc = 0;
        } else
            rc = create_key_from_ondisk_blobs(
                iq->usedb, ixnum, NULL, NULL, NULL,
                use_new_tag ? ".NEW..ONDISK" : ".ONDISK",
                use_new_tag ? (char *)sc_old : (char *)old_dta,
                0 /*not needed*/, keytag, key, NULL, del_idx_blobs,
                del_idx_blobs ? MAXBLOBS : 0, NULL);
        if (rc == -1) {
            logmsg(LOGMSG_ERROR, "upd_new_record oldgenid 0x%llx conversions -> "
                            "ix%d failed\n",
                    oldgenid, ixnum);
            if (iq->debug)
                reqprintf(iq, "CAN'T FORM OLD INDEX %d", ixnum);
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_DEL_RC_INVL_IDX, "cannot form old index %d",
                      ixnum);
            retrc = rc;
            goto err;
        }

        rc = ix_delk(iq, trans, key, ixnum, 2 /*rrn*/, oldgenid, ix_isnullk(iq->usedb, key, ixnum));
        if (iq->debug) {
            reqprintf(iq, "ix_delk IX %d KEY ", ixnum);
            reqdumphex(iq, key, getkeysize(iq->usedb, ixnum));
            reqmoref(iq, " RC %d", rc);
        }

        /* remap delete not found to retry */
        if (rc == IX_NOTFND)
            rc = RC_INTERNAL_RETRY;

        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "upd_new_record oldgenid 0x%llx ix_delk -> "
                            "ix%d, rc=%d failed\n",
                    oldgenid, ixnum, rc);
            retrc = rc;
            goto err;
        }
    }

    /* Add keys if we are not deferring.
     * If we are deferring, add will be called from delayed_key_adds() */
    if (!deferredAdd) {
        retrc = upd_new_record_add2indices(
            iq, trans, newgenid, use_new_tag ? sc_new : new_dta,
            use_new_tag ? iq->usedb->lrl : nd_len, ins_keys, use_new_tag,
            add_idx_blobs);
    } else
        reqprintf(iq, "is deferredAdd so will add to indices at the end");

err:
    if (sc_old)
        free(sc_old);
    if (sc_new)
        free(sc_new);
    if (iq->debug)
        reqpopprefixes(iq, prefixes);
    return retrc;
}

/*
 * Delete a single record from the new table, as part of a live schema
 * change.  This is done when you update or delete records behind the
 * cursor.
 *
 * The old data has to be passed in because we may not be rebuilding the
 * data file - in which case it's annoying and painful to have to get the
 * record from the other schema.
 */
int del_new_record(struct ireq *iq, void *trans, unsigned long long genid,
                   unsigned long long del_keys, const void *old_dta,
                   blob_buffer_t *del_idx_blobs)
{
    int retrc = 0;
    void *sc_old = NULL;
    unsigned long long ngenid;
    int prefixes = 0;
    int rc;
    int ixnum;

    int use_new_tag = 0;

    if (iq->debug) {
        reqpushprefixf(iq, "del_new_record: ");
        prefixes++;
    }

    if (!iq->usedb) {
        if (iq->debug)
            reqprintf(iq, "NO USEDB SET");
        retrc = ERR_BADREQ;
        goto err;
    }

    if (iq->debug) {
        reqpushprefixf(iq, "TBL %s ", iq->usedb->tablename);
        prefixes++;
    }

    if ((gbl_partial_indexes && iq->usedb->ix_partial) ||
         (gbl_expressions_indexes && iq->usedb->ix_expr)) {
        int ixnum;
        int rebuild_keys = 0;
        if (!gbl_use_plan || !iq->usedb->plan)
            rebuild_keys = 1;
        else {
            for (ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
                if (iq->usedb->plan->ix_plan[ixnum] == -1) {
                    rebuild_keys = 1;
                    break;
                }
            }
        }
        if (rebuild_keys) {
            if (iq->idxInsert || iq->idxDelete) {
                free_cached_idx(iq->idxInsert);
                free_cached_idx(iq->idxDelete);
                free(iq->idxInsert);
                free(iq->idxDelete);
                iq->idxInsert = iq->idxDelete = NULL;
            }
            del_keys = -1ULL;
        }
    }

    /* if the destination database does not have odh, mask the updateid */
    ngenid = bdb_normalise_genid(iq->usedb->handle, genid);

    /*fprintf(stderr, "DEL NEW GENID 0x%llx\n", ngenid);*/

    if (iq->usedb->has_datacopy_ix ||
        (gbl_partial_indexes && iq->usedb->ix_partial && del_keys == -1ULL) ||
        (gbl_expressions_indexes && iq->usedb->ix_expr && !iq->idxDelete)) {
        sc_old = malloc(iq->usedb->lrl);
        if (sc_old == NULL) {
            logmsg(LOGMSG_ERROR, "%s malloc failed\n", __func__);
            retrc = ERR_INTERNAL;
            goto err;
        }
        /* convert old_dta and oldblobs to ".NEW..ONDISK" */
        rc = stag_to_stag_buf_blobs(iq->usedb->tablename, ".ONDISK", old_dta,
                                    ".NEW..ONDISK", sc_old, NULL, del_idx_blobs,
                                    del_idx_blobs ? MAXBLOBS : 0, 1);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s failed to convert to new schema\n", __func__);
            retrc = rc;
            goto err;
        }

        /* re-verify keys on new table using ".NEW..ONDISK" */
        if (iq->usedb->ix_partial) {
            del_keys =
                verify_indexes(iq->usedb, sc_old, del_idx_blobs,
                               del_idx_blobs ? MAXBLOBS : 0, 0);
            if (del_keys == -1ULL) {
                fprintf(stderr, "%s: failed to verify_indexes\n", __func__);
                retrc = ERR_INTERNAL;
                goto err;
            }
        }

        /* use ".NEW..ONDISK" to form keys */
        use_new_tag = 1;
    }

    /* no plan:
     *  Delete data record.
     *  Bdblib automatically deletes associated blobs too.
     */
    if (!gbl_use_plan || !iq->usedb->plan || iq->usedb->plan->dta_plan == -1) {
        ngenid = bdb_normalise_genid(iq->usedb->handle, ngenid);
        rc = dat_del(iq, trans, 2 /*rrn*/, ngenid);
        if (iq->debug)
            reqprintf(iq, "DEL GENID 0x%llx RC %d", ngenid, rc);

        /* workaround a bug in current schema change; if we somehow
           fail to find the row in the new btree, try again */
        if (rc == ERR_VERIFY)
            rc = RC_INTERNAL_RETRY;

        if (rc != 0) {
            retrc = rc;
            goto err;
        }
    } else /* have a plan:  only delete blobs we are told to */
    {
        int blobn;

        /* No data file.  Delete blobs manually as required. */
        for (blobn = 0; blobn < iq->usedb->numblobs; blobn++) {
            if (iq->usedb->plan->blob_plan[blobn] == -1) {
                rc = blob_del(iq, trans, 2 /*rrn*/, ngenid, blobn);
                if (iq->debug)
                    reqprintf(iq, "DEL GENID 0x%llx RC %d", ngenid, rc);
                if (rc != 0) {
                    retrc = rc;
                    goto err;
                }
            }
        }
    }

    /*
     * Form and delete all keys.
     */
    for (ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char keytag[MAXTAGLEN];
        char key[MAXKEYLEN];
        char mangled_key[MAXKEYLEN];

        if (gbl_use_plan && iq->usedb->plan &&
            iq->usedb->plan->ix_plan[ixnum] != -1)
            continue;

        /* only delete keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(del_keys & (1ULL << ixnum)))
            continue;

        snprintf(keytag, sizeof(keytag), ".NEW..ONDISK_IX_%d", ixnum);

        /* Convert from OLD ondisk schema to NEW index schema - this
         * must work by definition. */
        /*
        rc = stag_to_stag_buf(iq->usedb->tablename, ".ONDISK", (char*) old_dta,
                keytag, key, NULL);
         */
        if (iq->idxDelete) {
            memcpy(key, iq->idxDelete[ixnum], iq->usedb->ix_keylen[ixnum]);
            rc = 0;
        } else
            rc = create_key_from_ondisk_blobs(
                iq->usedb, ixnum, NULL, NULL, NULL,
                use_new_tag ? ".NEW..ONDISK" : ".ONDISK",
                use_new_tag ? (char *)sc_old : (char *)old_dta,
                0 /*not needed */, keytag, key, NULL, del_idx_blobs,
                del_idx_blobs ? MAXBLOBS : 0, NULL);
        if (rc == -1) {
            logmsg(LOGMSG_ERROR, 
                    "del_new_record genid 0x%llx conversion -> ix%d failed\n",
                    genid, ixnum);
            if (iq->debug)
                reqprintf(iq, "CAN'T FORM INDEX %d", ixnum);
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_DEL_RC_INVL_IDX, "cannot form index %d",
                      ixnum);
            retrc = rc;
            goto err;
        }

        rc = ix_delk(iq, trans, key, ixnum, 2 /*rrn*/, ngenid, ix_isnullk(iq->usedb, key, ixnum));
        if (iq->debug) {
            reqprintf(iq, "ix_delk IX %d KEY ", ixnum);
            reqdumphex(iq, key, getkeysize(iq->usedb, ixnum));
            reqmoref(iq, " RC %d", rc);
        }

        /* remap delete not found to retry */
        if (rc == IX_NOTFND)
            rc = RC_INTERNAL_RETRY;

        if (rc != 0) {
            retrc = rc;
            goto err;
        }
    }

err:
    if (sc_old)
        free(sc_old);
    if (iq->debug)
        reqpopprefixes(iq, prefixes);
    return retrc;
}

/* copied and pasted from toblock.c */
static int check_blob_buffers(struct ireq *iq, blob_buffer_t *blobs,
                              size_t maxblobs, const char *tblname,
                              const char *tagname, struct schema *schema,
                              void *record, const void *nulls)
{
    extern int gbl_disable_blob_check;
    int cblob, num_cblobs;
    int ondisk = is_tag_ondisk_sc(schema);

    num_cblobs = schema->numblobs;

    if (gbl_disable_blob_check)
        return 0;

    ondisk = is_tag_ondisk(tblname, tagname);

    if (num_cblobs > maxblobs) {
        if (iq->debug)
            reqprintf(iq, "TOO FEW BLOBS - TAG %s HAS %d BLOBS", tagname,
                      num_cblobs);
        return maxblobs + 1;
    }

    /* Make sure we have consistent data for each blob. */
    for (cblob = 0; cblob < maxblobs; cblob++) {
        /* if this is ondisk format we're checking, length !=collected */
        if (blobs[cblob].exists && !ondisk &&
            blobs[cblob].length != blobs[cblob].collected) {
            if (iq->debug)
                reqprintf(iq, "GOT BAD BLOB BUFFERS FOR BLOB %d", cblob);
            return cblob + 1;
        }

        if (cblob >= num_cblobs) {
            if (blobs[cblob].exists) {
                if (iq->debug)
                    reqprintf(iq, "GOT TOO MANY BLOBS");
                return cblob + 1;
            }
        } else {
            client_blob_tp cltblob;
            int inconsistent;
            int idx = get_schema_blob_field_idx_sc(schema, cblob);
            client_blob_tp *blob;

            if (ondisk) {
                int isnull, outsz;
                void *sblob = get_field_ptr_in_buf(schema, idx, record);
                SERVER_BLOB_to_CLIENT_BLOB(
                    sblob, 5 /* length */, NULL /* conversion options */,
                    NULL /* blob */, &cltblob, sizeof(cltblob), &isnull, &outsz,
                    NULL /* conversion options */, NULL /* blob */);
                blob = &cltblob;
            } else {
                blob = get_field_ptr_in_buf(schema, idx, record);
            }

            /* Use the null map to free up any blobs that may have been sent
             * to us that we don't actually want. (this protects against
             * a boundary case with dynamic tags). */
            if (btst(nulls, idx) && blobs[cblob].exists) {
                logmsg(LOGMSG_ERROR, "GOT BLOB DATA FOR BLOB %d (NULL FIELD %d)\n", cblob,
                       idx);
                if (iq->debug)
                    reqprintf(iq, "GOT BLOB DATA FOR BLOB %d (NULL FIELD %d)",
                              cblob, idx);
                if (blobs[cblob].data)
                    free(blobs[cblob].data);
                bzero(&blobs[cblob], sizeof(blobs[cblob]));
                bzero(blob, sizeof(client_blob_tp));
            }

            /* If this is an osql optimized blob, we get a free pass. */
            if (0xfffffffe == (int)blobs[cblob].length) {
                inconsistent = 0;
            }
            /* if we found a schema earlier, and this blob is a vutf8 string,
             * and the string was small enough to fit in the record itself,
             * then the blob shouldn't exist */
            else if (schema && (schema->member[idx].type == SERVER_VUTF8 ||
                                schema->member[idx].type == SERVER_BLOB2) &&
                     ntohl(blob->length) <= schema->member[idx].len - 5 /*hdr*/)
                inconsistent = blobs[cblob].exists;

            /* otherwise, fall back to regular blob checks */
            else if (blob->notnull)
                inconsistent = !blobs[cblob].exists ||
                               blobs[cblob].length != ntohl(blob->length);

            else
                inconsistent = blobs[cblob].exists;

            if (inconsistent) {
                if (iq->debug) {
                    reqprintf(iq, "INCONSISTENT BLOB BUFFERS FOR BLOB %d",
                              cblob);
                    reqprintf(
                        iq, "blob->notnull=%d blob->length=%u "
                            "blobs[cblob].length=%u blobs[cblob].exists=%d",
                        ntohl(blob->notnull), (unsigned)ntohl(blob->length),
                        (unsigned)blobs[cblob].length, blobs[cblob].exists);
                }
                return cblob + 1;
            }
        }
    }
    return 0;
}

static int check_blob_sizes(struct ireq *iq, blob_buffer_t *blobs, int maxblobs)
{
    for (int i = 0; i < maxblobs; i++) {
        if (blobs[i].exists && blobs[i].length != -2 &&
            blobs[i].length > MAXBLOBLENGTH) {
            reqerrstr(iq, COMDB2_ADD_RC_INVL_BLOB,
                      "blob size (%d) exceeds maximum (%d)", blobs[i].length,
                      MAXBLOBLENGTH);
            return ERR_BLOB_TOO_LARGE;
        }
    }
    return 0;
}

/* find and remember the blobs for an rrn/genid. */
int save_old_blobs(struct ireq *iq, void *trans, const char *tag, const void *record,
                   int rrn, unsigned long long genid, blob_status_t *blobs)
{
    int rc;

    /* get schema info */
    if (gather_blob_data(iq, tag, blobs, tag) != 0)
        return ERR_INTERNAL;

    /* if we are in rowlocks, this will be a non-transactional read
       MAYBE this should always be a non-transactional read if I
       am saving this */
    if (gbl_rowlocks)
        trans = NULL;

    /* get all the blobs that we know of */
    rc = ix_find_blobs_by_rrn_and_genid_tran(
        iq, trans, rrn, genid, blobs->numcblobs, blobs->cblob_disk_ixs,
        blobs->bloblens, blobs->bloboffs, (void **)blobs->blobptrs);
    if (iq->debug)
        reqprintf(iq, "FIND OLD BLOBS RRN %d GENID 0x%llx RC %d", rrn, genid,
                  rc);
    if (rc != 0) {
        free_blob_status_data(blobs);
        return rc;
    }

    /* make sure the blobs are consistent with the record; if they're not then
     * we have a database corruption situation. */
    rc = check_blob_consistency(iq, iq->usedb->tablename, tag, blobs, record);
    if (iq->debug)
        reqprintf(iq, "CHECK OLD BLOB CONSISTENCY RRN %d GENID 0x%llx RC %d",
                  rrn, genid, rc);
    if (rc != 0) {
        free_blob_status_data(blobs);
        return ERR_CORRUPT;
    }

    return 0;
}

int updbykey_record(struct ireq *iq, void *trans, const uint8_t *p_buf_tag_name,
                    const uint8_t *p_buf_tag_name_end, uint8_t *p_buf_rec,
                    const uint8_t *p_buf_rec_end, const char *keyname,
                    const unsigned char fldnullmap[32], blob_buffer_t *blobs,
                    size_t maxblobs, int *opfailcode, int *ixfailnum, int *rrn,
                    unsigned long long *genid, int opcode, int blkpos,
                    int flags)
{
    char tag[MAXTAGLEN + 1];
    int conv_flags = 0;
    int rc = 0;
    int retrc = 0;
    int expected_dat_len;
    struct schema *dynschema = NULL;
    int prefixes = 0;
    unsigned char lclnulls[64];
    const char *ondisktag;
    int using_myblobs = 0;
    blob_buffer_t myblobs[MAXBLOBS];
    char key[MAXKEYLEN];
    int keysz = 0;
    int ixnum;
    char fndkey[MAXKEYLEN];
    int fndrrn;
    unsigned long long fndgenid;
    const char *tagdescr = (const char *)p_buf_tag_name;
    size_t taglen = p_buf_tag_name_end - p_buf_tag_name;
    char *record = (char *)p_buf_rec;
    size_t reclen = p_buf_rec_end - p_buf_rec;

    *ixfailnum = -1;

    if (!blobs) {
        bzero(myblobs, sizeof(myblobs));
        maxblobs = MAXBLOBS;
        blobs = myblobs;
        using_myblobs = 1;
    }

    if (flags & RECFLAGS_NEW_SCHEMA)
        ondisktag = ".NEW..ONDISK";
    else
        ondisktag = ".ONDISK";

    if (iq->debug) {
        reqpushprefixf(iq, "add_record: ");
        prefixes++;
    }

    if (!iq->usedb) {
        if (iq->debug)
            reqprintf(iq, "NO USEDB SET");
        retrc = ERR_BADREQ;
        ERR;
    }

    rc = bdb_lock_table_read(iq->usedb->handle, trans);
    if (rc == BDBERR_DEADLOCK) {
        if (iq->debug)
            reqprintf(iq, "LOCK TABLE READ DEADLOCK");
        retrc = RC_INTERNAL_RETRY;
        goto err;
    } else if (rc) {
        if (iq->debug)
            reqprintf(iq, "LOCK TABLE READ ERROR: %d", rc);
        *opfailcode = OP_FAILED_INTERNAL;
        retrc = ERR_INTERNAL;
        goto err;
    }

    if (iq->debug) {
        reqpushprefixf(iq, "TBL %s ", iq->usedb->tablename);
        prefixes++;
    }

    rc = resolve_tag_name(iq, tagdescr, taglen, &dynschema, tag, sizeof(tag));
    if (rc != 0) {
        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TAG,
                  "invalid tag description '%.*s'", taglen, tagdescr);
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        ERR;
    }

    if ((!dynschema && (flags & RECFLAGS_DYNSCHEMA_NULLS_ONLY)) ||
        (!fldnullmap)) {
        bzero(lclnulls, sizeof(lclnulls));
        fldnullmap = lclnulls;
    }

    /* Tweak blob-descriptors for static tags. */
    if (gbl_disallow_null_blobs && !dynschema &&
        (flags & RECFLAGS_DYNSCHEMA_NULLS_ONLY)) {
        static_tag_blob_conversion(iq->usedb->tablename, tag, record, blobs,
                                   maxblobs);
    }

    if (iq->debug) {
        reqpushprefixf(iq, "TAG %s ", tag);
        prefixes++;
    }
    struct schema *dbname_schema = find_tag_schema(iq->usedb->tablename, tag);
    if (dbname_schema == NULL) {
        if (iq->debug)
            if (iq->debug)
                reqprintf(iq, "UNKNOWN TAG %s TABLE %s\n", tag,
                          iq->usedb->tablename);
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        ERR;
    }

    expected_dat_len = get_size_of_schema(dbname_schema);
    if ((size_t)expected_dat_len > reclen) {
        if (iq->debug)
            reqprintf(iq, "BAD DTA LEN %u TAG %s EXPECTS DTALEN %u\n", reclen,
                      tag, expected_dat_len);
        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_ADD_RC_INVL_DTA,
                  "bad data length %u tag '%s' expects data length %u\n",
                  reclen, tag, expected_dat_len);
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        ERR;
    }

    reclen = expected_dat_len;

    if (!(flags & RECFLAGS_NO_BLOBS) &&
        check_blob_buffers(iq, blobs, maxblobs, iq->usedb->tablename, tag,
                           dbname_schema, record, fldnullmap) != 0) {
        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_ADD_RC_INVL_BLOB,
                  "no blobs flags with blob buffers");
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        ERR;
    }

    rc = getidxnumbyname(iq->usedb->tablename, keyname, &ixnum);
    if (rc != 0) {
        if (iq->debug) {
            reqprintf(iq, "BAD KEY %s", keyname);
        }

        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_ADD_RC_INVL_DTA,
                  "no blobs flags with blob buffers");
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        ERR;
    }

    if (iq->have_client_endian &&
        TAGGED_API_LITTLE_ENDIAN == iq->client_endian) {
        conv_flags |= CONVERT_LITTLE_ENDIAN_CLIENT;
    }

    if (strcmp(tag, ondisktag) == 0) {
        /* XXX support this? */
    } else {
        int od_len_int;
        struct convert_failure reason;
        char keytag[MAXTAGLEN];

        keysz = getkeysize(iq->usedb, ixnum);
        if (keysz < 0) {
            logmsg(LOGMSG_ERROR, "cannot get key size"
                                 " tbl %s. idx %d\n",
                   iq->usedb->tablename, ixnum);
            /* XXX is this an error? */
        }
        snprintf(keytag, sizeof(keytag), "%s_IX_%d", ondisktag, ixnum);

        rc = ctag_to_stag_blobs_tz(iq->usedb->tablename, tag, record,
                                   WHOLE_BUFFER, fldnullmap, keytag, key,
                                   conv_flags, &reason /*fail reason*/, blobs,
                                   maxblobs, iq->tzname);
        if (rc == -1) {
            char str[128];
            convert_failure_reason_str(&reason, iq->usedb->tablename, tag,
                                       ".ONDISK", str, sizeof(str));
            if (iq->debug) {
                reqprintf(iq, "ERR CONVERT DTA %s->.ONDISK '%s'", tag, str);
            }
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_ADD_RC_CNVT_DTA,
                      "error convert data %s->.ONDISK '%s'", tag, str);
            *opfailcode = OP_FAILED_CONVERSION;
            retrc = ERR_CONVERT_DTA;
            ERR;
        }
    }

    /*
      key now contains an ondisk key.  lets find the record (txnally of course)
      and using the found genid, call upd_record() in this very file to
      let it do the rest of the work */

    rc = ix_find_trans(iq, trans, ixnum, key, keysz, fndkey, &fndrrn, &fndgenid,
                       NULL, 0, 0);
    if (rc != 0) {
        if (iq->debug) {
            reqprintf(iq, "IX FIND FAILED ON IDX: %d", ixnum);
        }

        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_ADD_RC_INVL_BLOB, "failed to find on idx: %d",
                  ixnum);

        *opfailcode = OP_FAILED_VERIFY;

        retrc = rc;
        ERR;
    }

    rc = upd_record(iq, trans, NULL /*primkey*/, fndrrn, fndgenid,
                    (const unsigned char *)tagdescr,
                    (const unsigned char *)tagdescr + taglen, (uint8_t *)record,
                    (uint8_t *)record + reclen, NULL /*p_buf_vrec*/,
                    NULL /*p_buf_vrec_end*/, fldnullmap, NULL /*updCols*/,
                    blobs, maxblobs, &fndgenid, -1ULL, -1ULL, opfailcode,
                    ixfailnum, opcode, blkpos, flags);

    if (rc != 0) {
        if (iq->debug) {
            reqprintf(iq, "FAILED TO UPD %s", keyname);
        }

        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_ADD_RC_INVL_BLOB,
                  "failed to update record using idx: %d", ixnum);

        *opfailcode = OP_FAILED_INTERNAL;

        retrc = rc;
        ERR;
    }

    dbglog_record_db_write(iq, "updbykey");
    if (iq->__limits.maxcost && iq->cost > iq->__limits.maxcost)
        retrc = ERR_LIMIT;

err:
    if (iq->debug)
        reqpopprefixes(iq, prefixes);
    if (dynschema)
        free_dynamic_schema(iq->usedb->tablename, dynschema);
    if (using_myblobs)
        free_blob_buffers(myblobs, MAXBLOBS);
    return retrc;
}

void blob_status_to_blob_buffer(blob_status_t *bs, blob_buffer_t *bf)
{
    int blobno = 0;
    for (blobno = 0; blobno < bs->numcblobs; blobno++) {
        if (bs->blobptrs[blobno] != NULL) {
            bf[blobno].exists = 1;
            bf[blobno].data = bs->blobptrs[blobno];
            bf[blobno].length = bs->bloblens[blobno];
            bf[blobno].collected = bs->bloblens[blobno];
        }
    }
}

int bdb_add_rep_blob(bdb_state_type *bdb_state, tran_type *tran, int session,
                     int seqno, void *blob, int sz, int *bdberr);

void testrep(int niter, int recsz)
{
    tran_type *tran;
    int i;
    int bdberr;
    unsigned char *stuff;
    struct ireq iq;
    int rc;
    int now, last, n;

    stuff = malloc(recsz);

    init_fake_ireq(thedb, &iq);
    iq.usedb = thedb->dbs[0];

    n = 0;
    now = last = comdb2_time_epochms();

    for (i = 0; i < niter; i++) {
        tran = bdb_tran_begin(thedb->bdb_env, NULL, &bdberr);
        if (tran == NULL) {
            logmsg(LOGMSG_ERROR, "bdb_tran_begin rc %d\n", bdberr);
            goto done;
        }
        rc = bdb_add_rep_blob(thedb->bdb_env, tran, 0, i, stuff, sizeof(stuff),
                              &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_add_rep_blob rc %d bdberr %d\n", rc, bdberr);
            goto done;
        }
        rc = trans_commit(&iq, tran, gbl_mynode);
        if (rc) {
            logmsg(LOGMSG_ERROR, "commit rc %d\n", rc);
            goto done;
        }

        now = comdb2_time_epochms();
        if ((now - last) > 1000) {
            logmsg(LOGMSG_ERROR, "%d\n", n);
            n = 0;
            last = now;
        }
        n++;
    }
done:
    free(stuff);
}
