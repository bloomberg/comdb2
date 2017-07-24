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
   this is a thread pool of lightweight block processors that race with
   actual block processors (on the same buffer) and enqueue prefault ops
   to the prefault io threads.

   in the interest of simplicity, we only understand 3 ops here,
   tagged update, tagged add, tagged delete.

*/

#include <alloca.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>

#include <plbitlib.h>

#include "comdb2.h"
#include "tag.h"
#include "types.h"
#include "translistener.h"
#include "block_internal.h"
#include "prefault.h"

/* when we add a record, we can form all the keys right here - we already
   have the new dta record.  so we form all the keys and call enque_pfault_key
   on each */
static int add_record_prefault(
    struct ireq *iq, const uint8_t *p_buf_tag_name,
    const uint8_t *p_buf_tag_name_end, uint8_t *p_buf_rec,
    const uint8_t *p_buf_rec_end, unsigned char fldnullmap[32], int *opfailcode,
    int *ixfailnum, int *rrn, unsigned long long *genid, int blkpos,
    int helper_thread, unsigned int seqnum, int flags, int flush)
{
    char tag[MAXTAGLEN + 1];
    int taglen = p_buf_tag_name_end - p_buf_tag_name;
    int reclen = p_buf_rec_end - p_buf_rec;
    int is_od_tag;
    int rc;
    int retrc;
    int expected_dat_len;
    struct schema *dynschema = NULL;
    void *od_dta;
    size_t od_len;
    int prefixes = 0;
    unsigned char lclnulls[32];
    const char *ondisktag;
    int ixnum;
    unsigned char stackbuf[32768];
    int do_flush;

    *ixfailnum = -1;

/* if we only have 1 index, there isnt much potential to parallelize */
#if 0
   if (iq->usedb->nix == 1)
      return 0;
#endif

    iq->dbenv->prefault_stats.num_add_record++;

    if (flags & RECFLAGS_NEW_SCHEMA)
        ondisktag = ".NEW..ONDISK";
    else
        ondisktag = ".ONDISK";

    rc = resolve_tag_name(iq, (const char *)p_buf_tag_name, taglen, &dynschema,
                          tag, sizeof(tag));
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

    expected_dat_len = get_size_of_schema_by_name(iq->usedb->dbname, tag);
    if ((size_t)expected_dat_len != reclen) {
        if (iq->debug)
            reqprintf(iq, "BAD DTA LEN %u TAG %s EXPECTS DTALEN %u\n", reclen,
                      tag, expected_dat_len);
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        goto err;
    }

    if (strcmp(tag, ondisktag) == 0) {
        /* we have the ondisk data already, no conversion needed */
        od_dta = p_buf_rec;
        od_len = reclen;
        is_od_tag = 1;
    } else {
        struct convert_failure reason;

        /* we need to convert the record to ondisk format */

        od_dta = stackbuf;

        rc = ctag_to_stag_buf(iq->usedb->dbname, tag, (const char *)p_buf_rec,
                              WHOLE_BUFFER, fldnullmap, ondisktag, od_dta, 0,
                              &reason);
        if (rc == -1) {
            if (iq->debug) {
                char str[128];
                convert_failure_reason_str(&reason, iq->usedb->dbname, tag,
                                           ".ONDISK", str, sizeof(str));
                reqprintf(iq, "ERR CONVERT DTA %s->.ONDISK '%s'", tag, str);
            }
            *opfailcode = OP_FAILED_CONVERSION;
            retrc = ERR_CONVERT_DTA;
            goto err;
        }
    }

    /*
     * Form and enqueue prefaults for all the keys.
     */

    for (ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        int ixkeylen;
        char ixtag[MAXTAGLEN];
        char key[MAXKEYLEN];
        ixkeylen = getkeysize(iq->usedb, ixnum);
        if (ixkeylen < 0) {
            if (iq->debug)
                reqprintf(iq, "BAD INDEX %d OR KEYLENGTH %d", ixnum, ixkeylen);
            *ixfailnum = ixnum;
            *opfailcode = OP_FAILED_BAD_REQUEST;
            retrc = ERR_BADREQ;
            goto err;
        }

        snprintf(ixtag, sizeof(ixtag), "%s_IX_%d", ondisktag, ixnum);
        rc = stag_to_stag_buf(iq->usedb->dbname, ondisktag, od_dta, ixtag, key,
                              NULL);
        if (rc == -1) {
            if (iq->debug)
                reqprintf(iq, "CAN'T FORM INDEX %d", ixnum);
            *ixfailnum = ixnum;
            *opfailcode = OP_FAILED_INTERNAL + ERR_FORM_KEY;
            retrc = rc;
            goto err;
        }

        ixkeylen = getkeysize(iq->usedb, ixnum);

        /* only trigger a flush on the last one */
        if (((iq->usedb->nix - ixnum) == 1) && (flush == 1))
            do_flush = 1;
        else
            do_flush = 0;

        /*
        fprintf(stderr,
           "add_record_prefault: calling enque_pfault_newkey ix %d\n", ixnum);
        */

        rc = enque_pfault_newkey(
            iq->usedb, key, ixkeylen, ixnum, blkpos, helper_thread, seqnum,
            gbl_prefault_toblock_bcast, gbl_prefault_toblock_local, do_flush);
    }

    /* success */
    retrc = 0;

err:
    if (iq->debug)
        reqpopprefixes(iq, prefixes);

    if (dynschema)
        free_dynamic_schema(iq->usedb->dbname, dynschema);

    return retrc;
}

/* update is pretty complicated - but all we do here is hand off the
   op to enque_pfault_data_keys_newkeys() who does all the heavy lifting
   for us.  it generates ( (NUMIX * 2) + 1 ) prefault i/os.
   */

static int
upd_record_prefault(struct ireq *iq, void *primkey, int rrn,
                    unsigned long long vgenid, const uint8_t *p_buf_tag_name,
                    const uint8_t *p_buf_tag_name_end, uint8_t *p_buf_rec,
                    const uint8_t *p_buf_rec_end, uint8_t *p_buf_vrec,
                    const uint8_t *p_buf_vrec_end, unsigned char fldnullmap[32],
                    unsigned long long *genid, int *opfailcode, int *ixfailnum,
                    int blkpos, int helper_thread,
                    unsigned int seqnum, int flags, int flush)
{
    int taglen = p_buf_tag_name_end - p_buf_tag_name;
    int reclen = p_buf_rec_end - p_buf_rec;
    int rc, retrc = 0, prefixes = 0;
    int expected_dat_len;
    struct schema *dynschema = NULL;
    size_t od_len;
    char tag[MAXTAGLEN + 1];
    unsigned char lclnulls[32];

    *ixfailnum = -1;

/* if we only have 1 index, there isnt much potential to parallelize */
#if 0
   if (iq->usedb->nix == 1)
      return 0;
#endif

    iq->dbenv->prefault_stats.num_upd_record++;

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
        reqpushprefixf(iq, "TBL %s ", iq->usedb->dbname);
        prefixes++;
    }

    rc = resolve_tag_name(iq, (const char *)p_buf_tag_name, taglen, &dynschema,
                          tag, sizeof(tag));
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

    expected_dat_len = get_size_of_schema_by_name(iq->usedb->dbname, tag);
    if ((size_t)expected_dat_len != reclen) {
        if (iq->debug)
            reqprintf(iq, "BAD DTA LEN %u TAG %s EXPECTS DTALEN %u\n", reclen,
                      tag, expected_dat_len);
        *opfailcode = OP_FAILED_BAD_REQUEST;
        retrc = ERR_BADREQ;
        goto err;
    }

    rc = enque_pfault_olddata_oldkeys_newkeys(
        iq->usedb, vgenid, (char *)p_buf_tag_name, taglen, p_buf_rec, reclen,
        blkpos, helper_thread, seqnum, gbl_prefault_toblock_bcast,
        gbl_prefault_toblock_local, flush);

err:

    if (iq->debug)
        reqpopprefixes(iq, prefixes);

    if (dynschema)
        free_dynamic_schema(iq->usedb->dbname, dynschema);

    return retrc;
}

/* delete is easy - all we need to do is get the dta and keys for the
   record in question faulted in.  enque_pfault_data_keys() does all that
   for us */
static int del_record_prefault(struct ireq *iq, void *primkey,
                               int rrn, unsigned long long genid,
                               int *opfailcode, int *ixfailnum,
                               int blkpos, int helper_thread,
                               unsigned int seqnum, int flags, int flush)
{
    int retrc;
    int prefixes = 0;
    int rc;

/* if we only have 1 index, there isnt much potential to parallelize */
#if 0
   if (iq->usedb->nix == 1)
      return 0;
#endif

    *ixfailnum = -1;

    iq->dbenv->prefault_stats.num_del_record++;

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

    if (iq->debug) {
        reqpushprefixf(iq, "TBL %s ", iq->usedb->dbname);
        prefixes++;
    }

    rc = enque_pfault_olddata_oldkeys(iq->usedb, genid, blkpos, helper_thread,
                                      seqnum, gbl_prefault_toblock_bcast,
                                      gbl_prefault_toblock_local, flush);

    /* success */
    retrc = 0;

err:

    if (iq->debug)
        reqpopprefixes(iq, prefixes);

    return retrc;
}

/*
   scan through the block, picking off TAGGED add, delete, update requests

   add:    do form all keys, and enqueue PRFQ_KEY for each formed key.

   delete: do enqueue PFRQ_DATA_KEYS for the genid being deleted.
              this will cause the dta record to be faulted, then cause
              requests to fault all the keys formed from that record to
              be enqueued for faulting

   update: do enqueue PFRQ_DATA_KEYS_NEWKEYS for the genid
              being updated.
              this will cause the dta record to be faulted, then cause
              requests to fault all the keys formed from that record to
              be enqueued for faulting
              then it will form the new dta from the found dta + the newdta
              given as input and form all new keys and enqueue
              PRFQ_KEY for each formed key.

 */
int prefault_toblock(struct ireq *iq_in, void *ptr_in, int helper_thread,
                     unsigned int seqnum, int *abort)
{
    int ii, jj, datoff, datlen = 0, num_reqs, maxoff, curoff, lastoff;
    int rc, ixnum = 0, ixkeylen, rrnoff, rrn, dbnum;
    int vptr = 0, vlen, newlen, dtalen, irc, taglen = 0;
    int source_node;
    char *vdta, *newdta;
    int fndrrn = 0, fndlen = 0;
    char tbltag[64];
    union packedreq *packedreq;
    /* for updates */
    int ondisk_size = 0;
    int saved_rrn = 0;

    int addrrn;
    unsigned char nulls[MAXNULLBITS];
    unsigned long long genid = 0;
    int numerrs = 0;
    struct block_err err[1];
    int skipblock;
    int flush;
    struct ireq *iq;
    struct ireq iq_save;
    block_state_t blkstate_save;
    block_state_t *p_blkstate;

    /* p_blkstate = ptr_in;*/

    memcpy(&blkstate_save, ptr_in, sizeof(block_state_t));
    p_blkstate = &blkstate_save;

    /* take a copy of the iq - we're gonna pass this around to threads
       that may end up outliving the original iq */

    /* if this is really true, then it could have already outlived it .. */
    memcpy(&iq_save, iq_in, sizeof(struct ireq));

    iq = &iq_save;

    num_reqs = p_blkstate->numreq;

    addrrn = -1; /*for secafpri, remember last rrn. */

    /*which db currently operating on. */
    iq->usedb = iq->origdb; /*start with original db. */

    /*sleep(1);*/

    /*fprintf(stderr, "prefault_toblock: num_reqs = %d\n", num_reqs);*/

    flush = 1;

    for (ii = 0; ii < num_reqs; ii++, block_state_next(iq, p_blkstate)) {
        struct packedreq_hdr hdr;
        if (*abort) {
            iq->dbenv->prefault_stats.aborts++;
            break;
        }

        skipblock = 0;

        iq->p_buf_in =
            packedreq_hdr_get(&hdr, iq->p_buf_in, p_blkstate->p_buf_req_end);

        if (iq->p_buf_in == NULL)
            break;

        if (block_state_set_next(iq, p_blkstate, hdr.nxt))
            break;

        bzero(nulls, sizeof(nulls));

        switch (hdr.opcode) {
        case BLOCK2_ADDKL: {
            struct packedreq_addkl add_kless;

            const uint8_t *p_buf_tag_name;
            const uint8_t *p_buf_tag_name_end;
            uint8_t *p_buf_data;
            const uint8_t *p_buf_data_end;

            if (!(iq->p_buf_in =
                      packedreq_addkl_get(&add_kless, iq->p_buf_in,
                                          p_blkstate->p_buf_next_start))) {
                if (iq->debug)
                    reqprintf(iq, "PREFAULT FAILED TO UNPACK ADDKL REQUEST\n");
                break;
            }

            loadnullbmp(nulls, sizeof(nulls), add_kless.fldnullmap,
                        sizeof(add_kless.fldnullmap));

            if (add_kless.taglen >
                (p_blkstate->p_buf_next_start - iq->p_buf_in)) {
                if (iq->debug)
                    reqprintf(iq, "PREFAULT ADDKL INVALID TAGLEN\n");
                break;
            }

            p_buf_tag_name = iq->p_buf_in;
            iq->p_buf_in += add_kless.taglen;
            p_buf_tag_name_end = iq->p_buf_in;

            if (add_kless.reclen >
                (p_blkstate->p_buf_next_start - iq->p_buf_in)) {
                if (iq->debug)
                    reqprintf(iq, "PREFAULT ADDKL INVALID RECLEN\n");
                break;
            }
            p_buf_data = (uint8_t *)iq->p_buf_in;
            iq->p_buf_in += add_kless.reclen;
            p_buf_data_end = iq->p_buf_in;

            rc = add_record_prefault(
                iq, p_buf_tag_name, p_buf_tag_name_end, p_buf_data,
                p_buf_data_end, nulls, &err[numerrs].errcode,
                &err[numerrs].ixnum, &addrrn, &genid, ii, /*blkpos*/
                helper_thread, seqnum, RECFLAGS_DYNSCHEMA_NULLS_ONLY, flush);

            break;
        }

        case BLOCK2_UPDKL: {
            struct packedreq_updrrnkl updrrn_kless;
            const uint8_t *p_buf_tag_name;
            const uint8_t *p_buf_tag_name_end;
            uint8_t *p_buf_data;
            const uint8_t *p_buf_data_end;

            if (!(iq->p_buf_in =
                      packedreq_updrrnkl_get(&updrrn_kless, iq->p_buf_in,
                                             p_blkstate->p_buf_next_start))) {
                if (iq->debug)
                    reqprintf(iq, "PREFAULT UPDKL FAILED TO UNPACK");
                break;
            }

            loadnullbmp(nulls, sizeof(nulls), updrrn_kless.fldnullmap,
                        sizeof(updrrn_kless.fldnullmap));

            p_buf_tag_name = iq->p_buf_in;
            p_buf_tag_name_end = p_buf_tag_name + updrrn_kless.taglen;
            p_buf_data = (uint8_t *)p_buf_tag_name_end;
            p_buf_data_end = p_buf_data + updrrn_kless.rlen;

            rc = upd_record_prefault(
                iq, NULL, /*primary key*/
                updrrn_kless.rrn, updrrn_kless.genid, p_buf_tag_name,
                p_buf_tag_name_end, p_buf_data, p_buf_data_end, NULL,
                NULL, /*vrecord*/
                nulls, &genid, &err[numerrs].errcode, &err[numerrs].ixnum,
                ii, /*blkpos*/
                helper_thread, seqnum, RECFLAGS_DYNSCHEMA_NULLS_ONLY, flush);

            break;
        }

        case BLOCK2_DELKL: {
            struct packedreq_delkl delete_kless;

            if (!(iq->p_buf_in =
                      packedreq_delkl_get(&delete_kless, iq->p_buf_in,
                                          p_blkstate->p_buf_next_start))) {
                if (iq->debug)
                    reqprintf(iq, "PREFAULT FAILED TO UNPACK DELKL REQUEST");
                break;
            }

            rc = del_record_prefault(iq, NULL, /*primkey*/
                                     delete_kless.rrn, delete_kless.genid,
                                     &err[numerrs].errcode, &err[numerrs].ixnum,
                                     ii,               /*blkpos*/
                                     helper_thread, seqnum, 0, /* flags */
                                     flush);

            break;
        }

        case BLOCK_USE: {
            struct packedreq_use use;

            if (!(iq->p_buf_in = packedreq_use_get(
                      &use, iq->p_buf_in, p_blkstate->p_buf_next_start))) {
                if (iq->debug)
                    reqprintf(iq, "PREFAULT FAILED TO UNPACK USE REQUEST");
                break;
            }
            dbnum = use.dbnum;
            iq->usedb = getdbbynum(dbnum);
            if (iq->usedb == 0) {
                iq->usedb = iq->origdb;
                skipblock = 1;
            } else if (iq->debug)
                reqprintf(iq, "DB NUM %d '%s'", dbnum, iq->usedb->dbname);
            break;
        }

        case BLOCK2_USE: {
            struct packedreq_usekl usekl;

            if (!(iq->p_buf_in = packedreq_usekl_get(
                      &usekl, iq->p_buf_in, p_blkstate->p_buf_next_start)))

                if (usekl.taglen < 0 || usekl.taglen >= sizeof(tbltag)) {
                    if (iq->debug)
                        reqprintf(iq, "PREFAULT BLOCK2_USE INVALID TAG LEN\n");
                    break;
                }

            dbnum = usekl.dbnum;
            if (usekl.taglen) {
                bzero(tbltag, sizeof(tbltag));
                if (!(iq->p_buf_in =
                          buf_no_net_get(tbltag, usekl.taglen, iq->p_buf_in,
                                         p_blkstate->p_buf_next_start))) {
                    if (iq->debug)
                        reqprintf(iq,
                                  "PREFAULT BLOCK2_USE FAILED TO UNPACK TAG\n");
                    break;
                }
                iq->usedb = get_dbtable_by_name(tbltag);
                if (iq->usedb == NULL) {
                    iq->usedb = iq->origdb;
                    if (iq->debug)
                        reqprintf(iq, "ERROR DB NUM %d NOT"
                                      " IN TRANSACTION GROUP",
                                  dbnum);
                    skipblock = 1;
                } else if (iq->debug)
                    reqprintf(iq, "DB '%s'", iq->usedb->dbname);
            } else {
                iq->usedb = getdbbynum(dbnum);
                if (iq->usedb == 0) {
                    if (iq->debug)
                        reqprintf(iq, "ERROR DB NUM %d NOT IN "
                                      "TRANSACTION GROUP",
                                  dbnum);
                    iq->usedb = iq->origdb;
                    skipblock = 1;
                } else if (iq->debug)
                    reqprintf(iq, "DB NUM %d '%s'", dbnum, iq->usedb->dbname);
            }
            break;
        }
        }

        if (skipblock) {
            /* stop right here...some error happened in switch */
            return 0;
        }
    }

    return 0;
}
