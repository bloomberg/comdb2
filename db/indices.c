/*
   Copyright 2015, 2018, Bloomberg Finance L.P.

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


#include "indices.h"

#include "limit_fortify.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>

#include <plbitlib.h>
#include <alloca.h>
#include <memory_sync.h>

#include "comdb2.h"
#include <schemachange.h>
#include "tag.h"
#include "types.h"
#include "block_internal.h"
#include <assert.h>
#include "logmsg.h"
#include "views.h"

extern int gbl_partial_indexes;
extern int gbl_reorder_idx_writes;
extern __thread void *defered_index_tbl;

int add_record_indices(struct ireq *iq, void *trans, 
        blob_buffer_t *blobs, size_t maxblobs, int *opfailcode,
        int *ixfailnum, int *rrn, unsigned long long *genid,
        unsigned long long ins_keys, int opcode, int blkpos, 
        void *od_dta, size_t od_len,
        const char *ondisktag, struct schema *ondisktagsc)
{
    int retrc;
    char *od_dta_tail = NULL;
    int od_tail_len;
    void *cur = NULL;
    ctkey ctk = {0};
    bool reorder = gbl_reorder_idx_writes && iq->usedb->sc_from != iq->usedb;
    if (reorder) {
        cur = get_constraint_table_cursor(defered_index_tbl);
        if (cur == NULL) {
            logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
            return -1;
        }
        ctk.usedb = iq->usedb;
        ctk.type = CTK_ADD;
        ctk.genid = *genid;
    }
    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char *key = ctk.ixkey; // key points to chararray regardless reordering
        char mangled_key[MAXKEYLEN];

        if (gbl_use_plan && iq->usedb->plan &&
                iq->usedb->plan->ix_plan[ixnum] != -1)
            continue;

        /* only add keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
                !(ins_keys & (1ULL << ixnum)))
            continue;

        int ixkeylen = getkeysize(iq->usedb, ixnum);
        if (ixkeylen < 0) {
            if (iq->debug)
                reqprintf(iq, "BAD INDEX %d OR KEYLENGTH %d", ixnum, ixkeylen);
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_ADD_RC_INVL_KEY,
                    "bad index %d or keylength %d", ixnum, ixkeylen);
            *ixfailnum = ixnum;
            *opfailcode = OP_FAILED_BAD_REQUEST;
            retrc = ERR_BADREQ;
            return retrc;
        }

        int rc;
        if (iq->idxInsert)
            rc = create_key_from_ireq(iq, ixnum, 0, &od_dta_tail,
                    &od_tail_len, mangled_key, od_dta,
                    od_len, key);
        else {
            char ixtag[MAXTAGLEN];
            snprintf(ixtag, sizeof(ixtag), "%s_IX_%d", ondisktag, ixnum);
            rc = create_key_from_ondisk_sch_blobs(
                    iq->usedb, ondisktagsc, ixnum, &od_dta_tail, &od_tail_len,
                    mangled_key, ondisktag, od_dta, od_len, ixtag, key, NULL,
                    blobs, maxblobs, iq->tzname);
        }
        if (rc == -1) {
            if (iq->debug)
                reqprintf(iq, "CAN'T FORM INDEX %d", ixnum);
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_ADD_RC_INVL_IDX, "cannot form index %d",
                    ixnum);
            *ixfailnum = ixnum;
            *opfailcode = OP_FAILED_INTERNAL + ERR_FORM_KEY;
            retrc = rc;
            return retrc;
        }

        /* light the prefault kill bit for this subop - newkeys */
        prefault_kill_bits(iq, ixnum, PFRQ_NEWKEY);
        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step += 2;

        if (reorder) {
            //if not datacopy, no need to save od_dta_tail
            void *data = NULL;
            int datalen = 0;
            if (iq->usedb->ix_datacopy[ixnum] != 0) { //is datacopy
                data = od_dta_tail;
                datalen = od_tail_len;
            }
            ctk.ixnum = ixnum;
            int err = 0;
printf("AZ: inserttmptbl type %d, index %d, genid %llx\n", ctk.type, ctk.ixnum, ctk.genid);
            rc = bdb_temp_table_insert(thedb->bdb_env, cur, &ctk, sizeof(ctk),
                    data, datalen, &err);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n", __func__,
                        rc);
                return rc;
            }
            memset(ctk.ixkey, 0, ixkeylen);
        }
        else {
            /* add the key */
            rc = ix_addk(iq, trans, key, ixnum, *genid, *rrn, od_dta_tail,
                    od_tail_len, ix_isnullk(iq->usedb, key, ixnum));
printf("AZ: direct ix_addk genid=%llx rc %d\n", *genid, rc);
            if (iq->debug) {
                reqprintf(iq, "ix_addk IX %d LEN %u KEY ", ixnum, ixkeylen);
                reqdumphex(iq, key, ixkeylen);
                reqmoref(iq, " RC %d", rc);
            }

            if (rc == RC_INTERNAL_RETRY) {
                retrc = rc;
                return retrc;
            } else if (rc != 0) {
                retrc = rc;
                *ixfailnum = ixnum;
                /* If following changes, update OSQL_INSREC in osqlcomm.c */
                *opfailcode = OP_FAILED_UNIQ; /* really? */

                return retrc;
            }
        }
    }
    return 0;
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
        rc = insert_add_op(iq, p_buf_req_start,
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
printf("AZ: ix_addk genid=%llx rc %d\n", genid, rc);
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

int upd_record_indices(struct ireq *iq, void *trans,
        int *opfailcode, int *ixfailnum, int rrn, unsigned long long *newgenid,
        unsigned long long ins_keys, int opcode, int blkpos,
        void *od_dta, size_t od_len, void *old_dta,
        unsigned long long del_keys, int flags, blob_buffer_t *add_idx_blobs,
        blob_buffer_t *del_idx_blobs, int same_genid_with_upd,
        unsigned long long oldgenid, int *deferredAdd)
{
    char *od_dta_tail = NULL;
    int od_tail_len;
    char *od_olddta_tail = NULL;
    int od_oldtail_len;

    void *cur = NULL;
    ctkey delctk = {0};
    ctkey ctk = {0};
    bool reorder = gbl_reorder_idx_writes && iq->usedb->sc_from != iq->usedb;
    if (reorder) {
        cur = get_constraint_table_cursor(defered_index_tbl);
        if (cur == NULL) {
            logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
            return -1;
        }
        delctk.type = CTK_DEL;
        delctk.usedb = iq->usedb;
        ctk.usedb = iq->usedb;
    }

    int rc = 0;
    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        if (flags == RECFLAGS_UPGRADE_RECORD &&
            iq->usedb->ix_datacopy[ixnum] == 0)
            // skip non-datacopy indexes if it is a record upgrade
            continue;

        char keytag[MAXTAGLEN];
        char *oldkey = delctk.ixkey;
        char *newkey = ctk.ixkey;
        char mangled_oldkey[MAXKEYLEN];
        char mangled_newkey[MAXKEYLEN];

        /* index doesnt change */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(ins_keys & (1ULL << ixnum)) && !(del_keys & (1ULL << ixnum)))
            continue;

        int keysize = getkeysize(iq->usedb, ixnum);

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
            return ERR_CONVERT_IX;
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
            return ERR_CONVERT_IX;
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
              ixnum, *newgenid);*/

            gbl_upd_key++;
            if (reorder) {
                //if not datacopy, no need to save od_dta_tail
                void *data = NULL;
                int datalen = 0;
                if (iq->usedb->ix_datacopy[ixnum] != 0) { //is datacopy
                    data = od_dta_tail;
                    datalen = od_tail_len;
                }
                ctk.type = CTK_UPD;
                ctk.genid = oldgenid;
                ctk.newgenid = *newgenid;
                ctk.ixnum = ixnum;
                int err = 0;
printf("AZ: inserttmptbl type %d, index %d, genid %llx\n", ctk.type, ctk.ixnum, ctk.genid);
                rc = bdb_temp_table_insert(thedb->bdb_env, cur, &ctk, sizeof(ctk),
                        data, datalen, &err);
                if (rc != 0) {
                    logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n", __func__,
                            rc);
                    return rc;
                }
                memset(ctk.ixkey, 0, keysize);
            }
            else {
                rc = ix_upd_key(iq, trans, newkey, keysize,
                        ixnum, oldgenid, *newgenid, od_dta_tail, od_tail_len,
                        ix_isnullk(iq->usedb, newkey, ixnum));
                printf("AZ: direct ix_upd_key genid=%llx newwgenid=%llx rc %d\n", oldgenid, *newgenid, rc);
                if (iq->debug)
                    reqprintf(iq, "upd_key IX %d GENID 0x%016llx RC %d", ixnum,
                            *newgenid, rc);

                if (rc != 0) {
                    *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                    *ixfailnum = ixnum;
                    return rc;
                }
            }

            /* need to do this here as we're not adding the new key so we
               dont have the luxury of letting the constraint engine catch it
               later */
            verify_schema_change_constraint(iq, iq->usedb, trans, od_dta,
                                            ins_keys);
        } else { /* delete / add the key */
            /*
              fprintf(stderr, "IX %d changed, deleting key at genid 0x%016llx "
              "adding key at genid 0x%016llx\n",
              ixnum, oldgenid, *newgenid);
            */

            /* only delete keys when told */
            if (!gbl_partial_indexes || !iq->usedb->ix_partial ||
                (del_keys & (1ULL << ixnum))) {
                if (reorder) {
                    //if not datacopy, no need to save od_dta_tail
                    void *data = NULL;
                    int datalen = 0;
                    delctk.genid = oldgenid;
                    delctk.ixnum = ixnum;
                    int err = 0;
printf("AZ: inserttmptbl type %d, index %d, genid %llx\n", delctk.type, delctk.ixnum, delctk.genid);
                    rc = bdb_temp_table_insert(thedb->bdb_env, cur, &delctk, sizeof(delctk),
                            data, datalen, &err);
                    if (rc != 0) {
                        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n", __func__,
                                rc);
                        return rc;
                    }
                    memset(delctk.ixkey, 0, keysize);
                }
                else {
                    rc = ix_delk(iq, trans, oldkey, ixnum, rrn, oldgenid, ix_isnullk(iq->usedb, oldkey, ixnum));

printf("AZ: direct upd ix_delk genid=%llx newwgenid=%llx rc %d\n", oldgenid, *newgenid, rc);
                    if (iq->debug)
                        reqprintf(iq, "ix_delk IX %d RRN %d RC %d", ixnum, rrn, rc);

                    if (rc != 0) {
                        *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                        *ixfailnum = ixnum;
                        return rc;
                    }
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
            *deferredAdd |= (!do_inline);

            if (!gbl_partial_indexes || !iq->usedb->ix_partial ||
                (ins_keys & (1ULL << ixnum))) {
                if (reorder) {
                    //if not datacopy, no need to save od_dta_tail
                    void *data = NULL;
                    int datalen = 0;
                    if (iq->usedb->ix_datacopy[ixnum] != 0) { //is datacopy
                        data = od_dta_tail;
                        datalen = od_tail_len;
                    }
                    ctk.type = CTK_ADD;
                    ctk.genid = *newgenid;
                    ctk.ixnum = ixnum;
                    int err = 0;
printf("AZ: inserttmptbl type %d, index %d, genid %llx\n", ctk.type, ctk.ixnum, ctk.genid);
                    rc = bdb_temp_table_insert(thedb->bdb_env, cur, &ctk, sizeof(ctk),
                            data, datalen, &err);
                    if (rc != 0) {
                        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n", __func__,
                                rc);
                        return rc;
                    }
                    memset(ctk.ixkey, 0, keysize);
                }
                else {
                    rc = add_key(iq, trans, ixnum, ins_keys, rrn, *newgenid, od_dta,
                            od_len, opcode, blkpos, opfailcode, newkey,
                            od_dta_tail, od_tail_len, do_inline);

                    printf("AZ: direct upd add_key genid=%llx newwgenid=%llx rc %d\n", oldgenid, *newgenid, rc);
                    if (iq->debug)
                        reqprintf(iq, "add_key IX %d RRN %d RC %d", ixnum, rrn, rc);

                    if (rc != 0) {
                        *ixfailnum = ixnum;
                        return rc;
                    }
                }
            }
        }
    }
    return 0;
}

/* Form and delete all keys. */
int del_record_indices(struct ireq *iq, void *trans, int *opfailcode,
        int *ixfailnum, int rrn, unsigned long long genid, void *od_dta,
        unsigned long long del_keys, blob_buffer_t *del_idx_blobs,
        const char *ondisktag)
{
    int rc = 0;
    void *cur = NULL;
    ctkey delctk = {0};
    bool reorder = gbl_reorder_idx_writes && iq->usedb->sc_from != iq->usedb;
    if (reorder) {
        cur = get_constraint_table_cursor(defered_index_tbl);
        if (cur == NULL) {
            logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
            return -1;
        }
        delctk.type = CTK_DEL;
        delctk.usedb = iq->usedb;
    }

    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char keytag[MAXTAGLEN];
        char *key = delctk.ixkey;

        /* only delete keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
                !(del_keys & (1ULL << ixnum)))
            continue;

        int keysize = getkeysize(iq->usedb, ixnum);

        if (iq->idxDelete)
            memcpy(key, iq->idxDelete[ixnum], keysize);
        else {
            snprintf(keytag, sizeof(keytag), "%s_IX_%d", ondisktag, ixnum);
            rc = stag_to_stag_buf_blobs(
                    iq->usedb->tablename, ondisktag, od_dta, keytag, key,
                    NULL, del_idx_blobs, del_idx_blobs ? MAXBLOBS : 0, 0);
            if (rc == -1) {
                if (iq->debug)
                    reqprintf(iq, "CAN'T FORM INDEX %d", ixnum);
                reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
                reqerrstr(iq, COMDB2_DEL_RC_INVL_IDX, "cannot form index %d",
                        ixnum);
                *ixfailnum = ixnum;
                *opfailcode = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                return rc;
            }
        }

        /* handle the key special datacopy options */
        if (iq->usedb->ix_collattr[ixnum]) {
            /* handle key tails */
            rc = extract_decimal_quantum(iq->usedb, ixnum, key, NULL, 0, NULL);
            if (rc) {
                *ixfailnum = ixnum;
                *opfailcode = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                return rc;
            }
        }

        /* light the prefault kill bit for this subop - oldkeys */
        prefault_kill_bits(iq, ixnum, PFRQ_OLDKEY);
        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step += 2;

        if (reorder) {
            //if not datacopy, no need to save od_dta_tail
            void *data = NULL;
            int datalen = 0;
            delctk.genid = genid;
            delctk.ixnum = ixnum;
            int err = 0;
printf("AZ: inserttmptbl type %d, index %d, genid %llx\n", delctk.type, delctk.ixnum, delctk.genid);
            rc = bdb_temp_table_insert(thedb->bdb_env, cur, &delctk, sizeof(delctk),
                    data, datalen, &err);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n", __func__,
                        rc);
                return rc;
            }
            memset(delctk.ixkey, 0, keysize);
        }
        else {
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
                return rc;
            }
        }
    }
    return 0;
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

    void *cur = NULL;
    ctkey ctk = {0};
    bool reorder = gbl_reorder_idx_writes && iq->usedb->sc_from != iq->usedb;
    if (reorder) {
        cur = get_constraint_table_cursor(defered_index_tbl);
        if (cur == NULL) {
            logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
            return -1;
        }
        ctk.usedb = iq->usedb;
        ctk.type = CTK_ADD;
        ctk.genid = newgenid;
    }

    /* Add all keys */
    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char keytag[MAXTAGLEN];
        char *key = ctk.ixkey; // key points to chararray regardless reordering
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

        if (reorder) {
            //if not datacopy, no need to save od_dta_tail
            void *data = NULL;
            int datalen = 0;
            if (iq->usedb->ix_datacopy[ixnum] != 0) { //is datacopy
                data = od_dta_tail;
                datalen = od_tail_len;
            }
            ctk.ixnum = ixnum;
            int err = 0;
printf("AZ: inserttmptbl type %d, index %d, genid %llx\n", ctk.type, ctk.ixnum, ctk.genid);
            rc = bdb_temp_table_insert(thedb->bdb_env, cur, &ctk, sizeof(ctk),
                    data, datalen, &err);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n", __func__,
                        rc);
                return rc;
            }
            int ixkeylen = getkeysize(iq->usedb, ixnum);
            memset(ctk.ixkey, 0, ixkeylen);
        }
        else {
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
    }

    return rc;
}

int upd_new_record_indices( struct ireq *iq, void *trans,
        unsigned long long newgenid,
        unsigned long long ins_keys,
        const void *new_dta, const void *old_dta, int use_new_tag,
        void *sc_old, void *sc_new, int nd_len,
        unsigned long long del_keys, blob_buffer_t *add_idx_blobs,
        blob_buffer_t *del_idx_blobs,
        unsigned long long oldgenid, int deferredAdd)
{    
    void *cur = NULL;
    ctkey delctk = {0};
    bool reorder = gbl_reorder_idx_writes && iq->usedb->sc_from != iq->usedb;
    if (reorder) {
        cur = get_constraint_table_cursor(defered_index_tbl);
        if (cur == NULL) {
            logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
            return -1;
        }
        delctk.type = CTK_DEL;
        delctk.usedb = iq->usedb;
    }

    int rc;
    /* First delete all keys */
    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char keytag[MAXTAGLEN];
        char *oldkey = delctk.ixkey;
        char mangled_key[MAXKEYLEN];
        char *od_dta_tail = NULL;

        if (gbl_use_plan && iq->usedb->plan &&
            iq->usedb->plan->ix_plan[ixnum] != -1)
            continue;

        /* only delete keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(del_keys & (1ULL << ixnum)))
            continue;

        snprintf(keytag, sizeof(keytag), ".NEW..ONDISK_IX_%d", ixnum);

        int keysize = iq->usedb->ix_keylen[ixnum];
        if (iq->idxDelete) {
            memcpy(oldkey, iq->idxDelete[ixnum], keysize);
            rc = 0;
        } else
            rc = create_key_from_ondisk_blobs(
                iq->usedb, ixnum, NULL, NULL, NULL,
                use_new_tag ? ".NEW..ONDISK" : ".ONDISK",
                use_new_tag ? (char *)sc_old : (char *)old_dta,
                0 /*not needed*/, keytag, oldkey, NULL, del_idx_blobs,
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
            return rc;
        }

        if (reorder) {
            //if not datacopy, no need to save od_dta_tail
            void *data = NULL;
            int datalen = 0;
            delctk.genid = oldgenid;
            delctk.ixnum = ixnum;
            int err = 0;
            printf("AZ: inserttmptbl type %d, index %d, genid %llx\n", delctk.type, delctk.ixnum, delctk.genid);
            rc = bdb_temp_table_insert(thedb->bdb_env, cur, &delctk, sizeof(delctk),
                    data, datalen, &err);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n", __func__,
                        rc);
                return rc;
            }
            memset(delctk.ixkey, 0, keysize);
        }
        else {
            rc = ix_delk(iq, trans, oldkey, ixnum, 2 /*rrn*/, oldgenid, ix_isnullk(iq->usedb, oldkey, ixnum));
            if (iq->debug) {
                reqprintf(iq, "ix_delk IX %d KEY ", ixnum);
                reqdumphex(iq, oldkey, getkeysize(iq->usedb, ixnum));
                reqmoref(iq, " RC %d", rc);
            }

            /* remap delete not found to retry */
            if (rc == IX_NOTFND)
                rc = RC_INTERNAL_RETRY;

            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "upd_new_record oldgenid 0x%llx ix_delk -> "
                        "ix%d, rc=%d failed\n",
                        oldgenid, ixnum, rc);
                return rc;
            }
        }
    }

    /* Add keys if we are not deferring.
     * If we are deferring, add will be called from delayed_key_adds() */
    if (!deferredAdd) {
        return upd_new_record_add2indices(
            iq, trans, newgenid, use_new_tag ? sc_new : new_dta,
            use_new_tag ? iq->usedb->lrl : nd_len, ins_keys, use_new_tag,
            add_idx_blobs);
    } else
        reqprintf(iq, "is deferredAdd so will add to indices at the end");

    return 0;
}

//type: DEL = 0, ADD = 2
int insert_defered_tbl(struct ireq *iq, void *od_dta, size_t od_len,
                  const char *ondisktag, struct schema *ondisktagsc,
                  unsigned long long genid, int type)
{
    void *cur = get_constraint_table_cursor(defered_index_tbl);
    if (cur == NULL) {
        logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
        return -1;
    }
    //insert record by tbl,ixnum,ixkey, payload genid
    ctkey ctk = {0};
    ctk.usedb = iq->usedb;
    ctk.type = type;
    ctk.genid = genid;
    
    for(int i = 0; i < iq->usedb->nix; i++) {
printf("AZ: inserting for index %d\n", i);
        ctk.ixnum = i;
        char *key = ctk.ixkey;
        char *od_dta_tail = NULL;
        int od_tail_len;
        int rc;
        char mangled_key[MAXKEYLEN];
        int err = 0;
        memset(key, 0, MAXKEYLEN); 

        if (iq->idxInsert)
                rc = create_key_from_ireq(iq, i, 0, &od_dta_tail,
                                          &od_tail_len, mangled_key, od_dta,
                                          od_len, key);
        else {
            char ixtag[MAXTAGLEN];
            snprintf(ixtag, sizeof(ixtag), "%s_IX_%d", ondisktag, i);
            rc = create_key_from_ondisk_sch_blobs(
                iq->usedb, ondisktagsc, i, &od_dta_tail, &od_tail_len,
                mangled_key, ondisktag, od_dta, od_len, ixtag, key, NULL,
                NULL, 0, iq->tzname);
        }

        //if not datacopy, no need to save od_dta_tail
        rc = bdb_temp_table_insert(thedb->bdb_env, cur, &ctk, sizeof(ctk),
            od_dta_tail, od_tail_len, &err);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n", __func__,
                    rc);
            return rc;
        }
    }
    return 0;
}

int process_defered_table(struct ireq *iq, block_state_t *blkstate, void *trans,
                     int *blkpos, int *ixout, int *errout)
{
    void *cur = get_constraint_table_cursor(defered_index_tbl);
    if (cur == NULL) {
        if (iq->debug)
            reqprintf(iq, "%p:VERKYCNSTRT CANNOT GET ADD LIST CURSOR", trans);
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_CURSOR,
                  "verify key constraint cannot get add list cursor");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }

    int err;
    int rc = bdb_temp_table_first(thedb->bdb_env, cur, &err);
    if (rc != IX_OK) {
        //free_cached_delayed_indexes(iq);
        if (rc == IX_EMPTY) {
            if (iq->debug)
                reqprintf(iq, "%p:VERKYCNSTRT FOUND NO KEYS TO ADD", trans);
            rc = 0;
            goto done;
        }
        if (iq->debug)
            reqprintf(iq, "%p:VERKYCNSTRT CANNOT GET ADD LIST RECORD", trans);
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_REC,
                  "verify key constraint: cannot get add list record");
        *errout = OP_FAILED_INTERNAL;
        goto done;
    }
    while (rc == IX_OK) {
        ctkey *ctk = (ctkey *)bdb_temp_table_key(cur);
printf("AZ: working with type %d, index %d, genid %llx\n", ctk->type, ctk->ixnum, ctk->genid);
        void *od_dta_tail = bdb_temp_table_data(cur);
        int od_tail_len = bdb_temp_table_datasize(cur);
        int addrrn = 2;

        iq->usedb = ctk->usedb;

        if (ctk->type == CTK_ADD) {
            /* add the key */
            rc = ix_addk(iq, trans, ctk->ixkey, ctk->ixnum, ctk->genid, addrrn, od_dta_tail,
                    od_tail_len, ix_isnullk(ctk->usedb, ctk->ixkey, ctk->ixnum));
printf("AZ: pdt ix_addk genid=%llx rc %d\n", ctk->genid, rc);

            if (iq->debug) {
                reqprintf(iq, "%p:ADDKYCNSTRT  TBL %s IX %d RRN %d KEY ", trans,
                        ctk->usedb->tablename, ctk->ixnum, addrrn);
                int ixkeylen = getkeysize(ctk->usedb, ctk->ixnum);
                reqdumphex(iq, ctk->ixkey, ixkeylen);
                reqmoref(iq, " RC %d", rc);
            }

            if (rc == IX_DUP) {
                reqerrstr(iq, COMDB2_CSTRT_RC_DUP, "add key constraint "
                        "duplicate key '%s' on "
                        "table '%s' index %d",
                        get_keynm_from_db_idx(ctk->usedb, ctk->ixnum),
                        ctk->usedb->tablename, ctk->ixnum);

                //*blkpos = curop->blkpos;
                *errout = OP_FAILED_UNIQ;
                *ixout = ctk->ixnum;
                goto done;
            } else if (rc != 0) {
                reqerrstr(iq, COMDB2_CSTRT_RC_INTL_ERR,
                        "add key berkley error for key '%s' on index %d",
                        get_keynm_from_db_idx(ctk->usedb, ctk->ixnum), ctk->ixnum);

                //*blkpos = curop->blkpos;

                *errout = OP_FAILED_INTERNAL;
                *ixout = ctk->ixnum;

                if (ERR_INTERNAL == rc) {
                    /* Exit & have the cluster elect another master */
                    if (gbl_exit_on_internal_error) {
                        exit(1);
                    }

                    rc = ERR_NOMASTER;
                }
                goto done;
            }
        }
        else if (ctk->type == CTK_DEL) {
            rc = ix_delk(iq, trans, ctk->ixkey, ctk->ixnum, addrrn, ctk->genid, ix_isnullk(ctk->usedb, ctk->ixkey, ctk->ixnum));
            if (iq->debug) {
                reqprintf(iq, "ix_delk IX %d KEY ", ctk->ixnum);
                reqdumphex(iq, ctk->ixkey, getkeysize(ctk->usedb, ctk->ixnum));
                reqmoref(iq, " RC %d", rc);
            }
            if (rc != 0) {
                if (rc == IX_NOTFND) {
                    reqerrstrhdr(iq, "Table '%s' ", ctk->usedb->tablename);
                    reqerrstr(iq, COMDB2_DEL_RC_INVL_KEY,
                            "key not found on index %d", ctk->ixnum);
                }
                *errout = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                *ixout = ctk->ixnum;
                goto done;
            }

        }
        else {
            if(ctk->type != CTK_UPD)
                abort();

            rc = ix_upd_key(iq, trans, ctk->ixkey, ctk->usedb->ix_keylen[ctk->ixnum],
                            ctk->ixnum, ctk->genid, ctk->newgenid, od_dta_tail, od_tail_len,
                            ix_isnullk(ctk->usedb, ctk->ixkey, ctk->ixnum));
            if (iq->debug)
                reqprintf(iq, "upd_key IX %d GENID 0x%016llx RC %d", ctk->ixnum,
                          ctk->newgenid, rc);

            if (rc != 0) {
                *errout = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                *ixout = ctk->ixnum;
                goto done;
            }
        }

        /* get next record from table */
        rc = bdb_temp_table_next(thedb->bdb_env, cur, &err);
    }
    if (rc == IX_PASTEOF) rc = IX_OK;

done:
    close_constraint_table_cursor(cur);
    return rc;
}



