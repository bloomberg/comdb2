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

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>
#include "comdb2.h"
#include <schemachange.h>
#include "block_internal.h"
#include <assert.h>
#include "logmsg.h"
#include "indices.h"

extern int gbl_partial_indexes;

/* Check whether the key for the specified record is already present in
 * the index.
 *
 * @return 1 : yes/error
 *         0 : no
 */
static int check_index(struct ireq *iq, void *trans, int ixnum,
                       struct schema *ondisktagsc, blob_buffer_t *blobs,
                       size_t maxblobs, int *opfailcode, int *ixfailnum,
                       int *retrc, const char *ondisktag, void *od_dta,
                       size_t od_len, unsigned long long ins_keys)
{
    int ixkeylen;
    int rc;
    char ixtag[MAXTAGLEN];
    char key[MAXKEYLEN];
    char mangled_key[MAXKEYLEN];
    char *od_dta_tail = NULL;
    int od_tail_len;
    int fndrrn = 0;
    unsigned long long fndgenid = 0LL;

    ixkeylen = getkeysize(iq->usedb, ixnum);
    if (ixkeylen < 0) {
        if (iq->debug)
            reqprintf(iq, "BAD INDEX %d OR KEYLENGTH %d", ixnum, ixkeylen);
        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_ADD_RC_INVL_KEY, "bad index %d or keylength %d",
                  ixnum, ixkeylen);
        *ixfailnum = ixnum;
        *opfailcode = OP_FAILED_BAD_REQUEST;
        *retrc = ERR_BADREQ;
        return 1;
    }

    snprintf(ixtag, sizeof(ixtag), "%s_IX_%d", ondisktag, ixnum);

    if (iq->idxInsert)
        rc = create_key_from_ireq(iq, ixnum, 0, &od_dta_tail, &od_tail_len,
                                  mangled_key, od_dta, od_len, key);
    else
        rc = create_key_from_ondisk_sch_blobs(
            iq->usedb, ondisktagsc, ixnum, &od_dta_tail, &od_tail_len,
            mangled_key, ondisktag, od_dta, od_len, ixtag, key, NULL, blobs,
            maxblobs, iq->tzname);
    if (rc == -1) {
        if (iq->debug)
            reqprintf(iq, "CAN'T FORM INDEX %d", ixnum);
        reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
        reqerrstr(iq, COMDB2_ADD_RC_INVL_IDX, "cannot form index %d", ixnum);
        *ixfailnum = ixnum;
        *opfailcode = OP_FAILED_INTERNAL + ERR_FORM_KEY;
        *retrc = rc;
        return 1;
    }

    if (ix_isnullk(iq->usedb, key, ixnum)) {
        return 0;
    }

    rc = ix_find_by_key_tran(iq, key, ixkeylen, ixnum, key, &fndrrn, &fndgenid,
                             NULL, NULL, 0, trans);
    if (rc == IX_FND) {
        *ixfailnum = ixnum;
        /* If following changes, update OSQL_INSREC in osqlcomm.c */
        *opfailcode = OP_FAILED_UNIQ; /* really? */
        *retrc = IX_DUP;
        return 1;
    } else if (rc == RC_INTERNAL_RETRY) {
        *retrc = RC_INTERNAL_RETRY;
        return 1;
    } else if (rc != IX_FNDMORE && rc != IX_NOTFND && rc != IX_PASTEOF &&
               rc != IX_EMPTY) {
        *retrc = ERR_INTERNAL;
        logmsg(LOGMSG_ERROR, "%s:%d got unexpected error rc = %d\n", __func__,
               __LINE__, rc);
        return 1;
    }
    return 0;
}

/* If a specific index has been used in the ON CONFLICT clause (aka
 * upsert target/index), then we must move the check for that particular
 * index to the very end so that errors from other (non-ignorable)
 * unique indexes have already been verified before we check and ignore
 * the error (if any) from the upsert index.
 */
int check_for_upsert(struct ireq *iq, void *trans, struct schema *ondisktagsc,
                     blob_buffer_t *blobs, size_t maxblobs, int *opfailcode,
                     int *ixfailnum, int *retrc, const char *ondisktag,
                     void *od_dta, size_t od_len, unsigned long long ins_keys,
                     int rec_flags)
{
    int rc = 0;
    int upsert_idx = rec_flags >> 8;

    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        /* Skip check for upsert index, we'll do it after this loop. */
        if (ixnum == upsert_idx) {
            continue;
        }

        /* Ignore dup keys */
        if (iq->usedb->ix_dupes[ixnum] != 0) {
            continue;
        }

        /* Check for partial keys only when needed. */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(ins_keys & (1ULL << ixnum))) {
            continue;
        }

        rc = check_index(iq, trans, ixnum, ondisktagsc, blobs, maxblobs,
                         opfailcode, ixfailnum, retrc, ondisktag, od_dta,
                         od_len, ins_keys);
        if (rc) {
            return rc;
        }
    }

    /* Perform the check for upsert index that we skipped above. */
    if (upsert_idx != MAXINDEX + 1) {

        /* It must be a unique key. */
        assert(iq->usedb->ix_dupes[upsert_idx] == 0);

        /* Check for partial keys only when needed. */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(ins_keys & (1ULL << upsert_idx))) {
            /* NOOP */
        } else {
            rc = check_index(iq, trans, upsert_idx, ondisktagsc, blobs,
                             maxblobs, opfailcode, ixfailnum, retrc, ondisktag,
                             od_dta, od_len, ins_keys);
            if (rc) {
                return rc;
            }
        }
    }
    return 0;
}

int add_record_indices(struct ireq *iq, void *trans, blob_buffer_t *blobs,
                       size_t maxblobs, int *opfailcode, int *ixfailnum,
                       int *rrn, unsigned long long *genid,
                       unsigned long long vgenid, unsigned long long ins_keys,
                       int opcode, int blkpos, void *od_dta, size_t od_len,
                       const char *ondisktag, struct schema *ondisktagsc)
{
    char *od_dta_tail = NULL;
    int od_tail_len;
    if (iq->osql_step_ix)
        gbl_osqlpf_step[*(iq->osql_step_ix)].step += 1;
    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char key[MAXKEYLEN];
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
            return ERR_BADREQ;
        }

        int rc;
        if (iq->idxInsert)
            rc = create_key_from_ireq(iq, ixnum, 0, &od_dta_tail, &od_tail_len,
                                      mangled_key, od_dta, od_len, key);
        else {
            char ixtag[MAXTAGLEN];
            snprintf(ixtag, sizeof(ixtag), "%s_IX_%d", ondisktag, ixnum);
            rc = create_key_from_ondisk_sch_blobs(
                iq->usedb, ondisktagsc, ixnum, &od_dta_tail, &od_tail_len,
                mangled_key, ondisktag, od_dta, od_len, ixtag, key, NULL, blobs,
                maxblobs, iq->tzname);
        }
        if (rc == -1) {
            if (iq->debug)
                reqprintf(iq, "CAN'T FORM INDEX %d", ixnum);
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_ADD_RC_INVL_IDX, "cannot form index %d",
                      ixnum);
            *ixfailnum = ixnum;
            *opfailcode = OP_FAILED_INTERNAL + ERR_FORM_KEY;
            return rc;
        }

        /* light the prefault kill bit for this subop - newkeys */
        prefault_kill_bits(iq, ixnum, PFRQ_NEWKEY);
        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step += 2;

        int isnullk = ix_isnullk(iq->usedb, key, ixnum);

        if (vgenid && iq->usedb->ix_dupes[ixnum] == 0 && !isnullk) {
            int fndrrn = 0;
            unsigned long long fndgenid = 0ULL;
            rc = ix_find_by_key_tran(iq, key, ixkeylen, ixnum, NULL, &fndrrn,
                                     &fndgenid, NULL, NULL, 0, trans);
            if (rc == IX_FND && fndgenid == vgenid) {
                return ERR_VERIFY;
            } else if (rc == RC_INTERNAL_RETRY) {
                return RC_INTERNAL_RETRY;
            } else if (rc != IX_FNDMORE && rc != IX_NOTFND &&
                       rc != IX_PASTEOF && rc != IX_EMPTY) {
                logmsg(LOGMSG_ERROR, "%s:%d got unexpected error rc = %d\n",
                       __func__, __LINE__, rc);
                return ERR_INTERNAL;
            }

            /* The row is not in new btree, proceed with the add */
            vgenid = 0; // no need to verify again
        }

        /* add the key */
        rc = ix_addk(iq, trans, key, ixnum, *genid, *rrn, od_dta_tail,
                     od_tail_len, isnullk);

        if (vgenid && rc == IX_DUP) {
            if (iq->usedb->ix_dupes[ixnum] || isnullk) {
                return ERR_VERIFY;
            }
        }

        if (iq->debug) {
            reqprintf(iq, "ix_addk IX %d LEN %u KEY ", ixnum, ixkeylen);
            reqdumphex(iq, key, ixkeylen);
            reqmoref(iq, " RC %d", rc);
        }

        if (rc == RC_INTERNAL_RETRY) {
            return rc;
        } else if (rc != 0) {
            *ixfailnum = ixnum;
            /* If following changes, update OSQL_INSREC in osqlcomm.c */
            *opfailcode = OP_FAILED_UNIQ; /* really? */

            return rc;
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
                   char *od_dta_tail, int od_tail_len, int do_inline,
                   int rec_flags)
{
    int rc;

    if (!do_inline) {
        if ((iq->usedb->ix_disabled[ixnum] & INDEX_WRITE_DISABLED)) {
            if (iq->debug)
                reqprintf(iq, "%s: ix %d write disabled", __func__, ixnum);
            return 0;
        }
        rc = insert_add_op(iq, opcode, rrn, ixnum, genid, ins_keys, blkpos, 0);
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

int upd_record_indices(struct ireq *iq, void *trans, int *opfailcode,
                       int *ixfailnum, int rrn, unsigned long long *newgenid,
                       unsigned long long ins_keys, int opcode, int blkpos,
                       void *od_dta, size_t od_len, void *old_dta,
                       unsigned long long del_keys, int flags,
                       blob_buffer_t *add_idx_blobs,
                       blob_buffer_t *del_idx_blobs, int same_genid_with_upd,
                       unsigned long long vgenid, int *deferredAdd)
{
    char *od_dta_tail = NULL;
    int od_tail_len;
    char *od_olddta_tail = NULL;
    int od_oldtail_len;

    /* Delay key add if schema change has constraints so we can * verify them.
     * FIXME: What if the table does not have index to begin with?
     * (Redo based live sc works for this case)
     */
    int live_sc_delay = live_sc_delay_key_add(iq);

    int rc = 0;
    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        if (flags == RECFLAGS_UPGRADE_RECORD &&
            iq->usedb->ix_datacopy[ixnum] == 0)
            // skip non-datacopy indexes if it is a record upgrade
            continue;

        char keytag[MAXTAGLEN];
        char oldkey[MAXKEYLEN];
        char newkey[MAXKEYLEN];
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
          - *NOTE* the above is no longer always true if the 'uniqnulls'
            option is enabled for the key.  in that case, in place key update
            cannot be done if any key component is actually NULL.
          - if the key allows dups (has a genid on the right side of the key)
            then we can only do the in place update if the genid (minus the
            updateid portion) didnt change, ie if an in place dta update
            happened here. */
        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step += 1;

        int key_unique = (iq->usedb->ix_dupes[ixnum] == 0);
        int same_key = (memcmp(newkey, oldkey, keysize) == 0);
        if (!live_sc_delay && gbl_key_updates &&
            ((key_unique && !ix_isnullk(iq->usedb, newkey, ixnum)) ||
             same_genid_with_upd) &&
            same_key &&
            (!gbl_partial_indexes || !iq->usedb->ix_partial ||
             ((ins_keys & (1ULL << ixnum)) &&
              (del_keys & (1ULL << ixnum))))) { /* in place key update */

            /*logmsg(LOGMSG_DEBUG, "IX %d didnt change, poking genid 0x%016llx\n",
              ixnum, *genid);*/

            gbl_upd_key++;

            rc = ix_upd_key(iq, trans, newkey, keysize, ixnum, vgenid,
                            *newgenid, od_dta_tail, od_tail_len,
                            ix_isnullk(iq->usedb, newkey, ixnum));
            if (iq->debug)
                reqprintf(iq, "upd_key IX %d GENID 0x%016llx RC %d", ixnum,
                          *newgenid, rc);

            if (rc != 0) {
                *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                *ixfailnum = ixnum;
                return rc;
            }
        } else /* delete / add the key */
        {
            /*
              logmsg(LOGMSG_DEBUG, "IX %d changed, deleting key at genid 0x%016llx "
              "adding key at genid 0x%016llx\n",
              ixnum, vgenid, *newgenid);
            */

            /* only delete keys when told */
            if (!gbl_partial_indexes || !iq->usedb->ix_partial ||
                (del_keys & (1ULL << ixnum))) {
                rc = ix_delk(iq, trans, oldkey, ixnum, rrn, vgenid,
                             ix_isnullk(iq->usedb, oldkey, ixnum));

                if (iq->debug)
                    reqprintf(iq, "ix_delk IX %d RRN %d RC %d", ixnum, rrn, rc);

                if (rc != 0) {
                    *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                    *ixfailnum = ixnum;
                    return rc;
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
            if (live_sc_delay)
                do_inline = 0;
            *deferredAdd |= (!do_inline);

            if (!gbl_partial_indexes || !iq->usedb->ix_partial ||
                (ins_keys & (1ULL << ixnum))) {
                rc = add_key(iq, trans, ixnum, ins_keys, rrn, *newgenid, od_dta,
                             od_len, opcode, blkpos, opfailcode, newkey,
                             od_dta_tail, od_tail_len, do_inline, 0);

                if (iq->debug)
                    reqprintf(iq, "add_key IX %d RRN %d RC %d", ixnum, rrn, rc);

                if (rc != 0) {
                    *ixfailnum = ixnum;
                    return rc;
                }
            }
        }
    }
    return 0;
}

/* Form and delete all keys. */
int del_record_indices(struct ireq *iq, void *trans, int *opfailcode,
                       int *ixfailnum, int rrn, unsigned long long genid,
                       void *od_dta, unsigned long long del_keys,
                       blob_buffer_t *del_idx_blobs, const char *ondisktag)
{
    int rc = 0;
    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char keytag[MAXTAGLEN];
        char key[MAXKEYLEN];

        /* only delete keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(del_keys & (1ULL << ixnum)))
            continue;

        int keysize = getkeysize(iq->usedb, ixnum);

        if (iq->idxDelete)
            memcpy(key, iq->idxDelete[ixnum], keysize);
        else {
            snprintf(keytag, sizeof(keytag), "%s_IX_%d", ondisktag, ixnum);
            rc = stag_to_stag_buf_blobs(iq->usedb->tablename, ondisktag, od_dta,
                                        keytag, key, NULL, del_idx_blobs,
                                        del_idx_blobs ? MAXBLOBS : 0, 0);
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

        /* delete the key */
        rc = ix_delk(iq, trans, key, ixnum, rrn, genid,
                     ix_isnullk(iq->usedb, key, ixnum));
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
    return 0;
}

// in sc_schema.h
int verify_record_constraint(struct ireq *iq, struct dbtable *db, void *trans,
                             const void *old_dta, unsigned long long ins_keys,
                             blob_buffer_t *blobs, int maxblobs,
                             const char *from, int rebuild, int convert);
/*
 * Update a single record in the new table as part of a live schema
 * change.  This code is called to add to indices only, adding to
 * the dta files should be already done earlier.
 */
int upd_new_record_add2indices(struct ireq *iq, void *trans,
                               unsigned long long newgenid, const void *new_dta,
                               int nd_len, unsigned long long ins_keys,
                               int use_new_tag, blob_buffer_t *blobs,
                               int verify)
{
    int rc = 0;
#ifdef DEBUG
    logmsg(LOGMSG_DEBUG, "upd_new_record_add2indices: genid %llx\n", newgenid);
#endif

    if (!iq->usedb)
        return ERR_BADREQ;

    if (verify) {
        int rebuild = iq->usedb->plan && iq->usedb->plan->dta_plan;
        rc = verify_record_constraint(
            iq, iq->usedb, trans, new_dta, ins_keys, blobs, MAXBLOBS,
            use_new_tag ? ".NEW..ONDISK" : ".ONDISK", rebuild, !use_new_tag);
        if (rc) {
            int bdberr = 0;
            struct dbtable *to = iq->usedb;
            struct dbtable *from = to->sc_from;
            assert(from != NULL);
            iq->usedb = from;
            rc = ix_check_update_genid(iq, trans, newgenid, &bdberr);
            iq->usedb = to;
            if (rc == 1 && bdberr == IX_FND)
                return ERR_CONSTR;
            if (bdberr == RC_INTERNAL_RETRY)
                return RC_INTERNAL_RETRY;
            logmsg(LOGMSG_DEBUG, "%s: ignores constraints for genid %llx\n",
                   __func__, newgenid);
        }
    }

    unsigned long long vgenid = 0ULL;
    if (verify)
        vgenid = newgenid;

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

        int isnullk = ix_isnullk(iq->usedb, key, ixnum);

        if (vgenid && iq->usedb->ix_dupes[ixnum] == 0 && !isnullk) {
            int fndrrn = 0;
            unsigned long long fndgenid = 0ULL;
            rc = ix_find_by_key_tran(iq, key, getkeysize(iq->usedb, ixnum),
                                     ixnum, NULL, &fndrrn, &fndgenid, NULL,
                                     NULL, 0, trans);
            if (rc == IX_FND && fndgenid == vgenid) {
                return ERR_VERIFY;
            } else if (rc == RC_INTERNAL_RETRY) {
                return RC_INTERNAL_RETRY;
            } else if (rc != IX_FNDMORE && rc != IX_NOTFND &&
                       rc != IX_PASTEOF && rc != IX_EMPTY) {
                logmsg(LOGMSG_ERROR, "%s:%d got unexpected error rc = %d\n",
                       __func__, __LINE__, rc);
                return ERR_INTERNAL;
            }

            /* The row is not in new btree, proceed with the add */
            vgenid = 0; // no need to verify again
        }

        rc = ix_addk(iq, trans, key, ixnum, newgenid, 2, (void *)od_dta_tail,
                     od_tail_len, ix_isnullk(iq->usedb, key, ixnum));

        if (vgenid && rc == IX_DUP) {
            if (iq->usedb->ix_dupes[ixnum] || isnullk) {
                return ERR_VERIFY;
            }
        }

        if (iq->debug) {
            reqprintf(iq, "ix_addk IX %d KEY ", ixnum);
            reqdumphex(iq, key, getkeysize(iq->usedb, ixnum));
            reqmoref(iq, " RC %d", rc);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "upd_new_record_add2indices: ix_addk "
                   "newgenid 0x%llx ix_addk  ix%d rc=%d\n",
                   newgenid, ixnum, rc);
            fsnapf(stderr, key, getkeysize(iq->usedb, ixnum));
            break;
        }
    }

    return rc;
}

int upd_new_record_indices(
    struct ireq *iq, void *trans, unsigned long long newgenid,
    unsigned long long ins_keys, const void *new_dta, const void *old_dta,
    int use_new_tag, void *sc_old, void *sc_new, int nd_len,
    unsigned long long del_keys, blob_buffer_t *add_idx_blobs,
    blob_buffer_t *del_idx_blobs, unsigned long long oldgenid, int verify_retry,
    int deferredAdd)
{
    int rc = 0;
    /* First delete all keys */
    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char keytag[MAXTAGLEN];
        char key[MAXKEYLEN];

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
            memcpy(key, iq->idxDelete[ixnum], keysize);
            rc = 0;
        } else
            rc = create_key_from_ondisk_blobs(
                iq->usedb, ixnum, NULL, NULL, NULL,
                use_new_tag ? ".NEW..ONDISK" : ".ONDISK",
                use_new_tag ? (char *)sc_old : (char *)old_dta,
                0 /*not needed*/, keytag, key, NULL, del_idx_blobs,
                del_idx_blobs ? MAXBLOBS : 0, NULL);
        if (rc == -1) {
            logmsg(LOGMSG_ERROR,
                   "upd_new_record oldgenid 0x%llx conversions -> "
                   "ix%d failed\n",
                   oldgenid, ixnum);
            if (iq->debug)
                reqprintf(iq, "CAN'T FORM OLD INDEX %d", ixnum);
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_DEL_RC_INVL_IDX, "cannot form old index %d",
                      ixnum);
            return rc;
        }

        rc = ix_delk(iq, trans, key, ixnum, 2 /*rrn*/, oldgenid,
                     ix_isnullk(iq->usedb, key, ixnum));
        if (iq->debug) {
            reqprintf(iq, "ix_delk IX %d KEY ", ixnum);
            reqdumphex(iq, key, getkeysize(iq->usedb, ixnum));
            reqmoref(iq, " RC %d", rc);
        }

        /* remap delete not found to retry */
        if (rc == IX_NOTFND) {
            if (verify_retry)
                rc = RC_INTERNAL_RETRY;
            else
                rc = ERR_VERIFY;
        }

        if (rc != 0) {
            if (rc != ERR_VERIFY)
                logmsg(LOGMSG_ERROR,
                       "upd_new_record oldgenid 0x%llx ix_delk -> "
                       "ix%d, rc=%d failed\n",
                       oldgenid, ixnum, rc);
            return rc;
        }
    }

    /* Add keys if we are not deferring.
     * If we are deferring, add will be called from delayed_key_adds() */
    if (!deferredAdd) {
        rc = upd_new_record_add2indices(
            iq, trans, newgenid, use_new_tag ? sc_new : new_dta,
            use_new_tag ? iq->usedb->lrl : nd_len, ins_keys, use_new_tag,
            add_idx_blobs, !verify_retry);
    } else
        reqprintf(iq, "is deferredAdd so will add to indices at the end");

    return rc;
}

int del_new_record_indices(struct ireq *iq, void *trans,
                           unsigned long long ngenid, const void *old_dta,
                           int use_new_tag, void *sc_old,
                           unsigned long long del_keys,
                           blob_buffer_t *del_idx_blobs, int verify_retry)
{
    int rc = 0;
    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char keytag[MAXTAGLEN];
        char key[MAXKEYLEN];

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
                   "del_new_record ngenid 0x%llx conversion -> ix%d failed\n",
                   ngenid, ixnum);
            if (iq->debug)
                reqprintf(iq, "CAN'T FORM INDEX %d", ixnum);
            reqerrstrhdr(iq, "Table '%s' ", iq->usedb->tablename);
            reqerrstr(iq, COMDB2_DEL_RC_INVL_IDX, "cannot form index %d",
                      ixnum);
            return rc;
        }

        rc = ix_delk(iq, trans, key, ixnum, 2 /*rrn*/, ngenid,
                     ix_isnullk(iq->usedb, key, ixnum));
        if (iq->debug) {
            reqprintf(iq, "ix_delk IX %d KEY ", ixnum);
            reqdumphex(iq, key, getkeysize(iq->usedb, ixnum));
            reqmoref(iq, " RC %d", rc);
        }

        /* remap delete not found to retry */
        if (rc == IX_NOTFND) {
            if (verify_retry)
                rc = RC_INTERNAL_RETRY;
            else
                rc = ERR_VERIFY;
        }

        if (rc != 0) {
            return rc;
        }
    }
    return 0;
}
