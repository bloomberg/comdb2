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
#include <assert.h>
#include "schemachange.h"
#include "block_internal.h"
#include "logmsg.h"
#include "indices.h"
#include "sqloffload.h"

extern int gbl_partial_indexes;
extern int gbl_reorder_idx_writes;
static __thread void *defered_index_tbl = NULL;
static __thread void *defered_index_tbl_cursor = NULL;

//
// del needs to sort before adds because dels used to happen online

// defered index table types
// the _CC types signify that we need to check constraints
// for that key on the parent table
typedef enum {
    DIT_DEL,
    DIT_UPD,
    DIT_ADD,
    DIT_ADD_CC,
    DIT_DEL_CC,
    DIT_UPD_CC
} dit_t;

// defered index table
typedef struct {
    struct dbtable *usedb; // consider not storing usedb and processing each
                           // usedb separately
    short ixnum;
    short ixlen;
    char ixkey[MAXKEYLEN]; // consider storing up to the largest key
                           // for dups genid is appended to end of key
    dit_t type;
    unsigned long long genid;
    unsigned long long newgenid; // new genid used for update
} dtikey_t;

#define CMP_KEY_MEMBER(k1, k2, var)                                            \
    if (k1->var < k2->var) {                                                   \
        return -1;                                                             \
    }                                                                          \
    if (k1->var > k2->var) {                                                   \
        return 1;                                                              \
    }

#define MEMCMP_KEY_MEMBER(k1, k2, var, len)                                    \
    int __rc = memcmp(k1->var, k2->var, len);                                  \
    if (__rc != 0) {                                                           \
        return __rc;                                                           \
    }

static int defered_index_key_cmp(void *usermem, int key1len, const void *key1,
                                 int key2len, const void *key2)
{
    assert(sizeof(dtikey_t) == key1len);
    assert(sizeof(dtikey_t) == key2len);

    dtikey_t *k1 = (dtikey_t *)key1;
    dtikey_t *k2 = (dtikey_t *)key2;

    CMP_KEY_MEMBER(k1, k2, usedb);
    CMP_KEY_MEMBER(k1, k2, ixnum);
    assert(k1->ixlen == k2->ixlen);
    MEMCMP_KEY_MEMBER(k1, k2, ixkey, k1->ixlen);
    CMP_KEY_MEMBER(k1, k2, type);
    CMP_KEY_MEMBER(k1, k2, genid);
    return 0;
}

static inline void *create_defered_index_table()
{
    int bdberr = 0;
    struct temp_table *newtbl =
        (struct temp_table *)bdb_temp_array_create(thedb->bdb_env, &bdberr);
    if (newtbl == NULL || bdberr != 0) {
        logmsg(LOGMSG_ERROR, "failed to create temp table err %d\n", bdberr);
        return NULL;
    }
    // default ordering will not work -- memcmp is not ok
    bdb_temp_table_set_cmp_func(newtbl, defered_index_key_cmp);
    return newtbl;
}

/* If parameter createIfNull is set, this function
 * will create tbl and cursor and return it.
 * If parameter createIfNull is not set, we will return
 * cursor or NULL when tbl is null
 */
static inline void *get_defered_index_tbl_cursor(int createIfNull)
{
    if (defered_index_tbl_cursor) {
        assert(defered_index_tbl != NULL);
        return defered_index_tbl_cursor;
    }

    if (!defered_index_tbl) {
        if (!createIfNull)
            return NULL;

        defered_index_tbl = (void *)create_defered_index_table(NULL);
    }

    defered_index_tbl_cursor = get_constraint_table_cursor(defered_index_tbl);

    if (!defered_index_tbl_cursor)
        abort();

    return defered_index_tbl_cursor;
}

static inline void close_defered_index_tbl_cursor()
{
    if (!defered_index_tbl_cursor)
        return;

    close_constraint_table_cursor(defered_index_tbl_cursor);
    defered_index_tbl_cursor = NULL;
}

void truncate_defered_index_tbl()
{
    close_defered_index_tbl_cursor();

    if (defered_index_tbl) {
        truncate_constraint_table(defered_index_tbl);
    }
}

/* delete tbl and cursor
 * called from handle_buf.c to cleanup defered tbl */
void delete_defered_index_tbl()
{
    if (!defered_index_tbl)
        return;

    if (defered_index_tbl_cursor)
        close_defered_index_tbl_cursor();

    delete_constraint_table(defered_index_tbl);
    defered_index_tbl = NULL;
}

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

    rc = ix_find_by_key_tran(iq, key, ixkeylen, ixnum, NULL, &fndrrn, &fndgenid,
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

static inline void append_genid_to_key(dtikey_t *ditk, int ixkeylen)
{
    ditk->ixlen = ixkeylen + sizeof(ditk->genid);
    memcpy(&ditk->ixkey[ixkeylen], &ditk->genid, sizeof(ditk->genid));
}

int add_record_indices(struct ireq *iq, void *trans, blob_buffer_t *blobs,
                       size_t maxblobs, int *opfailcode, int *ixfailnum,
                       int *rrn, unsigned long long *genid,
                       unsigned long long vgenid, unsigned long long ins_keys,
                       int opcode, int blkpos, void *od_dta, size_t od_len,
                       const char *ondisktag, struct schema *ondisktagsc,
                       int flags, bool reorder)
{
    int rc = 0;
    char *od_dta_tail = NULL;
    int od_tail_len;
    if (iq->osql_step_ix)
        gbl_osqlpf_step[*(iq->osql_step_ix)].step += 1;

    void *cur = NULL;
    dtikey_t ditk = {0};

    if (reorder) {
        cur = get_defered_index_tbl_cursor(1);
        if (cur == NULL) {
            logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
            return -1;
        }
        ditk.type = DIT_ADD;
        ditk.genid = *genid;
        ditk.usedb = iq->usedb;
    }

    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char *key = ditk.ixkey; // key points to chararray regardless reordering
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
            rc = ERR_BADREQ;
            goto done;
        }

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
            goto done;
        }

        /* light the prefault kill bit for this subop - newkeys */
        prefault_kill_bits(iq, ixnum, PFRQ_NEWKEY);
        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step += 2;

        if (reorder) {
            // if not datacopy, no need to save od_dta_tail
            void *data = NULL;
            int datalen = 0;
            if (od_dta_tail) {
                // have a tail when index is datacopy or for decimal quantum
                data = od_dta_tail;
                datalen = od_tail_len;
            }
            ditk.ixnum = ixnum;
            ditk.ixlen = ixkeylen;
            if (od_dta_tail || iq->usedb->ix_dupes[ixnum] != 0)
                append_genid_to_key(&ditk, ixkeylen);
            int err = 0;
            rc = bdb_temp_table_insert(thedb->bdb_env, cur, &ditk, sizeof(ditk),
                                       data, datalen, &err);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n",
                       __func__, rc);
                goto done;
            }
            memset(ditk.ixkey, 0, ditk.ixlen);
        } else {
            int isnullk = ix_isnullk(iq->usedb, key, ixnum);

            if (vgenid && iq->usedb->ix_dupes[ixnum] == 0 && !isnullk) {
                int fndrrn = 0;
                unsigned long long fndgenid = 0ULL;
                rc =
                    ix_find_by_key_tran(iq, key, ixkeylen, ixnum, NULL, &fndrrn,
                                        &fndgenid, NULL, NULL, 0, trans);
                if (rc == IX_FND && fndgenid == vgenid) {
                    rc = ERR_VERIFY;
                    goto done;
                } else if (rc == RC_INTERNAL_RETRY) {
                    rc = RC_INTERNAL_RETRY;
                    goto done;
                } else if (rc != IX_FNDMORE && rc != IX_NOTFND &&
                           rc != IX_PASTEOF && rc != IX_EMPTY) {
                    logmsg(LOGMSG_ERROR, "%s:%d got unexpected error rc = %d\n",
                           __func__, __LINE__, rc);
                    rc = ERR_INTERNAL;
                    goto done;
                }

                /* The row is not in new btree, proceed with the add */
                vgenid = 0; // no need to verify again
            }

            /* add the key */
            rc = ix_addk(iq, trans, key, ixnum, *genid, *rrn, od_dta_tail,
                         od_tail_len, isnullk);

            if (vgenid && rc == IX_DUP) {
                if (iq->usedb->ix_dupes[ixnum] || isnullk) {
                    rc = ERR_VERIFY;
                    goto done;
                }
            }

            if (iq->debug) {
                reqprintf(iq, "ix_addk IX %d LEN %u KEY ", ixnum, ixkeylen);
                reqdumphex(iq, key, ixkeylen);
                reqmoref(iq, " RC %d", rc);
            }

            if (rc == RC_INTERNAL_RETRY) {
                goto done;
            } else if (rc != 0) {
                *ixfailnum = ixnum;
                /* If following changes, update OSQL_INSREC in osqlcomm.c */
                *opfailcode = OP_FAILED_UNIQ; /* really? */

                goto done;
            }
        }
    }
done:
    if (rc)
        close_defered_index_tbl_cursor();
    return rc;
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
    int rc = 0;
    char *od_dta_tail = NULL;
    int od_tail_len;
    char *od_olddta_tail = NULL;
    int od_oldtail_len;

    void *cur = NULL;
    dtikey_t delditk = {0}; // will serve as the delete key obj
    dtikey_t ditk = {0};    // will serve as the add or upd key obj
    bool reorder =
        osql_is_index_reorder_on(iq->osql_flags) && 
        iq->usedb->sc_from != iq->usedb &&
        iq->usedb->ix_expr == 0 && /* dont reorder if we have idx on expr */
        iq->usedb->n_constraints == 0; /* dont reorder if foreign constrts */

    if (reorder) {
        cur = get_defered_index_tbl_cursor(1);
        if (cur == NULL) {
            logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
            return -1;
        }
        delditk.type = DIT_DEL;
        delditk.usedb = iq->usedb;
        ditk.usedb = iq->usedb;
    }

    /* Delay key add if schema change has constraints so we can * verify them.
     * FIXME: What if the table does not have index to begin with?
     * (Redo based live sc works for this case)
     */
    int live_sc_delay = live_sc_delay_key_add(iq);

    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        if (flags == RECFLAGS_UPGRADE_RECORD &&
            iq->usedb->ix_datacopy[ixnum] == 0)
            // skip non-datacopy indexes if it is a record upgrade
            continue;

        char keytag[MAXTAGLEN];
        char *oldkey = delditk.ixkey;
        char *newkey = ditk.ixkey;
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
            rc = ERR_CONVERT_IX;
            goto done;
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
            rc = ERR_CONVERT_IX;
            goto done;
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
            if (reorder) {
                // if not datacopy, no need to save od_dta_tail
                void *data = NULL;
                int datalen = 0;
                if (od_dta_tail) {
                    // have a tail when index is datacopy or for decimal quantum
                    data = od_dta_tail;
                    datalen = od_tail_len;
                }
                ditk.type = DIT_UPD;
                ditk.genid = vgenid;
                ditk.newgenid = *newgenid;
                ditk.ixnum = ixnum;
                ditk.ixlen = keysize;

                if (od_dta_tail || iq->usedb->ix_dupes[ixnum] != 0)
                    append_genid_to_key(&ditk, keysize);
                int err = 0;
                rc = bdb_temp_table_insert(thedb->bdb_env, cur, &ditk,
                                           sizeof(ditk), data, datalen, &err);
                if (rc != 0) {
                    logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n",
                           __func__, rc);
                    goto done;
                }
                memset(ditk.ixkey, 0, ditk.ixlen);
            } else {
                rc = ix_upd_key(iq, trans, newkey, keysize, ixnum, vgenid,
                                *newgenid, od_dta_tail, od_tail_len,
                                ix_isnullk(iq->usedb, newkey, ixnum));
                if (iq->debug)
                    reqprintf(iq, "upd_key IX %d GENID 0x%016llx RC %d", ixnum,
                              *newgenid, rc);

                if (rc != 0) {
                    *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                    *ixfailnum = ixnum;
                    goto done;
                }
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
                if (reorder) {
                    // if not datacopy, no need to save od_dta_tail
                    void *data = NULL;
                    int datalen = 0;
                    delditk.genid = vgenid;
                    delditk.ixnum = ixnum;
                    delditk.ixlen = keysize;
                    if (od_dta_tail || iq->usedb->ix_dupes[ixnum] != 0)
                        append_genid_to_key(&delditk, keysize);
                    int err = 0;
                    rc = bdb_temp_table_insert(thedb->bdb_env, cur, &delditk,
                                               sizeof(delditk), data, datalen,
                                               &err);
                    if (rc != 0) {
                        logmsg(LOGMSG_ERROR,
                               "%s: bdb_temp_table_insert rc = %d\n", __func__,
                               rc);
                        goto done;
                    }
                    memset(delditk.ixkey, 0, delditk.ixlen);
                } else {
                    rc = ix_delk(iq, trans, oldkey, ixnum, rrn, vgenid,
                                 ix_isnullk(iq->usedb, oldkey, ixnum));

                    if (iq->debug)
                        reqprintf(iq, "ix_delk IX %d RRN %d RC %d", ixnum, rrn,
                                  rc);

                    if (rc != 0) {
                        *opfailcode = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                        *ixfailnum = ixnum;
                        goto done;
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
            if (live_sc_delay)
                do_inline = 0;
            *deferredAdd |= (!do_inline);

            if (!gbl_partial_indexes || !iq->usedb->ix_partial ||
                (ins_keys & (1ULL << ixnum))) {
                if (reorder) {
                    // if not datacopy, no need to save od_dta_tail
                    void *data = NULL;
                    int datalen = 0;
                    if (iq->usedb->ix_datacopy[ixnum] != 0) { // is datacopy
                        data = od_dta_tail;
                        datalen = od_tail_len;
                    }
                    ditk.type = DIT_ADD;
                    ditk.genid = *newgenid;
                    ditk.ixnum = ixnum;
                    ditk.ixlen = keysize;
                    if (od_dta_tail || iq->usedb->ix_dupes[ixnum] != 0)
                        append_genid_to_key(&ditk, keysize);
                    int err = 0;
                    rc = bdb_temp_table_insert(thedb->bdb_env, cur, &ditk,
                                               sizeof(ditk), data, datalen,
                                               &err);
                    if (rc != 0) {
                        logmsg(LOGMSG_ERROR,
                               "%s: bdb_temp_table_insert rc = %d\n", __func__,
                               rc);
                        goto done;
                    }
                    memset(ditk.ixkey, 0, ditk.ixlen);
                } else { // TODO: will also need add here for constraint
                         // checking purpose
                    rc = add_key(iq, trans, ixnum, ins_keys, rrn, *newgenid,
                                 od_dta, od_len, opcode, blkpos, opfailcode,
                                 newkey, od_dta_tail, od_tail_len, do_inline);

                    if (iq->debug)
                        reqprintf(iq, "add_key IX %d RRN %d RC %d", ixnum, rrn,
                                  rc);

                    if (rc != 0) {
                        *ixfailnum = ixnum;
                        goto done;
                    }
                }
            }
        }
    }

done:
    if (rc)
        close_defered_index_tbl_cursor();
    return rc;
}

/* Form and delete all keys. */
int del_record_indices(struct ireq *iq, void *trans, int *opfailcode,
                       int *ixfailnum, int rrn, unsigned long long genid,
                       void *od_dta, unsigned long long del_keys, int flags,
                       blob_buffer_t *del_idx_blobs, const char *ondisktag)
{
    int rc = 0;
    void *cur = NULL;
    dtikey_t delditk = {0};
    bool reorder =
        osql_is_index_reorder_on(iq->osql_flags) &&
        iq->usedb->sc_from != iq->usedb &&
        iq->usedb->ix_expr == 0 && /* dont reorder if we have idx on expr */
        iq->usedb->n_constraints == 0; /* dont reorder if foreign constrts */

    if (reorder) {
        cur = get_defered_index_tbl_cursor(1);
        if (cur == NULL) {
            logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
            return -1;
        }
        delditk.type = DIT_DEL;
        delditk.genid = genid;
        delditk.usedb = iq->usedb;
    }

    for (int ixnum = 0; ixnum < iq->usedb->nix; ixnum++) {
        char *key = delditk.ixkey;

        /* only delete keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(del_keys & (1ULL << ixnum)))
            continue;

        int keysize = getkeysize(iq->usedb, ixnum);

        if (iq->idxDelete)
            memcpy(key, iq->idxDelete[ixnum], keysize);
        else {
            char keytag[MAXTAGLEN];
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
                goto done;
            }
        }

        /* handle the key special datacopy options */
        if (iq->usedb->ix_collattr[ixnum]) {
            /* handle key tails */
            rc = extract_decimal_quantum(iq->usedb, ixnum, key, NULL, 0, NULL);
            if (rc) {
                *ixfailnum = ixnum;
                *opfailcode = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                goto done;
            }
        }

        /* light the prefault kill bit for this subop - oldkeys */
        prefault_kill_bits(iq, ixnum, PFRQ_OLDKEY);
        if (iq->osql_step_ix)
            gbl_osqlpf_step[*(iq->osql_step_ix)].step += 2;

        if (reorder) {
            // if not datacopy, no need to save od_dta_tail
            void *data = NULL;
            int datalen = 0;
            delditk.ixnum = ixnum;
            delditk.ixlen = keysize;
            if (iq->usedb->ix_dupes[ixnum] != 0)
                append_genid_to_key(&delditk, keysize);
            int err = 0;

            rc = bdb_temp_table_insert(thedb->bdb_env, cur, &delditk,
                                       sizeof(delditk), data, datalen, &err);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n",
                       __func__, rc);
                goto done;
            }
            memset(delditk.ixkey, 0, delditk.ixlen); // clear it for next round
        } else {
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
                goto done;
            }
        }
    }

done:
    if (rc)
        close_defered_index_tbl_cursor();
    return rc;
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

        /* form new index */
        if (iq->idxInsert)
            rc =
                create_key_from_ireq(iq, ixnum, 0, &od_dta_tail, &od_tail_len,
                                     mangled_key, (char *)new_dta, nd_len, key);
        else {
            char keytag[MAXTAGLEN];
            snprintf(keytag, sizeof(keytag), ".NEW..ONDISK_IX_%d", ixnum);

            rc = create_key_from_ondisk_blobs(
                iq->usedb, ixnum, &od_dta_tail, &od_tail_len, mangled_key,
                use_new_tag ? ".NEW..ONDISK" : ".ONDISK", (char *)new_dta,
                nd_len, keytag, key, NULL, blobs, blobs ? MAXBLOBS : 0, NULL);
        }

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
        char key[MAXKEYLEN];

        if (gbl_use_plan && iq->usedb->plan &&
            iq->usedb->plan->ix_plan[ixnum] != -1)
            continue;

        /* only delete keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(del_keys & (1ULL << ixnum)))
            continue;

        int keysize = getkeysize(iq->usedb, ixnum);
        if (iq->idxDelete) {
            memcpy(key, iq->idxDelete[ixnum], keysize);
        } else {
            char keytag[MAXTAGLEN];
            snprintf(keytag, sizeof(keytag), ".NEW..ONDISK_IX_%d", ixnum);

            rc = create_key_from_ondisk_blobs(
                iq->usedb, ixnum, NULL, NULL, NULL,
                use_new_tag ? ".NEW..ONDISK" : ".ONDISK",
                use_new_tag ? (char *)sc_old : (char *)old_dta,
                0 /*not needed*/, keytag, key, NULL, del_idx_blobs,
                del_idx_blobs ? MAXBLOBS : 0, NULL);
        }

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
        char key[MAXKEYLEN];

        if (gbl_use_plan && iq->usedb->plan &&
            iq->usedb->plan->ix_plan[ixnum] != -1)
            continue;

        /* only delete keys when told */
        if (gbl_partial_indexes && iq->usedb->ix_partial &&
            !(del_keys & (1ULL << ixnum)))
            continue;

        /* Convert from OLD ondisk schema to NEW index schema - this
         * must work by definition. */
        /*
        rc = stag_to_stag_buf(iq->usedb->tablename, ".ONDISK", (char*) old_dta,
                keytag, key, NULL);
         */
        if (iq->idxDelete) {
            memcpy(key, iq->idxDelete[ixnum], iq->usedb->ix_keylen[ixnum]);
            rc = 0;
        } else {
            char keytag[MAXTAGLEN];
            snprintf(keytag, sizeof(keytag), ".NEW..ONDISK_IX_%d", ixnum);

            rc = create_key_from_ondisk_blobs(
                iq->usedb, ixnum, NULL, NULL, NULL,
                use_new_tag ? ".NEW..ONDISK" : ".ONDISK",
                use_new_tag ? (char *)sc_old : (char *)old_dta,
                0 /*not needed */, keytag, key, NULL, del_idx_blobs,
                del_idx_blobs ? MAXBLOBS : 0, NULL);
        }

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

#if 0
//type: DEL = 0, ADD = 2
int insert_defered_tbl(struct ireq *iq, void *od_dta, size_t od_len,
                  const char *ondisktag, struct schema *ondisktagsc,
                  unsigned long long genid, int type)
{
    void *cur = get_defered_index_tbl_cursor(1);
    if (cur == NULL) {
        logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
        return -1;
    }
    //insert record by tbl,ixnum,ixkey, payload genid
    dtikey_t ditk= {0};
    ditk.usedb = iq->usedb;
    ditk.type = type;
    ditk.genid = genid;
    
    for(int i = 0; i < iq->usedb->nix; i++) {
        ditk.ixnum = i;
        char *key = ditk.ixkey;
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
        rc = bdb_temp_table_insert(thedb->bdb_env, cur, &ditk, sizeof(ditk),
            od_dta_tail, od_tail_len, &err);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n", __func__,
                    rc);
            return rc;
        }
    }
    return 0;
}
#endif

int process_defered_table(struct ireq *iq, block_state_t *blkstate, void *trans,
                          int *blkpos, int *ixout, int *errout)
{
    void *cur = get_defered_index_tbl_cursor(0);

    if (!cur) {
        // never inserted anything in tmp tbl
        return 0;
    }

#if DEBUG_REORDER
    logmsg(LOGMSG_DEBUG, "%s(): defered table content:\n", __func__);
    // if needed to check content of socksql temp table, dump with:
    void bdb_temp_table_debug_dump(bdb_state_type * bdb_state, void *cur, int);
    bdb_temp_table_debug_dump(thedb->bdb_env, cur, LOGMSG_DEBUG);
    int count = 0;
#endif

    int err;
    int rc = bdb_temp_table_first(thedb->bdb_env, cur, &err);
    if (rc != IX_OK) {
        if (rc == IX_EMPTY) {
            if (iq->debug)
                reqprintf(iq, "%p:VERKYCNSTRT FOUND NO KEYS TO ADD", trans);
            rc = 0;
            goto done;
        }
        if (iq->debug)
            reqprintf(iq, "%p: CANNOT GET ADD LIST RECORD", trans);
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_REC, "cannot get add list record");
        *errout = OP_FAILED_INTERNAL;
        goto done;
    }

    while (rc == IX_OK) {
        dtikey_t *ditk = (dtikey_t *)bdb_temp_table_key(cur);
        void *od_dta_tail = bdb_temp_table_data(cur);
        int od_tail_len = bdb_temp_table_datasize(cur);

        iq->usedb = ditk->usedb;

        if (ditk->type == DIT_ADD) {
            int addrrn = 2;
            /* add the key */
            rc = ix_addk(iq, trans, ditk->ixkey, ditk->ixnum, ditk->genid,
                         addrrn, od_dta_tail, od_tail_len,
                         ix_isnullk(iq->usedb, ditk->ixkey, ditk->ixnum));

            if (iq->debug) {
                reqprintf(iq, "%p:ADDKYCNSTRT  TBL %s IX %d RRN %d KEY ", trans,
                          ditk->usedb->tablename, ditk->ixnum, addrrn);
                int ixkeylen = getkeysize(ditk->usedb, ditk->ixnum);
                reqdumphex(iq, ditk->ixkey, ixkeylen);
                reqmoref(iq, " RC %d", rc);
            }

            if (rc == IX_DUP) {
                reqerrstr(iq, COMDB2_CSTRT_RC_DUP,
                          "add key constraint "
                          "duplicate key '%s' on "
                          "table '%s' index %d",
                          get_keynm_from_db_idx(ditk->usedb, ditk->ixnum),
                          ditk->usedb->tablename, ditk->ixnum);

                //*blkpos = curop->blkpos;
                *errout = OP_FAILED_UNIQ;
                *ixout = ditk->ixnum;
                goto done;
            } else if (rc != 0) {
                reqerrstr(iq, COMDB2_CSTRT_RC_INTL_ERR,
                          "add key berkley error for key '%s' on index %d",
                          get_keynm_from_db_idx(ditk->usedb, ditk->ixnum),
                          ditk->ixnum);

                //*blkpos = curop->blkpos;

                *errout = OP_FAILED_INTERNAL;
                *ixout = ditk->ixnum;

                if (ERR_INTERNAL == rc) {
                    /* Exit & have the cluster elect another master */
                    if (gbl_exit_on_internal_error) {
                        exit(1);
                    }

                    rc = ERR_NOMASTER;
                }
                goto done;
            }
        } else if (ditk->type == DIT_DEL) {
            int delrrn = 0;
#ifndef NDEBUG
            char *tblname = iq->usedb->tablename;
            struct dbtable *tbl = get_dbtable_by_name(tblname);
            assert(tbl == iq->usedb);
#endif
            rc = ix_delk(iq, trans, ditk->ixkey, ditk->ixnum, delrrn,
                         ditk->genid,
                         ix_isnullk(iq->usedb, ditk->ixkey, ditk->ixnum));
            if (iq->debug) {
                reqprintf(iq, "ix_delk IX %d KEY ", ditk->ixnum);
                reqdumphex(iq, ditk->ixkey,
                           getkeysize(ditk->usedb, ditk->ixnum));
                reqmoref(iq, " RC %d", rc);
            }
            if (rc != 0) {
                if (rc == IX_NOTFND) {
                    reqerrstrhdr(iq, "Table '%s' ", ditk->usedb->tablename);
                    reqerrstr(iq, COMDB2_DEL_RC_INVL_KEY,
                              "key not found on index %d", ditk->ixnum);
                }
                *errout = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                *ixout = ditk->ixnum;
                goto done;
            }

        } else if (ditk->type == DIT_UPD) {
            rc = ix_upd_key(
                iq, trans, ditk->ixkey, ditk->usedb->ix_keylen[ditk->ixnum],
                ditk->ixnum, ditk->genid, ditk->newgenid, od_dta_tail,
                od_tail_len, ix_isnullk(ditk->usedb, ditk->ixkey, ditk->ixnum));
            if (iq->debug)
                reqprintf(iq, "upd_key IX %d GENID 0x%016llx RC %d",
                          ditk->ixnum, ditk->newgenid, rc);

            if (rc != 0) {
                *errout = OP_FAILED_INTERNAL + ERR_DEL_KEY;
                *ixout = ditk->ixnum;
                goto done;
            }
        } else {
            abort();
        }

        /* get next record from table */
        rc = bdb_temp_table_next(thedb->bdb_env, cur, &err);
    }
    if (rc == IX_PASTEOF)
        rc = IX_OK;

done:
    truncate_defered_index_tbl();
    // We can also delete if we are done with the tmptbl
    return rc;
}
