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

#include "limit_fortify.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>
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
#include "indices.h"
#include "osqlsqlthr.h"


static char *get_temp_ct_dbname(long long *);
static int is_update_op(int op);
static int is_delete_op(int op);

extern int gbl_partial_indexes;

/**
 * Checks to see if there are any cascading deleletes/updates pointing to this
 * table
 * @param db    pointer to the db to check for cascading constraints
 * @param op    optype used to determine whether we care about update or delete
 *              constraints
 * @return 0 if there are no cascading constraints !0 otherwise
 */
int has_cascading_reverse_constraints(struct dbtable *db)
{
    int i;

    /* for each of the constraints pointing to this db */
    for (i = 0; i < db->n_rev_constraints; i++) {
        constraint_t *cnstrt = db->rev_constraints[i];

        if ((cnstrt->flags & CT_DEL_CASCADE) == CT_DEL_CASCADE)
            return 1;

        if ((cnstrt->flags & CT_UPD_CASCADE) == CT_UPD_CASCADE)
            return 1;
    }

    return 0;
}

/* this is for index on expressions */
static int insert_add_index(struct ireq *iq, unsigned long long genid)
{
    char key[MAXKEYLEN];
    void *foundkey = NULL;
    void *cur = NULL;
    int err = 0;
    int *pixnum;
    int i;
    int rc = 0;
    if (iq->idxInsert == NULL)
        return 0;

    struct thread_info *thdinfo = pthread_getspecific(unique_tag_key);
    if (thdinfo == NULL) {
        logmsg(LOGMSG_ERROR, "%s: no thdinfo\n", __func__);
        return -1;
    }
    cur = get_constraint_table_cursor(thdinfo->ct_add_index);
    if (cur == NULL) {
        logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
        return -1;
    }
    memcpy(key, &genid, sizeof(unsigned long long));
    pixnum = (int *)(key + sizeof(unsigned long long));
    *pixnum = 0;

    bdb_temp_table_find(thedb->bdb_env, cur, key,
                        sizeof(unsigned long long) + sizeof(int), NULL, &err);
    foundkey = bdb_temp_table_key(cur);
    if (foundkey && !memcmp(foundkey, &genid, sizeof(unsigned long long)))
        goto out;

    for (i = 0; i < iq->usedb->nix; i++) {
        if (iq->idxInsert[i] == NULL)
            continue;
        *pixnum = i;
        rc = bdb_temp_table_insert(
            thedb->bdb_env, cur, key, sizeof(unsigned long long) + sizeof(int),
            iq->idxInsert[i], getkeysize(iq->usedb, i), &err);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_insert rc = %d\n", __func__,
                    rc);
            goto out;
        }
    }
out:
    close_constraint_table_cursor(cur);
    return rc;
}

void free_cached_idx(uint8_t **cached_idx);
static int cache_delayed_indexes(struct ireq *iq, unsigned long long genid)
{
    char key[MAXKEYLEN];
    void *foundkey = NULL;
    void *cur = NULL;
    int err = 0;
    int *pixnum;

    if (iq->idxInsert || iq->idxDelete) {
        free_cached_idx(iq->idxInsert);
        free_cached_idx(iq->idxDelete);
        free(iq->idxInsert);
        free(iq->idxDelete);
        iq->idxInsert = iq->idxDelete = NULL;
    }

    struct thread_info *thdinfo = pthread_getspecific(unique_tag_key);
    if (thdinfo == NULL) {
        logmsg(LOGMSG_ERROR, "%s: no thdinfo\n", __func__);
        return -1;
    }
    cur = get_constraint_table_cursor(thdinfo->ct_add_index);
    if (cur == NULL) {
        logmsg(LOGMSG_ERROR, "%s : no cursor???\n", __func__);
        return -1;
    }
    memcpy(key, &genid, sizeof(unsigned long long));
    pixnum = (int *)(key + sizeof(unsigned long long));
    *pixnum = 0;

    bdb_temp_table_find(thedb->bdb_env, cur, key,
                        sizeof(unsigned long long) + sizeof(int), NULL, &err);
    foundkey = bdb_temp_table_key(cur);
    if (!foundkey || memcmp(foundkey, &genid, sizeof(unsigned long long))) {
        close_constraint_table_cursor(cur);
        return 0;
    } else {
        iq->idxInsert = calloc(MAXINDEX, sizeof(uint8_t *));
        iq->idxDelete = calloc(MAXINDEX, sizeof(uint8_t *));
        if (!iq->idxInsert || !iq->idxDelete) {
            logmsg(LOGMSG_ERROR, "%s failed to allocated indexes\n", __func__);
            close_constraint_table_cursor(cur);
            return -1;
        }
    }

    while (1) {
        pixnum = (int *)((char *)foundkey + sizeof(unsigned long long));
        iq->idxInsert[*pixnum] = malloc(getkeysize(iq->usedb, *pixnum));
        if (iq->idxInsert[*pixnum] == NULL) {
            logmsg(LOGMSG_ERROR, "%s failed to allocated indexes\n", __func__);
            close_constraint_table_cursor(cur);
            return -1;
        }
        memcpy(iq->idxInsert[*pixnum], bdb_temp_table_data(cur),
               getkeysize(iq->usedb, *pixnum));

        bdb_temp_table_next(thedb->bdb_env, cur, &err);
        foundkey = bdb_temp_table_key(cur);
        if (!foundkey || memcmp(foundkey, &genid, sizeof(unsigned long long)))
            break;
    }

    close_constraint_table_cursor(cur);
    return 0;
}

static inline void free_cached_delayed_indexes(struct ireq *iq)
{
    if (iq->idxInsert || iq->idxDelete) {
        free_cached_idx(iq->idxInsert);
        free_cached_idx(iq->idxDelete);
        free(iq->idxInsert);
        free(iq->idxDelete);
        iq->idxInsert = iq->idxDelete = NULL;
    }
}

enum ct_etype { CTE_ADD = 1, CTE_DEL, CTE_UPD };

struct forward_ct {
    unsigned long long genid;
    unsigned long long ins_keys;
    struct dbtable *usedb;
    int blkpos;
    int ixnum;
    int rrn;
    int optype;
    int flags;
};

struct backward_ct {
    struct dbtable *srcdb;
    struct dbtable *dstdb;
    int blkpos;
    int optype;
    char key[MAXKEYLEN];
    char newkey[MAXKEYLEN];
    int sixlen;
    int sixnum;
    int dixnum;
    int nonewrefs;
    int flags;
};

typedef struct cttbl_entry {
    int ct_type;
    union {
        struct forward_ct fwdct;
        struct backward_ct bwdct;
    } ctop;
} cte;

int insert_add_op(struct ireq *iq, int optype, int rrn, int ixnum,
                  unsigned long long genid, unsigned long long ins_keys,
                  int blkpos, int rec_flags)
{
    block_state_t *blkstate = iq->blkstate;
    int type = CTE_ADD, rc = 0;
    char key[MAXKEYLEN];
    cte cte_record;
    int err = 0;

    struct thread_info *thdinfo = pthread_getspecific(unique_tag_key);
    if (thdinfo == NULL) {
        logmsg(LOGMSG_ERROR, "insert_add_op: no thdinfo\n");
        return -1;
    }

    void *cur = get_constraint_table_cursor(thdinfo->ct_add_table);
    if (cur == NULL) {
        logmsg(LOGMSG_ERROR, "insert_add_op: no cursor???\n");
        return -1;
    }
    memcpy(key, &type, sizeof(type));
    memcpy(key + sizeof(type), &blkstate->ct_id_key,
           sizeof(blkstate->ct_id_key));
    cte_record.ct_type = CTE_ADD;
    struct forward_ct *fwdct = &cte_record.ctop.fwdct;
    fwdct->genid = genid;
    fwdct->ins_keys = ins_keys;
    fwdct->usedb = iq->usedb;
    fwdct->blkpos = blkpos;
    fwdct->ixnum = ixnum;
    fwdct->rrn = rrn;
    fwdct->optype = optype;
    fwdct->flags = rec_flags;

    rc = bdb_temp_table_insert(thedb->bdb_env, cur, key,
                               sizeof(int) + sizeof(long long), &cte_record,
                               sizeof(cte), &err);

    close_constraint_table_cursor(cur);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "insert_add_op: bdb_temp_table_insert rc = %d\n", rc);
        return -1;
    }
    rc = insert_add_index(iq, genid);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "insert_add_op: insert_add_index rc = %d\n", rc);
        return -1;
    }

    blkstate->ct_id_key++;
    return 0;
}

static int insert_del_op(block_state_t *blkstate, struct dbtable *srcdb,
                         struct dbtable *dstdb, int optype, int blkpos,
                         void *inkey, void *innewkey, int keylen, int sixnum,
                         int dixnum, int nonewrefs, int flags)
{
    void *cur = NULL;
    int type = CTE_DEL, rc = 0;
    char key[MAXKEYLEN];
    cte cte_record;
    int err = 0;

    struct thread_info *thdinfo = pthread_getspecific(unique_tag_key);
    if (thdinfo == NULL)
        return -1;

    cur = get_constraint_table_cursor(thdinfo->ct_del_table);
    if (cur == NULL)
        return -1;

    memcpy(key, &type, sizeof(type));
    memcpy(key + sizeof(type), &blkstate->ct_id_key,
           sizeof(blkstate->ct_id_key));
    cte_record.ct_type = CTE_DEL;
    struct backward_ct *bwdct = &cte_record.ctop.bwdct;
    bwdct->srcdb = srcdb;
    bwdct->dstdb = dstdb;
    bwdct->blkpos = blkpos;
    bwdct->sixlen = keylen;
    bwdct->sixnum = sixnum;
    bwdct->dixnum = dixnum;
    bwdct->optype = optype;
    bwdct->nonewrefs = nonewrefs;
    bwdct->flags = flags;
    memcpy(bwdct->key, inkey, keylen);
    if (innewkey == NULL) {
        memset(bwdct->newkey, 0, sizeof(bwdct->newkey));
    } else {
        /* always non-null in case of updates */
        memcpy(bwdct->newkey, innewkey, keylen);
    }

    rc = bdb_temp_table_insert(thedb->bdb_env, cur, key,
                               sizeof(int) + sizeof(long long), &cte_record,
                               sizeof(cte), &err);
    close_constraint_table_cursor(cur);
    if (rc != 0)
        return -1;

    blkstate->ct_id_key++;
    return 0;
}

static int should_skip_constraint_for_index(struct ireq *iq, int ixnum, int nulls)
{
    return (nulls && (gbl_nullfkey || iq->usedb->ix_nullsallowed[ixnum]));
}

/* rec_dta is in .ONDISK format..we have it from 'delete' operation in block
 * loop */

int check_delete_constraints(struct ireq *iq, void *trans,
                             block_state_t *blkstate, int op, void *rec_dta,
                             unsigned long long del_keys, int *errout)
{
    return check_update_constraints(iq, trans, blkstate, op, rec_dta, NULL,
                                    del_keys, errout);
}

int check_update_constraints(struct ireq *iq, void *trans,
                             block_state_t *blkstate, int op, void *rec_dta,
                             void *newrec_dta, unsigned long long del_keys,
                             int *errout)
{
    int i = 0, rc = 0;
    for (i = 0; i < iq->usedb->n_rev_constraints; i++) {
        int j = 0;
        constraint_t *cnstrt = iq->usedb->rev_constraints[i];
        char rkey[MAXKEYLEN];
        int rixnum = 0, rixlen = 0;
        char rondisk_tag[MAXTAGLEN];

        for (j = 0; j < cnstrt->nrules; j++) {
            char ondisk_tag[MAXTAGLEN];
            int ixnum = 0, ixlen = 0;
            char lkey[MAXKEYLEN];
            char nkey[MAXKEYLEN];
            char rnkey[MAXKEYLEN];
            int nornrefs = 0;

            if (strcasecmp(cnstrt->table[j], iq->usedb->tablename)) {
                continue;
            }
            rc = getidxnumbyname(cnstrt->table[j], cnstrt->keynm[j], &ixnum);
            if (rc) {
                if (iq->debug)
                    reqprintf(iq, "RTNKYCNSTRT: UNKNOWN KEYTAG %s",
                              cnstrt->keynm[j]);
                reqerrstr(iq, COMDB2_CSTRT_RC_INVL_KEY,
                          "key constraint: unknown keytag '%s'",
                          cnstrt->keynm[j]);
                *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                return ERR_CONVERT_IX;
            }
            if (gbl_partial_indexes && iq->usedb->ix_partial &&
                !(del_keys & (1ULL << ixnum))) {
                continue;
            }
            ixlen = getkeysize(iq->usedb, ixnum);
            snprintf(ondisk_tag, MAXTAGLEN, ".ONDISK_IX_%d", ixnum);
            /*
            rc=stag_to_stag_buf(iq->usedb->tablename, ".ONDISK",rec_dta,
               ondisk_tag, lkey, NULL);
             */
            if (iq->idxDelete)
                rc = create_key_from_ireq(iq, ixnum, 1, NULL, NULL, NULL,
                                          rec_dta, 0 /* not needed */, lkey);
            else
                rc = create_key_from_ondisk(
                    iq->usedb, ixnum, NULL, NULL, NULL, ".ONDISK", rec_dta,
                    0 /* not needed */, ondisk_tag, lkey, NULL, NULL);
            if (rc == -1) {
                if (iq->debug)
                    reqprintf(iq,
                              "RTNKYCNSTRT CANT FORM DST TBL %s INDEX %d (%s)",
                              iq->usedb->tablename, ixnum, cnstrt->keynm[j]);
                reqerrstr(iq, COMDB2_CSTRT_RC_INVL_IDX,
                          "key constraint cannot form destination table '%s' "
                          "index %d (%s)",
                          iq->usedb->tablename, ixnum, cnstrt->keynm[j]);
                *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                return ERR_CONVERT_IX;
            }

            /* this part is for update */
            if (newrec_dta != NULL) {
                /*
                rc=stag_to_stag_buf(iq->usedb->tablename, ".ONDISK",newrec_dta,
                   ondisk_tag, nkey, NULL);
                 */
                if (iq->idxInsert)
                    rc = create_key_from_ireq(iq, ixnum, 0, NULL, NULL, NULL,
                                              newrec_dta, 0 /* not needed */,
                                              nkey);
                else
                    rc = create_key_from_ondisk(iq->usedb, ixnum, NULL, NULL,
                                                NULL, ".ONDISK", newrec_dta,
                                                0 /* not needed */, ondisk_tag,
                                                nkey, NULL, NULL);
                if (rc == -1) {
                    if (iq->debug)
                        reqprintf(
                            iq,
                            "RTNKYCNSTRT NEWDTA CANT FORM TBL %s INDEX %d (%s)",
                            iq->usedb->tablename, ixnum, cnstrt->keynm[j]);
                    reqerrstr(iq, COMDB2_CSTRT_RC_INVL_IDX,
                              "key constraint: new data cannot form table '%s' "
                              "index %d (%s)",
                              iq->usedb->tablename, ixnum, cnstrt->keynm[j]);
                    *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                    return ERR_CONVERT_IX;
                }
                /* if new key  matches old key, we don't need to check this
                   delete constraint,
                   because we know that whatever records depend on this rule,
                   will still hold
                   after update */
                if (!memcmp(lkey, nkey, ixlen)) {
                    if (iq->debug)
                        reqprintf(iq, "RTNKYCNSTRT SKIP CNSTRT CHECK DUE TO "
                                      "SAME KEY DATA. TBL %s INDEX %d (%s)",
                                  iq->usedb->tablename, ixnum,
                                  cnstrt->keynm[j]);
                    continue;
                }
            }
            /* here we convert the key into return db format */
            rc = getidxnumbyname(cnstrt->lcltable->tablename,
                                 cnstrt->lclkeyname, &rixnum);
            if (rc) {
                if (iq->debug)
                    reqprintf(iq, "RTNKYCNSTRT: UNKNOWN TABLE %s KEYTAG %s",
                              cnstrt->lcltable->tablename, cnstrt->lclkeyname);
                reqerrstr(iq, COMDB2_CSTRT_RC_INVL_KEY,
                          "key constraint: unknown table '%s' keytag '%s'",
                          cnstrt->lcltable->tablename, cnstrt->lclkeyname);
                *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                return ERR_CONVERT_IX;
            }

            snprintf(rondisk_tag, MAXTAGLEN, ".ONDISK_IX_%d", rixnum);

            int nulls = 0;

            rixlen = rc = stag_to_stag_buf_ckey(
                iq->usedb->tablename, ondisk_tag, lkey,
                cnstrt->lcltable->tablename, rondisk_tag, rkey, &nulls, PK2FK);
            if (rc == -1) {

#if 0
            if (iq->debug)
               reqprintf(iq,
                  "RTNKYCNSTRT CANT FORM SRC TBL %s INDEX %d (%s)",
                  cnstrt->lcltable->tablename, rixnum,cnstrt->lclkeyname);
            *errout=OP_FAILED_INTERNAL + ERR_FORM_KEY;
            return ERR_CONVERT_IX;
#endif
                /* we cant form the key for source table/key pair in the
                   constraint.
                   This means that this record is not referenced..just let it
                   through!*/
                if (iq->debug)
                    reqprintf(iq, "RTNKYCNSTRT SRC TBL %s INDEX %d (%s). "
                                  "SKIPPING RULE CHECK.",
                              cnstrt->lcltable->tablename, rixnum,
                              cnstrt->lclkeyname);
                continue; /* just move on, there should be nothing to check */
            }

            if (should_skip_constraint_for_index(iq, rixnum, nulls)) {
                if (iq->debug)
                    reqprintf(iq, "RTNKYCNSTRT NULL COLUMN PREVENTS FOREIGN "
                                  "REF %s INDEX %d (%s). SKIPPING RULE CHECK.",
                              cnstrt->lcltable->tablename, rixnum,
                              cnstrt->lclkeyname);
                continue; /* just move on, there should be nothing to check */
            }

            if (cnstrt->lcltable->ix_collattr[rixnum]) {
                rc = extract_decimal_quantum(cnstrt->lcltable, rixnum, rkey, NULL,
                                           0, NULL);
                if (rc) {
                    abort(); /* called doesn't return error for these arguments,
                                at least not now */
                }
            }

            if (newrec_dta != NULL) {
                /*
                   this conversion is for new key data, in case we'll need to
                   cascade
                   and update any other records on the way.  this key is the
                   source of
                   new data for everyone else.
                */
                rixlen = rc =
                    stag_to_stag_buf_ckey(iq->usedb->tablename, ondisk_tag,
                                          nkey, cnstrt->lcltable->tablename,
                                          rondisk_tag, rnkey, NULL, PK2FK);
                if (rc == -1) {
/* same thing as for delete.  If the key cannot be formed,
   it is not a failure, no one's referencing us..so,
   just add empty key to be verified recursively */
#if 1
                    nornrefs = 1;
                    /* something could be found, we'll postpone additional check
                     * until verify time */

                    if (iq->debug)
                        reqprintf(iq, "RTNKYCNSTRT CANT FORM NEW SRC TBL %s "
                                      "INDEX %d (%s). PENDING RULE CHECK.",
                                  cnstrt->lcltable->tablename, rixnum,
                                  cnstrt->lclkeyname);
                    memcpy(rnkey, nkey, ixlen);
                    rixlen = ixlen;
#endif
#if 0
               if (iq->debug)
                  reqprintf(iq,
                     "RTNKYCNSTRT CANT FORM NEW SRC TBL %s INDEX %d (%s)",
                     cnstrt->lcltable->tablename, rixnum,cnstrt->lclkeyname);
               *errout=OP_FAILED_INTERNAL + ERR_FORM_KEY;
               return ERR_CONVERT_IX;
#endif
                } else if (cnstrt->lcltable->ix_collattr[rixnum]) {
                    rc = extract_decimal_quantum(cnstrt->lcltable, rixnum, rnkey,
                                               NULL, 0, NULL);
                    if (rc) {
                        abort(); /* called doesn't return error for these
                                    arguments, at least not now */
                    }
                }
            }

            rc = insert_del_op(blkstate, cnstrt->lcltable, iq->usedb, op, 0,
                               rkey, (newrec_dta == NULL) ? NULL : rnkey,
                               rixlen, rixnum, ixnum, nornrefs, cnstrt->flags);
            if (rc != 0) {
                if (iq->debug)
                    reqprintf(iq, "RTNKYCNSTRT CANT MALLOC\n");
                *errout = OP_FAILED_INTERNAL + ERR_INTERNAL;
                return ERR_INTERNAL;
            }
        }
    }

    return 0;
}

/* FOR UPDATES/DELETES, MUST VERIFY AGAINST DELETED RECORD'S TABLE TO SEE IF
 * THERE'RE ANY  KEYS WITH SAME VALUE.  IT IS OK TO DELETE IF THATS THE CASE */
int verify_del_constraints(struct ireq *iq, block_state_t *blkstate,
                           void *trans, blob_buffer_t *blobs, int *errout)
{
    int rc = 0, fndrrn = 0, err = 0;
    int keylen;
    char key[MAXKEYLEN];

    struct thread_info *thdinfo = pthread_getspecific(unique_tag_key);
    if (thdinfo == NULL) {
        if (iq->debug)
            reqprintf(iq, "VERKYCNSTRT CANNOT GET DEL LIST CURSOR");
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_CURSOR,
                  "verify constraint cannot get del list cursor");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }

    void *cur = get_constraint_table_cursor(thdinfo->ct_del_table);
    if (cur == NULL) {
        if (iq->debug)
            reqprintf(iq, "VERKYCNSTRT CANNOT GET DEL LIST CURSOR");
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_CURSOR,
                  "verify constraint cannot get del list cursor");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }

    rc = bdb_temp_table_first(thedb->bdb_env, cur, &err);
    if (rc != 0) {
        close_constraint_table_cursor(cur);
        if (rc == IX_EMPTY) {
            if (iq->debug)
                reqprintf(iq,
                          "VERKYCNSTRT FOUND NOTHING TO VERIFY IN DEL LIST");
            return 0;
        }
        if (iq->debug)
            reqprintf(iq, "VERKYCNSTRT ERROR GETTING FINDING IN DEL LIST");
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_REC,
                  "verify key constraint error getting finding in del list");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }

    while (rc == 0) {
        cte *ctrq = (cte *)bdb_temp_table_data(cur);
        if (ctrq == NULL) {
            if (iq->debug)
                reqprintf(iq, "VERKYCNSTRT CANNOT GET DEL LIST RECORD DATA");
            reqerrstr(iq, COMDB2_CSTRT_RC_INVL_REC,
                      "verify key constraint cannot get del list record data");
            *errout = OP_FAILED_INTERNAL;
            close_constraint_table_cursor(cur);
            return ERR_INTERNAL;
        }

        unsigned long long genid = 0LL, fndgenid = 0LL;
        int rrn = 0;
        int del_cascade = 0;
        int upd_cascade = 0;
        struct backward_ct *bct = &ctrq->ctop.bwdct;
        struct dbtable *currdb = iq->usedb; /* make a copy */
        char *skey = bct ? bct->key : "";

        if (is_delete_op(bct->optype) && (bct->flags & CT_DEL_CASCADE))
            del_cascade = 1;
        else if (is_update_op(bct->optype) && (bct->flags & CT_UPD_CASCADE))
            upd_cascade = 1;

        /* verify against source table...must be not found */
        iq->usedb = bct->srcdb;

        rc = ix_find_by_key_tran(iq, skey, bct->sixlen, bct->sixnum, key, &rrn,
                                 &genid, NULL, NULL, 0, trans);
        iq->usedb = currdb;

        if (rc == RC_INTERNAL_RETRY) {
            *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
            close_constraint_table_cursor(cur);
            return rc;
        }

        if (rc != IX_FND && rc != IX_FNDMORE) {
            // key was not found on source table, so nothing to do
            if (iq->debug) {
                reqprintf(
                    iq,
                    "VERBKYCNSTRT VERIFIED TBL %s IX %d AGAINST TBL %s IX %d ",
                    bct->dstdb->tablename, bct->dixnum, bct->srcdb->tablename,
                    bct->sixnum);
                reqdumphex(iq, bct->key, bct->sixlen);
                reqmoref(iq, " RC %d", rc);
            }
            rc = bdb_temp_table_next(thedb->bdb_env, cur, &err);
            continue;
        }

        /* Key was found, check the dependee (parent table) of the constraint.
         * If we find another key there with the same value, then we dont need
         * to do anything (if also key length and key are exactly the same).
         * If we dont find key, we need to cascade the delete. */
        char ondisk_tag[MAXTAGLEN], dondisk_tag[MAXTAGLEN];
        char dkey[MAXKEYLEN];
        if (bct->nonewrefs) {
            if (iq->debug)
                reqprintf(iq,
                          "VERBKYCNSTRT CANT FORM NEW DATA TBL %s "
                          "INDEX %d FROM %s INDEX %d ",
                          bct->dstdb->tablename, bct->dixnum,
                          bct->srcdb->tablename, bct->sixnum);
            reqmoref(iq, " RC %d", rc);
            reqerrstr(iq, COMDB2_CSTRT_RC_INVL_DTA,
                      "verify key constraint cannot form new data table "
                      "'%s' index %d from %s index %d ",
                      bct->dstdb->tablename, bct->dixnum, bct->srcdb->tablename,
                      bct->sixnum);
            *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
            close_constraint_table_cursor(cur);
            return ERR_CONVERT_IX;
        }
        snprintf(ondisk_tag, sizeof(ondisk_tag) - 1, ".ONDISK_IX_%d",
                 bct->sixnum);
        snprintf(dondisk_tag, sizeof(dondisk_tag) - 1, ".ONDISK_IX_%d",
                 bct->dixnum);

        int nullck = 0;

        keylen = rc = stag_to_stag_buf_ckey(bct->srcdb->tablename, ondisk_tag,
                                            skey, bct->dstdb->tablename,
                                            dondisk_tag, dkey, &nullck, FK2PK);
        if (rc == -1) {
            if (iq->debug)
                reqprintf(iq,
                          "VERBKYCNSTRT CANT FORM TBL %s INDEX %d FROM "
                          "%s INDEX %d KEY ",
                          bct->dstdb->tablename, bct->dixnum,
                          bct->srcdb->tablename, bct->sixnum);
            reqerrstr(iq, COMDB2_CSTRT_RC_INVL_DTA,
                      "verify key constraint cannot form table '%s' index "
                      "%d from %s index %d key '%s",
                      bct->dstdb->tablename, bct->dixnum, bct->srcdb->tablename,
                      bct->sixnum,
                      get_keynm_from_db_idx(bct->srcdb, bct->sixnum));
            reqdumphex(iq, skey, bct->sixlen);
            reqmoref(iq, " RC %d", rc);
            *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
            close_constraint_table_cursor(cur);
            return ERR_CONVERT_IX;
        }

        /* Ignore records with null columns if nullfkey is set */
        if (should_skip_constraint_for_index(iq, bct->sixnum, nullck)) {
            if (iq->debug) {
                reqprintf(iq,
                          "VERBKYCNSTRT NULL COLUMN PREVENTS FOREIGN "
                          "REF %s INDEX %d.",
                          bct->srcdb->tablename, bct->sixnum);
            }
            continue; /* TODO: why do we not get next? */
        }

        if (bct->dstdb->ix_collattr[bct->dixnum]) {
            rc = extract_decimal_quantum(bct->dstdb, bct->dixnum, dkey, NULL, 0,
                                         NULL);
            if (rc) {
                abort();
            }
        }

        iq->usedb = bct->dstdb;
        rc = ix_find_by_key_tran(iq, dkey, keylen, bct->dixnum, key, &fndrrn,
                                 &fndgenid, NULL, NULL, 0, trans);
        iq->usedb = currdb;

        if (rc == RC_INTERNAL_RETRY) {
            *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
            close_constraint_table_cursor(cur);
            return rc;
        }

        /* key was found in parent tbl, no need to delete */
        if (rc == IX_FND || rc == IX_FNDMORE) {
            if (iq->debug) {
                reqprintf(iq,
                          "VERBKYCNSTRT VERIFIED TBL %s IX %d AGAINST "
                          "TBL %s IX %d ",
                          bct->dstdb->tablename, bct->dixnum,
                          bct->srcdb->tablename, bct->sixnum);
                reqdumphex(iq, bct->key, bct->sixlen);
                reqmoref(iq, " RC %d", rc);
            }
            rc = bdb_temp_table_next(thedb->bdb_env, cur, &err);
            continue;
        }

        /* key was not found in parent tbl, will need to delete from this tbl */
        if (del_cascade) {
            /* do cascade logic here */
            int err = 0, idx = 0;
            if (iq->debug) {
                reqprintf(iq, "VERBKYCNSTRT CASCADE DELETE TBL %s RRN %d ",
                          bct->srcdb->tablename, rrn);
            }
            iq->usedb = bct->srcdb;
            if (iq->debug)
                reqpushprefixf(iq, "VERBKYCNSTRT CASCADE DEL:");
            /* TODO verify we have proper schema change locks */

            rc = del_record(iq, trans, NULL, rrn, genid, -1ULL, &err, &idx,
                            BLOCK2_DELKL,
                            RECFLAGS_DONT_LOCK_TBL | RECFLAGS_DONT_REORDER_IDX);
            if (iq->debug)
                reqpopprefixes(iq, 1);
            iq->usedb = currdb;

            if (rc != 0) {
                if (iq->debug) {
                    reqprintf(iq,
                              "VERBKYCNSTRT CANT CASCADE DELETE "
                              "TBL %s RRN %d RC %d ",
                              bct->srcdb->tablename, rrn, rc);
                }
                if (rc == ERR_TRAN_TOO_BIG) {
                    reqerrstr(iq, COMDB2_CSTRT_RC_CASCADE,
                              "cascaded delete exceeds max writes");
                    *errout = OP_FAILED_INTERNAL + ERR_TRAN_TOO_BIG;
                } else {
                    reqerrstr(iq, COMDB2_CSTRT_RC_CASCADE,
                              "verify key constraint cannot cascade delete "
                              "table '%s' rc %d",
                              bct->srcdb->tablename, rc);
                    *errout = OP_FAILED_INTERNAL + ERR_FIND_CONSTRAINT;
                }
                close_constraint_table_cursor(cur);
                return rc;
            }
            /* here, we need to retry to verify the constraint */
            /* sub 1 to go to current constraint again */
            continue;
        } else if (upd_cascade) {
            int err = 0, idx = 0;
            unsigned long long newgenid;
            int newkeylen;

            iq->usedb = bct->srcdb;
            newkeylen = getkeysize(iq->usedb, bct->sixnum);

            if (gbl_fk_allow_superset_keys && newkeylen > bct->sixlen) {
                memcpy(bct->newkey + bct->sixlen, key + bct->sixlen,
                       newkeylen - bct->sixlen);
            } else {
                newkeylen = bct->sixlen;
            }

            if (iq->debug)
                reqpushprefixf(iq, "VERBKYCNSTRT CASCADE UPD:");
            /* TODO verify we have proper schema change locks */

            rc = upd_record(
                iq, trans, NULL,                               /*primkey*/
                rrn, genid, (const unsigned char *)ondisk_tag, /*.ONDISK_IX_0*/
                (const unsigned char *)ondisk_tag + strlen(ondisk_tag),
                (unsigned char *)bct->newkey, /*p_buf_rec*/
                (const unsigned char *)bct->newkey + newkeylen,
                NULL /*p_buf_vrec*/, NULL /*p_buf_vrec_end*/,
                NULL, /*fldnullmap*/
                NULL, /*updCols*/
                NULL, /*blobs*/
                0,    /*maxblobs*/
                &newgenid, -1ULL, -1ULL, &err, &idx, BLOCK2_UPDKL, 0, /*blkpos*/
                UPDFLAGS_CASCADE | RECFLAGS_DONT_LOCK_TBL |
                    RECFLAGS_DONT_REORDER_IDX);
            if (iq->debug)
                reqpopprefixes(iq, 1);
            iq->usedb = currdb;

            if (rc != 0) {
                if (iq->debug) {
                    reqprintf(iq,
                              "VERBKYCNSTRT CANT CASCADE UPDATE "
                              "TBL %s RRN %d RC %d ",
                              bct->srcdb->tablename, rrn, rc);
                }
                if (rc == ERR_TRAN_TOO_BIG) {
                    reqerrstr(iq, COMDB2_CSTRT_RC_CASCADE,
                              "cascaded update exceeds max writes");
                    *errout = OP_FAILED_INTERNAL + ERR_TRAN_TOO_BIG;
                } else {
                    reqerrstr(iq, COMDB2_CSTRT_RC_CASCADE,
                              "verify key constraint cannot cascade update "
                              "table '%s' rc %d",
                              bct->srcdb->tablename, rc);
                    *errout = OP_FAILED_INTERNAL + ERR_FIND_CONSTRAINT;
                }
                close_constraint_table_cursor(cur);
                return rc;
            }
            /* here, we need to retry to verify the constraint */
            continue;
        } else { /* key was not found in parent tbl and we are not cascading */
            if (iq->debug) {
                reqprintf(iq,
                          "VERBKYCNSTRT CANT RESOLVE CONSTRAINT TBL %s "
                          "IDX '%d' KEY -> TBL %s IDX '%d' ",
                          bct->dstdb->tablename, bct->dixnum,
                          bct->srcdb->tablename, bct->sixnum);
                reqdumphex(iq, bct->key, bct->sixlen);
                reqmoref(iq, " RC %d", rc);
            }
            reqerrstr(iq, COMDB2_CSTRT_RC_INVL_KEY,
                      "verify key constraint cannot resolve constraint "
                      "table '%s' index '%d' key '%s' -> table '%s' index "
                      "'%d' ",
                      bct->dstdb->tablename, bct->dixnum,
                      get_keynm_from_db_idx(bct->dstdb, bct->dixnum),
                      bct->srcdb->tablename, bct->sixnum);
            *errout = OP_FAILED_INTERNAL + ERR_FIND_CONSTRAINT;
            close_constraint_table_cursor(cur);
            return ERR_BADREQ;
        }

        /* get next record from table */
        rc = bdb_temp_table_next(thedb->bdb_env, cur, &err);
    }
    close_constraint_table_cursor(cur);
    if (rc == IX_EMPTY || rc == IX_PASTEOF) {
        return 0;
    }
    if (iq->debug)
        reqprintf(iq, "VERKYCNSTRT ERROR READING DEL TABLE");
    reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TBL,
              "verify key constraint error reading del table");
    *errout = OP_FAILED_INTERNAL;
    return ERR_INTERNAL;
}


/* this is called twice so putting here to avoid mess */
#define LIVE_SC_DELAYED_KEY_ADDS(LAST)                                         \
    do {                                                                       \
        /* its ok to fail adding to newbtree indices -- SC will abort */       \
        int lrc = live_sc_delayed_key_adds(iq, trans, genid, od_dta, ins_keys, \
                                           ondisk_size);                       \
        if (lrc == RC_INTERNAL_RETRY) {                                        \
            logmsg(LOGMSG_ERROR, "%s: deadlock add2idx genid 0x%llx\n",        \
                   __func__, genid);                                           \
            /* if we failed to add due to deadlock, need to redo */            \
            *errout = OP_FAILED_INTERNAL;                                      \
            if (!LAST)                                                         \
                close_constraint_table_cursor(cur);                            \
            return lrc;                                                        \
        } else if (lrc == ERR_NOMASTER) {                                      \
            logmsg(LOGMSG_ERROR, "%s:%d: live sc downgrading\n", __func__,     \
                   __LINE__);                                                  \
            return ERR_NOMASTER;                                               \
        } else if (lrc)                                                        \
            logmsg(LOGMSG_USER,                                                \
                   "%s:%d: ERROR: failed add2idx rc %d genid 0x%llx\n",        \
                   __func__, __LINE__, lrc, genid);                            \
    } while (0);

int delayed_key_adds(struct ireq *iq, block_state_t *blkstate, void *trans,
                     int *blkpos, int *ixout, int *errout)
{
    int rc = 0, fndlen = 0, err = 0, limit = 0;
    int idx = 0, ixkeylen = -1;
    void *od_dta = NULL;
    char ondisk_tag[MAXTAGLEN];
    char key[MAXKEYLEN];
    char *od_dta_tail = NULL;
    int od_tail_len = 0;
    char mangled_key[MAXKEYLEN];

#if DEBUG_REORDER
    logmsg(LOGMSG_DEBUG, "%s(): entering\n", __func__);
#endif

    od_dta = alloca(20 * 1024 + 8);
    if (od_dta == NULL) {
        if (iq->debug)
            reqprintf(iq, "%p:ADDKYCNSTRT FAILED MALLOC", trans);
        reqerrstr(iq, COMDB2_CSTRT_RC_ALLOC,
                  "add key constraint failed malloc");
        *errout = OP_FAILED_INTERNAL;
        *blkpos = 0;
        return ERR_INTERNAL;
    }

    struct thread_info *thdinfo = pthread_getspecific(unique_tag_key);
    if (thdinfo == NULL) {
        if (iq->debug)
            reqprintf(iq, "%p:VERKYCNSTRT CANNOT GET ADD LIST CURSOR", trans);
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_CURSOR,
                  "verify key constraint: cannot get add list cursor");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }

    void *cur = get_constraint_table_cursor(thdinfo->ct_add_table);
    if (cur == NULL) {
        if (iq->debug)
            reqprintf(iq, "%p:VERKYCNSTRT CANNOT GET ADD LIST CURSOR", trans);
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_CURSOR,
                  "verify key constraint cannot get add list cursor");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }

    *ixout = -1;
    *blkpos = -1;

    int ondisk_size = 0;
    unsigned long long genid = 0LL;
    unsigned long long cached_index_genid = genid;
    unsigned long long ins_keys = 0ULL;
    rc = bdb_temp_table_first(thedb->bdb_env, cur, &err);
    if (rc != 0) {
        close_constraint_table_cursor(cur);
        free_cached_delayed_indexes(iq);
        if (rc == IX_EMPTY) {
            if (iq->debug)
                reqprintf(iq, "%p:VERKYCNSTRT FOUND NO KEYS TO ADD", trans);
            return 0;
        }
        if (iq->debug)
            reqprintf(iq, "%p:VERKYCNSTRT CANNOT GET ADD LIST RECORD", trans);
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_REC,
                  "verify key constraint: cannot get add list record");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }

    assert(iq->idxDelete == NULL);
    assert(iq->idxInsert == NULL);
    do {
        cte *ctrq = (cte *)bdb_temp_table_data(cur);
        /* do something */
        if (ctrq == NULL) {
            if (iq->debug)
                reqprintf(iq, "%p:VERKYCNSTRT CANNOT GET ADD LIST RECORD DATA",
                          trans);
            reqerrstr(iq, COMDB2_CSTRT_RC_INVL_DTA,
                      "verify key constraint: cannot get add list record data");
            close_constraint_table_cursor(cur);
            free_cached_delayed_indexes(iq);
            *errout = OP_FAILED_INTERNAL;
            return ERR_INTERNAL;
        }
        struct forward_ct *curop = &ctrq->ctop.fwdct;
        int flags = curop->flags;
        /* Keys for records from INSERT .. ON CONFLICT DO NOTHING have
         * already been added to the indexes in add_record() to ensure
         * we don't add duplicates in the data files. We still push them
         * to ct_add_table to be able to perform cascade updates to the
         * child tables.
         */
        if (flags & OSQL_IGNORE_FAILURE || flags & OSQL_ITEM_REORDERED) {
            goto next_record;
        }

        /* only do once per genid *after* processing all idxs from tmptbl 
         * (which are in sequence for the same genid): 
         * If a key is a dup violation then we don't want SC to fail,
         * rather the UPD should fail (when processing that idx in this loop).
         * So when table cursor points to next genid, only then we can call
         * live_sc on the stored genid. 
         */
        if (genid && genid != curop->genid) {
            LIVE_SC_DELAYED_KEY_ADDS(0 /* not last */);
        }

        iq->usedb = curop->usedb;
        int addrrn = curop->rrn;
        int ixnum = curop->ixnum;
        genid = curop->genid;
        ins_keys = curop->ins_keys;

        if (addrrn == -1) {
            if (iq->debug)
                reqprintf(iq, "%p:ADDKYCNSTRT (AFPRI) FAILED, NO RRN\n", trans);
            reqerrstr(iq, COMDB2_CSTRT_RC_INVL_RRN,
                      "add key constraint failed, no rrn");
            *errout = OP_FAILED_INTERNAL + ERR_ADD_RRN;
            *blkpos = curop->blkpos;
            close_constraint_table_cursor(cur);
            free_cached_delayed_indexes(iq);
            return ERR_BADREQ;
        }

        ondisk_size = getdatsize(iq->usedb);
        if (ondisk_size == -1) {
            if (iq->debug)
                reqprintf(iq, "%p:ADDKYCNSTRT BAD TABLE %s\n", trans,
                          iq->usedb->tablename);
            reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TBL,
                      "add key constraint bad table '%s'",
                      iq->usedb->tablename);
            *blkpos = curop->blkpos;
            close_constraint_table_cursor(cur);
            free_cached_delayed_indexes(iq);
            return ERR_BADREQ;
        }
        rc = ix_find_by_rrn_and_genid_tran(iq, addrrn, genid, od_dta, &fndlen,
                                           ondisk_size, trans);

        if (rc == RC_INTERNAL_RETRY) {
            *errout = OP_FAILED_INTERNAL;
            close_constraint_table_cursor(cur);
            free_cached_delayed_indexes(iq);
            return rc;
        }

        if (fndlen != ondisk_size) {
            if (iq->debug)
                reqprintf(iq, "%p:ADDKYCNSTRT FNDLEN %d != DTALEN %d RC %d",
                          trans, fndlen, ondisk_size, rc);
            reqerrstr(iq, COMDB2_CSTRT_RC_INVL_DTA,
                      "add key constraint: record not found in table %s",
                      iq->usedb->tablename);
            *errout = OP_FAILED_INTERNAL;
            *blkpos = curop->blkpos;
            close_constraint_table_cursor(cur);
            free_cached_delayed_indexes(iq);
            return ERR_INTERNAL;
        }

        if (cached_index_genid != genid) {
            if (cache_delayed_indexes(iq, genid)) {
                logmsg(LOGMSG_ERROR, "%s failed to cache delayed indexes\n",
                        __func__);
                *errout = OP_FAILED_INTERNAL;
                close_constraint_table_cursor(cur);
                free_cached_delayed_indexes(iq);
                return ERR_INTERNAL;
            }
            cached_index_genid = genid;
        }

        if (ixnum == -1) {
            /* add key for all indexes here */
            limit = iq->usedb->nix;
        } else
            limit = 1;

        for (idx = 0; idx < limit; idx++) {
            int doidx = ixnum;
            if (ixnum == -1) {
                doidx = idx;
                /* only add keys when told */
                if (gbl_partial_indexes && iq->usedb->ix_partial &&
                    !(ins_keys & (1ULL << doidx)))
                    continue;
            }

            ixkeylen = getkeysize(iq->usedb, doidx);
            snprintf(ondisk_tag, MAXTAGLEN, ".ONDISK_IX_%d", doidx);
            if (ixkeylen < 0) {
                if (iq->debug)
                    reqprintf(iq, "%p:ADDKYCNSTRT BAD INDEX %d OR KEYLENGTH %d",
                              trans, doidx, ixkeylen);
                reqerrstr(iq, COMDB2_CSTRT_RC_INVL_IDX,
                          "add key constraint bad index %d or keylength %d",
                          doidx, ixkeylen);
                *blkpos = curop->blkpos;
                *errout = OP_FAILED_BAD_REQUEST;
                close_constraint_table_cursor(cur);
                free_cached_delayed_indexes(iq);
                return ERR_BADREQ;
            }
            /*
            rc=stag_to_stag_buf(iq->usedb->tablename, ".ONDISK",od_dta,
               ondisk_tag, key, NULL);
             */
            if (iq->idxInsert)
                rc = create_key_from_ireq(iq, doidx, 0, &od_dta_tail,
                                          &od_tail_len, mangled_key, od_dta,
                                          ondisk_size, key);
            else
                rc = create_key_from_ondisk(iq->usedb, doidx, &od_dta_tail,
                                            &od_tail_len, mangled_key,
                                            ".ONDISK", od_dta, ondisk_size,
                                            ondisk_tag, key, NULL, iq->tzname);
            if (rc == -1) {
                if (iq->debug)
                    reqprintf(iq, "%p:ADDKYCNSTRT CANT FORM INDEX %d", trans,
                              ixnum);
                reqerrstr(iq, COMDB2_CSTRT_RC_INVL_IDX,
                          "add key constraint cannot form index %d", ixnum);
                *blkpos = curop->blkpos;
                *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                *ixout = doidx;
                close_constraint_table_cursor(cur);
                free_cached_delayed_indexes(iq);
                return ERR_CONVERT_IX;
            }

            /* light the prefault kill bit for this subop - newkeys */
            prefault_kill_bits(iq, doidx, PFRQ_NEWKEY);

            /* add the key */
            rc = ix_addk(iq, trans, key, doidx, genid, addrrn, od_dta_tail,
                         od_tail_len, ix_isnullk(iq->usedb, key, doidx));

            if (iq->debug) {
                reqprintf(iq, "%p:ADDKYCNSTRT  TBL %s IX %d RRN %d KEY ", trans,
                          iq->usedb->tablename, doidx, addrrn);
                reqdumphex(iq, key, ixkeylen);
                reqmoref(iq, " RC %d", rc);
            }

            if (rc == IX_DUP) {
                if ((flags & OSQL_FORCE_VERIFY) != 0) {
                    *errout = OP_FAILED_VERIFY;
                    rc = ERR_VERIFY;
                } else {
                    reqerrstr(iq, COMDB2_CSTRT_RC_DUP,
                              "add key constraint duplicate key '%s' on table "
                              "'%s' index %d",
                              get_keynm_from_db_idx(iq->usedb, doidx),
                              iq->usedb->tablename, doidx);
                    *errout = OP_FAILED_UNIQ;
                }

                *blkpos = curop->blkpos;
                *ixout = doidx;
                close_constraint_table_cursor(cur);
                free_cached_delayed_indexes(iq);
                return rc;
            } else if (rc != 0) {
                reqerrstr(iq, COMDB2_CSTRT_RC_INTL_ERR,
                          "add key berkley error for key '%s' on index %d",
                          get_keynm_from_db_idx(iq->usedb, doidx), doidx);

                *blkpos = curop->blkpos;
                *errout = OP_FAILED_INTERNAL;
                *ixout = doidx;
                close_constraint_table_cursor(cur);
                free_cached_delayed_indexes(iq);

                if (ERR_INTERNAL == rc) {
                    /* Exit & have the cluster elect another master */
                    if (gbl_exit_on_internal_error) {
                        exit(1);
                    }

                    rc = ERR_NOMASTER;
                }
                return rc;
            }
        } /* for each index */

    next_record:
        /* get next record from table */
        rc = bdb_temp_table_next(thedb->bdb_env, cur, &err);
    } while (rc == 0);

    close_constraint_table_cursor(cur);

    if (rc == IX_EMPTY || rc == IX_PASTEOF) {
        if (cached_index_genid != genid) {
            if (cache_delayed_indexes(iq, genid)) {
                logmsg(LOGMSG_ERROR, "%s failed to cache delayed indexes\n",
                        __func__);
                *errout = OP_FAILED_INTERNAL;
                close_constraint_table_cursor(cur); // AZ: this is wrong!?
                return ERR_INTERNAL;
            }
            cached_index_genid = genid;
        }
        LIVE_SC_DELAYED_KEY_ADDS(1); /* if no error, process last genid */
        free_cached_delayed_indexes(iq);
        return 0;
    }
    if (iq->debug)
        reqprintf(iq, "%p:ADDKYCNSTRT ERROR READING ADD TABLE", trans);
    reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TBL,
              "add key constraint error reading add table");
    *errout = OP_FAILED_INTERNAL;
    free_cached_delayed_indexes(iq);
    return ERR_INTERNAL;
}

/* go through all entries in ct_add_table and verify that
 * the key exists in the parent table if there are constraints */
int verify_add_constraints(struct ireq *iq, block_state_t *blkstate,
                           void *trans, int *errout)
{
    int rc = 0, fndrrn = 0, opcode = 0, err = 0;
    void *od_dta = NULL;
    char ondisk_tag[MAXTAGLEN];
    char key[MAXKEYLEN];
    int nulls;

    od_dta = (void *)alloca(20 * 1024 + 8);
    if (od_dta == NULL) {
        if (iq->debug)
            reqprintf(iq, "VERKYCNSTRT ERROR IN MALLOC");
        reqerrstr(iq, COMDB2_CSTRT_RC_ALLOC,
                  "verify key constraint error in malloc");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }

    struct thread_info *thdinfo = pthread_getspecific(unique_tag_key);
    if (thdinfo == NULL) {
        if (iq->debug)
            reqprintf(iq, "VERKYCNSTRT CANNOT GET ADD LIST CURSOR");
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_CURSOR,
                  "verify constraint cannot get add list cursor");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }

    void *cur = get_constraint_table_cursor(thdinfo->ct_add_table);
    if (cur == NULL) {
        if (iq->debug)
            reqprintf(iq, "VERKYCNSTRT CANNOT GET ADD LIST CURSOR");
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_CURSOR,
                  "verify constraint cannot get add list cursor");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }

    rc = bdb_temp_table_first(thedb->bdb_env, cur, &err);
    if (rc != 0) {
        close_constraint_table_cursor(cur);
        if (rc == IX_EMPTY) {
            if (iq->debug)
                reqprintf(iq, "VERKYCNSTRT FOUND NOTHING TO VERIFY ON ADD");
            return 0;
        }
        if (iq->debug)
            reqprintf(iq, "VERKYCNSTRT CANNOT GET ADD LIST RECORD");
        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_REC,
                  "verify key constraint error getting finding in add list");
        *errout = OP_FAILED_INTERNAL;
        return ERR_INTERNAL;
    }
    unsigned long long genid = 0ULL;
    unsigned long long cached_index_genid = 0ULL;
    unsigned long long ins_keys = 0ULL;
    while (rc == 0) {
        cte *ctrq = (cte *)bdb_temp_table_data(cur);
        struct forward_ct *curop = NULL;
        int ixnum = -1;
        int ondisk_size = 0;
        /* do something */
        if (ctrq == NULL) {
            if (iq->debug)
                reqprintf(iq, "VERKYCNSTRT CANNOT GET ADD LIST RECORD DATA");
            reqerrstr(iq, COMDB2_CSTRT_RC_INVL_REC,
                      "verify key constraint cannot get add list record data");
            *errout = OP_FAILED_INTERNAL;
            close_constraint_table_cursor(cur);
            return ERR_INTERNAL;
        }
        /*    fprintf(stderr, "%d %d %s\n", ctrq->ct_type,
         * ctrq->ctop.fwdct.optype,ctrq->ctop.fwdct.usedb->tablename);*/
        curop = &ctrq->ctop.fwdct;

        /* Only do once per genid --
         * (Same as LIVE_SC_DELAYED_KEY_ADDS in delayed_key_adds) */
        if (genid && genid != curop->genid) {
            verify_schema_change_constraint(iq, trans, genid, od_dta, ins_keys);
        }

        iq->usedb = curop->usedb;
        ixnum = curop->ixnum;
        genid = curop->genid;
        ins_keys = curop->ins_keys;
        opcode = curop->optype;
        /* if we are updating by key, check the constraints as if we're doing a
         * normal update */
        if (opcode == BLOCK2_UPDBYKEY)
            opcode = BLOCK2_UPDKL;
        switch (opcode) {
        case BLOCK_ADDSL:
        case BLOCK2_ADDDTA:
        case BLOCK2_ADDKL:
        case BLOCK2_ADDKL_POS:
        case BLOCK2_UPDKL:
        case BLOCK2_UPDKL_POS:
        case BLOCK2_UPDATE:
        case BLOCK_UPVRRN: {
            int nct = curop->usedb->n_constraints;
            int cidx = 0;
            if (opcode == BLOCK_ADDSL && ixnum != 0) {
                break;
            }
            ondisk_size = getdatsize(iq->usedb);
            if (ondisk_size == -1) {
                if (iq->debug)
                    reqprintf(iq, "VERKYCNSTRT BAD TABLE %s\n",
                              iq->usedb->tablename);
                reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TBL,
                          "verify key constraint bad table '%s'",
                          iq->usedb->tablename);
                *errout = OP_FAILED_BAD_REQUEST;
                rc = ERR_BADREQ;
                close_constraint_table_cursor(cur);
                return rc;
            }

            if (cached_index_genid != curop->genid) {
                if (cache_delayed_indexes(iq, curop->genid)) {
                    logmsg(LOGMSG_ERROR, "%s failed to cache delayed indexes\n",
                           __func__);
                    *errout = OP_FAILED_INTERNAL;
                    close_constraint_table_cursor(cur);
                    return ERR_INTERNAL;
                }
                cached_index_genid = curop->genid;
            }

            /* load original row in od_dta, needed to form the indices */
            int fndlen = 0;
            int addrrn = curop->rrn;
            rc = ix_find_by_rrn_and_genid_tran(iq, addrrn, genid, od_dta,
                                               &fndlen, ondisk_size, trans);

            if (rc) {
                *errout = OP_FAILED_INTERNAL;
                close_constraint_table_cursor(cur);

                if (rc == RC_INTERNAL_RETRY)
                    return rc;

                if (iq->debug)
                    reqprintf(iq,
                              "VERKYCNSTRT CASCADE DELETED GENID 0x%llx "
                              "FNDLEN %d DTALEN %d RC %d",
                              genid, fndlen, ondisk_size, rc);
                reqerrstr(iq, COMDB2_CSTRT_RC_INVL_DTA,
                          "verify key constraint: record not found in table %s "
                          "(cascaded)",
                          iq->usedb->tablename);
                return ERR_INTERNAL;
            }

            for (cidx = 0; cidx < nct; cidx++) {
                constraint_t *ct = &curop->usedb->constraints[cidx];
                int ridx = 0, lixnum = -1;
                char lkey[MAXKEYLEN];

                rc = getidxnumbyname(iq->usedb->tablename, ct->lclkeyname,
                                     &lixnum);
                if (rc) {
                    if (iq->debug)
                        reqprintf(iq, "VERKYCNSTRT: UNKNOWN LCL KEYTAG %s",
                                  ct->lclkeyname);
                    reqerrstr(iq, COMDB2_CSTRT_RC_INVL_KEY,
                              "verify constraint: unknown local keytag '%s'",
                              ct->lclkeyname);
                    *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                    free_cached_delayed_indexes(iq);
                    close_constraint_table_cursor(cur);
                    return ERR_CONVERT_IX;
                }

                /* only verify keys when told */
                if (gbl_partial_indexes && iq->usedb->ix_partial &&
                    !(ins_keys & (1ULL << lixnum))) {
                    continue;
                }

                snprintf(ondisk_tag, MAXTAGLEN, ".ONDISK_IX_%d", lixnum);
                /*
                rc=stag_to_stag_buf(iq->usedb->tablename, ".ONDISK",od_dta,
                   ondisk_tag, lkey, NULL);
                 */
                if (iq->idxInsert) {
                    rc = create_key_from_ireq(iq, lixnum, 0, NULL, NULL, NULL,
                                              od_dta, 0 /* not needed */, lkey);
                } else
                    rc = create_key_from_ondisk(
                        iq->usedb, lixnum, NULL, NULL, NULL, ".ONDISK", od_dta,
                        0 /* not needed */, ondisk_tag, lkey, NULL, NULL);

                if (rc == -1) {
                    if (iq->debug)
                        reqprintf(iq,
                                  "VERKYCNSTRT CANT FORM TBL %s INDEX %d (%s)",
                                  iq->usedb->tablename, lixnum, ct->lclkeyname);
                    reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TBL,
                              "verify key constraint cannot form table '%s' "
                              "index %d ('%s')",
                              iq->usedb->tablename, lixnum, ct->lclkeyname);
                    *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                    free_cached_delayed_indexes(iq);
                    close_constraint_table_cursor(cur);
                    return ERR_CONVERT_IX;
                }

                for (ridx = 0; ridx < ct->nrules; ridx++) {
                    struct dbtable *ftable = NULL, *currdb = NULL;
                    int fixnum = 0;
                    int fixlen = 0;
                    char fkey[MAXKEYLEN];
                    char fondisk_tag[MAXTAGLEN];

                    ftable = get_dbtable_by_name(ct->table[ridx]);
                    if (ftable == NULL) {
                        if (iq->debug)
                            reqprintf(iq, "VERKYCNSTRT BAD TABLE %s\n",
                                      ftable->tablename);
                        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TBL,
                                  "verify key constraint bad table '%s'",
                                  ftable->tablename);
                        *errout = OP_FAILED_BAD_REQUEST;
                        free_cached_delayed_indexes(iq);
                        close_constraint_table_cursor(cur);
                        return ERR_BADREQ;
                    }
                    rc = getidxnumbyname(ftable->tablename, ct->keynm[ridx],
                                         &fixnum);
                    if (rc) {
                        if (iq->debug)
                            reqprintf(iq, "VERKYCNSTRT: UNKNOWN KEYTAG %s",
                                      ct->keynm[ridx]);
                        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_KEY,
                                  "verify key constraint: unknown keytag '%s'",
                                  ct->keynm[ridx]);
                        *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                        free_cached_delayed_indexes(iq);
                        close_constraint_table_cursor(cur);
                        return ERR_CONVERT_IX;
                    }
                    fixlen = getkeysize(ftable, fixnum);

                    /*snprintf(ondisk_tag, MAXTAGLEN, ".ONDISK_IX_%d", fixnum);
                      rc=stag_to_stag_bufx(iq->usedb->tablename,
                      ".ONDISK",od_dta,
                      ftable->tablename, ondisk_tag, fkey);*/

                    snprintf(fondisk_tag, MAXTAGLEN, ".ONDISK_IX_%d", fixnum);
                    fixlen = rc = stag_to_stag_buf_ckey(
                        iq->usedb->tablename, ondisk_tag, lkey,
                        ftable->tablename, fondisk_tag, fkey, &nulls, FK2PK);
                    if (rc == -1) {
                        if (iq->debug)
                            reqprintf(
                                iq,
                                "VERKYCNSTRT CANT FORM TBL %s INDEX %d (%s)",
                                ftable->tablename, fixnum, ct->keynm[ridx]);
                        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TBL,
                                  "verify key constraint cannot form table "
                                  "'%s' index %d (%s)",
                                  ftable->tablename, fixnum, ct->keynm[ridx]);
                        *errout = OP_FAILED_INTERNAL + ERR_FORM_KEY;
                        free_cached_delayed_indexes(iq);
                        close_constraint_table_cursor(cur);
                        return ERR_CONVERT_IX;
                    }

                    if (ftable->ix_collattr[fixnum]) {
                        rc = extract_decimal_quantum(ftable, fixnum, fkey, NULL,
                                                   0, NULL);
                        if (rc) {
                            abort(); /* called doesn't return error for these
                                        arguments, at least not now */
                        }
                    }

                    /*     int ftblsz = getdatsize(ftable);
                     *     fprintf(stderr, "%s;%d-%s;%d\n",ftable->tablename,
                           ftblsz, ct->keynm[ridx], fixlen);*/

                    /* we'll do the find on created index to make sure
                       constraint is satisfied */
                    currdb = iq->usedb;
                    iq->usedb = ftable;

                    if (should_skip_constraint_for_index(iq, fixnum, nulls))
                        rc = IX_FND;
                    else
                        rc = ix_find_by_key_tran(iq, fkey, fixlen, fixnum, key,
                                                 &fndrrn, &genid, NULL, NULL, 0,
                                                 trans);

                    iq->usedb = currdb;

                    if (rc == RC_INTERNAL_RETRY) {
                        *errout = OP_FAILED_INTERNAL;
                        free_cached_delayed_indexes(iq);
                        close_constraint_table_cursor(cur);
                        return rc;
                    }

                    if (rc != IX_FND && rc != IX_FNDMORE) {
                        if (iq->debug) {
                            reqprintf(iq, "VERKYCNSTRT CANT RESOLVE CONSTRAINT "
                                          "TBL %s IDX '%s' KEY ",
                                      ftable->tablename, ct->keynm[ridx]);
                            reqdumphex(iq, fkey, fixlen);
                            reqmoref(iq, " RC %d", rc);
                        }
                        reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TBL,
                                  "verify key constraint cannot resolve "
                                  "constraint table '%s' index '%s'",
                                  ftable->tablename, ct->keynm[ridx]);
                        *errout = OP_FAILED_INTERNAL + ERR_FIND_CONSTRAINT;
                        free_cached_delayed_indexes(iq);
                        close_constraint_table_cursor(cur);
                        return ERR_BADREQ;
                    }
                    if (iq->debug) {
                        reqprintf(
                            iq,
                            "VERKYCNSTRT VERIFIED RC=%d %s:%s AGAINST %s:%s",
                            rc, iq->usedb->tablename, ct->lclkeyname,
                            ct->table[ridx], ct->keynm[ridx]);
                    }
                }
            }
        } break;
        case BLOCK2_DELKL:
            logmsg(LOGMSG_USER, "keyless del\n");
            break;
        case BLOCK2_DELDTA:
            logmsg(LOGMSG_USER, "keyless deldta\n");
            break;
        default:
            logmsg(LOGMSG_USER, "unhandled %d\n", opcode);
            break;
        }
        /* get next record from table */
        rc = bdb_temp_table_next(thedb->bdb_env, cur, &err);
    }
    close_constraint_table_cursor(cur);

    if (rc == IX_EMPTY || rc == IX_PASTEOF) {
        verify_schema_change_constraint(iq, trans, genid, od_dta, ins_keys);
        free_cached_delayed_indexes(iq);
        return 0;
    }

    free_cached_delayed_indexes(iq);
    if (iq->debug)
        reqprintf(iq, "VERKYCNSTRT ERROR READING ADD TABLE");
    reqerrstr(iq, COMDB2_CSTRT_RC_INVL_TBL,
              "verify key constrait error reading add table");
    *errout = OP_FAILED_INTERNAL;
    return ERR_INTERNAL;
}

void dump_all_constraints(struct dbenv *env)
{
    int i = 0;
    for (i = 0; i < env->num_dbs; i++) {
        dump_constraints(env->dbs[i]);
    }
    for (i = 0; i < env->num_dbs; i++) {
        dump_rev_constraints(env->dbs[i]);
    }
}

void dump_rev_constraints(struct dbtable *table)
{
    int i = 0;
    logmsg(LOGMSG_USER, "TABLE '%s' HAS %d REVSE CONSTRAINTS\n",
           table->tablename, table->n_rev_constraints);
    for (i = 0; i < table->n_rev_constraints; i++) {
        constraint_t *ct = table->rev_constraints[i];
        int j = 0;
        logmsg(LOGMSG_USER, "(%d)REV CONSTRAINT TBL: '%s' KEY '%s'  CSCUPD: %c "
                            "CSCDEL: %c #RULES %d:\n",
               i + 1, ct->lcltable->tablename, ct->lclkeyname,
               ((ct->flags & CT_UPD_CASCADE) == CT_UPD_CASCADE) ? 'T' : 'F',
               ((ct->flags & CT_DEL_CASCADE) == CT_DEL_CASCADE) ? 'T' : 'F',
               ct->nrules);
        for (j = 0; j < ct->nrules; j++) {
            logmsg(LOGMSG_USER, "  -> TBL '%s' KEY '%s'\n", ct->table[j],
                    ct->keynm[j]);
        }
    }
    logmsg(LOGMSG_USER, "\n");
}

void dump_constraints(struct dbtable *table)
{
    int i = 0;
    logmsg(LOGMSG_USER, "TABLE '%s' HAS %d CONSTRAINTS\n", table->tablename,
           table->n_constraints);
    for (i = 0; i < table->n_constraints; i++) {
        constraint_t *ct = &table->constraints[i];
        int j = 0;
        logmsg(LOGMSG_USER, 
                "(%d)CONSTRAINT KEY '%s'  CSCUPD: %c CSCDEL: %c #RULES %d:\n",
                i + 1, ct->lclkeyname,
                ((ct->flags & CT_UPD_CASCADE) == CT_UPD_CASCADE) ? 'T' : 'F',
                ((ct->flags & CT_DEL_CASCADE) == CT_DEL_CASCADE) ? 'T' : 'F',
                ct->nrules);
        for (j = 0; j < ct->nrules; j++) {
            logmsg(LOGMSG_USER, "  -> TBL '%s' KEY '%s'\n", ct->table[j],
                    ct->keynm[j]);
        }
    }
    logmsg(LOGMSG_USER, "\n");
}

int delete_constraint_table(void *table)
{
    int bdberr = 0;
    int rc = 0;
    if (table == NULL)
        return rc;
    rc = bdb_temp_table_close(thedb->bdb_env, table, &bdberr);
    return rc;
}

int truncate_constraint_table(void *table)
{
    int bdberr = 0;
    int rc = 0;
    if (table == NULL)
        return rc;
    rc = bdb_temp_table_truncate(thedb->bdb_env, table, &bdberr);
    return rc;
}

int clear_constraints_tables(void)
{
    struct thread_info *thdinfo = pthread_getspecific(unique_tag_key);
    if (thdinfo == NULL)
        return -1;

    truncate_constraint_table(thdinfo->ct_add_table);
    truncate_constraint_table(thdinfo->ct_del_table);
    truncate_constraint_table(thdinfo->ct_add_index);
    truncate_defered_index_tbl();

    return 0;
}

static int constraint_key_cmp(void *usermem, int key1len, const void *key1,
                              int key2len, const void *key2)
{
    const int *k1_type = (const int *)key1;
    const int *k2_type = (const int *)key2;
    unsigned long long k1_id;
    unsigned long long k2_id;

    memcpy(&k1_id, (char *)key1 + sizeof(int), sizeof(unsigned long long));
    memcpy(&k2_id, (char *)key2 + sizeof(int), sizeof(unsigned long long));

    assert(key1len == sizeof(int) + sizeof(long long));
    assert(key2len == sizeof(int) + sizeof(long long));

    if (*k1_type < *k2_type) {
        return -1;
    }

    if (*k1_type > *k2_type) {
        return 1;
    }

    if (k1_id < k2_id) {
        return -1;
    }

    if (k1_id > k2_id) {
        return 1;
    }

    return 0;
}

static int constraint_index_key_cmp(void *usermem, int key1len,
                                    const void *key1, int key2len,
                                    const void *key2)
{
    unsigned long long k1_genid;
    unsigned long long k2_genid;
    int k1_ixnum;
    int k2_ixnum;

    assert(key1len == sizeof(unsigned long long) + sizeof(int));
    assert(key2len == sizeof(unsigned long long) + sizeof(int));

    memcpy(&k1_genid, (char *)key1, sizeof(unsigned long long));
    memcpy(&k2_genid, (char *)key2, sizeof(unsigned long long));

    k1_ixnum = *((int *)((char *)key1 + sizeof(unsigned long long)));
    k2_ixnum = *((int *)((char *)key2 + sizeof(unsigned long long)));

    if (k1_genid < k2_genid) {
        return -1;
    }

    if (k1_genid > k2_genid) {
        return 1;
    }

    if (k1_ixnum < k2_ixnum) {
        return -1;
    }

    if (k1_ixnum > k2_ixnum) {
        return 1;
    }

    return 0;
}

void *create_constraint_table()
{
    struct temp_table *newtbl = NULL;
    int bdberr = 0;
    newtbl = (struct temp_table *)bdb_temp_list_create(thedb->bdb_env, &bdberr);
    if (newtbl == NULL || bdberr != 0) {
        logmsg(LOGMSG_ERROR, "failed to create temp table err %d\n", bdberr);
        return NULL;
    }
    bdb_temp_table_set_cmp_func(newtbl, constraint_key_cmp);
    return newtbl;
}

void *create_constraint_index_table()
{
    struct temp_table *newtbl = NULL;
    int bdberr = 0;
    newtbl =
        (struct temp_table *)bdb_temp_table_create(thedb->bdb_env, &bdberr);
    if (newtbl == NULL || bdberr != 0) {
        logmsg(LOGMSG_ERROR, "failed to create temp table err %d\n", bdberr);
        return NULL;
    }
    bdb_temp_table_set_cmp_func(newtbl, constraint_index_key_cmp);
    return newtbl;
}

inline void *get_constraint_table_cursor(void *table)
{
    if (table == NULL)
        abort();
    int err = 0;
    struct temp_cursor *cur =
        bdb_temp_table_cursor(thedb->bdb_env, table, NULL, &err);
    if (!cur) {
        logmsg(LOGMSG_ERROR, "Can't create cursor err=%d\n", err);
    }
    return cur;
}

inline int close_constraint_table_cursor(void *cursor)
{
    int err = 0, rc = 0;
    rc = bdb_temp_table_close_cursor(thedb->bdb_env, cursor, &err);
    if (rc != 0)
        return -1;
    return 0;
}

static char *get_temp_ct_dbname(long long *ctid)
{
    char *s;
    size_t buflen = strlen(thedb->basedir) + 64;
    s = malloc(buflen);
    snprintf(s, buflen, "%s/%s.tmpdbs/_temp_ct_%" PRIdPTR "_%llu.db",
             thedb->basedir, thedb->envname, (intptr_t)pthread_self(), *ctid);
    *ctid = *ctid + 1LL;
    return s;
}

static inline int is_delete_op(int op)
{
    if (op == BLOCK2_DELKL || op == BLOCK2_DELDTA || op == BLOCK_DELSC)
        return 1;
    return 0;
}

static inline int is_update_op(int op)
{
    if (op == BLOCK2_UPDKL || op == BLOCK2_UPDKL_POS || op == BLOCK2_UPDATE ||
        op == BLOCK_UPVRRN)
        return 1;
    return 0;
}

int find_constraint(struct dbtable *db, constraint_t *ct)
{
    int i = 0;
    for (i = 0; i < db->n_constraints; i++) {
        int j = 0;
        if (strcasecmp(ct->lclkeyname, db->constraints[i].lclkeyname))
            continue;
        if (db->constraints[i].nrules < ct->nrules)
            continue;
        for (j = 0; j < ct->nrules; j++) {
            int k = 0, found = 0;
            for (k = 0; k < db->constraints[i].nrules; k++) {
                if (strcasecmp(ct->table[j], db->constraints[i].table[k]))
                    continue;
                if (strcasecmp(ct->keynm[j], db->constraints[i].keynm[k]))
                    continue;
                found = 1;
                break;
            }
            if (!found)
                break;
        }
        if (j == ct->nrules)
            return 1;
    }
    return 0;
}

static inline int constraint_key_check(struct schema *fky, struct schema *bky)
{
    if (!fky || !bky)
        return 0;

    if (gbl_fk_allow_prefix_keys && gbl_fk_allow_superset_keys)
        return 0;

    if (gbl_fk_allow_prefix_keys) {
        if (fky->nmembers > bky->nmembers)
            return -1;
        else
            return 0;
    }

    if (gbl_fk_allow_superset_keys) {
        if (bky->nmembers > fky->nmembers)
            return -1;
        else
            return 0;
    }

    if (fky->nmembers != bky->nmembers)
        return -1;

    return 0;
}

static inline struct dbtable *get_newer_db(struct dbtable *db,
                                           struct dbtable *new_db)
{
    if (new_db && strcasecmp(db->tablename, new_db->tablename) == 0) {
        return new_db;
    } else {
        return db;
    }
}

static void constraint_err(struct schema_change_type *s, struct dbtable *db,
                           constraint_t *ct, int rule, const char *err)
{
    if (s && s->iq) {
        reqerrstr(s->iq, ERR_SC,
                  "constraint error for table \"%s\" key \"%s\" -> "
                  "<\"%s\":\"%s\">: %s",
                  db->tablename, ct->lclkeyname, ct->table[rule],
                  ct->keynm[rule], err);
    } else
        logmsg(LOGMSG_ERROR,
               "constraint error for table \"%s\" key \"%s\" -> "
               "<\"%s\":\"%s\">: %s\n",
               db->tablename, ct->lclkeyname, ct->table[rule], ct->keynm[rule],
               err);
}

static inline int key_has_expressions_members(struct schema *key)
{
    int i;
    for (i = 0; i < key->nmembers; i++) {
        if (key->member[i].idx < 0)
            return 1;
    }
    return 0;
}

/* Verify that the tables and keys referred to by this table's constraints all
 * exist & have the correct column count.  If they don't it's a bit of a show
 * stopper. */
int verify_constraints_exist(struct dbtable *from_db, struct dbtable *to_db,
                             struct dbtable *new_db,
                             struct schema_change_type *s)
{
    int ii, jj;
    char keytag[MAXTAGLEN];
    struct schema *bky, *fky;
    int n_errors = 0;

    if (!from_db) {
        for (ii = 0; ii < thedb->num_dbs; ii++) {
            from_db = get_newer_db(thedb->dbs[ii], new_db);
            n_errors += verify_constraints_exist(
                from_db, from_db == to_db ? NULL : to_db, new_db, s);
        }
        return n_errors;
    }

    for (ii = 0; ii < from_db->n_constraints; ii++) {
        constraint_t *ct = &from_db->constraints[ii];

        if (from_db == new_db) {
            snprintf(keytag, sizeof(keytag), ".NEW.%s", ct->lclkeyname);
        } else {
            snprintf(keytag, sizeof(keytag), "%s", ct->lclkeyname);
        }
        if (!(fky = find_tag_schema(from_db->tablename, keytag))) {
            /* Referencing a nonexistent key */
            constraint_err(s, from_db, ct, 0, "local key not found");
            n_errors++;
        }
        if (from_db->ix_expr && key_has_expressions_members(fky) &&
            (ct->flags & CT_UPD_CASCADE)) {
            constraint_err(s, from_db, ct, 0,
                           "no update cascade on expression indexes");
            n_errors++;
        }
        for (jj = 0; jj < ct->nrules; jj++) {
            struct dbtable *rdb;

            /* If we have a target table (to_db) only look at rules pointing
             * to that table. */
            if (to_db && strcasecmp(ct->table[jj], to_db->tablename) != 0)
                continue;

            rdb = get_dbtable_by_name(ct->table[jj]);
            if (rdb)
                rdb = get_newer_db(rdb, new_db);
            else if (strcasecmp(ct->table[jj], from_db->tablename) == 0)
                rdb = from_db;
            if (!rdb) {
                /* Referencing a non-existent table */
                constraint_err(s, from_db, ct, jj, "foreign table not found");
                n_errors++;
                continue;
            } else {
                if (timepart_is_shard(rdb->tablename, (!s || !s->views_locked),
                                      NULL)) {
                    constraint_err(s, from_db, ct, jj,
                                   "foreign table is a shard");
                    n_errors++;
                    continue;
                }
            }
            if (rdb == new_db) {
                snprintf(keytag, sizeof(keytag), ".NEW.%s", ct->keynm[jj]);
            } else {
                snprintf(keytag, sizeof(keytag), "%s", ct->keynm[jj]);
            }
            if (!(bky = find_tag_schema(ct->table[jj], keytag))) {
                /* Referencing a nonexistent key */
                constraint_err(s, from_db, ct, jj, "foreign key not found");
                n_errors++;
            }

            if (constraint_key_check(fky, bky)) {
                /* Invalid constraint index */
                constraint_err(s, from_db, ct, jj, "invalid number of columns");
                n_errors++;
            }
        }
    }

    return n_errors;
}

/* creates a reverse constraint in the referenced table for each of the db's
 * constraint rules, if the referenced table already has the constraint a
 * duplicate is not added
 * this func also does a lot of verifications
 * returns the number of erorrs encountered */
int populate_reverse_constraints(struct dbtable *db)
{
    int ii, n_errors = 0;

    for (ii = 0; ii < db->n_constraints; ii++) {
        int jj = 0;
        constraint_t *cnstrt = &db->constraints[ii];
        struct schema *sc = NULL;

        sc = find_tag_schema(db->tablename, cnstrt->lclkeyname);
        if (sc == NULL) {
            ++n_errors;
            logmsg(LOGMSG_ERROR,
                   "constraint error: key %s is not found in table %s\n",
                   cnstrt->lclkeyname, db->tablename);
        }

        for (jj = 0; jj < cnstrt->nrules; jj++) {
            struct dbtable *cttbl = NULL;
            struct schema *sckey = NULL;
            int rcidx = 0, dupadd = 0;
            cttbl = get_dbtable_by_name(cnstrt->table[jj]);
            if (cttbl == NULL &&
                strcasecmp(cnstrt->table[jj], db->tablename) == 0)
                cttbl = db;

            if (cttbl == NULL) {
                ++n_errors;
                logmsg(LOGMSG_ERROR, "constraint error for key %s: table %s is not found\n",
                       cnstrt->lclkeyname, cnstrt->table[jj]);
                continue;
            }

            sckey = find_tag_schema(cnstrt->table[jj], cnstrt->keynm[jj]);
            if (sckey == NULL) {
                ++n_errors;
                logmsg(LOGMSG_ERROR, "constraint error for key %s: key %s is not found in "
                       "table %s\n",
                       cnstrt->lclkeyname, cnstrt->keynm[jj],
                       cnstrt->table[jj]);
                continue;
            }

            for (rcidx = 0; rcidx < cttbl->n_rev_constraints; rcidx++) {
                if (cttbl->rev_constraints[rcidx] == cnstrt) {
                    dupadd = 1;
                    break;
                }
            }
            if (dupadd)
                continue;

            if (cttbl->n_rev_constraints >= MAXCONSTRAINTS) {
                ++n_errors;
               logmsg(LOGMSG_ERROR, "constraint error for table %s. too many reverse "
                       "constraints!\n",
                       cnstrt->table[jj]);
                continue;
            }

            cttbl->rev_constraints[cttbl->n_rev_constraints++] = cnstrt;
        }
    }

    return n_errors;
}
