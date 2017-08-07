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

#include <memory_sync.h>
#include "schemachange.h"
#include "sc_callbacks.h"
#include "sc_global.h"
#include "sc_add_table.h"
#include "sc_schema.h"
#include "sc_util.h"
#include "sc_lua.h"
#include "sc_queues.h"
#include "translistener.h"
#include "views.h"
#include "logmsg.h"
#include "bdb_net.h"

static int set_genid_format(bdb_state_type *bdb_state, scdone_t type)
{
    int bdberr, rc;
    switch (type) {
    case (genid48_enable):
        bdb_genid_set_format(bdb_state, LLMETA_GENID_48BIT);
        break;
    case (genid48_disable):
        bdb_genid_set_format(bdb_state, LLMETA_GENID_ORIGINAL);
        break;
    }
    return 0;
}

static int reload_rowlocks(bdb_state_type *bdb_state, scdone_t type)
{
    int bdberr, rc;
    rc = bdb_reload_rowlocks(bdb_state, type, &bdberr);
    switch (gbl_rowlocks) {
    case 0:
    case 1: gbl_sql_tranlevel_default = gbl_sql_tranlevel_preserved; break;
    case 2:
        gbl_sql_tranlevel_preserved = gbl_sql_tranlevel_default;
        gbl_sql_tranlevel_default = SQL_TDEF_SNAPISOL;
        break;
    }
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: bdb_llog_rowlocks returns %d bdberr=%d\n",
               __func__, rc, bdberr);
    }
    return rc;
}

/* if genid <= sc_genids[stripe] then schemachange has already processed up to
 * that point */
int is_genid_right_of_stripe_pointer(bdb_state_type *bdb_state,
                                     unsigned long long genid,
                                     unsigned long long stripe_ptr)
{
    return bdb_inplace_cmp_genids(bdb_state, genid, stripe_ptr) > 0;
}

/* delete from new btree when genid is older than schemachange position
 */
int live_sc_post_delete_int(struct ireq *iq, void *trans,
                            unsigned long long genid, const void *old_dta,
                            unsigned long long del_keys,
                            blob_buffer_t *oldblobs)
{
    struct dbtable *usedb = iq->usedb;

    iq->usedb = usedb->sc_to;
    if (iq->debug) {
        reqpushprefixf(iq, "live_sc_post_delete_int: ");
        reqprintf(iq, "deleting genid 0x%llx from new table", genid);
    }

    /*
       fprintf(stderr, "live 0x%llx cursor 0x%llx :: live is"
       " behind cursor - DELETE\n", genid, sc_genids[stripe]);
     */

    int rc = del_new_record(iq, trans, genid, del_keys, old_dta, oldblobs);
    iq->usedb = usedb;
    if (rc != 0 && rc != RC_INTERNAL_RETRY) {
        /* Leave this trace in.  We want to know if live schema change
         * is interfering with real updates. */
        logmsg(LOGMSG_ERROR,
               "live_sc_post_delete rcode %d for delete genid 0x%llx\n", rc,
               genid);
        /* If this goes wrong then abort the schema change. */
        logmsg(LOGMSG_ERROR,
               "Aborting schema change due to unexpected error\n");
        gbl_sc_abort = 1;
        MEMORY_SYNC;
    } else if (rc == 0) {
        (iq->sc_deletes)++;
    }
    if (iq->debug) {
        reqpopprefixes(iq, 1);
    }
    return rc;
}

/* re-compute new partial/expressions indexes for new table */
static unsigned long long revalidate_new_indexes(struct ireq *iq, struct dbtable *db,
                                                 uint8_t *new_dta,
                                                 blob_buffer_t *blobs,
                                                 size_t maxblobs)
{
    extern int gbl_partial_indexes;
    extern int gbl_expressions_indexes;
    void free_cached_idx(uint8_t * *cached_idx);
    unsigned long long ins_keys = -1ULL;
    if ((gbl_partial_indexes && db->ix_partial) ||
        (gbl_expressions_indexes && db->ix_expr)) {
        int ixnum;
        int rebuild_keys = 0;
        if (!gbl_use_plan || !db->plan)
            rebuild_keys = 1;
        else {
            for (ixnum = 0; ixnum < db->nix; ixnum++) {
                if (db->plan->ix_plan[ixnum] == -1) {
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

    extern int gbl_partial_indexes;
    if (gbl_partial_indexes && db->ix_partial)
        ins_keys = verify_indexes(db, new_dta, blobs, maxblobs, 0);

    return ins_keys;
}

/* this is called from delayed_key_adds() for adding keys to new btree
 * since adding them not-delayed could cause SC to abort erroneously
 */
int live_sc_post_update_delayed_key_adds_int(struct ireq *iq, void *trans,
                                             unsigned long long newgenid,
                                             const void *od_dta,
                                             unsigned long long ins_keys,
                                             int od_len)
{
    struct dbtable *usedb = iq->usedb;
    blob_status_t oldblobs[MAXBLOBS];
    blob_buffer_t add_blobs_buf[MAXBLOBS];
    blob_buffer_t *add_idx_blobs = NULL;
    int rc = 0;

    if (!(sc_live && usedb->sc_from == iq->usedb)) {
        return 0;
    }
#ifdef DEBUG_SC
    printf("live_sc_post_update_delayed_key_adds_int: looking at genid %llx\n",
           newgenid);
#endif
    /* need to check where the cursor is, even tho that check was done once in
     * post_update */
    int stripe = get_dtafile_from_genid(newgenid);
    if (stripe < 0 || stripe >= gbl_dtastripe) {
        logmsg(LOGMSG_ERROR,
               "live_sc_post_update_delayed_key_adds_int: newgenid 0x%llx "
               "stripe %d out of range!\n",
               newgenid, stripe);
        return 0;
    }

    int is_gen_gt_scptr = is_genid_right_of_stripe_pointer(
        iq->usedb->handle, newgenid, usedb->sc_to->sc_genids[stripe]);
    if (is_gen_gt_scptr) {
        if (iq->debug) {
            reqprintf(iq, "live_sc_post_update_delayed_key_adds_int: skip "
                          "genid 0x%llx to the right of scptr",
                      newgenid);
        }
        return 0;
    }

    bzero(oldblobs, sizeof(oldblobs));
    bzero(add_blobs_buf, sizeof(add_blobs_buf));
    if (iq->usedb->sc_to->ix_blob) {
        rc =
            save_old_blobs(iq, trans, ".ONDISK", od_dta, 2, newgenid, oldblobs);
        if (rc) {
            fprintf(stderr, "%s() save old blobs failed rc %d\n", __func__, rc);
            return rc;
        }
        blob_status_to_blob_buffer(oldblobs, add_blobs_buf);
        add_idx_blobs = add_blobs_buf;
    }

    /* Convert record from .ONDISK -> .NEW..ONDISK */
    void *new_dta = malloc(usedb->sc_to->lrl);
    if (new_dta == NULL) {
        logmsg(LOGMSG_ERROR, "%s() malloc failed\n", __func__);
        return 1;
    }
    struct convert_failure reason;
    rc = stag_to_stag_buf_blobs(usedb->sc_to->dbname, ".ONDISK", od_dta,
                                ".NEW..ONDISK", new_dta, &reason, add_idx_blobs,
                                add_idx_blobs ? MAXBLOBS : 0, 1);
    if (rc) {
        gbl_sc_abort = 1;
        MEMORY_SYNC;
        free(new_dta);
        free_blob_status_data(oldblobs);
        return rc;
    }

    ins_keys = revalidate_new_indexes(iq, usedb->sc_to, new_dta, add_idx_blobs,
                                      add_idx_blobs ? MAXBLOBS : 0);

    /* point to the new table */
    iq->usedb = usedb->sc_to;

    if (iq->debug) {
        reqpushprefixf(iq, "live_sc_post_update_delayed_key_adds_int: ");
        reqprintf(iq, "adding to indices genid 0x%llx in new table", newgenid);
    }

    rc = upd_new_record_add2indices(iq, trans, newgenid, new_dta,
                                    usedb->sc_to->lrl, ins_keys, 1,
                                    add_idx_blobs);
    iq->usedb = usedb;
    if (rc != 0 && rc != RC_INTERNAL_RETRY) {
        logmsg(LOGMSG_ERROR,
               "live_sc_post_update_delayed_key_adds_int rcode %d for "
               "add2indices genid 0x%llx\n",
               rc, newgenid);
        logmsg(LOGMSG_ERROR,
               "Aborting schema change due to unexpected error\n");
        gbl_sc_abort = 1;
        MEMORY_SYNC;
    }
    if (iq->debug) {
        reqpopprefixes(iq, 1);
    }
    free(new_dta);
    free_blob_status_data(oldblobs);
    return rc;
}

int live_sc_post_add_int(struct ireq *iq, void *trans, unsigned long long genid,
                         const uint8_t *od_dta, unsigned long long ins_keys,
                         blob_buffer_t *blobs, size_t maxblobs, int origflags,
                         int *rrn)

{
#ifdef DEBUG_SC
    printf("live_sc_post_add_int: looking at genid %llx\n", genid);
#endif
    // this is an INSERT of new row so add_record to sc_to
    char *tagname = ".NEW..ONDISK";
    uint8_t *p_tagname_buf = (uint8_t *)tagname,
            *p_tagname_buf_end = p_tagname_buf + 12;
    int opfailcode = 0;
    int ixfailnum = 0;
    int rc;
    struct dbtable *usedb = iq->usedb;

    /* Convert record from .ONDISK -> .NEW..ONDISK */

    void *new_dta = malloc(usedb->sc_to->lrl);
    if (new_dta == NULL) {
        logmsg(LOGMSG_ERROR, "%s() malloc failed\n", __func__);
        return 1;
    }
    struct convert_failure reason;
    rc = stag_to_stag_buf_blobs(usedb->sc_to->dbname, ".ONDISK", od_dta,
                                ".NEW..ONDISK", new_dta, &reason, blobs,
                                maxblobs, 1);
    if (rc) {
        gbl_sc_abort = 1;
        MEMORY_SYNC;
        free(new_dta);
        return rc;
    }

    ins_keys =
        revalidate_new_indexes(iq, usedb->sc_to, new_dta, blobs, maxblobs);

    if ((origflags & RECFLAGS_NO_CONSTRAINTS) && usedb->sc_to->n_constraints) {
        int rebuild = usedb->sc_to->plan && usedb->sc_to->plan->dta_plan;
#ifdef DEBUG_SC
        fprintf(stderr, "live_sc_post_add_int: need to "
                        "verify_record_constraint genid 0x%llx\n",
                genid);
#endif
        rc = verify_record_constraint(iq, usedb->sc_to, trans, new_dta,
                                      ins_keys, blobs, maxblobs, ".NEW..ONDISK",
                                      rebuild, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "live_sc_post_add_int: verify_record_constraint "
                   "rcode %d, genid 0x%llx\n",
                   rc, genid);
            logmsg(LOGMSG_ERROR, "Aborting schema change due to constraint "
                                 "violation in new schema\n");

            gbl_sc_abort = 1;
            MEMORY_SYNC;
            free(new_dta);
            return 0;
        }
    }

    if (iq->debug) {
        reqpushprefixf(iq, "live_sc_post_add_int: ");
        reqprintf(iq, "adding genid 0x%llx to new table", genid);
    }

    iq->usedb = usedb->sc_to;

    int addflags =
        RECFLAGS_NO_TRIGGERS | RECFLAGS_NEW_SCHEMA | RECFLAGS_KEEP_GENID;

    if (origflags & RECFLAGS_NO_CONSTRAINTS) {
        addflags |= RECFLAGS_NO_CONSTRAINTS;
    }

    rc = add_record(iq, trans, p_tagname_buf, p_tagname_buf_end, new_dta,
                    new_dta + usedb->sc_to->lrl, NULL, blobs, maxblobs,
                    &opfailcode, &ixfailnum, rrn, &genid, ins_keys,
                    BLOCK2_ADDKL, // opcode
                    0,            // blkpos
                    addflags);

    iq->usedb = usedb;

    if (rc != 0 && rc != RC_INTERNAL_RETRY) {
        logmsg(LOGMSG_ERROR, "live_sc_post_add_int rcode %d, genid 0x%llx\n",
               rc, genid);
        logmsg(LOGMSG_ERROR,
               "Aborting schema change due to unexpected error\n");
        gbl_sc_abort = 1;
        MEMORY_SYNC;
    }

    if (iq->debug) {
        reqpopprefixes(iq, 1);
    }

    iq->sc_adds++;
    free(new_dta);
    return rc;
}

/* both new and old are to the left of SC ptr, need to update
 */
int live_sc_post_update_int(struct ireq *iq, void *trans,
                            unsigned long long oldgenid, const void *old_dta,
                            unsigned long long newgenid, const void *new_dta,
                            unsigned long long ins_keys,
                            unsigned long long del_keys, int od_len,
                            int *updCols, blob_buffer_t *blobs, int deferredAdd,
                            blob_buffer_t *oldblobs, blob_buffer_t *newblobs)
{
    struct dbtable *usedb = iq->usedb;

#ifdef DEBUG_SC
    fprintf(stderr, "live_sc_post_update_int: oldgenid 0x%llx, newgenid "
                    "0x%llx, deferredAdd %d\n",
            oldgenid, newgenid, deferredAdd);
#endif

    int rc;
    /* point to the new table */
    iq->usedb = usedb->sc_to;

    if (iq->debug) {
        reqpushprefixf(iq, "live_sc_post_update_int: ");
        reqprintf(iq,
                  "updating genid 0x%llx to 0x%llx in new table (defered=%d)",
                  oldgenid, newgenid, deferredAdd);
    }

    rc = upd_new_record(iq, trans, oldgenid, old_dta, newgenid, new_dta,
                        ins_keys, del_keys, od_len, updCols, blobs, deferredAdd,
                        oldblobs, newblobs);
    iq->usedb = usedb;
    if (rc != 0 && rc != RC_INTERNAL_RETRY) {
        logmsg(LOGMSG_ERROR,
               "live_sc_post_update_int: rcode %d for update genid "
               "0x%llx to 0x%llx\n",
               rc, oldgenid, newgenid);
        logmsg(LOGMSG_ERROR,
               "Aborting schema change due to unexpected error\n");
        gbl_sc_abort = 1;
        MEMORY_SYNC;
    } else if (rc == 0) {
        (iq->sc_updates)++;
    }
    if (iq->debug) {
        reqpopprefixes(iq, 1);
    }
    return rc;
}

/*
 * Called by the bdb layer when the master is trying to downgrade.
 */
int schema_change_abort_callback(void)
{
    pthread_mutex_lock(&gbl_sc_lock);
    /* if a schema change is in progress */
    if (gbl_schema_change_in_progress) {
        /* we should safely stop the sc here, but until we find a good way to do
         * that, just kill us */
        exit(1);
    }
    pthread_mutex_unlock(&gbl_sc_lock);

    return 0;
}

/* Deletes all the files that are no longer needed after a schema change.  Also
 * sets a timer that the checkpoint thread checks by calling
 * sc_del_unused_files_check_progress() */
void sc_del_unused_files_tran(struct dbtable *db, tran_type *tran)
{
    int bdberr;

    pthread_mutex_lock(&gbl_sc_lock);
    sc_del_unused_files_start_ms = time_epochms();
    pthread_mutex_unlock(&gbl_sc_lock);

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DELAYED_OLDFILE_CLEANUP)) {
        if (bdb_list_unused_files_tran(db->handle, tran, &bdberr,
                                       "schemachange") ||
            bdberr != BDBERR_NOERROR)
            logmsg(LOGMSG_WARN, "errors listing old files\n");
    } else {
        if (bdb_del_unused_files_tran(db->handle, tran, &bdberr) ||
            bdberr != BDBERR_NOERROR)
            logmsg(LOGMSG_WARN, "errors deleting files\n");
    }

    pthread_mutex_lock(&gbl_sc_lock);
    sc_del_unused_files_start_ms = 0;
    pthread_mutex_unlock(&gbl_sc_lock);
}

void sc_del_unused_files(struct dbtable *db)
{
    return sc_del_unused_files_tran(db, NULL);
}

/* Checks to see if a schema change has been trying to delete files for longer
 * then gbl_sc_del_unused_files_threshold_ms, if so it exits */
void sc_del_unused_files_check_progress(void)
{
    int start_ms;

    pthread_mutex_lock(&gbl_sc_lock);
    start_ms = sc_del_unused_files_start_ms;
    pthread_mutex_unlock(&gbl_sc_lock);

    /* if a schema change is in progress */
    if (start_ms) {
        int diff_ms = time_epochms() - start_ms;
        if (diff_ms > gbl_sc_del_unused_files_threshold_ms) {
            logmsg(LOGMSG_FATAL,
                   "Schema change has been waiting %dms for files to "
                   "be deleted, exiting.\nPlease let the comdb2 team know "
                   "about this, and run 'send <dbname> delfiles "
                   "<schema_changed_table>' on the new master to clean up the "
                   "files we didn't delete\n",
                   diff_ms);
            exit(1);
        }
    }
}

static int delete_table_rep(char *table, void *tran)
{
    struct dbtable *db;
    int rc, bdberr;
    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "delete_table_rep : invalid table %s\n", table);
        return -1;
    }

    remove_constraint_pointers(db);

    if ((rc = bdb_close_only(db->handle, &bdberr))) {
        logmsg(LOGMSG_ERROR, "bdb_close_only rc %d bdberr %d\n", rc, bdberr);
        return -1;
    }

    /* update the delayed deleted files */
    rc =
        bdb_list_unused_files_tran(db->handle, tran, &bdberr, (char *)__func__);
    if (rc) {
        logmsg(LOGMSG_ERROR, "bdb_list_unused_files rc %d bdberr %d\n", rc,
               bdberr);
    }

    delete_db(table);
    MEMORY_SYNC;
    delete_schema(table);
    return 0;
}

static int bthash_callback(const char *table)
{
    int bthashsz;
    logmsg(LOGMSG_INFO, "Replicant bthashing table: %s\n", table);
    struct dbtable *db = get_dbtable_by_name(table);
    if (db && get_db_bthash(db, &bthashsz) == 0) {
        if (bthashsz) {
            logmsg(LOGMSG_INFO,
                   "Building bthash for table %s, size %dkb per stripe\n",
                   db->dbname, bthashsz);
            bdb_handle_dbp_add_hash(db->handle, bthashsz);
        } else {
            logmsg(LOGMSG_INFO, "Deleting bthash for table %s\n", db->dbname);
            bdb_handle_dbp_drop_hash(db->handle);
        }
        return 0;
    } else {
        logmsg(LOGMSG_ERROR, "%s: error updating bthash for %s.\n", __func__,
               table);
        return 1;
    }
}

static int replicant_reload_views(const char *name)
{
    int rc;

    rc = views_handle_replicant_reload(name);

    return rc;
}

/**
 * Updates in-memory representations of a specified sequence when
 * schema change is executed.
 * 
 * @param bdb_state_type *bdb_state
 * @param const char seq_name[]
 * @param scdone_t type 
 */
int update_sequence_description(bdb_state_type *bdb_state, const char seq_name[],
                                scdone_t type)
{
    void *tran = NULL;
    int rc = 0;
    int bdberr;
    uint32_t lid = 0;
    extern uint32_t gbl_rep_lockid;
    char *name = strdup(seq_name);

    tran = bdb_tran_begin(bdb_state, NULL, &bdberr);
    if (tran == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d can't begin transaction rc %d\n", __FILE__,
               __LINE__, bdberr);
        rc = bdberr;
        goto done;
    }
    bdb_get_tran_lockerid(tran, &lid);
    bdb_set_tran_lockerid(tran, gbl_rep_lockid);

    // Remove existing entry in memory
    int i;
    for (i = 0; i < thedb->num_sequences; i++) {
        if (strcasecmp(thedb->sequences[i]->name, name) == 0) {
            // Remove sequence from dbenv
            thedb->num_sequences--;

            if (thedb->num_sequences > 0) {
                thedb->sequences[i] = thedb->sequences[thedb->num_sequences];
            }

            thedb->sequences[thedb->num_sequences] = NULL;
        }
    }
    
    if (type == llmeta_sequence_add || type == llmeta_sequence_alter ) {
        // Sequence attributes
        long long min_val; // Minimum value 
        long long max_val; // Maximum value
        long long next_val; // Next valid value to be dispensed
        long long increment; // Value to increment by for dispensed values
        long long start_val; // Start value for the sequence
        long long next_start_val; // First valid value of the next allocated chunk
        long long remaining_vals; // Remaining valid values in the allocated chunk
        long long chunk_size; // Size of allocated chunk
        bool cycle; // Flag for cyclic behaviour in sequence
        char flags; // Flags for sequence (cdb2_constants.h)
        
        // Get sequence configuration from llmeta
        rc = bdb_llmeta_get_sequence(tran, name, &min_val, &max_val, &increment,
                                     &cycle, &start_val, &next_start_val,
                                     &chunk_size, &flags, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "can't get information for sequence \"%s\"\n",
                   name);
            goto done;
        }

        // Create new sequence in memory
        sequence_t *seq = new_sequence(name, min_val, max_val, next_val,
                                       increment, cycle, start_val, chunk_size,
                                       flags, 0, next_start_val);

        if (seq == NULL) {
            logmsg(LOGMSG_ERROR, "can't create sequence \"%s\"\n", name);
            goto done;
        }
        thedb->sequences[thedb->num_sequences++] = seq;
    }

done:
    if (name) {
        free(name);
    }
    
    if (tran) {
        bdb_set_tran_lockerid(tran, lid);
        rc = bdb_tran_abort(thedb->bdb_env, tran, &bdberr);
        if (rc) {
            logmsg(LOGMSG_FATAL, "%s:%d failed to abort transaction\n",
                   __FILE__, __LINE__);
            exit(1);
        }
    }
}

/* TODO fail gracefully now that inline? */
/* called by bdb layer through a callback as a detached thread,
 * we take ownership of table string
 * run on the replecants after the master is done so that they can reload/update
 * their copies of the modified database
 * if this fails, we panic so that we will be restarted back into a consistent
 * state */
int scdone_callback(bdb_state_type *bdb_state, const char table[],
                    scdone_t type)
{
    switch (type) {
    case luareload:
        return reload_lua();
    case sc_analyze:
        return replicant_reload_analyze_stats();
    case bthash:
        return bthash_callback(table);
    case views:
        return replicant_reload_views(table);
    case rowlocks_on:
    case rowlocks_on_master_only:
    case rowlocks_off: return reload_rowlocks(thedb->bdb_env, type);
    case llmeta_queue_add:
    case llmeta_queue_alter:
    case llmeta_queue_drop:
        return perform_trigger_update_replicant(table, type);
    case genid48_enable:
    case genid48_disable: return set_genid_format(thedb->bdb_env, type);
    case lua_sfunc: return reload_lua_sfuncs();
    case lua_afunc: return reload_lua_afuncs();
    case llmeta_sequence_add:
    case llmeta_sequence_alter:
    case llmeta_sequence_drop:
        return update_sequence_description(bdb_state, table, type);
    }

    int add_new_db = 0;
    int rc = 0;
    char *csc2text = NULL;
    char *table_copy = NULL;
    struct dbtable *db;
    void *tran = NULL;
    int bdberr;
    int highest_ver;
    int dbnum;
    uint32_t lid = 0;
    extern uint32_t gbl_rep_lockid;

    struct dbtable *olddb = get_dbtable_by_name(table);
    tran = bdb_tran_begin(bdb_state, NULL, &bdberr);
    if (tran == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d can't begin transaction rc %d\n", __FILE__,
               __LINE__, bdberr);
        rc = bdberr;
        goto done;
    }
    bdb_get_tran_lockerid(tran, &lid);
    bdb_set_tran_lockerid(tran, gbl_rep_lockid);

    if (olddb) {
        /* protect us from getting rep_handle_dead'ed to death */
        rc = bdb_get_csc2_highest(tran, table, &highest_ver, &bdberr);
        if (rc && bdberr == BDBERR_DEADLOCK) {
            rc = bdberr;
            goto done;
        }
    }

    if (type != drop) {
        if (get_csc2_file_tran(table, -1, &csc2text, NULL, tran)) {
            logmsg(LOGMSG_ERROR, "%s: error getting schema for %s.\n", __func__,
                   table);
            exit(1);
        }
        db = get_dbtable_by_name(table);
        table_copy = strdup(table);
        /* if we can't find a table with that name, we must be trying to add one
         */
        add_new_db = (db == NULL);
    }

    if (type == setcompr) {
        logmsg(LOGMSG_INFO,
               "Replicant setting compression flags for table:%s\n", table);
    } else if (type == add && add_new_db) {
        logmsg(LOGMSG_INFO, "Replicant adding table:%s\n", table);
        if (add_table_to_environment(table_copy, csc2text, NULL, NULL, tran)) {
            logmsg(LOGMSG_FATAL, "%s: error adding table "
                                 "%s.\n",
                   __func__, table);
            exit(1);
        }
    } else if (type == drop) {
        logmsg(LOGMSG_INFO, "Replicant dropping table:%s\n", table);
        if (delete_table_rep((char *)table, tran)) {
            logmsg(LOGMSG_FATAL, "%s: error deleting table "
                                 " %s.\n",
                   __func__, table);
            exit(1);
        }
        if (create_sqlmaster_records(tran)) {
            logmsg(LOGMSG_FATAL,
                   "create_sqlmaster_records: error creating sqlite "
                   "master records for %s.\n",
                   table);
            exit(1);
        }
        create_master_tables();
        ++gbl_dbopen_gen;
        goto done;
    } else if (type == bulkimport) {
        logmsg(LOGMSG_INFO, "Replicant bulkimporting table:%s\n", table);
        reload_after_bulkimport(db, tran);
    } else {
        logmsg(LOGMSG_INFO, "Replicant %s table:%s\n",
               type == alter ? "altering" : "fastinit-ing", table);
        extern int gbl_broken_max_rec_sz;
        int saved_broken_max_rec_sz = gbl_broken_max_rec_sz;
        if (db->lrl > COMDB2_MAX_RECORD_SIZE)
            gbl_broken_max_rec_sz = db->lrl - COMDB2_MAX_RECORD_SIZE;
        if (reload_schema(table_copy, csc2text, tran)) {
            logmsg(LOGMSG_FATAL, "%s: error reloading schema for %s.\n",
                   __func__, table);
            exit(1);
        }
        gbl_broken_max_rec_sz = saved_broken_max_rec_sz;

        if (create_sqlmaster_records(tran)) {
            logmsg(LOGMSG_FATAL,
                   "create_sqlmaster_records: error creating sqlite "
                   "master records for %s.\n",
                   table);
            exit(1);
        }
        create_master_tables(); /* create sql statements */

        /* update the delayed deleted files */
        assert(db && !add_new_db);
        rc = bdb_list_unused_files_tran(db->handle, tran, &bdberr,
                                        (char *)__func__);
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_list_unused_files rc %d bdberr %d\n", rc,
                   bdberr);
        }
    }

    free(table_copy);
    free(csc2text);

    /* if we just added the table, get a pointer for it */
    if (add_new_db) {
        db = get_dbtable_by_name(table);
        if (!db) {
            logmsg(LOGMSG_FATAL, "%s: could not find newly created db: %s.\n",
                   __func__, table);
            exit(1);
        }
    }

    set_odh_options_tran(db, tran);
    db->tableversion = table_version_select(db, tran);

    /* Make sure to add a version 1 schema for instant-schema change tables */
    if (add_new_db && db->odh && db->instant_schema_change) {
        struct schema *ondisk_schema;
        struct schema *ver_one;
        char tag[MAXTAGLEN];

        ondisk_schema = find_tag_schema(db->dbname, ".ONDISK");
        if (NULL == ondisk_schema) {
            logmsg(LOGMSG_FATAL, ".ONDISK not found in %s! PANIC!!\n",
                   db->dbname);
            exit(1);
        }
        ver_one = clone_schema(ondisk_schema);
        sprintf(tag, gbl_ondisk_ver_fmt, 1);
        free(ver_one->tag);
        ver_one->tag = strdup(tag);
        if (ver_one->tag == NULL) {
            logmsg(LOGMSG_FATAL, "strdup failed %s @ %d\n", __func__, __LINE__);
            exit(1);
        }
        add_tag_schema(db->dbname, ver_one);
    }

    ++gbl_dbopen_gen;
    llmeta_dump_mapping_tran(tran, thedb);
    llmeta_dump_mapping_table_tran(tran, thedb, table, 1);

    /* Fetch the correct dbnum for this table.  We need this step because db
     * numbers aren't stored in the schema, and it's not handed to us during
     * schema change.  But it is committed to the llmeta table, so we can fetch
     * it from there. */
    dbnum = llmeta_get_dbnum_tran(tran, db->dbname, &bdberr);
    if (dbnum == -1) {
        logmsg(LOGMSG_ERROR, "failed to fetch dbnum for table \"%s\"\n",
               db->dbname);
        rc = BDBERR_MISC;
        goto done;
    }
    db->dbnum = dbnum;

    fix_lrl_ixlen_tran(tran);

    rc = 0;
done:
    if (tran) {
        bdb_set_tran_lockerid(tran, lid);
        rc = bdb_tran_abort(thedb->bdb_env, tran, &bdberr);
        if (rc) {
            logmsg(LOGMSG_FATAL, "%s:%d failed to abort transaction\n",
                   __FILE__, __LINE__);
            exit(1);
        }
    }

    sc_set_running(0 /*running*/, 0 /*seed*/, NULL, 0);
    return rc; /* success */
}

void getMachineAndTimeFromFstSeed(const char **mach, time_t *timet)
{
    /* fastseeds are formed from an epoch time, machine number and
     * duplication factor, so we can decode the fastseed to get the
     * master machine that started the schema change and the time at which
     * it was done. */
    unsigned int *iptr = (unsigned int *)&sc_seed;

    *mach = get_hostname_with_crc32(thedb->bdb_env, sc_host);
    *timet = ntohl(iptr[0]);
    return;
}
