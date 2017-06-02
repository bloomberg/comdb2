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

#include "schemachange.h"
#include "schemachange_int.h"
#include "sc_delete_table.h"
#include "autoanalyze.h"
#include "logmsg.h"

int delete_table(char *table)
{
    struct db *db;
    int rc, bdberr;
    db = getdbbyname(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "delete_table : invalid table %s\n", table);
        return -1;
    }

    remove_constraint_pointers(db);

    if ((rc = bdb_close_only(db->handle, &bdberr))) {
        logmsg(LOGMSG_ERROR, "bdb_close_only rc %d bdberr %d\n", rc, bdberr);
        return -1;
    }

    delete_db(table);
    MEMORY_SYNC;
    delete_schema(table);
    bdb_del_table_csonparameters(NULL, table);
    return 0;
}

static inline void set_implicit_options(struct schema_change_type *s,
                                        struct db *db, struct scinfo *scinfo)
{
    if (s->headers != db->odh)
        s->header_change = s->force_dta_rebuild = s->force_blob_rebuild = 1;
    if (s->compress != scinfo->olddb_compress)
        s->force_dta_rebuild = 1;
    if (s->compress_blobs != scinfo->olddb_compress_blobs)
        s->force_blob_rebuild = 1;
    if (scinfo->olddb_inplace_updates && !s->ip_updates)
        s->force_rebuild = 1;
    if (scinfo->olddb_instant_sc && !s->instant_sc)
        s->force_rebuild = 1;
}

int do_fastinit_int(struct schema_change_type *s)
{
    struct db *db;
    int rc;

    struct ireq iq;
    int bdberr;
    struct db *newdb;
    int datacopy_odh = 0;

    gbl_use_plan = 1;

    init_fake_ireq(thedb, &iq);

    db = s->db = getdbbyname(s->table);

    if (db == NULL) {
        sc_errf(s, "Table doesn't exists\n");
        return SC_TABLE_DOESNOT_EXIST;
    }
    if (db->n_rev_constraints > 0) {
        sc_errf(
            s, "Fastinit not supported for tables with foreign constraints \n");
        return -1;
    }

    char new_prefix[32];
    int foundix;

    struct scinfo scinfo;

    set_schemachange_options(s, db, &scinfo);

    set_implicit_options(s, db, &scinfo);

    iq.usedb = db;

    /*************************************************************************/
    extern int gbl_broken_max_rec_sz;
    int saved_broken_max_rec_sz = gbl_broken_max_rec_sz;
    if(s->db->lrl > COMDB2_MAX_RECORD_SIZE) {
        //we want to allow fastiniting and dropping this tbl
        gbl_broken_max_rec_sz = s->db->lrl - COMDB2_MAX_RECORD_SIZE;
    }

    if ((rc = load_db_from_schema(s, thedb, &foundix, NULL)))
        return rc;

    gbl_broken_max_rec_sz = saved_broken_max_rec_sz;
    /*************************************************************************/

    /* open a db using the loaded schema
     * TODO del NULL param, pre-llmeta holdover */
    db->sc_to = newdb = s->newdb =
        create_db_from_schema(thedb, s, db->dbnum, foundix, 1);
    if (newdb == NULL)
        return SC_INTERNAL_ERROR;

    if (add_cmacc_stmt(newdb, 1) != 0) {
        backout_schemas(newdb->dbname);
        sc_errf(s, "Failed to process schema\n");
        return -1;
    }

    /* hi please don't leak memory? */
    csc2_free_all();
    /**********************************************************************/
    /* force a "change" for a fastinit */
    schema_change = SC_TAG_CHANGE;

    /* Create temporary tables.  To try to avoid strange issues always
     * use a unqiue prefix.  This avoids multiple histories for these
     * new. files in our logs.
     *
     * Since the prefix doesn't matter and bdb needs to be able to unappend
     * it, we let bdb choose the prefix */
    /* ignore failures, there shouln't be any and we'd just have a
     * truncated prefix anyway */
    bdb_get_new_prefix(new_prefix, sizeof(new_prefix), &bdberr);

    rc = open_temp_db_resume(newdb, new_prefix, 0, 1);
    if (rc) {
        /* TODO: clean up db */
        sc_errf(s, "Failed opening new db\n");
        change_schemas_recover(s->table);
        return -1;
    }

    /* we can resume sql threads at this point */

    /* Must do this before rebuilding, otherwise we'll have the wrong
     * blobstripe_genid. */
    transfer_db_settings(db, newdb);

    get_db_datacopy_odh(db, &datacopy_odh);
    if (s->fastinit || s->force_rebuild || /* we're first to set */
        newdb->instant_schema_change)      /* we're doing instant sc*/
    {
        datacopy_odh = 1;
    }

    /* we set compression /odh options in BDB ONLY here.
       For full operation they also need to be set in the meta tables.
       However the new db gets its meta table assigned further down,
       so we can't set meta options until we're there. */
    set_bdb_option_flags(newdb, s->headers, s->ip_updates,
                         newdb->instant_schema_change, newdb->version,
                         s->compress, s->compress_blobs, datacopy_odh);

    /* set sc_genids, 0 them if we are starting a new schema change, or
     * restore them to their previous values if we are resuming */
    if (init_sc_genids(newdb, s)) {
        sc_errf(s, "Failed initializing sc_genids\n");
        /*trans_abort( &iq, transaction );*/
        /*live_sc_leave_exclusive_all(db->handle, transaction);*/
        delete_temp_table(s, newdb);
        change_schemas_recover(s->table);
        return -1;
    }

    else {
        MEMORY_SYNC;
    }

    return SC_OK;
}

int finalize_fastinit_table(struct schema_change_type *s)
{
    int dbnum, newdbnum;
    struct db *db = s->db;
    struct db *newdb = s->newdb;
    struct ireq iq;
    void *tran = NULL;
    void *transac = NULL;
    bdb_state_type *old_bdb_handle, *new_bdb_handle;
    int got_new_handle = 0;
    int retries = 0;
    int rc;
    int bdberr = 0;
    int is_sqlite =
        strncmp((s->table), "sqlite_stat", sizeof("sqlite_stat") - 1) == 0;
    int olddb_bthashsz;

    init_fake_ireq(thedb, &iq);
    iq.usedb = db;

    if (get_db_bthash(db, &olddb_bthashsz) != 0)
        olddb_bthashsz = 0;

    sc_printf(s, "---- Deleting old files. ---- \n");

    if (transac == NULL) {
        trans_start_sc(&iq, NULL, &transac);
    }

    /* Before this handle is closed, lets wait for all the db reads to finish*/
    bdb_lock_table_write(db->handle, transac);

    /* From this point on failures should goto either backout if recoverable
     * or failure if unrecoverable */
    tran = NULL;

    /* close the newly created db as well */
    if (s->same_schema || s->drop_table) {
        rc = bdb_close_temp_state(newdb->handle, &bdberr);
        if (rc) {
            sc_errf(s, "Failed closing new db handle, bdberr\n", bdberr);
            goto backout;
        } else
            sc_printf(s, "Close new db ok\n");
    }

    newdb->meta = db->meta;

    /* at this point if a backup is going on, it will be bad */
    gbl_sc_commit_count++;

    /*
       at this point we've closed the old table and closed the new table.
       now we do the file manipulation portion.  we do this by updating ll
       pointers to files.
    */

retry_fastinit:

    if (++retries >= gbl_maxretries) {
        sc_errf(s, "Updating version giving up after %d retries\n", retries);
        goto backout;
    }

    if (tran) /* if this is a retry and not the first pass */
    {
        trans_abort(&iq, tran);
        tran = NULL;

        sc_errf(s, "Updating version failed for %s rc %d bdberr %d "
                   "attempting retry\n",
                s->table, rc, bdberr);
        /*poll(NULL, 0, rand() % 100);*/
    }

    rc = trans_start_sc(&iq, NULL, &tran);
    if (rc) {
        goto retry_fastinit;
    }

    /*Now that we don't have any data, please clear unwanted schemas.*/
    bdberr = bdb_reset_csc2_version(tran, db->dbname, db->version);
    if (bdberr != BDBERR_NOERROR)
        goto retry_fastinit;

    sc_printf(s, "Start version update transaction ok\n");

    /* load new csc2 data */
    rc = load_new_table_schema_tran(thedb, tran, s->table, s->newcsc2);
    if (rc != 0) {
        sc_errf(s, "Error loading new schema into meta tables, "
                   "trying again\n");
        goto retry_fastinit;
    }

    if ((rc = set_header_and_properties(tran, newdb, s, 1, olddb_bthashsz)))
        goto retry_fastinit;

    /*set all metapointers to new files*/
    rc = bdb_commit_temp_file_version_all(newdb->handle, tran, &bdberr);
    if (rc)
        goto retry_fastinit;

    /* delete any new file versions this table has */
    if (bdb_del_file_versions(newdb->handle, tran, &bdberr) ||
        bdberr != BDBERR_NOERROR) {
        sc_errf(s, "%s: bdb_del_file_versions failed\n", __func__);
        goto retry_fastinit;
    }

    if ((rc = mark_schemachange_over(tran, db->dbname)))
        goto retry_fastinit;

    rc = trans_commit_adaptive(&iq, tran, gbl_mynode);
    if (rc) {
        sc_errf(s, "Failed version update commit\n");
        goto retry_fastinit;
    }

    sc_printf(s, "Update version commit ok\n");
    tran = NULL;

    /* remove the new.NUM. prefix */
    bdb_remove_prefix(newdb->handle);

    /* TODO: need to free db handle - right now we just leak some memory */
    /* replace the old db definition with a new one */

    newdb->plan = NULL;

    if (s->drop_table) {
        /* drop table */
        /*
        if (newdb->n_constraints) {
          fprintf(stderr, "Dropping table with constraints.\n");
          newdb->n_constraints = 0;
          restore_constraint_pointers(db,newdb);
        }
        */

        delete_table(db->dbname);
        bdb_reset_csc2_version(transac, db->dbname, db->version);
        if ((rc = bdb_del(db->handle, transac, &bdberr))) {
            sc_errf(s, "%s: bdb_del failed with rc: %d bdberr: %d\n", __func__,
                    rc, bdberr);
            goto failed;
        } else if ((rc = bdb_del_file_versions(db->handle, transac, &bdberr))) {
            sc_errf(s, "%s: bdb_del_file_versions failed with rc: %d bdberr: "
                       "%d\n",
                    __func__, rc, bdberr);
            goto failed;
        }
        if (table_version_upsert(db, transac, &bdberr)) {
            sc_errf(s, "Failed updating table version bdberr %d\n", bdberr);
            goto failed;
        }
        if (llmeta_set_tables(transac, thedb)) {
            sc_errf(s, "Failed to set table names in low level meta\n");
            goto failed;
        }
    } else {

        new_bdb_handle = newdb->handle;
        old_bdb_handle = db->handle;
        bdb_handle_reset(old_bdb_handle);
        bdb_handle_reset(new_bdb_handle);

        dbnum = get_dbnum_by_handle(old_bdb_handle);

        bdb_close_only(old_bdb_handle, &bdberr);

        rc = bdb_free_and_replace(old_bdb_handle, new_bdb_handle, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "bdb_free_and_replace rc %d bdberr %d\n", rc,
                    bdberr);
            goto failed;
        }

        if (dbnum != -1) {
            /* poke us back into parent array */
            bdb_replace_handle(thedb->bdb_env, dbnum, old_bdb_handle);
        }

        logmsg(LOGMSG_DEBUG, "old: %p   new %p\n", old_bdb_handle, new_bdb_handle);

        free_db_and_replace(db, newdb);
        fix_constraint_pointers(db, newdb);
        commit_schemas(s->table);
        MEMORY_SYNC;

        db->handle =
            old_bdb_handle; /* free_db_and_replace changes this value,
                               but the handle pointer must not change because
                               code can latch it.  bdb_free_and_replace
                               copies the new handle value into the new
                               handle, so get back to the original pointer
                               for it. */
        fix_lrl_ixlen();

        if (s->same_schema)
            bdb_set_csc2_version(db->handle, db->version);

        if (olddb_bthashsz) {
            logmsg(LOGMSG_INFO, "Rebuilding bthash for table %s, size %dkb per stripe\n",
                   db->dbname, olddb_bthashsz);
            bdb_handle_dbp_add_hash(db->handle, olddb_bthashsz);
        }

        /* losing flags during shenanigans above; set again */
        set_odh_options(db);

        got_new_handle = 1;
    }

    if (create_sqlmaster_records(transac)) {
        sc_errf(s, "create_sqlmaster_records failed\n");
        goto failed;
    }
    create_master_tables(); /* create sql statements */

    trans_commit(&iq, transac, gbl_mynode);
    transac = NULL;

    rc = bdb_llog_scdone(db->handle, s->drop_table ? drop : fastinit, 1,
                         &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        sc_errf(s, "Failed to send logical log scdone rc=%d bdberr=%d\n", rc,
                bdberr);
        /* TODO don't quite know what to do here, the schema change is
         * already commited but one or more replicants didn't get the
         * message so they didn't reload their tables.  Our options:
         * goto backout: we can't the changes have been comitted
         * goto failed: to what purpose? this node is fine
         *
         * we really need to somehow bounce the replicants, but there's no
         * way to do this.
         */
    }

    live_sc_off(db);

    if (!s->drop_table) /* We already deleted all the schemas. */
        update_dbstore(db);

    if (!gbl_create_mode) {
        logmsg(LOGMSG_INFO, "Table %s is at version: %d\n", db->dbname, db->version);
    }

    llmeta_dump_mapping_table(thedb, db->dbname, 1);

    if (gbl_replicate_local)
        local_replicant_write_clear(db);

    /* delete files we don't need now */
    if (s->drop_table)
        sc_del_unused_files(db);
    else
        sc_del_unused_files(newdb);

    if (got_new_handle) {
        int newdbnum;
        /* we are about to free new_bdb_handle: make sure it isn't in the
         * bdb_state->children array */
        newdbnum = get_dbnum_by_handle(new_bdb_handle);
        assert(newdbnum == -1);
        free(new_bdb_handle);
        free(newdb);
    }

    if (!s->drop_table && bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AUTOANALYZE) &&
        !is_sqlite) {
        // triger analyze of table -- when fastinit
        pthread_t analyze;
        char *tblname =
            strdup((char *)db->dbname); // will be freed in auto_analyze_table()
        pthread_create(&analyze, &gbl_pthread_attr_detached, auto_analyze_table,
                       tblname);
    }
    sc_set_running(0, sc_seed, gbl_mynode, time(NULL));

    return 0;

backout:
    if (transac) {
        rc = trans_abort(&iq, transac);
        if (rc)
            sc_errf(s, "%s:trans_abort rc %d\n", __func__, rc);
    }

    if (tran) {
        rc = trans_abort(&iq, tran);
        if (rc)
            sc_errf(s, "%s:trans_abort rc %d\n", __func__, rc);
    }

    delete_temp_table(s, newdb);
    change_schemas_recover(s->table);

    rc = bdb_open_again(db->handle, &bdberr);
    if (rc) {
        sc_errf(s, "Failed reopening db, bdberr %d\n", bdberr);
        goto failed;
    }

    if (got_new_handle) {
        free(new_bdb_handle);
        free(newdb);
    }
    return -1;

failed:
    if (got_new_handle) {
        free(new_bdb_handle);
        free(newdb);
    }
    if (transac) {
        rc = trans_abort(&iq, transac);
        if (rc)
            sc_errf(s, "%s:trans_abort rc %d\n", __func__, rc);
    }
    sc_errf(s, "Fatal error during schema change.  Exiting\n");
    /* from exit msgtrap */
    clean_exit();
    return -1;
}
