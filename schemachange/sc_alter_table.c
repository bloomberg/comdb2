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

#include <unistd.h>
#include <crc32c.h>
#include <memory_sync.h>
#include "schemachange.h"
#include "sc_alter_table.h"
#include "logmsg.h"
#include "sc_global.h"
#include "sc_schema.h"
#include "sc_struct.h"
#include "sc_csc2.h"
#include "sc_util.h"
#include "sc_logic.h"
#include "sc_records.h"

static int prepare_sc_plan(struct schema_change_type *s, int old_changed,
                           struct dbtable *db, struct dbtable *newdb,
                           struct scplan *theplan)
{
    int changed = old_changed;
    /* don't create schema change plan if we're not going to use it */
    if (s->force_rebuild) {
        s->use_plan = gbl_use_plan = 0;
    } else {
        int rc = create_schema_change_plan(s, db, newdb, theplan);
        if (rc != 0) {
            sc_printf(s, "not using plan.  error in plan module.\n");
            changed = SC_TAG_CHANGE;
        } else if (!s->use_plan ||
                   s->force_rebuild) // TODO This if does not make sense!
        {
            sc_printf(s, "not using plan.  full rebuild\n");
            s->use_plan = gbl_use_plan = 0;
            changed = SC_TAG_CHANGE;
        } else {
            sc_printf(s, "Using plan.\n");
            newdb->plan = theplan;
            if (newdb->plan->dta_plan) changed = SC_TAG_CHANGE;
        }
        s->retry_bad_genids = 0;
    }

    return changed;
}

static int prepare_changes(struct schema_change_type *s, struct dbtable *db,
                           struct dbtable *newdb, struct scplan *theplan,
                           struct scinfo *scinfo)
{
    int changed = ondisk_schema_changed(s->table, newdb, stderr, s);

    if (changed == SC_BAD_NEW_FIELD) {
        /* we want to capture cases when "alter" is used
           to add a field to a table that has no dbstore or
           isnull specified
           It is still possible to do so by using "fastinit"
           with the new schema instead of "alter"
         */
        if (s->fastinit) {
            changed = SC_TAG_CHANGE;
        }
    }
    if (changed < 0) {
        /* some errors during constraint verifications */
        backout_schemas(newdb->dbname);
        resume_threads(thedb); /* can now restart stopped threads */

        /* these checks should be present in dryrun_int as well */
        if (changed == SC_BAD_NEW_FIELD) {
            sc_errf(s, "cannot add new field without dbstore or null\n");
        } else if (changed == SC_BAD_INDEX_CHANGE) {
            sc_errf(s, "cannot change index referenced by other tables\n");
        }
        sc_errf(s, "Failed to process schema!\n");
        return -1;
    }

    /* force change if we need to add/remove headers (or anything else which
     * rebuilds) */
    if (db->odh != s->headers) {
        changed = SC_TAG_CHANGE;
    }

    /* bump the version for inplace-updates. */
    if (!scinfo->olddb_inplace_updates && s->ip_updates) {
        changed = SC_TAG_CHANGE;
    }

    if (s->compress != scinfo->olddb_compress) {
        s->force_dta_rebuild = 1;
    }
    if (s->compress_blobs != scinfo->olddb_compress_blobs) {
        s->force_blob_rebuild = 1;
    }

    /* force a change if instant schema-change is being enabled.  reset version
     * to 0. */
    if (!scinfo->olddb_instant_sc && s->instant_sc) {
        changed = SC_TAG_CHANGE;
    }

    /* force a change if requested.  reset version to 0. */
    if (s->force_rebuild) {
        changed = SC_TAG_CHANGE;
    }

    /* rebuild if ran out of version numbers */
    if (newdb->version >= MAXVER && newdb->instant_schema_change) {
        sc_printf(s, "exhausted versions. forcing rebuild at %d\n",
                  newdb->version);
        if (s->dryrun)
            sbuf2printf(s->sb, ">table version: %d. will have to rebuild.\n",
                        newdb->version);
        s->force_rebuild = 1;
        changed = SC_TAG_CHANGE;
    }
    return changed;
}

static void adjust_version(int changed, struct scinfo *scinfo,
                           struct schema_change_type *s, struct dbtable *db,
                           struct dbtable *newdb)
{
    /* if we don't want to merely bump the version, reset it to 0. */
    if (changed == SC_TAG_CHANGE && newdb->instant_schema_change) {
        newdb->version = 0;
    }
    /* if we're disabling instant sc, reset the version to 0. */
    else if (db->instant_schema_change && !newdb->instant_schema_change) {
        /* instant schema change is being removed */
        newdb->version = 0;
    }
    /* if we are enabling instant sc, set the version to 1. */
    else if (db->odh && !db->instant_schema_change &&
             newdb->instant_schema_change && !s->fastinit) {
        /* old db had odh but not instant schema change.
         * the physical records will have version 0 &
         * will correspond to the latest csc2 version. start
         * instant version at 2 and treat physical version 0
         * as version 1 */
        newdb->version = 1;
    }

    /* if only index or constraints have changed don't bump version */
    int ondisk_changed = compare_tag(s->table, ".ondisk", NULL);
    if (ondisk_changed != SC_NO_CHANGE /* something changed in .ondisk */
        || s->fastinit                 /* fastinit */
        || !s->use_plan                /* full rebuild due to some prob */
        || s->force_rebuild            /* full rebuild requested */
        || (!scinfo->olddb_instant_sc &&
            s->instant_sc) /* bump version if enabled instant sc */
        ) {
        ++newdb->version;
    } else if (ondisk_changed == SC_NO_CHANGE /* nothing changed ondisk */
               && changed == SC_TAG_CHANGE    /* plan says it did change */
               && newdb->version == 0 && newdb->plan->dta_plan == -1 &&
               newdb->plan->plan_convert == 1) {
        ++newdb->version;
    }
}

static int prepare_version_for_dbs_without_instant_sc(tran_type *tran,
                                                      struct dbtable *db,
                                                      struct dbtable *newdb)
{
    int rc;
    int bdberr;

    if (db->odh && !db->instant_schema_change && newdb->instant_schema_change &&
        newdb->version == 2) {
        /* Old db had ODH but not instant schema change.
         * The physical records will have version 0 &
         * will correspond to the latest csc2 version. Start
         * instant version at 2 and treat physical version 0
         * as version 1 */

        struct schema *ver_one;
        if ((rc = prepare_table_version_one(tran, db, &ver_one))) return rc;
        replace_tag_schema(newdb, ver_one);
    }

    return SC_OK;
}

static int switch_versions_with_plan(void *tran, struct dbtable *db,
                                     struct dbtable *newdb)
{
    int rc, bdberr;
    int blobno, ixnum;
    unsigned long long file_versions[MAXINDEX];

    if (newdb->plan->dta_plan == -1) {
        /*set main data metapointer to new data file*/
        rc = bdb_commit_temp_file_version_data(newdb->handle, tran,
                                               0 /*dtanum*/, &bdberr);
        if (rc) return SC_BDB_ERROR;
    }

    /* get all out old file versions */
    bzero(file_versions, sizeof(file_versions));
    for (blobno = 0; blobno < MAXBLOBS + 1; blobno++) {
        bdb_get_file_version_data(db->handle, tran, blobno,
                                  file_versions + blobno, &bdberr);
    }

    for (blobno = 0; blobno < newdb->numblobs; blobno++) {
        if (newdb->plan->blob_plan[blobno] == blobno) {
            /* do nothing with this blob */
        } else if (newdb->plan->blob_plan[blobno] >= 0) {
            logmsg(LOGMSG_INFO, "bdb_file_version_change_dtanum:"
                                " %d -> %d : data %d now points to %016llx\n",
                   newdb->plan->blob_plan[blobno] + 1, blobno + 1, blobno + 1,
                   file_versions[newdb->plan->blob_plan[blobno] + 1]);

            rc = bdb_new_file_version_data(
                db->handle, tran, blobno + 1,
                file_versions[newdb->plan->blob_plan[blobno] + 1], &bdberr);

            /* we're reusing the old blob */

            if (rc) return SC_BDB_ERROR;
        } else if (newdb->plan->blob_plan[blobno] == -1) {
            /* we made a new blob */
            rc = bdb_commit_temp_file_version_data(newdb->handle, tran,
                                                   blobno + 1, &bdberr);
            if (rc) return SC_BDB_ERROR;
        }
    }

    /* get all out old file versions */
    bzero(file_versions, sizeof(file_versions));
    for (ixnum = 0; ixnum < MAXINDEX; ixnum++) {
        rc = bdb_get_file_version_index(db->handle, tran, ixnum,
                                        file_versions + ixnum, &bdberr);
    }

    for (ixnum = 0; ixnum < newdb->nix; ixnum++) {
        if (newdb->plan->ix_plan[ixnum] == ixnum) {
            logmsg(LOGMSG_INFO, "ix %d being left alone\n", ixnum);

            /* do nothing with this index */
        } else if (newdb->plan->ix_plan[ixnum] >= 0) {
            logmsg(LOGMSG_INFO, "bdb_file_version_change_ixnum:"
                                " %d -> %d : ix %d now points to %016llx\n",
                   newdb->plan->ix_plan[ixnum], ixnum, ixnum,
                   file_versions[newdb->plan->ix_plan[ixnum]]);

            rc = bdb_new_file_version_index(
                db->handle, tran, ixnum,
                file_versions[newdb->plan->ix_plan[ixnum]], &bdberr);

            /* we're re-using the old index */
            if (rc) return SC_BDB_ERROR;

        } else if (newdb->plan->ix_plan[ixnum] == -1) {
            /* we rebuilt this index */
            rc = bdb_commit_temp_file_version_index(newdb->handle, tran, ixnum,
                                                    &bdberr);
            if (rc) return SC_BDB_ERROR;
        }
    }

    return SC_OK;
}

static void backout(struct dbtable *db)
{
    backout_schemas(db->dbname);
    live_sc_off(db);
}

static inline void wait_to_resume(struct schema_change_type *s)
{
    if (s->resume) {
        int stm = BDB_ATTR_GET(thedb->bdb_attr, SC_RESTART_SEC);
        if (stm == 0) return;

        logmsg(LOGMSG_WARN, "%s: Schema change will resume in %d seconds\n",
               __func__, stm);
        sleep(stm);
        logmsg(LOGMSG_WARN, "%s: Schema change resuming.\n", __func__);
    }
}

int do_alter_table_int(struct ireq *iq, tran_type *tran)
{
    struct schema_change_type *s = iq->sc;
    struct dbtable *db;
    int rc;
    int bdberr = 0;
    int trying_again = 0;
    struct dbtable *newdb;
    int datacopy_odh = 0;
    int stop_tag_thds = 0;
    int retries = 0;
    int changed;
    int i;
    int olddb_instant_sc;
    char new_prefix[32];
    int foundix;

    struct scinfo scinfo;

    gbl_use_plan = 1;
    gbl_sc_last_writer_time = 0;

    db = get_dbtable_by_name(s->table);
    if (db == NULL) {
        sc_errf(s, "Table not found:%s\n", s->table);
        return SC_TABLE_DOESNOT_EXIST;
    }

    set_schemachange_options_tran(s, db, &scinfo, tran);

    if ((rc = check_option_coherency(s, db, &scinfo))) return rc;

    sc_printf(s, "starting schema update with seed %llx\n", sc_seed);

    if ((rc = load_db_from_schema(s, thedb, &foundix, iq))) return rc;

    db->sc_to = newdb = create_db_from_schema(thedb, s, db->dbnum, foundix, -1);

    if (newdb == NULL) {
        sc_errf(s, "Internal error\n");
        return SC_INTERNAL_ERROR;
    }
    newdb->version = get_csc2_version(newdb->dbname);

    newdb->iq = iq;

    if (add_cmacc_stmt(newdb, 1) != 0) {
        backout(newdb);
        cleanup_newdb(newdb);
        sc_errf(s, "Failed to process schema!\n");
        return -1;
    }

    extern int gbl_partial_indexes;
    extern int gbl_expressions_indexes;
    if ((gbl_partial_indexes && newdb->ix_partial) ||
        (gbl_expressions_indexes && newdb->ix_expr)) {
        int ret = 0;
        char temp_newdb_name[MAXTABLELEN];
        struct dbtable *temp_newdb;
        int len = strlen(s->table);
        len = crc32c(s->table, len);
        snprintf(temp_newdb_name, MAXTABLELEN, "sc_alter_temp_%X", len);
        wrlock_schema_lk();
        {
            temp_newdb = newdb_from_schema(thedb, temp_newdb_name, NULL, 0,
                                           thedb->num_dbs, 0);
            if (temp_newdb == NULL) {
                rc = SC_INTERNAL_ERROR;
                goto pi_done;
            }
            temp_newdb->dtastripe = gbl_dtastripe;
            if (add_cmacc_stmt(temp_newdb, 0)) {
                logmsg(LOGMSG_ERROR, "%s: add_cmacc_stmt failed\n", __func__);
                rc = SC_CSC2_ERROR;
                goto pi_done;
            }

            if (verify_constraints_exist(temp_newdb, NULL, NULL, s) != 0) {
                logmsg(LOGMSG_ERROR, "%s: Verify constraints failed \n",
                       __func__);
                rc = -1;
                goto pi_done;
            }

            if (!sc_via_ddl_only() && validate_ix_names(temp_newdb)) {
                rc = -1;
                goto pi_done;
            }

            if (temp_newdb->dbenv->master == gbl_mynode) {
                /* I am master: create new db */
                logmsg(LOGMSG_DEBUG, "create new db\n");
                temp_newdb->handle = bdb_create_tran(
                    temp_newdb->dbname, thedb->basedir, temp_newdb->lrl,
                    temp_newdb->nix, temp_newdb->ix_keylen,
                    temp_newdb->ix_dupes, temp_newdb->ix_recnums,
                    temp_newdb->ix_datacopy, temp_newdb->ix_collattr,
                    temp_newdb->ix_nullsallowed, temp_newdb->numblobs + 1,
                    thedb->bdb_env, 0, &bdberr, tran);
                open_auxdbs(temp_newdb, 1);
            } else {
                /* I am NOT master: open replicated db */
                logmsg(LOGMSG_DEBUG, "open replicated db\n");
                temp_newdb->handle = bdb_open_more(
                    temp_newdb->dbname, thedb->basedir, temp_newdb->lrl,
                    temp_newdb->nix, temp_newdb->ix_keylen,
                    temp_newdb->ix_dupes, temp_newdb->ix_recnums,
                    temp_newdb->ix_datacopy, temp_newdb->ix_collattr,
                    temp_newdb->ix_nullsallowed, temp_newdb->numblobs + 1,
                    thedb->bdb_env, &bdberr);
                open_auxdbs(temp_newdb, 0);
            }
            if (temp_newdb->handle == NULL) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to open table %s/%s, rcode %d\n", __func__,
                       thedb->basedir, temp_newdb->dbname, bdberr);
                rc = SC_BDB_ERROR;
                goto pi_done;
            }

            thedb->dbs =
                realloc(thedb->dbs, (thedb->num_dbs + 1) * sizeof(struct dbtable *));
            thedb->dbs[thedb->num_dbs++] = temp_newdb;
            /* Add table to the hash. */
            hash_add(thedb->db_hash, temp_newdb);
            create_sqlmaster_records(tran);
            create_master_tables(); /* create sql statements */
            ret = new_indexes_syntax_check(iq);
            if (bdb_close_only(temp_newdb->handle, &bdberr) != 0) {
                logmsg(LOGMSG_ERROR,
                       "%s: failed to close table %s/%s, rcode %d\n", __func__,
                       thedb->basedir, temp_newdb->dbname, bdberr);
            }
            newdb->ix_blob = temp_newdb->ix_blob;
            newdb->schema->ix_blob = newdb->ix_blob;
            delete_schema(temp_newdb_name);
            delete_db(temp_newdb_name);
            cleanup_newdb(temp_newdb);
            create_sqlmaster_records(tran);
            create_master_tables(); /* create sql statements */
            if (ret) {
                sc_errf(s, "New indexes syntax error\n");
                ret = SC_CSC2_ERROR;
                goto pi_done;
            } else {
                sc_printf(s, "New indexes ok\n");
            }
        }
    pi_done:
        unlock_schema_lk();
        if (rc) return rc;
        if (ret) {
            backout(newdb);
            cleanup_newdb(newdb);
            return ret;
        }
    }

    if (verify_constraints_exist(NULL, newdb, newdb, s) != 0) {
        backout(newdb);
        cleanup_newdb(newdb);
        sc_errf(s, "Failed to process schema!\n");
        return -1;
    }

    schema_change = changed = prepare_changes(s, db, newdb, &s->plan, &scinfo);
    if (changed == SC_UNKNOWN_ERROR) {
        backout(newdb);
        cleanup_newdb(newdb);
        sc_errf(s, "Internal error");
        return SC_INTERNAL_ERROR;
    }

    adjust_version(changed, &scinfo, s, db, newdb);
    schema_change = changed = prepare_sc_plan(s, changed, db, newdb, &s->plan);
    print_schemachange_info(s, db, newdb);

    /*************** open  tables ********************************************/

    /* create temporary tables.  to try to avoid strange issues always
     * use a unqiue prefix.  this avoids multiple histories for these
     * new. files in our logs.
     *
     * since the prefix doesn't matter and bdb needs to be able to unappend
     * it, we let bdb choose the prefix */
    /* ignore failures, there shouln't be any and we'd just have a
     * truncated prefix anyway */
    bdb_get_new_prefix(new_prefix, sizeof(new_prefix), &bdberr);

    rc = open_temp_db_resume(newdb, new_prefix, s->resume, 0, tran);
    if (rc) {
        /* todo: clean up db */
        sc_errf(s, "failed opening new db\n");
        change_schemas_recover(s->table);
        return -1;
    }

    if (verify_new_temp_sc_db(db, newdb, tran)) {
        sc_errf(s, "some of the newdb's file versions are the same or less "
                   "than some of db's.\n"
                   "we will delete the newdb's file version entries from "
                   "llmeta and quit.\n"
                   "the new master should be able to resume this sc starting "
                   "over with a fresh newdb\n");

        if ((rc = bdb_del_file_versions(newdb->handle, tran, &bdberr)) ||
            bdberr != BDBERR_NOERROR) {
            sc_errf(s, "failed to clear newdb's file versions, hopefully "
                       "the new master can sort it out\n");
        }

        clean_exit();
    }

    /* we can resume sql threads at this point */

    /* must do this before rebuilding, otherwise we'll have the wrong
     * blobstripe_genid. */
    transfer_db_settings(db, newdb);

    get_db_datacopy_odh_tran(db, &datacopy_odh, tran);
    if (s->force_rebuild ||           /* we're first to set */
        newdb->instant_schema_change) /* we're doing instant sc*/
    {
        datacopy_odh = 1;
    }

    /* we set compression /odh options in bdb only here.
       for full operation they also need to be set in the meta tables.
       however the new db gets its meta table assigned further down,
       so we can't set meta options until we're there. */
    set_bdb_option_flags(newdb, s->headers, s->ip_updates,
                         newdb->instant_schema_change, newdb->version,
                         s->compress, s->compress_blobs, datacopy_odh);

    /* set sc_genids, 0 them if we are starting a new schema change, or
     * restore them to their previous values if we are resuming */
    if (init_sc_genids(newdb, s)) {
        sc_errf(s, "failed initilizing sc_genids\n");
        delete_temp_table(iq, newdb);
        change_schemas_recover(s->table);
        return -1;
    }

    sc_live = 1;
    db->sc_from = s->db = db;
    db->sc_to = s->newdb = newdb;
    gbl_sc_resume_start = 0; // for resuming SC/toblock_main: pointers are set
    MEMORY_SYNC;

    reset_sc_stat();
    wait_to_resume(s);

    /* skip converting records for fastinit and planned schema change
     * that doesn't require rebuilding anything. */
    if ((!newdb->plan || newdb->plan->plan_convert) ||
        changed == SC_CONSTRAINT_CHANGE) {
        doing_conversion = 1;
        rc = convert_all_records(db, newdb, newdb->sc_genids, s);
        if (rc == 1) rc = 0;
        doing_conversion = 0;
    } else
        rc = 0;

    if (rc)
        rc = SC_CONVERSION_FAILED;
    else if (stopsc)
        rc = SC_MASTER_DOWNGRADE;

    if (s->convert_sleep > 0) {
        sc_printf(s, "Sleeping after conversion for %d...\n", s->convert_sleep);
        logmsg(LOGMSG_DEBUG, "Sleeping after conversion for %d...\n",
               s->convert_sleep);
        sleep(s->convert_sleep);
        sc_printf(s, "...slept for %d\n", s->convert_sleep);
    }

    if (rc && rc != SC_MASTER_DOWNGRADE) {
        /* For live schema change, MUST do this before trying to remove
         * the .new tables or you get crashes */
        live_sc_off(db);

        if (gbl_sc_abort) {
            sc_errf(s, "convert_all_records aborted\n");
        } else {
            sc_errf(s, "convert_all_records failed\n");
        }

        for (i = 0; i < gbl_dtastripe; i++) {
            sc_errf(s, "  > stripe %2d was at 0x%016llx\n", i,
                    newdb->sc_genids[i]);
        }

        backout_constraint_pointers(newdb, db);
        delete_temp_table(iq, newdb);
        change_schemas_recover(s->table);
        return rc;
    }
    newdb->iq = NULL;
    return SC_OK;
}

#define FREE_ARRS                                                              \
    if (polddb_bthashsz != &olddb_bthashsz) {                                  \
        if (polddb_bthashsz) free(polddb_bthashsz);                            \
        if (pold_bdb_handle) free(pold_bdb_handle);                            \
        if (pnew_bdb_handle) free(pnew_bdb_handle);                            \
    }

int finalize_alter_table(struct ireq *iq, tran_type *transac)
{
    struct schema_change_type *s = iq->sc;
    int retries = 0;
    int rc, bdberr;
    struct dbtable *db;
    struct dbtable *newdb;
    void *old_bdb_handle, *new_bdb_handle;
    int olddb_bthashsz;
    struct dbtable **dbs;
    struct dbtable **newdbs;
    void **pold_bdb_handle, **pnew_bdb_handle;
    int *polddb_bthashsz;
    int indx;
    int maxindx;

    if (s->timepart_nshards) {
        maxindx = s->timepart_nshards;

        dbs = s->timepart_dbs;
        newdbs = s->timepart_newdbs;

        polddb_bthashsz = (int *)malloc(maxindx * sizeof(int));
        pold_bdb_handle = (void **)malloc(maxindx * sizeof(void *));
        pnew_bdb_handle = (void **)malloc(maxindx * sizeof(void *));
    } else {
        maxindx = 1;

        dbs = &s->db;
        newdbs = &s->newdb;

        polddb_bthashsz = &olddb_bthashsz;
        pold_bdb_handle = &old_bdb_handle;
        pnew_bdb_handle = &new_bdb_handle;
    }

    for (indx = 0; indx < maxindx; indx++) {
        db = dbs[indx];
        newdb = newdbs[indx];

        iq->usedb = db;

        if (get_db_bthash_tran(db, &polddb_bthashsz[indx], transac) != 0)
            polddb_bthashsz[indx] = 0;

        bdb_lock_table_write(db->handle, transac);

        /* All records converted ok.  Whether this is live schema change or
         * not, the db is readonly at this point so we can reset the live
         * schema change flag. */

        sc_printf(s, "---- All records copied --- \n");

        /* Before this handle is closed, lets wait for all the db reads to
         * finish*/

        sc_live = 0;

        /* No insert transactions should happen after this
           so lock the table. */
        rc = restore_constraint_pointers(db, newdb);
        if (rc != 0) {
            sc_errf(s, "Error restoring constraing pointers!\n");
            goto backout;
        }

        /* from this point on failures should goto either backout if recoverable
         * or failure if unrecoverable */

        newdb->meta = db->meta;
    }

    /* TODO: at this point if a backup is going on, it will be bad */
    gbl_sc_commit_count++;

    for (indx = 0; indx < maxindx; indx++) {
        db = dbs[indx];
        newdb = newdbs[indx];

        /*begin updating things*/
        if (newdb->version == 1) {
            /* newdb's version has been reset */
            bdberr = bdb_reset_csc2_version(transac, db->dbname, db->version);
            if (bdberr != BDBERR_NOERROR) goto backout;
        }

        if ((rc = prepare_version_for_dbs_without_instant_sc(transac, db,
                                                             newdb)))
            goto backout;

        /* load new csc2 data */
        rc = load_new_table_schema_tran(thedb, transac, /*s->table*/ db->dbname,
                                        s->newcsc2);
        if (rc != 0) {
            sc_errf(s, "Error loading new schema into meta tables, "
                       "trying again\n");
            goto backout;
        }

        if ((rc = set_header_and_properties(transac, newdb, s, 1,
                                            polddb_bthashsz[indx])))
            goto backout;

        /*update necessary versions and delete unnecessary files from newdb*/
        if (gbl_use_plan && newdb->plan) {
            logmsg(LOGMSG_INFO, " Updating versions with plan\n");
            rc = switch_versions_with_plan(transac, db, newdb);
        } else {
            logmsg(LOGMSG_INFO, " Updating versions without plan\n");
            rc = bdb_commit_temp_file_version_all(
                newdb->handle, transac,
                &bdberr) /*set all metapointers to new files*/;
        }

        if (rc) goto backout;

        /* delete any new file versions this table has */
        if (bdb_del_file_versions(newdb->handle, transac, &bdberr) ||
            bdberr != BDBERR_NOERROR) {
            sc_errf(s, "%s: bdb_del_file_versions failed\n", __func__);
            goto backout;
        }

        if ((rc = mark_schemachange_over_tran(db->dbname, transac)))
            goto backout;
    }

    for (indx = 0; indx < maxindx; indx++) {
        db = dbs[indx];
        newdb = newdbs[indx];

        /* remove the new.NUM. prefix */
        bdb_remove_prefix(newdb->handle);

        /* TODO: need to free db handle - right now we just leak some memory */
        /* replace the old db definition with a new one */

        newdb->plan = NULL;
        db->schema = clone_schema(newdb->schema);

        pnew_bdb_handle[indx] = newdb->handle;
        pold_bdb_handle[indx] = db->handle;

        free_db_and_replace(db, newdb);
        fix_constraint_pointers(db, newdb);

        /* update tags in memory */
        commit_schemas(/*s->table*/ db->dbname);
        update_dbstore(db); // update needs to occur after refresh of hashtbl
    }
    MEMORY_SYNC;

    if (!have_all_schemas()) sc_errf(s, "Missing schemas (internal error)\n");

    /* kludge: fix lrls */
    fix_lrl_ixlen_tran(transac);

    if (create_sqlmaster_records(transac)) {
        sc_errf(s, "create_sqlmaster_records failed\n");
        goto failed;
    }
    create_master_tables(); /* create sql statements */

    for (indx = 0; indx < maxindx; indx++) {
        db = dbs[indx];
        live_sc_off(db);
    }

    /* artificial sleep to aid testing */
    if (s->commit_sleep) {
        sc_printf(s, "artificially sleeping for %d...\n", s->commit_sleep);
        logmsg(LOGMSG_DEBUG, "artificially sleeping for %d...\n",
               s->commit_sleep);
        sleep(s->commit_sleep);
        sc_printf(s, "...slept for %d\n", s->commit_sleep);
    }

    if (!gbl_create_mode) {
        logmsg(LOGMSG_INFO, "Table %s is at version: %d\n", newdb->dbname,
               newdb->version);
    }

    for (indx = 0; indx < maxindx; indx++) {
        db = dbs[indx];
        newdb = newdbs[indx];

        llmeta_dump_mapping_table_tran(transac, thedb, db->dbname, 1);
    }

    sc_printf(s, "Schema change ok\n");

    for (indx = 0; indx < maxindx; indx++) {
        rc = bdb_close_only(pold_bdb_handle[indx], &bdberr);
        if (rc) {
            sc_errf(s, "Failed closing new db, bdberr\n", bdberr);
            goto failed;
        } else
            sc_printf(s, "Close new db ok\n");

        bdb_handle_reset_tran(pnew_bdb_handle[indx], transac);

        rc = bdb_free_and_replace(pold_bdb_handle[indx], pnew_bdb_handle[indx],
                                  &bdberr);
        if (rc) {
            sc_errf(s, "Failed freeing old db, bdberr %d\n", bdberr);
            goto failed;
        } else
            sc_printf(s, "bdb free ok\n");
    }

    for (indx = 0; indx < maxindx; indx++) {
        db = dbs[indx];
        newdb = newdbs[indx];

        /* reliable per table versioning */
        rc = table_version_upsert(db, transac, &bdberr);
        if (rc) {
            sc_errf(s, "Failed updating table version bdberr %d\n", bdberr);
            goto failed;
        }

        set_odh_options_tran(db, transac);

        if (polddb_bthashsz[indx]) {
            logmsg(LOGMSG_INFO,
                   "Rebuilding bthash for table %s, size %dkb per stripe\n",
                   db->dbname, polddb_bthashsz[indx]);
            bdb_handle_dbp_add_hash(db->handle, polddb_bthashsz[indx]);
        }
    }

    for (indx = 0; indx < maxindx; indx++) {
        db = dbs[indx];
        newdb = newdbs[indx];

        /* This happens in lockstep with bdb_set_in_schema_change */
        /* delete files we don't need now */
        sc_del_unused_files_tran(db, transac);
        memset(newdb, 0xff, sizeof(struct dbtable));
        free(newdb);
    }

    FREE_ARRS;
    sc_printf(s, "Schema change finished, seed %llx\n", sc_seed);
    return 0;

backout:
    for (indx = 0; indx < maxindx; indx++) {
        db = dbs[indx];
        newdb = newdbs[indx];

        backout_constraint_pointers(newdb, db);
#if 0 /* bp sc backout deals with this */
        delete_temp_table(iq, newdb);
#endif
        change_schemas_recover(/*s->table*/ db->dbname);

        logmsg(LOGMSG_WARN,
               "##### BACKOUT #####   %s v: %d sc:%d lrl: %d odh:%d bdb:%p\n",
               db->dbname, db->version, db->instant_schema_change, db->lrl,
               db->odh, db->handle);
    }

    FREE_ARRS;

    return -1;

failed:
    /* TODO why do we do this stuff if we're just going to clean_exit()? */
    for (indx = 0; indx < maxindx; indx++) {
        db = dbs[indx];
        live_sc_off(db);
    }

    FREE_ARRS;

    sc_errf(s, "Fatal error during schema change.  Exiting\n");
    /* from exit msgtrap */
    clean_exit();
    return -1;
}

int do_upgrade_table_int(struct schema_change_type *s)
{
    int rc = SC_OK;
    int i;

    struct dbtable *db;
    struct scinfo scinfo;

    db = get_dbtable_by_name(s->table);
    if (db == NULL) return SC_TABLE_DOESNOT_EXIST;

    s->db = db;

    if (s->start_genid != 0) s->scanmode = SCAN_DUMP;

    // check whether table is ready for upgrade
    set_schemachange_options(s, db, &scinfo);
    if ((rc = check_option_coherency(s, db, &scinfo))) return rc;

    if (s->fulluprecs) {
        print_schemachange_info(s, db, db);
        sc_printf(s, "Starting FULL table upgrade.\n");
    }

    if (init_sc_genids(db, s)) {
        sc_errf(s, "failed initilizing sc_genids\n");
        return SC_LLMETA_ERR;
    }

    live_sc_off(db);
    MEMORY_SYNC;

    reset_sc_stat();

    doing_upgrade = 1;
    rc = upgrade_all_records(db, db->sc_genids, s);
    doing_upgrade = 0;

    if (stopsc)
        rc = SC_MASTER_DOWNGRADE;
    else if (rc) {
        rc = SC_CONVERSION_FAILED;
        if (gbl_sc_abort)
            sc_errf(s, "upgrade_all_records aborted\n");
        else
            sc_errf(s, "upgrade_all_records failed\n");

        for (i = 0; i < gbl_dtastripe; i++) {
            sc_errf(s, "  > stripe %2d was at 0x%016llx\n", i,
                    db->sc_genids[i]);
        }
    }

    return rc;
}

int finalize_upgrade_table(struct schema_change_type *s)
{
    int rc;
    int nretries;
    tran_type *tran = NULL;

    struct ireq iq;
    init_fake_ireq(thedb, &iq);
    iq.usedb = s->db;

    for (nretries = 0, rc = 1; rc != 0 && nretries++ <= gbl_maxretries;) {
        if (tran != NULL) {
            trans_abort(&iq, tran);
            tran = NULL;
        }

        rc = trans_start_sc(&iq, NULL, &tran);
        if (rc != 0) continue;

        rc = mark_schemachange_over_tran(s->db->dbname, tran);
        if (rc != 0) continue;

        rc = trans_commit(&iq, tran, gbl_mynode);
    }

    return rc;
}
