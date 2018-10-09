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
#include "sc_stripes.h"

void apply_new_stripe_settings(int newdtastripe, int newblobstripe)
{
    gbl_dtastripe = newdtastripe;
    if (newblobstripe) gbl_blobstripe = gbl_dtastripe;
    bdb_attr_set(thedb->bdb_attr, BDB_ATTR_DTASTRIPE, gbl_dtastripe);
    bdb_attr_set(thedb->bdb_attr, BDB_ATTR_BLOBSTRIPE, gbl_blobstripe);
    logmsg(LOGMSG_INFO, "Set new stripe settings in bdb OK\n");
}

/*
 * Dynamically increase dtastripe factor.
 *
 * 1. Stop threads
 * 2. Close all tables
 * 3. Rename blob files if applicable
 * 4. Create new dtas files
 * 5. Set new dtastripe settings
 * 6. Open all tables
 * 7. Start threads
 */
int do_alter_stripes_int(struct schema_change_type *s)
{
    int ii, rc, bdberr;
    int newdtastripe = s->newdtastripe;
    int newblobstripe = s->blobstripe;
    struct dbtable *db;
    tran_type *tran = NULL, *sc_logical_tran = NULL, *phys_tran;
    struct ireq iq = {0};

    wrlock_schema_lk();

    init_fake_ireq(thedb, &iq);
    iq.usedb = thedb->dbs[0];
    iq.tranddl = 1;

    rc = trans_start_logical_sc(&iq, &sc_logical_tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "morestripe: %d: trans_start rc %d\n", __LINE__,
               rc);
        unlock_schema_lk();
        return SC_FAILED_TRANSACTION;
    }

    bdb_ltran_get_schema_lock(sc_logical_tran);

    if ((phys_tran = bdb_get_physical_tran(sc_logical_tran)) == NULL) {
        if (bdb_tran_abort(thedb->bdb_env, sc_logical_tran, &bdberr) != 0)
            abort();
        logmsg(LOGMSG_ERROR, "morestripe: %d: get_physical-tran returns NULL\n",
               __LINE__);
        unlock_schema_lk();
        return SC_FAILED_TRANSACTION;
    }

    /* Get writelocks on all tables */
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        db = thedb->dbs[ii];
        if ((rc = bdb_lock_table_write(db->handle, phys_tran)) != 0) {
            logmsg(LOGMSG_ERROR,
                   "morestripe: couldn't acquire table writelocks\n");
            unlock_schema_lk();
            return SC_INTERNAL_ERROR;
        }
    }

    unlock_schema_lk();

    if (close_all_dbs() != 0)
        exit(1);

    /* RENAME BLOB FILES */
    if (newblobstripe && !gbl_blobstripe) {

        rc = trans_start(&iq, NULL, &tran);
        if (rc) {
            logmsg(LOGMSG_ERROR, "morestripe: %d: trans_start rc %d\n", __LINE__,
                    rc);
            return SC_FAILED_TRANSACTION;
        }

        for (ii = 0; ii < thedb->num_dbs; ii++) {
            unsigned long long genid;
            db = thedb->dbs[ii];
            rc = bdb_rename_blob1(db->handle, tran, &genid, &bdberr);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "morestripe: couldn't rename blob 1 for table "
                                "'%s' bdberr %d\n",
                        db->tablename, bdberr);
                trans_abort(&iq, tran);
                return SC_BDB_ERROR;
            }

            /* record the genid for the conversion in the table's meta database
             */
            rc = put_blobstripe_genid(db, tran, genid);
            if (rc != 0) {
                logmsg(LOGMSG_ERROR,
                        "morestripe: couldn't record genid for table '%s'\n",
                        db->tablename);
                trans_abort(&iq, tran);
                resume_threads(thedb);
                return SC_INTERNAL_ERROR;
            }

            /* remember this - will need it if we do a schema change later on..
             */
            db->blobstripe_genid = genid;

            logmsg(LOGMSG_INFO,"Converted table '%s' to blobstripe with genid 0x%llx\n",
                   db->tablename, genid);
        }

        rc = trans_commit(&iq, tran, gbl_mynode);
        if (rc) {
            logmsg(LOGMSG_ERROR, "morestripe: couldn't commit rename trans\n");
            return SC_FAILED_TRANSACTION;
        }
    }

    /* CREATE ALL NEW FILES */
    logmsg(LOGMSG_INFO, "Creating new files...\n");
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        db = thedb->dbs[ii];
        rc = bdb_create_stripes(db->handle, newdtastripe, newblobstripe,
                                &bdberr);
        if (rc != 0) {
            logmsg(
                LOGMSG_ERROR,
                "morestripe: failed making extra stripes for table '%s': %d\n",
                db->tablename, bdberr);
            return SC_BDB_ERROR;
        }
    }
    logmsg(LOGMSG_INFO, "Created new files OK\n");

    bdb_set_global_stripe_info(NULL, newdtastripe, newblobstripe, &bdberr);
    apply_new_stripe_settings(newdtastripe, newblobstripe);

    if (open_all_dbs() != 0) exit(1);

    if ((rc = bdb_llog_scdone_tran(thedb->bdb_env, change_stripe, phys_tran,
                                   NULL, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "morestripe: couldn't write scdone record\n");
        return SC_INTERNAL_ERROR;
    }

    rc = trans_commit(&iq, sc_logical_tran, gbl_mynode);
    if (rc) {
        logmsg(LOGMSG_ERROR, "morestripe: couldn't commit rename trans\n");
        return SC_FAILED_TRANSACTION;
    }

    logmsg(LOGMSG_INFO, "MORESTRIPED SUCCESSFULLY\n");
    logmsg(LOGMSG_INFO, "New settings are: dtastripe %d blobstripe? %s\n", newdtastripe,
           newblobstripe ? "YES" : "NO");
    return SC_OK;
}
