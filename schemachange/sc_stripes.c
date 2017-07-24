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
    printf("Set new stripe settings in bdb OK\n");
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

    /* STOP THREADS */
    stop_threads(thedb);
    broadcast_quiesce_threads();

    /* CLOSE ALL TABLES */
    if (close_all_dbs() != 0) exit(1);
    broadcast_close_all_dbs();

    /* RENAME BLOB FILES */
    if (newblobstripe && !gbl_blobstripe) {
        tran_type *tran = NULL;
        struct ireq iq = {0};

        init_fake_ireq(thedb, &iq);
        iq.usedb = thedb->dbs[0];

        rc = trans_start(&iq, NULL, &tran);
        if (rc) {
            fprintf(stderr, "morestripe: %d: trans_start rc %d\n", __LINE__,
                    rc);
            broadcast_resume_threads();
            resume_threads(thedb);
            return SC_FAILED_TRANSACTION;
        }

        for (ii = 0; ii < thedb->num_dbs; ii++) {
            unsigned long long genid;
            db = thedb->dbs[ii];
            rc = bdb_rename_blob1(db->handle, tran, &genid, &bdberr);
            if (rc != 0) {
                fprintf(stderr, "morestripe: couldn't rename blob 1 for table "
                                "'%s' bdberr %d\n",
                        db->dbname, bdberr);
                trans_abort(&iq, tran);
                broadcast_resume_threads();
                resume_threads(thedb);
                return SC_BDB_ERROR;
            }

            /* record the genid for the conversion in the table's meta database
             */
            rc = put_blobstripe_genid(db, tran, genid);
            if (rc != 0) {
                fprintf(stderr,
                        "morestripe: couldn't record genid for table '%s'\n",
                        db->dbname);
                trans_abort(&iq, tran);
                broadcast_resume_threads();
                resume_threads(thedb);
                return SC_INTERNAL_ERROR;
            }

            /* remember this - will need it if we do a schema change later on..
             */
            db->blobstripe_genid = genid;

            printf("Converted table '%s' to blobstripe with genid 0x%llx\n",
                   db->dbname, genid);
        }

        rc = trans_commit(&iq, tran, gbl_mynode);
        if (rc) {
            fprintf(stderr, "morestripe: couldn't commit rename trans\n");
            broadcast_resume_threads();
            resume_threads(thedb);
            return SC_FAILED_TRANSACTION;
        }
        /* After this point there is no backing out */
    }

    /* CREATE ALL NEW FILES */
    printf("Creating new files...\n");
    for (ii = 0; ii < thedb->num_dbs; ii++) {
        db = thedb->dbs[ii];
        rc = bdb_create_stripes(db->handle, newdtastripe, newblobstripe,
                                &bdberr);
        if (rc != 0) {
            fprintf(
                stderr,
                "morestripe: failed making extra stripes for table '%s': %d\n",
                db->dbname, bdberr);
            return SC_BDB_ERROR;
        }
    }
    printf("Created new files OK\n");

    /* SET NEW STRIPE FACTORS */
    apply_new_stripe_settings(newdtastripe, newblobstripe);

    /* OPEN ALL TABLES */
    if (open_all_dbs() != 0) exit(1);
    broadcast_morestripe_and_open_all_dbs(newdtastripe, newblobstripe);

    /* START THREADS */
    broadcast_resume_threads();
    resume_threads(thedb);

    printf("\n");
    printf("MORESTRIPED SUCCESSFULLY\n");
    printf("New settings are: dtastripe %d blobstripe? %s\n", newdtastripe,
           newblobstripe ? "YES" : "NO");
    printf("**************************************\n");
    printf("* BE SURE TO FOLLOW UP BY MAKING THE *\n");
    printf("* APPROPRIATE CHANGE TO THE LRL FILE *\n");
    printf("* ON EACH CLUSTER NODE               *\n");
    printf("**************************************\n");

    return SC_OK;
}
