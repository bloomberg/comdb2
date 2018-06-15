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
#include <translistener.h>

#include "schemachange.h"
#include "sc_fastinit_table.h"
#include "sc_schema.h"
#include "sc_struct.h"
#include "sc_csc2.h"
#include "sc_global.h"
#include "sc_logic.h"
#include "sc_callbacks.h"
#include "sc_records.h"
#include "sc_drop_table.h"
#include "sc_add_table.h"
#include "sc_alter_table.h"

int do_fastinit(struct ireq *iq, struct schema_change_type *s, tran_type *tran)
{
    struct dbtable *db;
    struct dbtable *newdb;
    int rc = 0;
    int bdberr = 0;
    int datacopy_odh = 0;
    char new_prefix[32];
    int foundix;
    struct scinfo scinfo;

    iq->usedb = db = s->db = get_dbtable_by_name(s->table);
    if (db == NULL) {
        sc_errf(s, "Table doesn't exists\n");
        reqerrstr(iq, ERR_SC, "Table doesn't exists");
        return SC_TABLE_DOESNOT_EXIST;
    }
    if (db->n_rev_constraints > 0) {
        sc_errf(s, "Can't drop tables with foreign constraints\n");
        reqerrstr(iq, ERR_SC, "Can't drop tables with foreign constraints");
        return -1;
    }

    set_schemachange_options_tran(s, db, &scinfo, tran);

    extern int gbl_broken_max_rec_sz;
    int saved_broken_max_rec_sz = gbl_broken_max_rec_sz;

    if (s->db->lrl > COMDB2_MAX_RECORD_SIZE) {
        // we want to allow fastiniting this tbl
        gbl_broken_max_rec_sz = s->db->lrl - COMDB2_MAX_RECORD_SIZE;
    }

    pthread_mutex_lock(&csc2_subsystem_mtx);
    if ((rc = load_db_from_schema(s, thedb, &foundix, iq))) {
        pthread_mutex_unlock(&csc2_subsystem_mtx);
        return rc;
    }

    gbl_broken_max_rec_sz = saved_broken_max_rec_sz;

    newdb = s->newdb = create_db_from_schema(thedb, s, db->dbnum, foundix, 1);
    if (newdb == NULL) {
        sc_errf(s, "Internal error\n");
        pthread_mutex_unlock(&csc2_subsystem_mtx);
        return SC_INTERNAL_ERROR;
    }

    newdb->iq = iq;

    if (add_cmacc_stmt(newdb, 1) != 0) {
        backout_schemas(newdb->tablename);
        cleanup_newdb(newdb);
        sc_errf(s, "Failed to process schema!\n");
        pthread_mutex_unlock(&csc2_subsystem_mtx);
        return -1;
    }
    pthread_mutex_unlock(&csc2_subsystem_mtx);

    /* create temporary tables.  to try to avoid strange issues always
     * use a unqiue prefix.  this avoids multiple histories for these
     * new. files in our logs.
     *
     * since the prefix doesn't matter and bdb needs to be able to unappend
     * it, we let bdb choose the prefix */
    /* ignore failures, there shouln't be any and we'd just have a
     * truncated prefix anyway */
    bdb_get_new_prefix(new_prefix, sizeof(new_prefix), &bdberr);

    rc = open_temp_db_resume(newdb, new_prefix, 0, 0, tran);
    if (rc) {
        /* todo: clean up db */
        sc_errf(s, "failed opening new db\n");
        change_schemas_recover(s->table);
        return -1;
    }

    /* must do this before rebuilding, otherwise we'll have the wrong
     * blobstripe_genid. */
    transfer_db_settings(db, newdb);

    get_db_datacopy_odh_tran(db, &datacopy_odh, tran);
    if (s->fastinit || s->force_rebuild || /* we're first to set */
        newdb->instant_schema_change)      /* we're doing instant sc*/
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

    MEMORY_SYNC;

    return SC_OK;
}

int finalize_fastinit_table(struct ireq *iq, struct schema_change_type *s,
                            tran_type *tran)
{
    int rc = 0;
    rc = finalize_alter_table(iq, s, tran);
    return rc;
}
