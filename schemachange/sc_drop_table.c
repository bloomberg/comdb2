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
#include <autoanalyze.h>
#include <translistener.h>

#include "schemachange.h"
#include "sc_drop_table.h"
#include "sc_schema.h"
#include "sc_global.h"
#include "sc_callbacks.h"

static int delete_table(struct dbtable *db, tran_type *tran)
{
    remove_constraint_pointers(db);

    int rc, bdberr;
    if ((rc = bdb_close_only(db->handle, &bdberr))) {
        fprintf(stderr, "bdb_close_only rc %d bdberr %d\n", rc, bdberr);
        return -1;
    }

    char *table = db->dbname;
    delete_db(table);
    MEMORY_SYNC;
    delete_schema(table);
    bdb_del_table_csonparameters(tran, table);
    return 0;
}

int do_drop_table(struct ireq *iq, tran_type *tran)
{
    struct schema_change_type *s = iq->sc;
    struct dbtable *db;
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

    return SC_OK;
}

int finalize_drop_table(struct ireq *iq, tran_type *tran)
{
    struct schema_change_type *s = iq->sc;
    struct dbtable *db = s->db;
    int rc = 0;
    int bdberr = 0;

    /* Before this handle is closed, lets wait for all the db reads to finish*/
    bdb_lock_table_write(db->handle, tran);

    /* at this point if a backup is going on, it will be bad */
    gbl_sc_commit_count++;

    if ((rc = mark_schemachange_over_tran(db->dbname, tran))) return rc;

    delete_table(db, tran);
    /*Now that we don't have any data, please clear unwanted schemas.*/
    bdberr = bdb_reset_csc2_version(tran, db->dbname, db->version);
    if (bdberr != BDBERR_NOERROR) return -1;

    if ((rc = bdb_del_file_versions(db->handle, tran, &bdberr))) {
        sc_errf(s, "%s: bdb_del_file_versions failed with rc: %d bdberr: "
                   "%d\n",
                __func__, rc, bdberr);
        return rc;
    }

    if (s->drop_table && (rc = table_version_upsert(db, tran, &bdberr)) != 0) {
        sc_errf(s, "Failed updating table version bdberr %d\n", bdberr);
        return rc;
    }

    if ((rc = llmeta_set_tables(tran, thedb)) != 0) {
        sc_errf(s, "Failed to set table names in low level meta\n");
        return rc;
    }

    if ((rc = create_sqlmaster_records(tran)) != 0) {
        sc_errf(s, "create_sqlmaster_records failed\n");
        return rc;
    }
    create_master_tables(); /* create sql statements */

    live_sc_off(db);

    if (!gbl_create_mode) {
        logmsg(LOGMSG_INFO, "Table %s is at version: %d\n", db->dbname,
               db->version);
    }

    if (gbl_replicate_local) local_replicant_write_clear(db);

    /* delete files we don't need now */
    sc_del_unused_files_tran(db, tran);

    return 0;
}
