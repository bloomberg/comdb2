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
#include "sc_rename_table.h"
#include "sc_schema.h"
#include "sc_global.h"
#include "sc_callbacks.h"

int gbl_lightweight_rename = 0;

int do_rename_table(struct ireq *iq, struct schema_change_type *s,
                    tran_type *tran)
{
    struct dbtable *db, *db2;
    iq->usedb = db = s->db = get_dbtable_by_name(s->tablename);
    if (db == NULL) {
        sc_client_error(s, "Table doesn't exist");
        return SC_TABLE_DOESNOT_EXIST;
    }
    if (((db2 = get_dbtable_by_name(s->newtable)) != NULL) && db != db2) {
        sc_client_error(s, "New table name exists");
        return SC_TABLE_ALREADY_EXIST;
    }
    if (db->n_rev_constraints > 0) {
        /* we could revise this later on */
        sc_client_error(s, "Cannot rename a table referenced by a foreign key");
        return -1;
    }

    return SC_OK;
}

int finalize_alias_table(struct ireq *iq, struct schema_change_type *s,
                         tran_type *tran)
{
    struct dbtable *db = s->db;
    int rc = 0;

    assert(s->kind == SC_ALIASTABLE);

    /* Before this handle is closed, lets wait for all the db reads to finish*/
    bdb_lock_table_write(db->handle, tran);
    bdb_lock_tablename_write(db->handle, s->newtable, tran);

    s->already_finalized = 1;

    hash_sqlalias_db(db, s->newtable);

    rc = bdb_set_table_sqlalias(db->tablename, tran, db->sqlaliasname);
    if (rc) {
        /* undo alias */
        sc_errf(s, "bdb_set_table_sqlalias failed rc %d\n", rc);
        hash_sqlalias_db(db, db->tablename);
        return rc;
    }

    gbl_sc_commit_count++;

    /* TODO: review this, do we need it?*/
    live_sc_off(db);

    return 0;
}

int finalize_rename_table(struct ireq *iq, struct schema_change_type *s,
                          tran_type *tran)
{
    struct dbtable *db = s->db;
    char *newname = strdup(s->newtable);
    int rc = 0;
    int bdberr = 0;
    char *oldname = NULL;

    assert(s->kind == SC_RENAMETABLE);

    if (!newname) {
        sc_errf(s, "strdup error\n");
        return -1;
    }

    /* Before this handle is closed, lets wait for all the db reads to finish*/
    bdb_lock_table_write(db->handle, tran);
    bdb_lock_tablename_write(db->handle, newname, tran);

    s->already_finalized = 1;

    /* renamed table schema gets bumped */
    rc = table_version_upsert(db, tran, &bdberr);
    if (rc) {
        sc_errf(s, "Failed updating table version bdberr %d\n", bdberr);
        goto tran_error;
    }

    /* update all associated metadata */
    rc = bdb_rename_table_metadata(db->handle, tran, newname,
                                   db->schema_version, &bdberr);
    if (rc) {
        sc_errf(s, "Failed to rename metadata structure for %s\n",
                db->tablename);
        goto tran_error;
    }

    /* update the table options */
    rc = rename_table_options(tran, db, newname);
    if (rc) {
        sc_errf(s, "Failed to rename table options for %s\n", db->tablename);
        goto tran_error;
    }

    /* Update table sequences */
    rc = rename_table_sequences(tran, db, newname);
    if (rc) {
        sc_errf(s, "Failed to rename table sequences for %s\n", db->tablename);
        goto tran_error;
    }

    /* fragile, handle with care */
    oldname = db->tablename;
    rc = rename_db(db, newname);
    if (rc) {
        /* crash the schema change, next master will hopefully have more memory
         */
        abort();
    }

    MEMORY_SYNC;

    /* update the list of tables in llmeta */
    rc = llmeta_set_tables(tran, thedb);
    if (rc) {
        sc_errf(s, "Failed to set table names in low level meta\n");
        goto recover_memory;
    }

    /* set table version for the renamed name */
    rc = table_version_set(tran, newname, db->tableversion + 1);
    if (rc) {
        sc_errf(s, "Failed to set table version for %s\n", db->tablename);
        goto tran_error;
    }

    gbl_sc_commit_count++;

    live_sc_off(db);

    if (oldname)
        free(oldname);

    return rc;

recover_memory:
    /* backout memory changes */
    rename_db(db, oldname);
    return rc;

tran_error:
    if (newname)
        free(newname);
    return rc;
}
