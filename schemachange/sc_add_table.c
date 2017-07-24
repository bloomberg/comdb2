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

#include <translistener.h>

#include "schemachange.h"
#include "sc_add_table.h"
#include "logmsg.h"
#include "sc_schema.h"
#include "sc_util.h"
#include "sc_logic.h"
#include "sc_csc2.h"

static inline int adjust_master_tables(struct dbtable *newdb, const char *csc2,
                                       struct ireq *iq, void *trans)
{
    int rc;
    int pi = 0; // partial indexes

    fix_lrl_ixlen_tran(trans);
    /* fix_lrl_ixlen() usually sets csc2_schema/csc2_schema_len, but for llmeta
     * dbs, it grabs this info from llmeta and in the case where we are adding a
     * table the schema is not yet in llmeta */
    if (!newdb->csc2_schema && csc2) {
        newdb->csc2_schema = strdup(csc2);
        newdb->csc2_schema_len = strlen(newdb->csc2_schema);
    }
    rc = create_sqlmaster_records(trans);

    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "create_sqlmaster_records failed rc %d\n", rc);
        return SC_INTERNAL_ERROR;
    }
    /* TODO: ask why this function has no return codes */
    create_master_tables(); /* create sql statements */

    extern int gbl_partial_indexes;
    extern int gbl_expressions_indexes;
    if (((gbl_partial_indexes && newdb->ix_partial) ||
         (gbl_expressions_indexes && newdb->ix_expr)) &&
        newdb->dbenv->master == gbl_mynode)
        rc = new_indexes_syntax_check(iq);

    if (rc)
        return SC_CSC2_ERROR;
    else
        return 0;
}

static inline int get_db_handle(struct dbtable *newdb, void *trans)
{
    int bdberr;
    if (newdb->dbenv->master == gbl_mynode) {
        /* I am master: create new db */
        newdb->handle = bdb_create_tran(
            newdb->dbname, thedb->basedir, newdb->lrl, newdb->nix,
            newdb->ix_keylen, newdb->ix_dupes, newdb->ix_recnums,
            newdb->ix_datacopy, newdb->ix_collattr, newdb->ix_nullsallowed,
            newdb->numblobs + 1, thedb->bdb_env, 0, &bdberr, trans);
        open_auxdbs(newdb, 1);
    } else {
        /* I am NOT master: open replicated db */
        newdb->handle = bdb_open_more_tran(
            newdb->dbname, thedb->basedir, newdb->lrl, newdb->nix,
            newdb->ix_keylen, newdb->ix_dupes, newdb->ix_recnums,
            newdb->ix_datacopy, newdb->ix_collattr, newdb->ix_nullsallowed,
            newdb->numblobs + 1, thedb->bdb_env, trans, &bdberr);
        open_auxdbs(newdb, 0);
    }

    if (newdb->handle == NULL) {
        logmsg(LOGMSG_ERROR, "bdb_open:failed to open table %s/%s, rcode %d\n",
               thedb->basedir, newdb->dbname, bdberr);
        return SC_BDB_ERROR;
    }

    return SC_OK;
}

static inline int init_bthashsize_tran(struct dbtable *newdb, tran_type *tran)
{
    int bthashsz;

    if (get_db_bthash_tran(newdb, &bthashsz, tran) != 0) bthashsz = 0;

    if (bthashsz) {
        logmsg(LOGMSG_INFO, "Init with bthash size %dkb per stripe\n",
               bthashsz);
        bdb_handle_dbp_add_hash(newdb->handle, bthashsz);
    }

    return SC_OK;
}

/* Add a new table.  Assume new table has no db number.
 * If csc2 is provided then a filename is chosen, the lrl updated and
 * the file written with the csc2. */
int add_table_to_environment(char *table, const char *csc2,
                             struct schema_change_type *s, struct ireq *iq,
                             tran_type *trans)
{
    int rc;
    struct dbtable *newdb;

    if (!csc2) {
        logmsg(LOGMSG_ERROR, "%s: no filename or csc2!\n", __func__);
        return -1;
    }

    rc = dyns_load_schema_string((char *)csc2, thedb->envname, table);

    if (rc) {
        char *err;
        char *syntax_err;
        err = csc2_get_errors();
        syntax_err = csc2_get_syntax_errors();
        if (iq) reqerrstr(iq, ERR_SC, "%s", syntax_err);
        sc_errf(s, "%s\n", err);
        sc_errf(s, "error adding new table locally\n");
        logmsg(LOGMSG_WARN, "Failed to load schema for table %s\n", table);
        logmsg(LOGMSG_WARN, "Dumping schema for reference: '%s'\n", csc2);
        return SC_CSC2_ERROR;
    }
    newdb = newdb_from_schema(thedb, table, NULL, 0, thedb->num_dbs, 0);

    if (newdb == NULL) return SC_INTERNAL_ERROR;

    newdb->dtastripe = gbl_dtastripe;

    newdb->iq = iq;

    if (add_cmacc_stmt(newdb, 0)) {
        logmsg(LOGMSG_ERROR, "%s: add_cmacc_stmt failed\n", __func__);
        rc = SC_CSC2_ERROR;
        goto err;
    }

    if (verify_constraints_exist(newdb, NULL, NULL, s) != 0) {
        logmsg(LOGMSG_ERROR, "%s: Verify constraints failed \n", __func__);
        rc = -1;
        goto err;
    }

    if (populate_reverse_constraints(newdb)) {
        logmsg(LOGMSG_ERROR, "%s: populating reverse constraints failed\n",
               __func__);
        rc = -1;
        goto err;
    }

    if (!sc_via_ddl_only() && validate_ix_names(newdb)) {
        rc = -1;
        goto err;
    }

    if ((rc = get_db_handle(newdb, trans))) goto err;

    gbl_sc_commit_count++;
    if (s && s->fastinit && s->db) {
        replace_db_idx(newdb, s->db->dbs_idx);
        free(s->db->handle);
        freedb(s->db);
    } else {
        thedb->dbs =
            realloc(thedb->dbs, (thedb->num_dbs + 1) * sizeof(struct dbtable *));
        newdb->dbs_idx = thedb->num_dbs;
        thedb->dbs[thedb->num_dbs++] = newdb;

        /* Add table to the hash. */
        hash_add(thedb->db_hash, newdb);
    }

    rc = adjust_master_tables(newdb, csc2, iq, trans);
    if (rc) {
        gbl_sc_commit_count--;
        --thedb->num_dbs;
        /* Remove table from the hash. */
        hash_del(thedb->db_hash, thedb->dbs[thedb->num_dbs]);
        thedb->dbs[thedb->num_dbs] = NULL;
        if (rc == SC_CSC2_ERROR) sc_errf(s, "New indexes syntax error\n");
        goto err;
    }
    newdb->schema->ix_blob = newdb->ix_blob;

    /*
    ** if ((rc = write_csc2_file(newdb, csc2))) {
    **     rc = SC_INTERNAL_ERROR;
    **     goto err;
    ** }
    */

    newdb->iq = NULL;
    init_bthashsize_tran(newdb, trans);

    return SC_OK;

err:
    newdb->iq = NULL;
    backout_schemas(newdb->dbname);
    cleanup_newdb(newdb);
    return rc;
}

static inline void set_empty_options(struct schema_change_type *s)
{
    if (s->headers == -1) s->headers = gbl_init_with_odh;
    if (s->compress == -1) s->compress = gbl_init_with_compr;
    if (s->compress_blobs == -1) s->compress_blobs = gbl_init_with_compr_blobs;
    if (s->ip_updates == -1) s->ip_updates = gbl_init_with_ipu;
    if (s->instant_sc == -1) s->instant_sc = gbl_init_with_instant_sc;
}

int do_add_table(struct ireq *iq, tran_type *trans)
{
    struct schema_change_type *s = iq->sc;
    int rc = SC_OK;
    struct dbtable *db;
    set_empty_options(s);

    if ((rc = check_option_coherency(s, NULL, NULL))) return rc;

    if ((db = get_dbtable_by_name(s->table))) {
        sc_errf(s, "Table %s already exists", s->table);
        logmsg(LOGMSG_ERROR, "Table %s already exists\n", s->table);
        return SC_TABLE_ALREADY_EXIST;
    }

    rc = add_table_to_environment(s->table, s->newcsc2, s, iq, trans);
    if (rc) {
        sc_errf(s, "error adding new table locally\n");
        return rc;
    }

    if (!(db = get_dbtable_by_name(s->table))) return SC_INTERNAL_ERROR;

    iq->usedb = db->sc_to = s->db = db;
    db->odh = s->headers;
    db->inplace_updates = s->ip_updates;
    db->version = 1;

    /* compression algorithms set to 0 for new table - this
       will have to be changed manually by the operator */
    set_bdb_option_flags(db, s->headers, s->ip_updates, s->instant_sc,
                         db->version, s->compress, s->compress_blobs, 1);

    return 0;
}

int finalize_add_table(struct ireq *iq, tran_type *tran)
{
    struct schema_change_type *s = iq->sc;
    int rc, bdberr;
    struct dbtable *db = s->db;

    sc_printf(s, "Start add table transaction ok\n");
    rc = load_new_table_schema_tran(thedb, tran, s->table, s->newcsc2);
    if (rc != 0) {
        sc_errf(s, "error recording new table schema\n");
        return rc;
    }

    /* Set instant schema-change */
    db->instant_schema_change = db->odh && s->instant_sc;

    if ((rc = set_header_and_properties(tran, db, s, 0, gbl_init_with_bthash)))
        return rc;

    if (llmeta_set_tables(tran, thedb)) {
        sc_errf(s, "Failed to set table names in low level meta\n");
        return rc;
    }

    if ((rc = bdb_table_version_select(db->handle, tran, &db->tableversion,
                                       &bdberr)) != 0) {
        sc_errf(s, "Failed fetching table version bdberr %d\n", bdberr);
        return rc;
    }

    if ((rc = mark_schemachange_over_tran(db->dbname, tran))) return rc;

    /* Save .ONDISK as schema version 1 if instant_sc is enabled. */
    if (db->odh && db->instant_schema_change) {
        struct schema *ver_one;
        if ((rc = prepare_table_version_one(tran, db, &ver_one))) return rc;
        add_tag_schema(db->dbname, ver_one);
    }

    fix_lrl_ixlen_tran(tran);

    create_sqlmaster_records(tran);
    create_master_tables();

    db->sc_to = NULL;
    update_dbstore(db);
    sc_printf(s, "Add table ok\n");

    if (gbl_init_with_bthash) {
        logmsg(LOGMSG_INFO, "Init table with bthash size %dkb per stripe\n",
               gbl_init_with_bthash);
        if (put_db_bthash(db, tran, gbl_init_with_bthash) != 0) {
            logmsg(LOGMSG_ERROR, "Failed to write bthash to meta table\n");
            return -1;
        }
        bdb_handle_dbp_add_hash(db->handle, gbl_init_with_bthash);
    }

    sc_printf(s, "Schema change ok\n");
    return 0;
}
