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
#include "sc_util.h"
#include "views.h"
#include "macc_glue.h"

extern int gbl_broken_max_rec_sz;

/* return old value */
int fix_broken_max_rec_sz(int lrl)
{
    int ret = gbl_broken_max_rec_sz;

    if (lrl > COMDB2_MAX_RECORD_SIZE) {
        /* NOTE: this algebra seems funny, shouldn't we
         * set lrl to COMDB2_MAX_RECORD_SIZE here?
         */
        // we want to allow fastiniting this tbl
        gbl_broken_max_rec_sz = lrl - COMDB2_MAX_RECORD_SIZE;
    }

    return ret;
}

int do_fastinit(struct ireq *iq, struct schema_change_type *s, tran_type *tran)
{
    struct dbtable *db;
    struct dbtable *newdb;
    int rc = 0;
    int bdberr = 0;
    int datacopy_odh = 0;
    char new_prefix[32];
    struct scinfo scinfo;
    struct errstat err = {0};

    iq->usedb = db = s->db = get_dbtable_by_name(s->tablename);
    if (db == NULL) {
        sc_errf(s, "Table doesn't exists\n");
        reqerrstr(iq, ERR_SC, "Table doesn't exists");
        return SC_TABLE_DOESNOT_EXIST;
    }

    if ((!iq || iq->tranddl <= 1) && db->n_rev_constraints > 0 &&
        !self_referenced_only(db)) {
        sc_client_error(s, "Can't truncate a table referenced by a foreign key");
        return -1;
    }

    set_schemachange_options_tran(s, db, &scinfo, tran);


    Pthread_mutex_lock(&csc2_subsystem_mtx);

    int saved_broken_max_rec_sz = fix_broken_max_rec_sz(s->db->lrl);
    newdb = s->newdb =
        create_new_dbtable(thedb, s->tablename, s->newcsc2, db->dbnum,
                           1 /* sc_alt_name */, 1 /* allow ull */, 0, &err);
    gbl_broken_max_rec_sz = saved_broken_max_rec_sz;

    if (!newdb) {
        sc_client_error(s, "%s", err.errstr);
        Pthread_mutex_unlock(&csc2_subsystem_mtx);
        return SC_INTERNAL_ERROR;
    }

    newdb->dtastripe = gbl_dtastripe; // we have only one setting currently
    newdb->odh = s->headers;
    /* don't lose precious flags like this */
    newdb->instant_schema_change = s->headers && s->instant_sc;
    newdb->inplace_updates = s->headers && s->ip_updates;
    newdb->iq = iq;

    /* reset csc2? */
    newdb->schema_version = 1;

    Pthread_mutex_unlock(&csc2_subsystem_mtx);

    /* create temporary tables.  to try to avoid strange issues always
     * use a unqiue prefix.  this avoids multiple histories for these
     * new. files in our logs.
     *
     * since the prefix doesn't matter and bdb needs to be able to unappend
     * it, we let bdb choose the prefix */
    /* ignore failures, there shouln't be any and we'd just have a
     * truncated prefix anyway */
    bdb_get_new_prefix(new_prefix, sizeof(new_prefix), &bdberr);

    int local_lock = 0;
    if (!iq->sc_locked) {
        local_lock = 1;
        wrlock_schema_lk();
    }
    rc = open_temp_db_resume(iq, newdb, new_prefix, 0, 0, tran);
    if (local_lock)
        unlock_schema_lk();
    if (rc) {
        cleanup_newdb(newdb);
        sc_errf(s, "failed opening new db\n");
        change_schemas_recover(s->tablename);
        return -1;
    }

    /* must do this before rebuilding, otherwise we'll have the wrong
     * blobstripe_genid. */
    transfer_db_settings(db, newdb);

    get_db_datacopy_odh_tran(db, &datacopy_odh, tran);
    if (IS_FASTINIT(s) || s->force_rebuild || /* we're first to set */
        newdb->instant_schema_change)         /* we're doing instant sc*/
    {
        datacopy_odh = 1;
    }

    /* we set compression /odh options in bdb only here.
       for full operation they also need to be set in the meta tables.
       however the new db gets its meta table assigned further down,
       so we can't set meta options until we're there. */
    set_bdb_option_flags(newdb, s->headers, s->ip_updates,
                         newdb->instant_schema_change, newdb->schema_version,
                         s->compress, s->compress_blobs, datacopy_odh);

    MEMORY_SYNC;

    return SC_OK;
}

int finalize_fastinit_table(struct ireq *iq, struct schema_change_type *s,
                            tran_type *tran)
{
    int rc = 0;
    struct dbtable *db = s->db;
    if (db->n_rev_constraints > 0 && !self_referenced_only(db)) {
        int i;
        struct schema_change_type *sc_pending;
        for (i = 0; i < db->n_rev_constraints; i++) {
            constraint_t *cnstrt = db->rev_constraints[i];
            sc_pending = iq->sc_pending;
            while (sc_pending != NULL) {
                if (strcasecmp(sc_pending->tablename,
                               cnstrt->lcltable->tablename) == 0)
                    break;
                sc_pending = sc_pending->sc_next;
            }
            if (sc_pending && IS_FASTINIT(sc_pending))
                logmsg(LOGMSG_INFO,
                       "Fastinit '%s' and %s'%s' transactionally\n",
                       s->tablename,
                       sc_pending->kind == SC_DROPTABLE ? "drop " : "",
                       sc_pending->tablename);
            else {
                sc_client_error(s, "Can't truncate a table referenced by a foreign key");
                return ERR_SC;
            }
        }
    }

    rc = finalize_alter_table(iq, s, tran);

    /* if this is a shard, it is probably a truncation rollout */
    if (!rc && db->timepartition_name && s->newpartition) {
        rc = partition_truncate_callback(tran, s);
    }

    return rc;
}
