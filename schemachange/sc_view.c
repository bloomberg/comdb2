/*
   Copyright 2019 Bloomberg Finance L.P.

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
#include "comdb2.h"

int add_view(struct dbview *view);
int bdb_llmeta_put_view_def(tran_type *in_trans, const char *view_name,
                            char *view_def, int view_version, int *bdberr);
int bdb_llmeta_del_view_def(tran_type *in_tran, const char *view_name,
                            int view_version, int *bdberr);

int do_add_view(struct ireq *iq, struct schema_change_type *s, tran_type *tran)
{
    int rc;
    int bdberr;
    struct dbview *view;

    view = malloc(sizeof(struct dbview));
    if (view == 0) {
        return -1;
    }

    view->view_name = strdup(s->tablename);
    if (view->view_name == 0) {
        return -1;
    }
    view->view_def = strdup(s->newcsc2);
    if (view->view_def == 0) {
        return -1;
    }

    rc = bdb_llmeta_put_view_def(tran, s->tablename, s->newcsc2, 0, &bdberr);
    if (rc != 0) {
        return rc;
    }

    add_view(view);

    if (llmeta_set_views(tran, thedb)) {
        sc_errf(s, "Failed to set table names in low level meta\n");
        return -1;
    }

    if (create_sqlmaster_records(tran)) {
        sc_errf(s, "create_sqlmaster_records failed\n");
        return -1;
    }
    create_sqlite_master();
    ++gbl_dbopen_gen;

    return 0;
}

int finalize_add_view(struct ireq *iq, struct schema_change_type *s,
                      tran_type *tran)
{
    int rc;
    int bdberr;
    struct dbview *view;

    view = malloc(sizeof(struct dbview));
    if (view == 0) {
        return -1;
    }

    view->view_name = strdup(s->tablename);
    if (view->view_name == 0) {
        return -1;
    }
    view->view_def = strdup(s->newcsc2);
    if (view->view_def == 0) {
        return -1;
    }

    rc = bdb_llmeta_put_view_def(tran, s->tablename, s->newcsc2, 0, &bdberr);
    if (rc != 0) {
        return rc;
    }

    add_view(view);

    if (llmeta_set_views(tran, thedb)) {
        sc_errf(s, "Failed to set table names in low level meta\n");
        return -1;
    }

    s->addonly = SC_DONE_ADD;
    gbl_sc_commit_count++;

    if (s->finalize) {
        if (create_sqlmaster_records(tran)) {
            sc_errf(s, "create_sqlmaster_records failed\n");
            return -1;
        }
        create_sqlite_master();
    }

    sc_printf(s, "Schema change ok\n");
    return 0;
}

int do_drop_view(struct ireq *iq, struct schema_change_type *s, tran_type *tran)
{
    return SC_OK;
}

int finalize_drop_view(struct ireq *iq, struct schema_change_type *s,
                       tran_type *tran)
{
    int rc = 0;

    if ((rc = bdb_llmeta_del_view_def(tran, s->tablename, 0, 0)) != 0) {
        return rc;
    }

/* TODO (NC): Do we need this for views? */
#if 0
    if ((rc = llmeta_set_views(tran, thedb)) != 0) {
        sc_errf(s, "Failed to set view names in low level meta\n");
        return rc;
    }
#endif

    if (s->finalize) {
        if (create_sqlmaster_records(tran)) {
            sc_errf(s, "create_sqlmaster_records failed\n");
            return -1;
        }
        create_sqlite_master();
    }

        // live_sc_off(db);

#if 0
    if (!gbl_create_mode) {
        logmsg(LOGMSG_INFO, "Table %s is at version: %lld\n", db->tablename,
               db->tableversion);
    }
#endif

/* TODO (NC): study */
#if 0
    if (gbl_replicate_local)
        local_replicant_write_clear(iq, tran, db);
#endif

    return 0;
}
