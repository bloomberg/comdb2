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
#include "bdb_schemachange.h"
#include "comdb2.h"

int add_view(struct dbview *view);
void delete_view(char *view_name);

int finalize_add_view(struct ireq *iq, struct schema_change_type *s,
                      tran_type *tran)
{
    int rc;
    int bdberr;
    struct dbview *view;

    view = calloc(1, sizeof(struct dbview));
    if (view == NULL) {
        sc_errf(s, "Failed to alloc memory (%s:%d)\n", __func__, __LINE__);
        return -1;
    }

    view->view_name = strdup(s->tablename);
    if (view->view_name == NULL) {
        sc_errf(s, "Failed to alloc memory (%s:%d)\n", __func__, __LINE__);
        rc = -1;
        goto err;
    }
    view->view_def = strdup(s->newcsc2);
    if (view->view_def == NULL) {
        sc_errf(s, "Failed to alloc memory (%s:%d)\n", __func__, __LINE__);
        rc = -1;
        goto err;
    }

    rc = bdb_put_view(tran, s->tablename, s->newcsc2);
    if (rc != 0) {
        sc_errf(s, "Failed to set view definition in low level meta\n");
        goto err;
    }

    rc = add_view(view);
    if (rc != 0) {
        sc_errf(s, "Failed to add view to the thedb->view_hash\n");
        goto err;
    }

    s->addonly = SC_DONE_ADD;
    gbl_sc_commit_count++;

    if (create_sqlmaster_records(tran)) {
        sc_errf(s, "create_sqlmaster_records failed\n");
        goto err;
    }
    create_sqlite_master();

    rc = bdb_llog_view(thedb->bdb_env, user_view, 1, &bdberr);
    if (rc != 0) {
        sc_errf(s, "Failed to log view info\n");
        goto err;
    }
    gbl_user_views_gen++;

    sc_printf(s, "Schema change ok\n");
    return 0;

err:
    free(view->view_name);
    free(view->view_def);
    free(view);

    return rc;
}

int finalize_drop_view(struct ireq *iq, struct schema_change_type *s,
                       tran_type *tran)
{
    int rc = 0;
    int bdberr = 0;

    if ((rc = bdb_del_view(tran, s->tablename)) != 0) {
        return rc;
    }

    delete_view(s->tablename);

    if (create_sqlmaster_records(tran)) {
        sc_errf(s, "create_sqlmaster_records failed\n");
        return -1;
    }
    create_sqlite_master();

    rc = bdb_llog_view(thedb->bdb_env, user_view, 1, &bdberr);
    if (rc != 0) {
        return rc;
    }
    gbl_user_views_gen++;

/* TODO (NC): study */
#if 0
    live_sc_off(db);
    if (gbl_replicate_local)
        local_replicant_write_clear(iq, tran, db);
#endif

    return 0;
}
