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

    gbl_sc_commit_count++;

    if (create_sqlmaster_records(tran)) {
        sc_errf(s, "create_sqlmaster_records failed\n");
        goto err;
    }
    create_sqlite_master();

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

    if ((rc = bdb_del_view(tran, s->tablename)) != 0) {
        return rc;
    }

    delete_view(s->tablename);

    if (create_sqlmaster_records(tran)) {
        sc_errf(s, "create_sqlmaster_records failed\n");
        return -1;
    }
    create_sqlite_master();

    return 0;
}

int do_add_view(struct ireq *iq, struct schema_change_type *s, tran_type *tran)
{
    if ((get_dbtable_by_name(s->tablename)) ||
        (get_view_by_name(s->tablename))) {
        sc_errf(s, "Table/view already exists\n");
        reqerrstr(iq, ERR_SC, "Table/view already exists");
        return ERR_SC;
    }
    return 0;
}

int do_drop_view(struct ireq *iq, struct schema_change_type *s, tran_type *tran)
{
    if (!(get_view_by_name(s->tablename))) {
        sc_errf(s, "View doesn't exists\n");
        reqerrstr(iq, ERR_SC, "View doesn't exists");
        return ERR_SC;
    }
    return 0;
}
