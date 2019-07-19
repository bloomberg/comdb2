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

#define SQLITE_CORE 1

#include <pthread.h>
#include <comdb2systblInt.h>
#include <ezsystables.h>
#include "sql.h"
#include "plhash.h"

sqlite3_module systblViewsModule = {
    .access_flag = CDB2_ALLOW_USER,
};

typedef struct view_entry {
    char *name; /* Name of the view */
    char *def;  /* View definition */
} view_entry_t;

static void release_views(void *data, int npoints)
{
    view_entry_t *pViews = (view_entry_t *)data;
    if (pViews != NULL) {
        for (int i = 0; i < npoints; i++) {
            free(pViews[i].name);
            free(pViews[i].def);
        }
        free(pViews);
    }
}

static int get_views(void **data, int *npoints)
{
    int rc = SQLITE_OK;
    *npoints = 0;
    *data = NULL;
    rdlock_schema_lk();
    if (thedb->view_hash != NULL) {
        int count;
        hash_info(thedb->view_hash, NULL, NULL, NULL, NULL, &count, NULL, NULL);
        if (count > 0) {
            view_entry_t *pViews = calloc(count, sizeof(view_entry_t));
            if (pViews != NULL) {
                struct dbview *pView;
                int copied = 0;
                void *hash_cur;
                unsigned int bkt;
                pView = hash_first(thedb->view_hash, &hash_cur, &bkt);
                while (pView != NULL) {
                    assert(copied < count);
                    pViews[copied].name = strdup(pView->view_name);
                    pViews[copied].def = strdup(pView->view_def);
                    copied++;
                    pView = hash_next(thedb->view_hash, &hash_cur, &bkt);
                }
                *data = pViews;
                *npoints = count;
            } else {
                rc = SQLITE_NOMEM;
            }
        }
    }
    unlock_schema_lk();
    return rc;
}

int systblViewsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_views", &systblViewsModule,
        get_views, release_views, sizeof(view_entry_t),
        CDB2_CSTRING, "name", -1, offsetof(view_entry_t, name),
        CDB2_CSTRING, "definition", -1, offsetof(view_entry_t, def),
        SYSTABLE_END_OF_FIELDS);
}
