/*
   Copyright 2019-2020 Bloomberg Finance L.P.

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

#include <stdlib.h>

#include <comdb2systblInt.h>
#include <ezsystables.h>
#include <sql.h>
#include <translistener.h>

static void release_triggers(void *data, int n)
{
    struct trigger_entry *t = data;
    for (int i = 0; i < n; i++) {
        free(t[i].name);
        free(t[i].type);
        free(t[i].tbl_name);
        free(t[i].event);
        free(t[i].col);
        free(t[i].seq);
    }
    free(data);
}

static int gather_trigger_cb(struct gather_triggers_arg *arg, struct trigger_entry *entry)
{
    if (arg->n == arg->capacity) {
        arg->capacity = arg->n + 32;
        void *space = realloc(arg->entries, arg->capacity * sizeof(struct trigger_entry));
        if (!space) return SQLITE_NOMEM;
        arg->entries = space;
    }
    struct trigger_entry *e = arg->entries + arg->n;
    ++arg->n;
    e->name = strdup(entry->name);
    e->type = strdup(entry->type);
    e->tbl_name = strdup(entry->tbl_name);
    e->event = strdup(entry->event);
    e->col = strdup(entry->col);
    e->seq = strdup(entry->seq);
    return 0;
}

static int get_triggers(void **data, int *npoints)
{
    *npoints = 0;
    *data = NULL;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct gather_triggers_arg a = {
        .tran = curtran_gettran(),
        .func = gather_trigger_cb,
        .user = thd->clnt->current_user.name
    };
    int rc = gather_triggers(&a);
    curtran_puttran(a.tran);
    if (rc) {
        release_triggers(a.entries, a.n);
    } else {
        *data = a.entries;
        *npoints = a.n;
    }
    return rc;
}

static sqlite3_module systblTriggersModule = {
  .access_flag = CDB2_ALLOW_ALL,
  .systable_lock = "comdb2_queues",
};

int systblTriggersInit(sqlite3 *db)
{
    return create_system_table(
            db, "comdb2_triggers", &systblTriggersModule,
            get_triggers, release_triggers, sizeof(struct trigger_entry),
            CDB2_CSTRING, "name", -1, offsetof(struct trigger_entry, name),
            CDB2_CSTRING, "type", -1, offsetof(struct trigger_entry, type),
            CDB2_CSTRING, "tbl_name", -1, offsetof(struct trigger_entry, tbl_name),
            CDB2_CSTRING, "event", -1, offsetof(struct trigger_entry, event),
            CDB2_CSTRING, "col", -1, offsetof(struct trigger_entry, col),
            CDB2_CSTRING, "seq", -1, offsetof(struct trigger_entry, seq),
            SYSTABLE_END_OF_FIELDS);
}
