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
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include <stddef.h>
#include "sqlite3.h"
#include "ezsystables.h"
#include "cron_systable.h"

static int systblCronSchedulersInit(sqlite3 *db);
static int systblCronEventsInit(sqlite3 *db);

int systblCronInit(sqlite3*db)
{
    int rc;

    rc = systblCronSchedulersInit(db);
    if (rc)
        return rc;
    return systblCronEventsInit(db);
}

static int systblCronSchedulersInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_cron_schedulers", cron_systable_schedulers_collect,
        cron_systable_schedulers_free,  sizeof(systable_cron_scheds_t),
        CDB2_CSTRING, "name", -1, offsetof(systable_cron_scheds_t, name),
        CDB2_CSTRING, "type", -1, offsetof(systable_cron_scheds_t, type),
        CDB2_INTEGER, "running", -1, offsetof(systable_cron_scheds_t, running),
        CDB2_INTEGER, "nevents", -1, offsetof(systable_cron_scheds_t, nevents),
        CDB2_CSTRING, "description", -1, offsetof(systable_cron_scheds_t, description),
        SYSTABLE_END_OF_FIELDS);
}

static int systblCronEventsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_cron_events", cron_systable_events_collect,
        cron_systable_events_free,  sizeof(systable_cron_events_t),
        CDB2_CSTRING, "name", -1, offsetof(systable_cron_events_t, name),
        CDB2_CSTRING, "type", -1, offsetof(systable_cron_events_t, type),
        CDB2_CSTRING, "arg1", -1, offsetof(systable_cron_events_t, arg1),
        CDB2_CSTRING, "arg2", -1, offsetof(systable_cron_events_t, arg2),
        CDB2_CSTRING, "arg3", -1, offsetof(systable_cron_events_t, arg3),
        CDB2_CSTRING, "sourceid", -1, offsetof(systable_cron_events_t, sourceid),
        SYSTABLE_END_OF_FIELDS);
}


#endif /* SQLITE_BUILDING_FOR_COMDB2 */
