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
#if (!defined(SQLITE_CORE) || defined(SQLITE_BUILDING_FOR_COMDB2)) \
    && !defined(SQLITE_OMIT_VIRTUALTABLE)

#if defined(SQLITE_BUILDING_FOR_COMDB2) && !defined(SQLITE_CORE)
# define SQLITE_CORE 1
#endif

#include "comdb2systbl.h"
#include "ezsystables.h"
#include "timepart_systable.h"
#include "cron_systable.h"

static int systblTimepartitionsInit(sqlite3 *db);
static int systblTimepartitionsShardsInit(sqlite3 *db);
static int systblTimepartitionsEventsInit(sqlite3 *db);

sqlite3_module systblTimepartitionsModule = {
    .access_flag = CDB2_ALLOW_USER,
    .systable_lock = "comdb2_tables",
};
sqlite3_module systblTimepartitionShardsModule = {
    .access_flag = CDB2_ALLOW_USER,
    .systable_lock = "comdb2_tables",
};
sqlite3_module systblTimepartitionEventsModule = {
    .access_flag = CDB2_ALLOW_USER,
    .systable_lock = "comdb2_tables",
};


int systblTimepartInit(sqlite3*db)
{
    int rc;

    rc = systblTimepartitionsInit(db);
    if (rc)
        return rc;
    rc = systblTimepartitionsShardsInit(db);
    if (rc)
        return rc;
    return systblTimepartitionsEventsInit(db);
}

static int systblTimepartitionsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_timepartitions", &systblTimepartitionsModule,
        timepart_systable_timepartitions_collect,
        timepart_systable_timepartitions_free,  sizeof(systable_timepartition_t),
        CDB2_CSTRING, "name", -1, offsetof(systable_timepartition_t, name),
        CDB2_CSTRING, "period", -1, offsetof(systable_timepartition_t, period),
        CDB2_INTEGER, "retention", -1, offsetof(systable_timepartition_t, retention),
        CDB2_INTEGER, "nshards", -1, offsetof(systable_timepartition_t, nshards),
        CDB2_INTEGER, "version", -1, offsetof(systable_timepartition_t, version),
        CDB2_CSTRING, "shard0name", -1, offsetof(systable_timepartition_t, shard0name),
        CDB2_INTEGER, "starttime", -1, offsetof(systable_timepartition_t, starttime),
        CDB2_CSTRING, "sourceid", -1, offsetof(systable_timepartition_t, sourceid),
        SYSTABLE_END_OF_FIELDS);
}

static int systblTimepartitionsShardsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_timepartshards", &systblTimepartitionShardsModule,
        timepart_systable_timepartshards_collect,
        timepart_systable_timepartshards_free,  sizeof(systable_timepartshard_t),
        CDB2_CSTRING, "name", -1, offsetof(systable_timepartshard_t, name),
        CDB2_CSTRING, "shardname", -1, offsetof(systable_timepartshard_t, shardname),
        CDB2_INTEGER, "low", -1, offsetof(systable_timepartshard_t, low),
        CDB2_INTEGER, "high", -1, offsetof(systable_timepartshard_t, high),
        SYSTABLE_END_OF_FIELDS);
}

static int systblTimepartitionsEventsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_timepartevents", &systblTimepartitionEventsModule,
        timepart_systable_timepartevents_collect,
        timepart_systable_timepartevents_free,  sizeof(systable_cron_events_t),
        CDB2_CSTRING, "name", -1, offsetof(systable_cron_events_t, name),
        CDB2_CSTRING, "type", -1, offsetof(systable_cron_events_t, type),
        CDB2_CSTRING, "arg1", -1, offsetof(systable_cron_events_t, arg1),
        CDB2_CSTRING, "arg2", -1, offsetof(systable_cron_events_t, arg2),
        CDB2_CSTRING, "arg3", -1, offsetof(systable_cron_events_t, arg3),
        CDB2_INTEGER, "arg4", -1, offsetof(systable_cron_events_t, arg4),
        CDB2_CSTRING, "sourceid", -1, offsetof(systable_cron_events_t, sourceid),
        SYSTABLE_END_OF_FIELDS);
}

#endif /* SQLITE_BUILDING_FOR_COMDB2 */
