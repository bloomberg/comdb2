/*
   Copyright 2022 Bloomberg Finance L.P.

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
#include <string.h>
#include <stddef.h>
#include "comdb2.h"
#include "sqlite3.h"
#include "ezsystables.h"
#include "autoanalyze.h"
#include "sql.h"
#include "comdb2_atomic.h"

extern struct dbenv *thedb;

sqlite3_module systblAutoAnalyzeTablesModule = {
    .access_flag = CDB2_ALLOW_USER,
    .systable_lock = "comdb2_tables", // TODO: Check locks, think should be ok
};

typedef struct systable_auto_analyze_tables {
    char *tablename;
    int64_t newautoanalyze_counter;
    int64_t saved_counter;
    int64_t delta;
    double new_aa_percnt;
    cdb2_client_datetime_t *lastepoch;
    cdb2_client_datetime_t *needs_analyze_time;
} systable_auto_analyze_tables_t;

int auto_analyze_tables_systable_collect(void **data, int *nrecords)
{
    *nrecords = 0;
    *data = NULL;
    // refresh from saved if we are not master
    if (thedb->master != gbl_myhostname)
        load_auto_analyze_counters();

    int include_updates = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_AA_COUNT_UPD);

    if (NULL == get_dbtable_by_name("sqlite_stat1")) { // TODO: check if this will work
        logmsg(LOGMSG_USER, "ANALYZE REQUIRES sqlite_stat1 to run but table is MISSING\n");
        return 0;
    }

    systable_auto_analyze_tables_t *arr = calloc(thedb->num_dbs, sizeof(systable_auto_analyze_tables_t));
    int delta;
    int saved_counter;
    unsigned int newautoanalyze_counter;
    double new_aa_percnt;
    int idx;
    for (int i = 0; i < thedb->num_dbs; i++) {
        struct dbtable *tbl = thedb->dbs[i];
        if (is_sqlite_stat(tbl->tablename))
            continue;

        get_auto_analyze_tbl_stats(tbl, include_updates, &delta, &saved_counter, &newautoanalyze_counter, &new_aa_percnt);
        idx = (*nrecords)++;
        arr[idx].tablename = strdup(tbl->tablename);
        arr[idx].newautoanalyze_counter = newautoanalyze_counter;
        arr[idx].saved_counter = saved_counter;
        arr[idx].delta = delta;
        arr[idx].new_aa_percnt = new_aa_percnt;
        int64_t lastepoch = ATOMIC_LOAD64(tbl->aa_lastepoch);
        if (lastepoch != 0) {
            arr[idx].lastepoch = calloc(1, sizeof(*arr[0].lastepoch));
            gmtime_r(&lastepoch, (struct tm*)&arr[idx].lastepoch->tm);
            strcpy(arr[idx].lastepoch->tzname, "UTC");
        }
        int64_t needs_analyze_time = ATOMIC_LOAD64(tbl->aa_needs_analyze_time);
        if (needs_analyze_time != 0) {
            arr[idx].needs_analyze_time = calloc(1, sizeof(*arr[0].needs_analyze_time));
            gmtime_r(&needs_analyze_time, (struct tm*)&arr[idx].needs_analyze_time->tm);
            strcpy(arr[idx].needs_analyze_time->tzname, "UTC");
        }
    }
    *data = arr;
    return 0;
}

void auto_analyze_tables_systable_free(void *arr, int nrecords)
{
    systable_auto_analyze_tables_t *parr = (systable_auto_analyze_tables_t *)arr;
    int i;

    for (i = 0; i < nrecords; i++) {
        free(parr[i].tablename);
        free(parr[i].needs_analyze_time);
        free(parr[i].lastepoch);
    }
    free(arr);
}

int systblAutoAnalyzeTablesInit(sqlite3*db)
{
    return create_system_table(
        db, "comdb2_auto_analyze_tables", &systblAutoAnalyzeTablesModule,
        auto_analyze_tables_systable_collect, auto_analyze_tables_systable_free,
        sizeof(systable_auto_analyze_tables_t),
        CDB2_CSTRING, "tablename", -1, offsetof(systable_auto_analyze_tables_t, tablename),
        CDB2_INTEGER, "counter", -1, offsetof(systable_auto_analyze_tables_t, newautoanalyze_counter),
        CDB2_INTEGER, "saved", -1, offsetof(systable_auto_analyze_tables_t, saved_counter),
        CDB2_INTEGER, "new", -1, offsetof(systable_auto_analyze_tables_t, delta),
        CDB2_REAL, "percent_of_tbl", -1, offsetof(systable_auto_analyze_tables_t, new_aa_percnt),
        CDB2_DATETIME | SYSTABLE_FIELD_NULLABLE, "last_run_time", -1, offsetof(systable_auto_analyze_tables_t, lastepoch),
        CDB2_DATETIME | SYSTABLE_FIELD_NULLABLE, "needs_analyze_time", -1, offsetof(systable_auto_analyze_tables_t, needs_analyze_time),
        SYSTABLE_END_OF_FIELDS);
}
