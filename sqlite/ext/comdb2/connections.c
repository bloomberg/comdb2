/*
   Copyright 2020 Bloomberg Finance L.P.

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
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "types.h"

sqlite3_module systblConnectionsModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int get_connections(void **data, int *num_points) {
    struct connection_info *info;
    int rc = gather_connection_info(&info, num_points);
    int now = comdb2_time_epochms();
    if (rc)
        return rc;
    *data = info;

    /* resolve timestamps into times, resolve ips from node numbers */
    for (int i = 0; i < *num_points; i++) {
        dttz_t d = {.dttz_sec = info[i].connect_time_int, .dttz_frac = 0, .dttz_prec = DTTZ_PREC_MSEC};
        dttz_to_client_datetime(&d, "UTC", (cdb2_client_datetime_t*) &info[i].connect_time);

        d = (dttz_t) { .dttz_sec = info[i].last_reset_time_int, .dttz_frac = 0, .dttz_prec = DTTZ_PREC_MSEC };
        dttz_to_client_datetime(&d, "UTC", (cdb2_client_datetime_t*) &info[i].last_reset_time);

        info[i].time_in_state.sign = 1;
        int ms = now - info[i].time_in_state_int;
        info[i].time_in_state.days = ms / 86400000; ms %= 86400000;
        info[i].time_in_state.hours = ms / 3600000; ms %= 3600000;
        info[i].time_in_state.mins = ms / 60000; ms %= 60000;
        info[i].time_in_state.sec = ms / 1000; ms %= 1000;
        info[i].time_in_state.msec = ms;

        switch (info[i].state_int) {
            case CONNECTION_NEW:
                info[i].state = "new";
                break;
            case CONNECTION_IDLE:
                info[i].state = "idle";
                break;
            case CONNECTION_RESET:
                info[i].state = "reset";
                break;
            case CONNECTION_QUEUED:
                info[i].state = "queued";
                break;
            case CONNECTION_RUNNING:
                info[i].state = "running";
                break;
        }
    }
    return 0;
}

void free_connections(void *data, int num_points) {
    free_connection_info((struct connection_info *)data, num_points);
}

int systblConnectionsInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_connections", 
            &systblConnectionsModule, get_connections, free_connections, sizeof(struct connection_info),
            CDB2_CSTRING, "host", -1, offsetof(struct connection_info, host),
            CDB2_INTEGER, "connection_id", -1, offsetof(struct connection_info, connection_id),
            CDB2_DATETIME, "connect_time", -1, offsetof(struct connection_info, connect_time),
            CDB2_DATETIME, "last_reset_time", -1, offsetof(struct connection_info, last_reset_time),
            CDB2_INTEGER, "pid", -1, offsetof(struct connection_info, pid),
            CDB2_INTEGER, "total_sql", -1, offsetof(struct connection_info, total_sql),
            CDB2_INTEGER, "sql_since_reset", -1, offsetof(struct connection_info, sql_since_reset),
            CDB2_INTEGER, "num_resets", -1, offsetof(struct connection_info, num_resets),
            CDB2_CSTRING, "state", -1, offsetof(struct connection_info, state),
            CDB2_INTERVALDS, "time_in_state", -1, offsetof(struct connection_info, time_in_state),
            CDB2_CSTRING, "sql", -1, offsetof(struct connection_info, sql),
            CDB2_CSTRING, "fingerprint", -1, offsetof(struct connection_info, fingerprint),
            CDB2_INTEGER, "is_admin", -1, offsetof(struct connection_info, is_admin),
            CDB2_INTEGER, "is_ssl", -1, offsetof(struct connection_info, is_ssl),
            CDB2_INTEGER, "has_cert", -1, offsetof(struct connection_info, has_cert),
            CDB2_CSTRING, "common_name", -1, offsetof(struct connection_info, common_name),
            CDB2_INTEGER, "in_local_cache", -1, offsetof(struct connection_info, in_local_cache),
            SYSTABLE_END_OF_FIELDS);
}
