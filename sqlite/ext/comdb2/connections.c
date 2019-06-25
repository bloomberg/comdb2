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
        dttz_t d;
        d = (dttz_t) { .dttz_sec = info[i].connect_time_int, .dttz_frac = 0, .dttz_frac = DTTZ_PREC_MSEC };
        dttz_to_client_datetime(&d, "UTC", (cdb2_client_datetime_t*) &info[i].connect_time);

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
    struct connection_info *info = (struct connection_info*) data;
    for (int i = 0; i < num_points; i++) {
        if (info[i].sql)
            free(info[i].sql);
        /* state is static, don't free */
    }
    free(data);
}

int systblConnectionsInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_connections", 
            &systblConnectionsModule, get_connections, free_connections, sizeof(struct connection_info),
            CDB2_CSTRING, "host", -1, offsetof(struct connection_info, host),
            CDB2_INTEGER, "connection_id", -1, offsetof(struct connection_info, connection_id),
            CDB2_DATETIME, "connect_time", -1, offsetof(struct connection_info, connect_time),
            CDB2_DATETIME, "last_reset_time", -1, offsetof(struct connection_info, connect_time),
            CDB2_INTEGER, "pid", -1, offsetof(struct connection_info, pid),
            CDB2_INTEGER, "total_sql", -1, offsetof(struct connection_info, total_sql),
            CDB2_INTEGER, "sql_since_reset", -1, offsetof(struct connection_info, sql_since_reset),
            CDB2_INTEGER, "num_resets", -1, offsetof(struct connection_info, num_resets),
            CDB2_CSTRING, "state", -1, offsetof(struct connection_info, state),
            CDB2_INTERVALDS, "time_in_state", -1, offsetof(struct connection_info, time_in_state),
            CDB2_CSTRING, "sql", -1, offsetof(struct connection_info, sql),
            SYSTABLE_END_OF_FIELDS);
}
