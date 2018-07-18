#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "ezsystables.h"
#include <activelocks.h>
#include "cdb2api.h"

typedef struct systable_activelocks {
    int64_t                 threadid;
    uint32_t                lockerid;
    char                    *mode;
    char                    *status;
    char                    *table;
    char                    *type;
} systable_activelocks_t;

typedef struct getactivelocks {
    int count;
    int alloc;
    systable_activelocks_t *records;
} getactivelocks_t;

static int get_activelocks(void **data, int *records)
{
    getactivelocks_t a = {0};
}

static void free_activelocks(void *p, int n)
{
    free(p);
}

static int collect(void *args, int64_t threadid, int32_t lockerid,
        char *mode, char *status, char *table, char *rectype)
{
    getactivelocks_t *a = (getactivelocks_t *)args;
    a->count++;
    if (a->count >= a->alloc) {
        if (a->alloc == 0) a->alloc = 16;
        else a->alloc = a->alloc * 2;
        a->records = realloc(a->records, a->alloc * sizeof(systable_activelocks_t));
    }
}

static int get_activelocks(void **date, int *records)
{
}

static int get_rep_net_queues(void **data, int *records) 
{
    net_get_records_t gr = {0};
    bdb_state_type *bdb_state = thedb->bdb_env;
    net_queue_stat_iterate(bdb_state->repinfo->netinfo, net_to_systable, &gr);
    *data = gr.records;
    *records = gr.count;
    return 0;
}

static void free_rep_net_queues(void *p, int n)
{
    systable_rep_qstat_t *s = (systable_rep_qstat_t *)p;
    for(int i=0; i<n;i++) {
        free(s[i].machine);
        free(s[i].min_lsn);
        free(s[i].max_lsn);
    }
    free(p);
}

int systblRepNetQueueStatInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_locks", get_rep_net_queues,
            free_rep_net_queues, sizeof(systable_rep_qstat_t),
            CDB2_CSTRING, "machine", offsetof(systable_rep_qstat_t, machine),
            CDB2_INTEGER, "total", offsetof(systable_rep_qstat_t, total),
            CDB2_CSTRING, "min_lsn", offsetof(systable_rep_qstat_t, min_lsn),
            CDB2_CSTRING, "max_lsn", offsetof(systable_rep_qstat_t, max_lsn),
            CDB2_INTEGER, "alive", offsetof(systable_rep_qstat_t, alive),
            CDB2_INTEGER, "alive_req", offsetof(systable_rep_qstat_t, alive_req),
            CDB2_INTEGER, "all_req", offsetof(systable_rep_qstat_t, all_req),
            CDB2_INTEGER, "dupmaster", offsetof(systable_rep_qstat_t, dupmaster),
            CDB2_INTEGER, "file", offsetof(systable_rep_qstat_t, file),
            CDB2_INTEGER, "file_req", offsetof(systable_rep_qstat_t, file_req),
            CDB2_INTEGER, "log", offsetof(systable_rep_qstat_t, log),
            CDB2_INTEGER, "log_more", offsetof(systable_rep_qstat_t, log_more),
            CDB2_INTEGER, "log_req", offsetof(systable_rep_qstat_t, log_req),
            CDB2_INTEGER, "master_req", offsetof(systable_rep_qstat_t, master_req),
            CDB2_INTEGER, "newclient", offsetof(systable_rep_qstat_t, newclient),
            CDB2_INTEGER, "newfile", offsetof(systable_rep_qstat_t, newfile),
            CDB2_INTEGER, "newmaster", offsetof(systable_rep_qstat_t, newmaster),
            CDB2_INTEGER, "newsite", offsetof(systable_rep_qstat_t, newsite),
            CDB2_INTEGER, "page", offsetof(systable_rep_qstat_t, page),
            CDB2_INTEGER, "page_req", offsetof(systable_rep_qstat_t, page_req),
            CDB2_INTEGER, "plist", offsetof(systable_rep_qstat_t, plist),
            CDB2_INTEGER, "plist_req", offsetof(systable_rep_qstat_t, plist_req),
            CDB2_INTEGER, "verify", offsetof(systable_rep_qstat_t, verify),
            CDB2_INTEGER, "verify_fail", offsetof(systable_rep_qstat_t, verify_fail),
            CDB2_INTEGER, "verify_req", offsetof(systable_rep_qstat_t, verify_req),
            CDB2_INTEGER, "vote1", offsetof(systable_rep_qstat_t, vote1),
            CDB2_INTEGER, "vote2", offsetof(systable_rep_qstat_t, vote2),
            CDB2_INTEGER, "log_logput", offsetof(systable_rep_qstat_t, log_logput),
            CDB2_INTEGER, "pgdump_req", offsetof(systable_rep_qstat_t, pgdump_req),
            CDB2_INTEGER, "gen_vote1", offsetof(systable_rep_qstat_t, gen_vote1),
            CDB2_INTEGER, "gen_vote2", offsetof(systable_rep_qstat_t, gen_vote2),
            CDB2_INTEGER, "log_fill", offsetof(systable_rep_qstat_t, log_fill),
            CDB2_INTEGER, "uncategorized", offsetof(systable_rep_qstat_t, uncategorized),
            CDB2_INTEGER, "unknown", offsetof(systable_rep_qstat_t, unknown),
            SYSTABLE_END_OF_FIELDS);
}
