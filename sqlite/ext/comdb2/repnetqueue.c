#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "cdb2api.h"
#include <dbinc/rep_types.h>
#include "bdb_int.h"
#include <rep_qstat.h>
#include <net.h>

typedef struct systable_rep_qstat {
    char                    *machine;
    int64_t                 total;
    char                    *min_lsn;
    char                    *max_lsn;
    int64_t                 alive;
    int64_t                 alive_req;
    int64_t                 all_req;
    int64_t                 dupmaster;
    int64_t                 file;
    int64_t                 file_req;
    int64_t                 log;
    int64_t                 log_more;
    int64_t                 log_req;
    int64_t                 master_req;
    int64_t                 newclient;
    int64_t                 newfile;
    int64_t                 newmaster;
    int64_t                 newsite;
    int64_t                 page;
    int64_t                 page_req;
    int64_t                 plist;
    int64_t                 plist_req;
    int64_t                 verify;
    int64_t                 verify_fail;
    int64_t                 verify_req;
    int64_t                 vote1;
    int64_t                 vote2;
    int64_t                 log_logput;
    int64_t                 pgdump_req;
    int64_t                 gen_vote1;
    int64_t                 gen_vote2;
    int64_t                 log_fill;
    int64_t                 uncategorized;
    int64_t                 unknown;
} systable_rep_qstat_t;

typedef struct net_get_records {
    int count;
    int alloc;
    systable_rep_qstat_t *records;
} net_get_records_t;

#define MAX_LSN_STR 64

static void net_to_systable(struct netinfo_struct *netinfo_ptr, void *arg,
        void *qstat)
{
    int i;
    net_get_records_t *gr = (net_get_records_t *)arg;
    net_queue_stat_t *n = (net_queue_stat_t *)qstat;

    if (n == NULL)
        return;

    gr->count++;
    if (gr->count >= gr->alloc) {
        if (gr->alloc == 0) gr->alloc = 16;
        else gr->alloc = gr->alloc * 2;
        gr->records = realloc(gr->records, gr->alloc * 
                sizeof(systable_rep_qstat_t));
    }

    systable_rep_qstat_t *s = &gr->records[gr->count - 1];
    memset(s, 0, sizeof(*s));
    s->max_lsn = malloc(MAX_LSN_STR);
    s->min_lsn = malloc(MAX_LSN_STR);
    pthread_mutex_lock(&n->lock);
    s->machine = strdup(n->hostname);
    s->total = n->total_count;
    /* "unknown" messages are net-level */
    s->uncategorized = n->unknown_count;
    snprintf(s->max_lsn, MAX_LSN_STR, "{%d:%d}", n->max_lsn.file,
            n->max_lsn.offset);
    snprintf(s->min_lsn, MAX_LSN_STR, "{%d:%d}", n->min_lsn.file,
            n->min_lsn.offset);
    s->unknown = 0;
    for (i = 0 ; i <= n->max_type; i++) {
        switch(i) {
            case REP_ALIVE: s->alive = n->type_counts[i]; break;
            case REP_ALIVE_REQ: s->alive_req = n->type_counts[i]; break;
            case REP_ALL_REQ: s->all_req = n->type_counts[i]; break;
            case REP_DUPMASTER: s->dupmaster = n->type_counts[i]; break;
            case REP_FILE: s->file = n->type_counts[i]; break;
            case REP_FILE_REQ: s->file_req = n->type_counts[i]; break;
            case REP_LOG: s->log = n->type_counts[i]; break;
            case REP_LOG_MORE: s->log_more = n->type_counts[i]; break;
            case REP_LOG_REQ: s->log_req = n->type_counts[i]; break;
            case REP_MASTER_REQ: s->master_req = n->type_counts[i]; break;
            case REP_NEWCLIENT: s->newclient = n->type_counts[i]; break;
            case REP_NEWFILE: s->newfile = n->type_counts[i]; break;
            case REP_NEWMASTER: s->newmaster = n->type_counts[i]; break;
            case REP_NEWSITE: s->newsite = n->type_counts[i]; break;
            case REP_PAGE: s->page = n->type_counts[i]; break;
            case REP_PAGE_REQ: s->page_req = n->type_counts[i]; break;
            case REP_PLIST: s->plist = n->type_counts[i]; break;
            case REP_PLIST_REQ: s->plist_req = n->type_counts[i]; break;
            case REP_VERIFY: s->verify = n->type_counts[i]; break;
            case REP_VERIFY_FAIL: s->verify_fail = n->type_counts[i]; break;
            case REP_VERIFY_REQ: s->verify_req = n->type_counts[i]; break;
            case REP_VOTE1: s->vote1 = n->type_counts[i]; break;
            case REP_VOTE2: s->vote2 = n->type_counts[i]; break;
            case REP_LOG_LOGPUT: s->log_logput = n->type_counts[i]; break;
            case REP_PGDUMP_REQ: s->pgdump_req = n->type_counts[i]; break;
            case REP_GEN_VOTE1: s->gen_vote1 = n->type_counts[i]; break;
            case REP_GEN_VOTE2: s->gen_vote2 = n->type_counts[i]; break;
            case REP_LOG_FILL: s->log_fill = n->type_counts[i]; break;
            default: s->unknown += n->type_counts[i]; break;
        }
    }
done:
    pthread_mutex_unlock(&n->lock);
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
    return create_system_table(db, "comdb2_replication_netqueue", get_rep_net_queues,
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
