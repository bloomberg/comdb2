#include <net.h>
#include <string.h>
#include <build/db.h>
#include <dbinc/rep_types.h>
#include "bdb_int.h"
#include <rep_qstat.h>

int net_get_lsn_rectype(bdb_state_type *bdb_state, const void *buf,
        int buflen, DB_LSN *lsn, int *myrectype);

static __thread void *reader_qstat;

static void *net_init_queue_stats_rtn(netinfo_type *netinfo_type, 
        const char *nettype, const char hostname[]) {
    net_queue_stat_t *n = calloc(sizeof(*n), 1);
    pthread_mutex_init(&n->lock, NULL);
    n->nettype = strdup(nettype);
    n->hostname = strdup(hostname);
    n->max_type = -1;
    return n;
}

static void net_start_reader(netinfo_type *netinfo_type, void *netstat)
{
    reader_qstat = netstat;
}

static void net_clear_queue_stats_rtn(netinfo_type *netinfo_type, void *netstat)
{
    net_queue_stat_t *n = (net_queue_stat_t *)netstat;
    pthread_mutex_lock(&n->lock);
    if (n->type_counts)
        bzero(n->type_counts, sizeof(int) * (n->max_type + 1));
    bzero(&n->max_lsn, sizeof(n->max_lsn));
    bzero(&n->min_lsn, sizeof(n->min_lsn));
    n->unknown_count = n->total_count = 0;
    pthread_mutex_unlock(&n->lock);
}

static void net_enque_write_rtn(netinfo_type *netinfo_ptr, void *netstat,
        void *rec, int len)
{
    net_queue_stat_t *n = (net_queue_stat_t *)netstat;
    bdb_state_type *bdb_state;
    DB_LSN lsn;
    int rectype, rc;

    /* Get a pointer back to our bdb_state */
    bdb_state = net_get_usrptr(netinfo_ptr);

    rc = net_get_lsn_rectype(bdb_state, rec, len, &lsn, &rectype);
    pthread_mutex_lock(&n->lock);
    if (rc == 0) {
        if (rectype > n->max_type) {
            n->type_counts = realloc(n->type_counts, sizeof(int) * 
                    (rectype + 1));
            for (int i = n->max_type + 1; i <= rectype ; i++) {
                n->type_counts[i] = 0;
            }
            n->max_type = rectype;
        }
        n->type_counts[rectype]++;

        if (n->min_lsn.file == 0) {
            n->min_lsn = n->max_lsn = lsn;
        } else {
            if (log_compare(&lsn, &n->min_lsn) < 0)
                n->min_lsn = lsn;
            if (log_compare(&lsn, &n->max_lsn) > 0)
                n->max_lsn = lsn;
        }
        n->total_count++;
    } else {
        n->unknown_count++;
        n->total_count++;
    }
    pthread_mutex_unlock(&n->lock);
}

static void net_enque_free(netinfo_type *netinfo_ptr, void *netstat)
{
    net_queue_stat_t *n = (net_queue_stat_t *)netstat;
    free(n->nettype);
    free(n->hostname);
    if (n->type_counts)
        free(n->type_counts);
    pthread_mutex_destroy(&n->lock);
    free(n);
}

void net_rep_qstat_init(netinfo_type *netinfo_ptr) 
{
    net_register_queue_stat(netinfo_ptr, net_init_queue_stats_rtn, 
            net_start_reader, net_enque_write_rtn, net_clear_queue_stats_rtn,
            net_enque_free);
}

int rep_qstat_has_master_req(void)
{
    int ret = 0;
    net_queue_stat_t *n = (net_queue_stat_t *)reader_qstat;
    if (n == NULL) 
        return 0;
    pthread_mutex_lock(&n->lock);
    if (n->max_type >= REP_MASTER_REQ) {
        ret += n->type_counts[REP_MASTER_REQ];
    }
    pthread_mutex_unlock(&n->lock);
    return ret ? 1 : 0;
}

int rep_qstat_has_fills(void)
{
    int ret = 0;
    net_queue_stat_t *n = (net_queue_stat_t *)reader_qstat;
    if (n == NULL) 
        return 0;
    pthread_mutex_lock(&n->lock);
    if (n->max_type >= REP_LOG_FILL) {
        ret += n->type_counts[REP_LOG_FILL];
        ret += n->type_counts[REP_LOG_MORE];
    }
    pthread_mutex_unlock(&n->lock);
    return ret ? 1 : 0;
}

void rep_qstat_lsn_range(DB_LSN *min_lsn, DB_LSN *max_lsn)
{
    net_queue_stat_t *n = (net_queue_stat_t *)reader_qstat;
    if (n == NULL) 
        return;
    pthread_mutex_lock(&n->lock);
    *min_lsn = n->min_lsn;
    *max_lsn = n->max_lsn;
    pthread_mutex_unlock(&n->lock);
}

/*
struct { int type, char *colname; } rep_columns[28] = {
    [0].type = REP_ALIVE, [0].colname = "REP_ALIVE",
    [1].type = REP_ALIVE_REQ, [1].colname = "REP_ALIVE_REQ",
    [2].type = REP_ALL_REQ, [2].colname = "REP_ALL_REQ",
    [3].type = REP_DUPMASTER, [3].colname = "REP_DUPMASTER",
    [4].type = REP_FILE, [4].colname = "REP_FILE",
    [5].type = REP_FILE_REQ, [5].colname = "REP_FILE_REQ",
    [6].type = REP_LOG, [6].colname = "REP_LOG",
    [7].type = REP_LOG_MORE, [7].colname = "REP_LOG_MORE",
    [8].type = REP_LOG_REQ, [8].colname = "REP_LOG_REQ",
    [9].type = REP_MASTER_REQ, [9].colname = "REP_MASTER_REQ",
    [10].type = REP_NEWCLIENT, [10].colname = "REP_NEWCLIENT",
    [11].type = REP_NEWFILE, [11].colname = "REP_NEWFILE",
    [12].type = REP_NEWMASTER, [12].colname = "REP_NEWMASTER",
    [13].type = REP_NEWSITE, [13].colname = "REP_NEWSITE",
    [14].type = REP_PAGE, [14].colname = "REP_PAGE",
    [15].type = REP_PAGE_REQ, [15].colname = "REP_PAGE_REQ",
    [16].type = REP_PLIST, [16].colname = "REP_PLIST",
    [17].type = REP_PLIST_REQ, [17].colname = "REP_PLIST_REQ",
    [18].type = REP_VERIFY, [18].colname = "REP_VERIFY",
    [19].type = REP_VERIFY_FAIL, [19].colname = "REP_VERIFY_FAIL",
    [20].type = REP_VERIFY_REQ, [20].colname = "REP_VERIFY_REQ",
    [21].type = REP_VOTE1, [21].colname = "REP_VOTE1",
    [22].type = REP_VOTE2, [22].colname = "REP_VOTE2",
    [23].type = REP_LOG_LOGPUT, [23].colname = "REP_LOG_LOGPUT",
    [24].type = REP_PGDUMP_REQ, [24].colname = "REP_PGDUMP_REQ",
    [25].type = REP_GEN_VOTE1, [25].colname = "REP_GEN_VOTE1",
    [26].type = REP_GEN_VOTE2, [26].colname = "REP_GEN_VOTE2",
    [27].type = REP_LOG_FILL, [27].colname = "REP_LOG_FILL"
};
*/

struct { int type; char *colname; } rep_columns[28] = {
    { REP_ALIVE, "REP_ALIVE" },
    { REP_ALIVE_REQ, "REP_ALIVE_REQ" },
    { REP_ALL_REQ, "REP_ALL_REQ" },
    { REP_DUPMASTER, "REP_DUPMASTER" },
    { REP_FILE, "REP_FILE" },
    { REP_FILE_REQ, "REP_FILE_REQ" },
    { REP_LOG, "REP_LOG" },
    { REP_LOG_MORE, "REP_LOG_MORE" },
    { REP_LOG_REQ, "REP_LOG_REQ" },
    { REP_MASTER_REQ, "REP_MASTER_REQ" },
    { REP_NEWCLIENT, "REP_NEWCLIENT" },
    { REP_NEWFILE, "REP_NEWFILE" },
    { REP_NEWMASTER, "REP_NEWMASTER" },
    { REP_NEWSITE, "REP_NEWSITE" },
    { REP_PAGE, "REP_PAGE" },
    { REP_PAGE_REQ, "REP_PAGE_REQ" },
    { REP_PLIST, "REP_PLIST" },
    { REP_PLIST_REQ, "REP_PLIST_REQ" },
    { REP_VERIFY, "REP_VERIFY" },
    { REP_VERIFY_FAIL, "REP_VERIFY_FAIL" },
    { REP_VERIFY_REQ, "REP_VERIFY_REQ" },
    { REP_VOTE1, "REP_VOTE1" },
    { REP_VOTE2, "REP_VOTE2" },
    { REP_LOG_LOGPUT, "REP_LOG_LOGPUT" },
    { REP_PGDUMP_REQ, "REP_PGDUMP_REQ" },
    { REP_GEN_VOTE1, "REP_GEN_VOTE1" },
    { REP_GEN_VOTE2, "REP_GEN_VOTE2" },
    { REP_LOG_FILL, "REP_LOG_FILL" }
};



int rep_type_to_col(int type)
{
    switch(type)
    {
        case REP_ALIVE: return 0;
        case REP_ALIVE_REQ: return 1;
        case REP_ALL_REQ: return 2;
        case REP_DUPMASTER: return 3;
        case REP_FILE: return 4;
        case REP_FILE_REQ: return 5;
        case REP_LOG: return 6;
        case REP_LOG_MORE: return 7;
        case REP_LOG_REQ: return 8;
        case REP_MASTER_REQ: return 9;
        case REP_NEWCLIENT: return 10;
        case REP_NEWFILE: return 11;
        case REP_NEWMASTER: return 12;
        case REP_NEWSITE: return 13;
        case REP_PAGE: return 14;
        case REP_PAGE_REQ: return 15;
        case REP_PLIST: return 16;
        case REP_PLIST_REQ: return 17;
        case REP_VERIFY: return 18;
        case REP_VERIFY_FAIL: return 19;
        case REP_VERIFY_REQ: return 20;
        case REP_VOTE1: return 21;
        case REP_VOTE2: return 22;
        case REP_LOG_LOGPUT: return 23;
        case REP_PGDUMP_REQ: return 24;
        case REP_GEN_VOTE1: return 25;
        case REP_GEN_VOTE2: return 26;
        case REP_LOG_FILL: return 27;
        default: return -1;
    }
}

char *rep_type_to_str(int type)
{
    switch(type)
    {
        case REP_ALIVE: return "REP_ALIVE";
        case REP_ALIVE_REQ: return "REP_ALIVE_REQ";
        case REP_ALL_REQ: return "REP_ALL_REQ";
        case REP_DUPMASTER: return "REP_DUPMASTER";
        case REP_FILE: return "REP_FILE";
        case REP_FILE_REQ: return "REP_FILE_REQ";
        case REP_LOG: return "REP_LOG";
        case REP_LOG_MORE: return "REP_LOG_MORE";
        case REP_LOG_REQ: return "REP_LOG_REQ";
        case REP_MASTER_REQ: return "REP_MASTER_REQ";
        case REP_NEWCLIENT: return "REP_NEWCLIENT";
        case REP_NEWFILE: return "REP_NEWFILE";
        case REP_NEWMASTER: return "REP_NEWMASTER";
        case REP_NEWSITE: return "REP_NEWSITE";
        case REP_PAGE: return "REP_PAGE";
        case REP_PAGE_REQ: return "REP_PAGE_REQ";
        case REP_PLIST: return "REP_PLIST";
        case REP_PLIST_REQ: return "REP_PLIST_REQ";
        case REP_VERIFY: return "REP_VERIFY";
        case REP_VERIFY_FAIL: return "REP_VERIFY_FAIL";
        case REP_VERIFY_REQ: return "REP_VERIFY_REQ";
        case REP_VOTE1: return "REP_VOTE1";
        case REP_VOTE2: return "REP_VOTE2";
        case REP_LOG_LOGPUT: return "REP_LOG_LOGPUT";
        case REP_PGDUMP_REQ: return "REP_PGDUMP_REQ";
        case REP_GEN_VOTE1: return "REP_GEN_VOTE1";
        case REP_GEN_VOTE2: return "REP_GEN_VOTE2";
        case REP_LOG_FILL: return "REP_LOG_FILL";
        default: return "UNKNOWN";
    }
}
