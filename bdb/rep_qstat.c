#include <net.h>
#include <string.h>
#include <strings.h>
#include <build/db.h>
#include <dbinc/rep_types.h>
#include "bdb_int.h"
#include <rep_qstat.h>
#include "sys_wrap.h"

static __thread void *reader_qstat;

static void *net_init_queue_stats_rtn(netinfo_type *netinfo_type,
                                      const char *nettype,
                                      const char hostname[])
{
    net_queue_stat_t *n = calloc(sizeof(*n), 1);
    Pthread_mutex_init(&n->lock, NULL);
    n->nettype = strdup(nettype);
    n->hostname = strdup(hostname);
    n->max_type = -1;
    return n;
}

static void net_start_reader(netinfo_type *netinfo_type, void *netstat)
{
    reader_qstat = netstat;
}

static void net_dump_queue_stats_rtn(netinfo_type *netinfo_type, void *netstat,
                                     FILE *f)
{
    net_queue_stat_t *n = (net_queue_stat_t *)netstat;
    int unknown = 0;
    Pthread_mutex_lock(&n->lock);
    fprintf(f, "Net-queue node=%s minlsn=[%d:%d] maxlsn=[%d:%d] ",
        n->hostname, n->min_lsn.file, n->min_lsn.offset, n->max_lsn.file,
        n->max_lsn.offset);
    for (int i = 0 ; i <= n->max_type; i++) {
        if (n->type_counts[i] > 0) {
            switch(i) {
                case REP_ALIVE: fprintf(f, "alive=%d ", n->type_counts[i]); break;
                case REP_ALIVE_REQ: fprintf(f, "alive_req=%d ", n->type_counts[i]); break;
                case REP_ALL_REQ: fprintf(f, "all_req=%d ", n->type_counts[i]); break;
                case REP_DUPMASTER: fprintf(f, "dupmaster=%d ", n->type_counts[i]); break;
                case REP_FILE: fprintf(f, "file=%d ", n->type_counts[i]); break;
                case REP_FILE_REQ: fprintf(f, "file_req=%d ", n->type_counts[i]); break;
                case REP_LOG: fprintf(f, "log=%d ", n->type_counts[i]); break;
                case REP_LOG_MORE: fprintf(f, "log_more=%d ", n->type_counts[i]); break;
                case REP_LOG_REQ: fprintf(f, "log_req=%d ", n->type_counts[i]); break;
                case REP_MASTER_REQ: fprintf(f, "master_req=%d ", n->type_counts[i]); break;
                case REP_NEWCLIENT: fprintf(f, "newclient=%d ", n->type_counts[i]); break;
                case REP_NEWFILE: fprintf(f, "newfile=%d ", n->type_counts[i]); break;
                case REP_NEWMASTER: fprintf(f, "newmaster=%d ", n->type_counts[i]); break;
                case REP_NEWSITE: fprintf(f, "newsite=%d ", n->type_counts[i]); break;
                case REP_PAGE: fprintf(f, "page=%d ", n->type_counts[i]); break;
                case REP_PAGE_REQ: fprintf(f, "page_req=%d ", n->type_counts[i]); break;
                case REP_PLIST: fprintf(f, "plist=%d ", n->type_counts[i]); break;
                case REP_PLIST_REQ: fprintf(f, "plist_req=%d ", n->type_counts[i]); break;
                case REP_VERIFY: fprintf(f, "verify=%d ", n->type_counts[i]); break;
                case REP_VERIFY_FAIL: fprintf(f, "verify_fail=%d ", n->type_counts[i]); break;
                case REP_VERIFY_REQ: fprintf(f, "verify_req=%d ", n->type_counts[i]); break;
                case REP_VOTE1: fprintf(f, "vote1=%d ", n->type_counts[i]); break;
                case REP_VOTE2: fprintf(f, "vote2=%d ", n->type_counts[i]); break;
                case REP_LOG_LOGPUT: fprintf(f, "log_logput=%d ",  n->type_counts[i]); break;
                case REP_PGDUMP_REQ: fprintf(f, "pgdump_req=%d ", n->type_counts[i]); break;
                case REP_GEN_VOTE1: fprintf(f, "gen_vote1=%d ", n->type_counts[i]); break;
                case REP_GEN_VOTE2: fprintf(f, "gen_vote2=%d ", n->type_counts[i]); break;
                case REP_LOG_FILL: fprintf(f, "log_fill=%d ", n->type_counts[i]); break;
                default: 
                    fprintf(f, "unknown(%d)=%d ", i, n->type_counts[i]);
                    unknown += n->type_counts[i]; break;
            }
        }
    }
    fprintf(f, "total-unknown=%d\n", unknown);
    Pthread_mutex_unlock(&n->lock);
}

static void net_clear_queue_stats_rtn(netinfo_type *netinfo_type, void *netstat)
{
    net_queue_stat_t *n = (net_queue_stat_t *)netstat;
    Pthread_mutex_lock(&n->lock);
    if (n->type_counts)
        memset(n->type_counts, 0, sizeof(int) * (n->max_type + 1));
    memset(&n->max_lsn, 0, sizeof(n->max_lsn));
    memset(&n->min_lsn, 0, sizeof(n->min_lsn));
    n->unknown_count = n->total_count = 0;
    Pthread_mutex_unlock(&n->lock);
}

static void net_enque_write_rtn(netinfo_type *netinfo_ptr, void *netstat,
                                void *rec, int len)
{
    net_queue_stat_t *n = (net_queue_stat_t *)netstat;
    DB_LSN lsn;
    int rectype, rc;

    /* If we're exiting, some fields might have been
       destroyed before we get here. Return now. */
    if (net_is_exiting(netinfo_ptr)) return;

    rc = net_get_lsn_rectype(rec, len, &lsn, &rectype);
    Pthread_mutex_lock(&n->lock);
    if (rc == 0 && rectype < REP_MAX_TYPE) {
        if (n->type_counts == NULL) {
            n->type_counts = calloc(REP_MAX_TYPE, sizeof(int));
            n->max_type = REP_MAX_TYPE - 1;
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
    Pthread_mutex_unlock(&n->lock);
}

static void net_enque_free(netinfo_type *netinfo_ptr, void *netstat)
{
    net_queue_stat_t *n = (net_queue_stat_t *)netstat;
    if (netstat == NULL)
        return;
    free(n->nettype);
    free(n->hostname);
    if (n->type_counts)
        free(n->type_counts);
    Pthread_mutex_destroy(&n->lock);
    free(n);
}

void net_rep_qstat_init(netinfo_type *netinfo_ptr)
{
    net_register_queue_stat(netinfo_ptr, net_init_queue_stats_rtn,
                            net_start_reader, net_enque_write_rtn,
                            net_dump_queue_stats_rtn, 
                            net_clear_queue_stats_rtn, net_enque_free);
}

int rep_qstat_has_allreq(void)
{
    int ret = 0;
    net_queue_stat_t *n = (net_queue_stat_t *)reader_qstat;
    if (n == NULL)
        return 0;
    Pthread_mutex_lock(&n->lock);
    if (n->max_type >= REP_ALL_REQ) {
        ret += n->type_counts[REP_ALL_REQ];
    }
    Pthread_mutex_unlock(&n->lock);
    return ret ? 1 : 0;
}

int rep_qstat_has_master_req(void)
{
    int ret = 0;
    net_queue_stat_t *n = (net_queue_stat_t *)reader_qstat;
    if (n == NULL)
        return 0;
    Pthread_mutex_lock(&n->lock);
    if (n->max_type >= REP_MASTER_REQ) {
        ret += n->type_counts[REP_MASTER_REQ];
    }
    Pthread_mutex_unlock(&n->lock);
    return ret ? 1 : 0;
}

int rep_qstat_has_fills(void)
{
    int ret = 0;
    net_queue_stat_t *n = (net_queue_stat_t *)reader_qstat;
    if (n == NULL)
        return 0;
    Pthread_mutex_lock(&n->lock);
    if (n->max_type >= REP_LOG_FILL) {
        ret += n->type_counts[REP_LOG_FILL];
        ret += n->type_counts[REP_LOG_MORE];
    }
    Pthread_mutex_unlock(&n->lock);
    return ret ? 1 : 0;
}
