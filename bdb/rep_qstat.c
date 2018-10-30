#include <net.h>
#include <string.h>
#include <build/db.h>
#include <dbinc/rep_types.h>
#include "bdb_int.h"
#include <rep_qstat.h>

int net_get_lsn_rectype(bdb_state_type *bdb_state, const void *buf, int buflen,
                        DB_LSN *lsn, int *myrectype);

static __thread void *reader_qstat;

static void *net_init_queue_stats_rtn(netinfo_type *netinfo_type,
                                      const char *nettype,
                                      const char hostname[])
{
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
    pthread_mutex_unlock(&n->lock);
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
    pthread_mutex_destroy(&n->lock);
    free(n);
}

void net_rep_qstat_init(netinfo_type *netinfo_ptr)
{
    net_register_queue_stat(netinfo_ptr, net_init_queue_stats_rtn,
                            net_start_reader, net_enque_write_rtn,
                            net_clear_queue_stats_rtn, net_enque_free);
}

int rep_qstat_has_allreq(void)
{
    int ret = 0;
    net_queue_stat_t *n = (net_queue_stat_t *)reader_qstat;
    if (n == NULL)
        return 0;
    pthread_mutex_lock(&n->lock);
    if (n->max_type >= REP_ALL_REQ) {
        ret += n->type_counts[REP_ALL_REQ];
    }
    pthread_mutex_unlock(&n->lock);
    return ret ? 1 : 0;
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
