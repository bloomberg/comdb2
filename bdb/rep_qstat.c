#include <net.h>
#include <string.h>
#include <strings.h>
#include <build/db.h>
#include <dbinc/rep_types.h>
#include "bdb_int.h"
#include <rep_qstat.h>
#include "locks_wrap.h"
#include <plbitlib.h>
#include <logmsg.h>
#include <unistd.h>
#include <str0.h>
#ifdef _LINUX_SOURCE
#include <sys/syscall.h>
#endif

int net_get_lsn_rectype(bdb_state_type *bdb_state, const void *buf, int buflen,
                        DB_LSN *lsn, int *myrectype);

static __thread void *reader_qstat;

unsigned long long track_qstat = 0;
int qstat_stack_threshold = 0;

static void *net_init_queue_stats_rtn(netinfo_type *netinfo_type,
                                      const char *nettype,
                                      const char hostname[])
{
    net_queue_stat_t *n = calloc(sizeof(*n), 1);
    Pthread_mutex_init(&n->lock, NULL);
    n->nettype = strdup(nettype);
    n->hostname = strdup(hostname);
    n->max_type = -1;
    n->did_pstack = 0;
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
    if (!n) return;
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
    n->did_pstack = 0;
    Pthread_mutex_unlock(&n->lock);
}

static void net_enque_write_rtn(netinfo_type *netinfo_ptr, void *netstat,
                                void *rec, int len)
{
    net_queue_stat_t *n = (net_queue_stat_t *)netstat;
    bdb_state_type *bdb_state;
    DB_LSN lsn;
    int rectype, rc;

    /* If we're exiting, some fields might have been
       destroyed before we get here. Return now. */
    if (net_is_exiting(netinfo_ptr))
        return;

    /* Get a pointer back to our bdb_state */
    bdb_state = net_get_usrptr(netinfo_ptr);

    rc = net_get_lsn_rectype(bdb_state, rec, len, &lsn, &rectype);
    Pthread_mutex_lock(&n->lock);
    if (rc == 0 && rectype < REP_MAX_TYPE) {
        if (n->type_counts == NULL) {
            n->type_counts = calloc(REP_MAX_TYPE, sizeof(int));
            n->max_type = REP_MAX_TYPE - 1;
        }
        n->type_counts[rectype]++;

        if (n->did_pstack == 0 && btst(&track_qstat, rectype) &&
                qstat_stack_threshold > 0 && n->type_counts[rectype] >
                qstat_stack_threshold) {
            pid_t tid;
#ifdef _LINUX_SOURCE
            tid = syscall(__NR_gettid);
#else
            tid = getpid();
#endif
            logmsg(LOGMSG_USER, "pstack: tracked rep-type %d > threshold %d\n",
                    rectype, qstat_stack_threshold);
            char pstack_cmd[128];
            snprintf(pstack_cmd, sizeof(pstack_cmd), "pstack %d", (int)tid);
            rc = system(pstack_cmd);
            if (rc != 0) {
                logmsg(LOGMSG_USER, "%s: error pstacking: %d\n", __func__, rc);
            } else {
                n->did_pstack = 1;
            }
        }

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

void rep_qstat_lsn_range(DB_LSN *min_lsn, DB_LSN *max_lsn)
{
    net_queue_stat_t *n = (net_queue_stat_t *)reader_qstat;
    if (n == NULL)
        return;
    Pthread_mutex_lock(&n->lock);
    *min_lsn = n->min_lsn;
    *max_lsn = n->max_lsn;
    Pthread_mutex_unlock(&n->lock);
}

int rep_qstat_tunable_track(void *context, void *in)
{
    char *repstr = (char *)in;
    int reptype = atoi(repstr);

    if (reptype < 0 || reptype >= REP_MAX_TYPE) {
        return -1;
    }
    bset(&track_qstat, reptype);
    return 0;
}

int rep_qstat_tunable_untrack(void *context, void *in)
{
    char *repstr = (char *)in;
    int reptype = atoi(repstr);
    if (reptype < 0 || reptype >= REP_MAX_TYPE) {
        return -1;
    }
    bclr(&track_qstat, reptype);
    return 0;
}

void *rep_qstat_tunable_value(void *context)
{
    static char value[256] = {0};
    value[0] = '\0';
    int first = 0;

    for (int i = 0; i < REP_MAX_TYPE; i++) {
        if (btst(&track_qstat, i)) {
            char n[5];
            if (first) {
                snprintf(n, sizeof(n), ",%d", i);
            } else {
                snprintf(n, sizeof(n), "%d", i);
                first = 1;
            }
            strncat0(value, n, sizeof(value));
        }
    }
    return value;
}
