/*
   Copyright 2015, 2022, Bloomberg Finance L.P.

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

/* berkeley db specific rep stuff in this file, keep net.c free
   of berkeley db dependencies */

#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <poll.h>

#include "debug_switches.h"

#include "bdb_int.h"
#include "locks.h"
#include "locks_wrap.h"
#include <endian_core.h>
#include "memory_sync.h"
#include "ctrace.h"
#include "nodemap.h"
#include "util.h"
#include "crc32c.h"
#include "gettimeofday_ms.h"

#include <build/db_int.h>
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include <trigger.h>
#include "printformats.h"
#include "phys_rep.h"
#include "phys_rep_lsn.h"
#include <compat.h>
#include "str0.h"
#include <thrman.h>
#ifdef _LINUX_SOURCE
#include <sys/syscall.h>
#endif

#define REP_PRI 100     /* we are all equal in the eyes of god */
#define REPTIME 3000000 /* default 3 second timeout on election */

#define MILLISEC 1000

/* XXX this is dangerous i think. */
/*#define DECOM_LOGIC*/

int gbl_watcher_thread_ran = 0;
int gbl_lost_master_time = 0;
int gbl_ignore_lost_master_time = 0;
int gbl_prefault_latency = 0;
int gbl_long_log_truncation_warn_thresh_sec = INT_MAX;
int gbl_long_log_truncation_abort_thresh_sec = INT_MAX;
int gbl_dump_sql_on_repwait_sec = 10;
int gbl_debug_drop_nth_rep_message = 0;

extern struct thdpool *gbl_udppfault_thdpool;
extern int gbl_commit_delay_trace;

/* osqlcomm.c code, hurray! */
extern void osql_decom_node(char *decom_host);

extern void thd_dump(void);

void *mymalloc(size_t size);
void *myrealloc(void *ptr, size_t size);
void reset_aa_counter(char *tblname);
void create_coherency_lease_thread(bdb_state_type *bdb_state);
void create_master_lease_thread(bdb_state_type *bdb_state);

int gbl_net_lmt_upd_incoherent_nodes = 70;

char *lsn_to_str(char lsn_str[], DB_LSN *lsn);
void comdb2_dump_blockers(DB_ENV *);

static int bdb_wait_for_seqnum_from_node_nowait_int(bdb_state_type *bdb_state,
                                                    seqnum_type *seqnum,
                                                    char *host);

static void bdb_zap_lsn_waitlist(bdb_state_type *bdb_state, const char *host);

static int last_slow_node_check_time = 0;
static pthread_mutex_t slow_node_check_lk = PTHREAD_MUTEX_INITIALIZER;

struct rep_type_berkdb_rep_buf_hdr {
    int recbufsz;
    int recbufcrc;
};

enum { REP_TYPE_BERKDB_REP_BUF_HDR_LEN = 4 + 4 };

BB_COMPILE_TIME_ASSERT(rep_type_berkdb_rep_buf_hdr,
                       sizeof(struct rep_type_berkdb_rep_buf_hdr) ==
                           REP_TYPE_BERKDB_REP_BUF_HDR_LEN);

static uint8_t *rep_type_berkdb_rep_buf_hdr_put(
    const struct rep_type_berkdb_rep_buf_hdr *p_rep_type_berkdb_rep_buf_hdr,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        REP_TYPE_BERKDB_REP_BUF_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_rep_type_berkdb_rep_buf_hdr->recbufsz),
                    sizeof(p_rep_type_berkdb_rep_buf_hdr->recbufsz), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_rep_type_berkdb_rep_buf_hdr->recbufcrc),
                    sizeof(p_rep_type_berkdb_rep_buf_hdr->recbufcrc), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *rep_type_berkdb_rep_buf_hdr_get(
    struct rep_type_berkdb_rep_buf_hdr *p_rep_type_berkdb_rep_buf_hdr,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        REP_TYPE_BERKDB_REP_BUF_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_rep_type_berkdb_rep_buf_hdr->recbufsz),
                    sizeof(p_rep_type_berkdb_rep_buf_hdr->recbufsz), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_rep_type_berkdb_rep_buf_hdr->recbufcrc),
                    sizeof(p_rep_type_berkdb_rep_buf_hdr->recbufcrc), p_buf,
                    p_buf_end);

    return p_buf;
}

struct rep_type_berkdb_rep_ctrlbuf_hdr {
    int controlbufsz;
    int controlbufcrc;
};

enum { REP_TYPE_BERKDB_REP_CTRLBUF_HDR_LEN = 4 + 4 };

BB_COMPILE_TIME_ASSERT(rep_type_berkdb_rep_ctrlbuf_hdr,
                       sizeof(struct rep_type_berkdb_rep_ctrlbuf_hdr) ==
                           REP_TYPE_BERKDB_REP_CTRLBUF_HDR_LEN);

static uint8_t *rep_type_berkdb_rep_ctrlbuf_hdr_put(
    const struct rep_type_berkdb_rep_ctrlbuf_hdr *
        p_rep_type_berkdb_rep_ctrlbuf_hdr,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        REP_TYPE_BERKDB_REP_CTRLBUF_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_rep_type_berkdb_rep_ctrlbuf_hdr->controlbufsz),
                    sizeof(p_rep_type_berkdb_rep_ctrlbuf_hdr->controlbufsz),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_rep_type_berkdb_rep_ctrlbuf_hdr->controlbufcrc),
                    sizeof(p_rep_type_berkdb_rep_ctrlbuf_hdr->controlbufcrc),
                    p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *rep_type_berkdb_rep_ctrlbuf_hdr_get(
    struct rep_type_berkdb_rep_ctrlbuf_hdr *p_rep_type_berkdb_rep_ctrlbuf_hdr,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        REP_TYPE_BERKDB_REP_CTRLBUF_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_rep_type_berkdb_rep_ctrlbuf_hdr->controlbufsz),
                    sizeof(p_rep_type_berkdb_rep_ctrlbuf_hdr->controlbufsz),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_rep_type_berkdb_rep_ctrlbuf_hdr->controlbufcrc),
                    sizeof(p_rep_type_berkdb_rep_ctrlbuf_hdr->controlbufcrc),
                    p_buf, p_buf_end);

    return p_buf;
}

struct rep_type_berkdb_rep_seqnum {
    int seqnum;
};

enum { REP_TYPE_BERKDB_REP_SEQNUM = 4 };

BB_COMPILE_TIME_ASSERT(rep_type_berkdb_rep_seqnum,
                       sizeof(struct rep_type_berkdb_rep_seqnum) ==
                           REP_TYPE_BERKDB_REP_SEQNUM);

static uint8_t *rep_type_berkdb_rep_seqnum_put(
    const struct rep_type_berkdb_rep_seqnum *p_rep_type_berkdb_rep_seqnum,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || REP_TYPE_BERKDB_REP_SEQNUM > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_rep_type_berkdb_rep_seqnum->seqnum), sizeof(int), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *rep_type_berkdb_rep_seqnum_get(
    struct rep_type_berkdb_rep_seqnum *p_rep_type_berkdb_rep_seqnum,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || REP_TYPE_BERKDB_REP_SEQNUM > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_rep_type_berkdb_rep_seqnum->seqnum), sizeof(int), p_buf,
                    p_buf_end);

    return p_buf;
}

uint8_t *rep_berkdb_seqnum_type_put(const seqnum_type *p_seqnum_type,
                                    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BDB_SEQNUM_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_seqnum_type->lsn.file), sizeof(p_seqnum_type->lsn.file),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_seqnum_type->lsn.offset),
                    sizeof(p_seqnum_type->lsn.offset), p_buf, p_buf_end);
    p_buf = buf_put(&(p_seqnum_type->issue_time),
                    sizeof(p_seqnum_type->issue_time), p_buf, p_buf_end);
    p_buf = buf_put(&(p_seqnum_type->lease_ms), sizeof(p_seqnum_type->lease_ms),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_seqnum_type->commit_generation),
                    sizeof(p_seqnum_type->commit_generation), p_buf, p_buf_end);
    p_buf = buf_put(&(p_seqnum_type->generation),
                    sizeof(p_seqnum_type->generation), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *rep_berkdb_seqnum_type_get(seqnum_type *p_seqnum_type,
                                                 const uint8_t *p_buf,
                                                 const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BDB_SEQNUM_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_seqnum_type->lsn.file), sizeof(p_seqnum_type->lsn.file),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_seqnum_type->lsn.offset),
                    sizeof(p_seqnum_type->lsn.offset), p_buf, p_buf_end);
    p_buf = buf_get(&(p_seqnum_type->issue_time),
                    sizeof(p_seqnum_type->issue_time), p_buf, p_buf_end);
    p_buf = buf_get(&(p_seqnum_type->lease_ms), sizeof(p_seqnum_type->lease_ms),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_seqnum_type->commit_generation),
                    sizeof(p_seqnum_type->commit_generation), p_buf, p_buf_end);
    p_buf = buf_get(&(p_seqnum_type->generation),
                    sizeof(p_seqnum_type->generation), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *rep_udp_filepage_type_put(const filepage_type *p_filepage_type,
                                   uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BDB_FILEPAGE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_filepage_type->fileid), sizeof(p_filepage_type->fileid),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_filepage_type->pgno), sizeof(p_filepage_type->pgno),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *rep_udp_filepage_type_get(filepage_type *p_filepage_type,
                                         const uint8_t *p_buf,
                                         const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BDB_FILEPAGE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_filepage_type->fileid), sizeof(p_filepage_type->fileid),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_filepage_type->pgno), sizeof(p_filepage_type->pgno),
                    p_buf, p_buf_end);

    return p_buf;
}

enum { DB_LSN_TYPE_SIZE = 4 + 4 };

BB_COMPILE_TIME_ASSERT(db_lsn_type, sizeof(DB_LSN) == DB_LSN_TYPE_SIZE);

const uint8_t *db_lsn_type_get(DB_LSN *p_db_lsn, const uint8_t *p_buf,
                               const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || DB_LSN_TYPE_SIZE > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_db_lsn->file), sizeof(p_db_lsn->file), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_lsn->offset), sizeof(p_db_lsn->offset), p_buf,
                    p_buf_end);

    return p_buf;
}

const uint8_t *db_lsn_type_put(const DB_LSN *p_db_lsn, uint8_t *p_buf,
                               const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || DB_LSN_TYPE_SIZE > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_db_lsn->file), sizeof(p_db_lsn->file), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_lsn->offset), sizeof(p_db_lsn->offset), p_buf,
                    p_buf_end);

    return p_buf;
}

uint8_t *pgcomp_snd_type_put(const pgcomp_snd_t *p_snd, uint8_t *p_buf,
                             const uint8_t *p_buf_end, const void *data)
{
    if (p_buf_end < p_buf || BDB_PGCOMP_SND_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_snd->id), sizeof(p_snd->id), p_buf, p_buf_end);
    p_buf = buf_put(&(p_snd->size), sizeof(p_snd->size), p_buf, p_buf_end);
    p_buf = buf_no_net_put(data, p_snd->size, p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *pgcomp_snd_type_get(pgcomp_snd_t *p_snd, const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BDB_PGCOMP_SND_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_snd->id), sizeof(p_snd->id), p_buf, p_buf_end);
    p_buf = buf_get(&(p_snd->size), sizeof(p_snd->size), p_buf, p_buf_end);

    return p_buf;
}

/* XXX ripped from berkdb innards, only used for informational puroposes */
typedef struct {
    u_int32_t rep_version; /* Replication version number. */
    u_int32_t log_version; /* Log version number. */

    DB_LSN lsn;        /* Log sequence number. */
    u_int32_t rectype; /* Message type. */
    u_int32_t gen;     /* Generation number. */
    u_int32_t flags;   /* log_put flag value. */
} rep_control_type;

#include <dbinc/rep_types.h>

int is_electable(bdb_state_type *bdb_state, int *out_num_up,
                 int *out_num_connected);

extern int gbl_rep_node_pri;
extern int gbl_handoff_node;
extern int gbl_use_node_pri;

void bdb_losemaster(bdb_state_type *bdb_state)
{
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost) {
        logmsg(LOGMSG_INFO, "trapped to lost mastership\n");
        bdb_state->need_to_downgrade_and_lose = 1;
    }
}


int bdb_is_an_unconnected_master(bdb_state_type *bdb_state)
{
    if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost) {
        /*not a master, so not standalone */
        return 0;
    }

    const char *hostlist[REPMAX];
    return (net_get_all_nodes_connected(bdb_state->repinfo->netinfo, hostlist) == 0);
}


void bdb_transfermaster(bdb_state_type *bdb_state)
{
    int rc = 0;
    const char *hostlist[REPMAX];

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;


    if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost) {
        /*not a master */
        return;
    }

    if(net_get_all_nodes_connected(bdb_state->repinfo->netinfo, hostlist) == 0) {
        logmsg(LOGMSG_INFO, "This was standalone\n");
        return;
    }

    rc = bdb_downgrade(bdb_state, 0, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d bdb_downgrade failed rc=%d ?\n", __FILE__,
                __LINE__, rc);
    }
}

int gbl_set_coherent_state_trace = 1;

char *coherent_state_to_str(int state)
{
    switch (state) {
    case STATE_INCOHERENT:
        return "INCOHERENT";
    case STATE_INCOHERENT_WAIT:
        return "INCOHERENT_WAIT";
    case STATE_INCOHERENT_SLOW:
        return "INCOHERENT_SLOW";
    case STATE_COHERENT:
        return "COHERENT";
    default:
        return "?UNKNOWN_STATE?";
    }
}

/* You should have the lock */
static inline void set_coherent_state(bdb_state_type *bdb_state,
                                      const char *hostname, int state,
                                      const char *func, int line)
{
    if (bdb_state->coherent_state[nodeix(hostname)] != state) {
        bdb_state->coherent_state[nodeix(hostname)] = state;
        if (gbl_set_coherent_state_trace) {
            logmsg(LOGMSG_USER, "%s line %d setting %s coherent state to %s\n",
                   func, line, hostname, coherent_state_to_str(state));
        }
    }
}

void bdb_transfermaster_tonode(bdb_state_type *bdb_state, char *tohost)
{
    const char *hostlist[REPMAX];
    int count;
    int i;
    int rc;
    int numsleeps;
    int electable;
    int num_up;
    int num_connected;
    char *myhost = bdb_state->repinfo->myhost;

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /* get out of here if we arent the master */
    if (bdb_state->repinfo->master_host != myhost) {
        return;
    }

    /* get out of here if the cluster isnt electable */
    electable = is_electable(bdb_state, &num_up, &num_connected);
    if (!electable) {
        logmsg(LOGMSG_ERROR, "%s: cluster unelectable\n", __func__);
        return;
    }

    count = net_get_all_nodes_connected(bdb_state->repinfo->netinfo, hostlist);

    /* the goal is to give it up quickly.  stop looking for candidates as
       soon as you find anyone */
    for (i = 0; i < count; i++) {
        if (hostlist[i] == tohost)
            break;
    }

    if (hostlist[i] != tohost) {
        logmsg(LOGMSG_ERROR, "%s: node %s not in the connection list\n", __func__,
                tohost);
        return;
    }

    /* give me a break, mr watcher thread */
    bdb_state->repinfo->disable_watcher = comdb2_time_epoch() + 10;

    if (bdb_state->callback->scabort_rtn)
        /* if there is a schema change going on, kill it */
        bdb_state->callback->scabort_rtn();

    /* we are about to lose mastership, but there is no big lock to protect
       writes anymore;
       downgrade w/out election so we stop temporarely writes from coming in
       while I try to pass mastership to the provided node */
    bdb_state->repinfo->dont_elect_untill_time = comdb2_time_epoch() + 5;
    bdb_downgrade_noelect(bdb_state);

    numsleeps = 0;
    DB_LSN tohost_lsn = bdb_state->seqnum_info->seqnums[nodeix(tohost)].lsn;
    DB_LSN myhost_lsn = bdb_state->seqnum_info->seqnums[nodeix(myhost)].lsn;
again:
    if (((tohost_lsn.file > myhost_lsn.file) ||
         (tohost_lsn.file == myhost_lsn.file &&
          tohost_lsn.offset >= myhost_lsn.offset)) &&
        (bdb_state->callback->nodeup_rtn(bdb_state, tohost))) {
        logmsg(LOGMSG_INFO,
               "%s: to-be-upgraded node %s is up-to-date %d:%d, us %d:%d\n",
               __func__, tohost, tohost_lsn.file, tohost_lsn.offset,
               myhost_lsn.file, myhost_lsn.offset);
    } else {
        numsleeps++;
        if (numsleeps > 2) {
            logmsg(LOGMSG_ERROR, "transfer master falling back to election\n");
            bdb_downgrade(bdb_state, 0, NULL);
            return;
        }

        logmsg(LOGMSG_WARN, "node %s is still behind, waiting 1 second\n", tohost);
        sleep(1);
        goto again;
    }

    logmsg(LOGMSG_INFO, "%s: transferring master to %s\n", __func__, tohost);

    int hostlen;
    hostlen = strlen(tohost) + 1;

    /* wait 1 second for ack */
    rc = net_send_message(bdb_state->repinfo->netinfo, tohost,
                          USER_TYPE_TRANSFERMASTER_NAME, tohost, hostlen, 1,
                          1 * 1000);

    /* didnt ack the message?  kick off an election */
    if (rc != 0) {
        bdb_state->repinfo->dont_elect_untill_time = comdb2_time_epoch();
        call_for_election(bdb_state, __func__, __LINE__);
    }
}

int bdb_sync_cluster(bdb_state_type *bdb_state, int sync_all)
{
    int rc;
    seqnum_type seqnum;

    /* The DB is in async mode, force a checkpoint at this point. */
    bdb_flush(bdb_state, &rc);

    if (rc) {
        logmsg(LOGMSG_ERROR, "failed to Flush DB %d\n", rc);
        return rc;
    }

    /* fill in the lsn part in seqnum */
    get_master_lsn(bdb_state, &(seqnum.lsn));
    if (sync_all) {
        rc = bdb_wait_for_seqnum_from_all(bdb_state, (seqnum_type *)&seqnum);
    } else {
        rc = bdb_wait_for_seqnum_from_room(bdb_state, (seqnum_type *)&seqnum);
    }
    return rc;
}

static void send_context_to_all(bdb_state_type *bdb_state)
{
    if (!bdb_state->attr->net_send_gblcontext)
        return;
    /* only the master can send these */
    if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost)
        return;
    unsigned long long gblcontext = get_gblcontext(bdb_state);
    void *data[] = {&gblcontext};
    int sz[] = {sizeof(gblcontext)};
    int type[] = {USER_TYPE_GBLCONTEXT};
    int flag[] = {0};
    net_send_all(bdb_state->repinfo->netinfo, 1, data, sz, type, flag);
}

static inline int is_incoherent_complete(bdb_state_type *bdb_state,
                                         const char *host, int *incohwait)
{
    int is_incoherent, state;

    is_incoherent = 0;

    if (incohwait)
        *incohwait = 0;

    Pthread_mutex_lock(&(bdb_state->coherent_state_lock));
    state = bdb_state->coherent_state[nodeix(host)];
    Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));

    /* STATE_COHERENT and STATE_INCOHERENT_LOCAL return COHERENT. */
    if (state == STATE_INCOHERENT || state == STATE_INCOHERENT_SLOW)
        is_incoherent = 1;

    if (incohwait && state == STATE_INCOHERENT_WAIT)
        *incohwait = 1;

    return is_incoherent;
}

int is_incoherent(bdb_state_type *bdb_state, const char *host)
{
    return is_incoherent_complete(bdb_state, host, NULL);
}

int gbl_throttle_logput_trace = 0;
int gbl_incoherent_logput_window = 0;

static int throttle_updates_incoherent_nodes(netinfo_type *netinfo_ptr, const char *host)
{
    bdb_state_type *bdb_state = net_get_usrptr(netinfo_ptr);
    int ret = 0, now, pr = 0;
    static int lastpr = 0;
    static unsigned long long throttles = 0;
    unsigned long long cntbytes;
    uint32_t window = gbl_incoherent_logput_window;

    /* Disabled */
    if (window <= 0) {
        return 0;
    }

    if (gbl_throttle_logput_trace && ((now = time(NULL)) - lastpr)) {
        pr = 1;
        lastpr = now;
    }

    /* INCOHERENT & INCOHERENT_SLOW */
    if (is_incoherent(bdb_state, host)) {

        DB_LSN *lsnp, *masterlsn;

        lsnp = &bdb_state->seqnum_info->seqnums[nodeix(host)].lsn;
        masterlsn = &bdb_state->seqnum_info->seqnums[nodeix(bdb_state->repinfo->master_host)].lsn;
        cntbytes = subtract_lsn(bdb_state, masterlsn, lsnp);
        if (cntbytes > window) {
            ret = 1;
            throttles++;
            if (pr) {
                logmsg(LOGMSG_USER,
                       "%s throttling logput to %s, incoherent"
                       " %llu bytes behind, total throttles=%llu\n",
                       __func__, host, cntbytes, throttles);
            }
        } else {
            if (pr) {
                logmsg(LOGMSG_USER,
                       "%s NOT throttling logput to %s, "
                       "incoherent and %llu bytes behind, total, "
                       "throttles=%llu\n",
                       __func__, host, cntbytes, throttles);
            }
        }
    } else if (pr) {
        logmsg(LOGMSG_USER, "%s allowing logput to %s\n", __func__, host);
    }

    return ret;
}

void net_rep_throttle_init(netinfo_type *netinfo_ptr)
{
    net_register_throttle(netinfo_ptr, throttle_updates_incoherent_nodes);
}

extern int gbl_rowlocks;

static unsigned long long callcount = 0;
static unsigned long long bytecount = 0;

unsigned long long rep_get_send_callcount(void) { return callcount; }

unsigned long long rep_get_send_bytecount(void) { return bytecount; }

void rep_reset_send_callcount(void) { callcount = 0; }

void rep_reset_send_bytecount(void) { bytecount = 0; }

extern int gbl_decoupled_logputs;

int berkdb_send_rtn(DB_ENV *dbenv, const DBT *control, const DBT *rec,
                    const DB_LSN *lsnp, char *host, uint32_t flags,
                    void *usr_ptr)
{
    bdb_state_type *bdb_state;
    int bufsz;
    int rc;
    int outrc;
    struct rep_type_berkdb_rep_ctrlbuf_hdr p_rep_type_berkdb_rep_ctrlbuf_hdr = {0};
    struct rep_type_berkdb_rep_buf_hdr p_rep_type_berkdb_rep_buf_hdr = {0};
    struct rep_type_berkdb_rep_seqnum p_rep_type_berkdb_rep_seqnum = {0};
    uint8_t *p_buf, *p_buf_end;
    int rectype = 0;
    int *seqnum;
    int nodelay;
    int useheap = 0;
    int is_logput = 0;
    tran_type *tran = NULL;

    /* get a pointer back to our bdb_state */
    bdb_state = (bdb_state_type *)dbenv->app_private;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    callcount++;

    /*
       int now;
       static int lastpr = 0;
       if((now=comdb2_time_epoch()) - lastpr)
       {
           fprintf(stderr, "Called %s %llu times
       flags=0x%x\n",__func__,callcount,flags);
           lastpr=now;
       }
    */

    rep_control_type *rep_control;
    rep_control = control->data;

    /* We want to be able to throttle log propagation to avoid filling
       the net queue; this will allow signal messages and catching up
       log replies to be transferred even though the database is under heavy
       load.
       Problem is, in berkdb_send_rtn both regular log messages and catching
       up log replies are coming as REP_LOG
       In __log_push we replace REP_LOG with REP_LOG_LOGPUT so we know
       that this must be throttled; we revert to REP_LOG in the same routine
       before sending the message
     */

    p_buf = (uint8_t *)&rep_control->rectype;
    p_buf_end =
        ((uint8_t *)&rep_control->rectype + sizeof(rep_control->rectype));

    buf_get(&rectype, sizeof(rectype), p_buf, p_buf_end);

    if (rectype == REP_LOG_LOGPUT) {
        is_logput = 1;
        rectype = REP_LOG;
        p_buf = (uint8_t *)&rep_control->rectype;
        p_buf_end =
            ((uint8_t *)&rep_control->rectype + sizeof(rep_control->rectype));
        buf_put(&rectype, sizeof(rectype), p_buf, p_buf_end);
    }

    /* good return code until proven otherwise */
    outrc = 0;
    /*
       if (__bdb_no_send)
       return 0;
       */

    bufsz = sizeof(int) +   /* seqnum */
            sizeof(int) +   /* recsz */
            sizeof(int) +   /* reccrc */
            rec->size +     /* recbuf */
            sizeof(int) +   /* controlsz */
            sizeof(int) +   /* controlcrc */
            control->size;  /* controlbuf */

    if (bufsz > 1024 * 65)
        useheap = 1;

    char *buf = useheap ? malloc(bufsz) : alloca(bufsz);

    bytecount += bufsz;

    p_buf = (uint8_t *)buf;
    p_buf_end = ((uint8_t *)buf + bufsz);

    /* not included in the buf headers as it's set multiple times */
    seqnum = (int *)p_buf;
    *seqnum = 0;
    p_buf += sizeof(int);

    if (bdb_state->rep_trace) {
        char str[80];
        DB_LSN tmp;
        tmp.file = ntohl(rep_control->lsn.file);
        tmp.offset = ntohl(rep_control->lsn.offset);

        if (host == db_eid_broadcast)
            logmsg(LOGMSG_USER, "--- berkdb told us to send to all "
                            "(rectype %d lsn %s)\n",
                    rectype, lsn_to_str(str, &tmp));
        else
            logmsg(LOGMSG_USER, "--- berkdb told us to send to %s (%s) "
                                "(rectype %d lsn %s)\n",
                   host, host, rectype, lsn_to_str(str, &tmp));
    }

    if (bdb_state->attr->repchecksum) {
        /*fprintf(stderr, "1) repchecksum\n");*/
        p_rep_type_berkdb_rep_buf_hdr.recbufcrc = crc32c(rec->data, rec->size);
        p_rep_type_berkdb_rep_ctrlbuf_hdr.controlbufcrc =
            crc32c(control->data, control->size);
    }

    p_rep_type_berkdb_rep_buf_hdr.recbufsz = rec->size;
    p_rep_type_berkdb_rep_ctrlbuf_hdr.controlbufsz = control->size;

    /* pack buffer header */
    p_buf = rep_type_berkdb_rep_buf_hdr_put(&(p_rep_type_berkdb_rep_buf_hdr),
                                            p_buf, p_buf_end);

    /* pack buffer */
    p_buf = buf_no_net_put(rec->data, rec->size, p_buf, p_buf_end);

    /* pack control buffer header */
    p_buf = rep_type_berkdb_rep_ctrlbuf_hdr_put(
        &(p_rep_type_berkdb_rep_ctrlbuf_hdr), p_buf, p_buf_end);

    /* pack control buffer payload */
    p_buf = buf_no_net_put(control->data, control->size, p_buf, p_buf_end);

    nodelay = 0;

    tran = pthread_getspecific(bdb_state->seqnum_info->key);
    if (tran && tran->is_rowlocks_trans) {
        /* if about to commit, remember the lsn to wait for as the first
           lsn of the logical transaction. if not about to commit the
           logical transaction, don't wait for replication. */
        if (tran->is_about_to_commit) {
            nodelay = 1;
        }
    } else {
        if ((flags & DB_REP_PERMANENT) || (flags & DB_REP_NOBUFFER))
            nodelay = 1;
    }

    if (flags & DB_REP_FLUSH)
        nodelay = 1;

    if ((flags & DB_REP_PERMANENT) || (flags & DB_REP_LOGPROGRESS)) {
        if (lsnp) {

            if (tran != NULL) {
                if (!tran->is_rowlocks_trans ||
                    (tran->is_rowlocks_trans && tran->is_about_to_commit)) {
                    /* squirel away the lsn into the memory we allocated in
                       tran_begin.  we wait on it and free the memory in
                       tran_commit */

                    p_buf = (uint8_t *)lsnp;
                    p_buf_end = (uint8_t *)lsnp + sizeof(DB_LSN);
                    db_lsn_type_get(&(tran->savelsn), p_buf, p_buf_end);

                    /*
                       char str[80];
                       fprintf(stderr, "setting savesln to
                       %s\n",lsn_to_str(str,savelsn));
                       */

                    /* just sanity check that we arent screwing up */
                    if (tran->savelsn.file == 0) {
                        logmsg(LOGMSG_FATAL, "set lsn to 0\n");
                        exit(1);
                    }
                }
            }
        }
    }

    if (host == db_eid_broadcast) {
        void *data[2];
        int sz[2];
        int type[2];
        int flag[2];
        int num = 0;
        uint64_t gblcontext;
        if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost &&
            is_logput && tran && bdb_state->attr->net_send_gblcontext &&
            nodelay && !gbl_rowlocks &&
            (gblcontext = get_gblcontext(bdb_state)) != 0
        ) {
            if (gblcontext == -1ULL) {
                logmsg(LOGMSG_ERROR, "SENDING context -1 to all nodes\n");
                cheap_stack_trace();
            }
            data[num] = &gblcontext;
            sz[num] = sizeof(gblcontext);
            type[num] = USER_TYPE_GBLCONTEXT;
            flag[num] = 0;
            ++num;
        }
        data[num] = buf;
        sz[num] = bufsz;
        type[num] = USER_TYPE_BERKDB_REP;
        flag[num] = (is_logput ? NET_SEND_LOGPUT : (NET_SEND_NODROP | NET_SEND_NODELAY)) |
                    ((flags & DB_REP_NODROP) ? NET_SEND_NODROP : 0) |
                    (bdb_state->attr->net_inorder_logputs ? NET_SEND_INORDER : 0) |
                    (nodelay ? NET_SEND_NODELAY : 0) |
                    (flags & DB_REP_TRACE ? NET_SEND_TRACE : 0);
        ++num;
        rc = net_send_all(bdb_state->repinfo->netinfo, num, data, sz, type, flag);
    } else {
        int tmpseq;
        uint8_t *p_seq_num = (uint8_t *)seqnum;
        uint8_t *p_seq_num_end = ((uint8_t *)seqnum + sizeof(int));

        p_rep_type_berkdb_rep_seqnum.seqnum = tmpseq =
            get_seqnum(bdb_state, host);
        rep_type_berkdb_rep_seqnum_put(&p_rep_type_berkdb_rep_seqnum, p_seq_num,
                                       p_seq_num_end);

        p_seq_num = (uint8_t *)seqnum;
        p_seq_num_end = ((uint8_t *)seqnum + sizeof(int));

        rep_type_berkdb_rep_seqnum_put(&p_rep_type_berkdb_rep_seqnum, p_seq_num,
                                       p_seq_num_end);

        if (bdb_state->rep_trace)
            logmsg(LOGMSG_USER, "--- sending seq %d to %s, nodelay is %d\n", tmpseq,
                    host, nodelay);

        uint32_t sendflags = 0;

        if (!is_logput)
            sendflags |= (NET_SEND_NODROP | NET_SEND_NODELAY);

        if (flags & DB_REP_NODROP)
            sendflags |= NET_SEND_NODROP;

        if (bdb_state->attr->net_inorder_logputs)
            sendflags |= NET_SEND_INORDER;

        if (nodelay)
            sendflags |= NET_SEND_NODELAY;

        if (flags & DB_REP_TRACE) {
            logmsg(LOGMSG_USER, "%s line %d calling net_send_flags\n", __func__,
                   __LINE__);
            sendflags |= NET_SEND_TRACE;
        }

        rc = net_send_flags(bdb_state->repinfo->netinfo, host,
                            USER_TYPE_BERKDB_REP, buf, bufsz, sendflags);
    }

    if (rc != 0) {
        outrc = 1;
    }

    if (useheap)
        free(buf);

    return outrc;
}

enum { DONT_LOSE = 1, LOSE = 2, REOPEN_AND_LOSE = 3 };

typedef struct {
    bdb_state_type *bdb_state;
    int op;
} elect_thread_args_type;

static int get_num_up(bdb_state_type *bdb_state)
{
    const char *nodelist[REPMAX];
    int num_up;
    int tot;
    int i;

    if (!bdb_state->callback->nodeup_rtn)
        return net_count_nodes(bdb_state->repinfo->netinfo);

    num_up = 0;

    tot = net_get_all_nodes(bdb_state->repinfo->netinfo, nodelist);

    for (i = 0; i < tot; i++)
        if ((bdb_state->callback->nodeup_rtn)(bdb_state, nodelist[i]))
            num_up++;

/* now check for me, cause we werent counted in the first list */
#if 0
   if ( (bdb_state->callback->nodeup_rtn)(bdb_state, 
      bdb_state->repinfo->mynode) )
      num_up++;
#endif
    /* Count me as always up - if I am about to participate in an election,
       it doesn't make sense that I am not counted */
    num_up++;

    return num_up;
}

/* decide if we have enough rtcpu'd machines connected to us to safely elect
 * a master. */
int is_electable(bdb_state_type *bdb_state, int *out_num_up,
                 int *out_num_connected)
{
    int num_up, num_connected, rc;

    num_up = get_num_up(bdb_state);
    num_connected = net_count_connected_nodes(bdb_state->repinfo->netinfo);

    if (bdb_state->attr->elect_forbid_perfect_netsplit) {
        if (num_connected < ((num_up / 2) + 1))
            rc = 0;
        else
            rc = 1;
    } else {
        if (num_connected < (num_up / 2))
            rc = 0;
        else
            rc = 1;
    }

    if (out_num_up)
        *out_num_up = num_up;
    if (out_num_connected)
        *out_num_connected = num_connected;
    return rc;
}

void defer_commits_for_upgrade(bdb_state_type *bdb_state, const char *host,
                               const char *func);

void set_repinfo_master_host(bdb_state_type *bdb_state, char *master,
                             const char *func, uint32_t line);

/* Abort election and reset in_election flag if db is exiting. */
static void abort_election_on_exit(bdb_state_type *bdb_state)
{
    if (!bdb_state->exiting)
        return;

    print(bdb_state, "elect_thread: exiting\n");

    Pthread_mutex_lock(&(bdb_state->repinfo->elect_mutex));
    bdb_state->repinfo->in_election = 0;
    Pthread_mutex_unlock(&(bdb_state->repinfo->elect_mutex));

    bdb_thread_event(bdb_state, 0);
    pthread_exit(NULL);
}

int gbl_elect_priority_bias = 0;

int gbl_rand_elect_timeout = 1;
uint32_t gbl_rand_elect_min_ms = 1000;
int gbl_rand_elect_max_ms = 7000;

static int elect_random_timeout(void)
{
    int range = (gbl_rand_elect_max_ms - gbl_rand_elect_min_ms), timeout_ms;
    range = range > 0 ? range : 2000;
    timeout_ms = gbl_rand_elect_min_ms + (rand() % range);
    if (timeout_ms <= 0)
        timeout_ms = 2000;
    return (timeout_ms * 1000);
}

time_t gbl_election_time_completed;
uint64_t gbl_last_election_time_ms;
uint64_t gbl_total_election_time_ms;
uint64_t gbl_election_count;

static void *elect_thread(void *args)
{
    int rc, count, i;
    bdb_state_type *bdb_state;
    char *master_host;
    int num;
    int end, start;
    int num_connected;
    int node_not_up = 0;
    uint32_t newgen;
    elect_thread_args_type *elect_thread_args;
    int elect_time;
    const char *hostlist[REPMAX];
    char *hoststring = NULL, *myhost;

    int rep_pri;
    int elect_count;

    int op;
    int done = 0;
    int elect_again = 0;

    thread_started("bdb election");

    elect_thread_args = (elect_thread_args_type *)args;
    bdb_state = elect_thread_args->bdb_state;
    op = elect_thread_args->op;

    bdb_thread_event(bdb_state, 1);

    free(elect_thread_args);

    /* Don't ctrace in the election codepath.  because /bb/data
     * was filled, writes to it (the ctrace file) were taking between 600 and
     * 1400 ms.  This prevents election from completeing in a reasonable amount
     * of time.  */

    Pthread_mutex_lock(&(bdb_state->repinfo->elect_mutex));

    if (bdb_state->repinfo->in_election) {
        logmsg(LOGMSG_INFO, "election already in progress, exiting\n");
        print(bdb_state, "election already in progress, exiting\n");
        Pthread_mutex_unlock(&(bdb_state->repinfo->elect_mutex));

        bdb_thread_event(bdb_state, 0);
        return NULL;
    }

    logmsg(LOGMSG_INFO, "thread 0x%p in election\n", (void *)pthread_self());

    bdb_state->repinfo->in_election = 1;
    start = comdb2_time_epochms();

    Pthread_mutex_unlock(&(bdb_state->repinfo->elect_mutex));

    abort_election_on_exit(bdb_state);

    if (op == REOPEN_AND_LOSE) {
        rc = bdb_reopen_inline(bdb_state);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "XXX reopen rc %d\n", rc);
            exit(1);
        }
    }

    /* base is in millesecondss, we need microseconds */
    elect_time = bdb_state->attr->electtimebase * 1000;

elect_again:
    /* Database may be exiting. Re-check before we re-elect. */
    abort_election_on_exit(bdb_state);

    /* Poll so we don't send whomaster reqs in a tight loop */
    if (elect_again)
        poll(NULL, 0, 100);

    if (bdb_state->callback->electsettings_rtn) {
        int elect_time_microsecs = 0;
        int elect_time_max = 0;

        if (bdb_state->callback->electsettings_rtn(
                bdb_state, &elect_time_microsecs) == 0) {
            if (elect_time_microsecs > 0)
                elect_time_max = elect_time_microsecs;
        }

        if (elect_time > elect_time_max)
            elect_time = elect_time_max;
    }

    /* Ignore that completely if rand-elect-time is set */
    if (gbl_rand_elect_timeout) {
        elect_time = elect_random_timeout();
    }

    if (!is_electable(bdb_state, &num, &num_connected)) {
        logmsg(LOGMSG_USER,
              "election will not be held, connected to %d of %d nodes\n",
              num_connected, num);
        print(bdb_state,
              "election will not be held, connected to %d of %d nodes\n",
              num_connected, num);

        Pthread_mutex_lock(&(bdb_state->repinfo->elect_mutex));
        bdb_state->repinfo->in_election = 0;
        Pthread_mutex_unlock(&(bdb_state->repinfo->elect_mutex));

        bdb_thread_event(bdb_state, 0);
        return NULL;
    }

    elect_count = MAX(num, num_connected);

    /* set our priority.  ordinarily we all use the same priority.
       lower our priority if we are rtcpued off.  moved this below
       elect_again so that, again, we can change this as conditions
       change.  note that it does not make sense to force ourselves
       to lose unless we are currently rtcpu'd off. */
    rep_pri = REP_PRI;
    if (bdb_state->callback->nodeup_rtn) {
        if (!(bdb_state->callback->nodeup_rtn(bdb_state,
                                              bdb_state->repinfo->myhost))) {
            rep_pri = rep_pri - 1;
            node_not_up = 1;
        }
    }

    if (gbl_elect_priority_bias && !node_not_up) {
        rep_pri = REP_PRI + gbl_elect_priority_bias;
    } else if (gbl_use_node_pri &&
               rep_pri ==
                   REP_PRI) { /* if the node is up, then apply priorities. */
        rep_pri = REP_PRI + gbl_rep_node_pri; /* priority should be > priority
                                                 of nodes which are down.*/
    }

    if ((op == LOSE) || (op == REOPEN_AND_LOSE))
        rep_pri = 1;

    count = net_get_all_nodes_connected(bdb_state->repinfo->netinfo, hostlist);

    myhost = net_get_mynode(bdb_state->repinfo->netinfo);
    hoststring = malloc(strlen(myhost) + 2);
    hoststring[0] = '\0';
    strcat(hoststring, myhost);
    strcat(hoststring, " ");

    for (i = 0; i < count; i++) {
        hoststring =
            realloc(hoststring, strlen(hoststring) + strlen(hostlist[i]) + 2);
        strcat(hoststring, hostlist[i]);
        strcat(hoststring, " ");
    }

    logmsg(LOGMSG_INFO,
           "0x%p: calling for election with cluster"
           " of %d nodes (%d connected) : %s,  %f secs timeout and priority %d\n",
           (void *)pthread_self(), elect_count, num_connected, hoststring, ((double)elect_time) / 1000000.00, rep_pri);

    free(hoststring);

    /* we're calling for election.  if we are doing this, we don't know who the
       master is.  ensure that master_eid isnt latched to the previous master
       here.  */
    set_repinfo_master_host(bdb_state, db_eid_invalid, __func__, __LINE__);

    int already_master = 0;

    /* Should be holding bdb readlock .. */
    BDB_READLOCK("rep_elect");

    rc = bdb_state->dbenv->rep_elect(bdb_state->dbenv, elect_count, rep_pri, elect_time, &newgen, &already_master,
                                     &master_host);
    BDB_RELLOCK();

    if (rc != 0) {
        if (rc == DB_REP_UNAVAIL)
            logmsg(LOGMSG_WARN, "failed to reach consensus in election with %f secs timeout\n",
                ((double)elect_time) / 1000000.00);
        else
            logmsg(LOGMSG_ERROR, "got %d from rep_elect\n", rc);

        /* ignored if rand_elect_timeout is set */
        elect_time *= 2;
        elect_again++;
        if (elect_again > 30) {
            logmsg(LOGMSG_ERROR, "election not proceeding, giving up\n");
            goto give_up;
        }

        goto elect_again;
    }
    /* replace now: if i was already master, rep-start wont be called */
    set_repinfo_master_host(bdb_state, master_host, __func__, __LINE__);

    /* Berkley says we are already master.  We won't get a rep-message and
     * shouldn't call the newmaster-callback here (which would normally set this
     * Just set thedb->master. */
    if (already_master) {
        thedb_set_master(master_host);
    }

    /* Check if it's us. */
    if (rc == 0) {

        if (master_host == bdb_state->repinfo->myhost) {
            logmsg(LOGMSG_INFO, "elect_thread: we won the election\n");
            /* Upgrade here if we are the only participant.  Otherwise,
             * defer upgrade until process_berkdb */
            if (elect_count == 1) {
                rc = bdb_upgrade(bdb_state, newgen, &done);
                print(bdb_state, "back from bdb_upgrade%s\n",
                        (!done) ? " (nop)" : "");
                if (rc != 0) {
                    logmsg(LOGMSG_FATAL, "bdb_upgrade returned bad rcode %d\n", rc);
                    exit(1);
                }
                Pthread_mutex_lock(&(bdb_state->repinfo->elect_mutex));
                bdb_state->repinfo->in_election = 0;
                Pthread_mutex_unlock(&(bdb_state->repinfo->elect_mutex));
                bdb_thread_event(bdb_state, 0);
                return NULL;
            }
        }
    }

give_up:
    end = comdb2_time_epochms();
    gbl_election_time_completed = time(NULL);
    gbl_last_election_time_ms = (end - start);
    gbl_total_election_time_ms += gbl_last_election_time_ms;
    gbl_election_count++;

    Pthread_mutex_lock(&(bdb_state->repinfo->elect_mutex));
    bdb_state->repinfo->in_election = 0;
    Pthread_mutex_unlock(&(bdb_state->repinfo->elect_mutex));

    bdb_thread_event(bdb_state, 0);

    return NULL;
}

static void call_for_election_int(bdb_state_type *bdb_state, int op)
{
    int rc;

    pthread_t elect_thr;
    elect_thread_args_type *elect_thread_args;

    if (bdb_state->exiting)
        return;

    if (bdb_state->repinfo->dont_elect_untill_time > comdb2_time_epoch()) {
        logmsg(LOGMSG_INFO, "Short-circuiting election until master is transferred.\n");
        return;
    }

    elect_thread_args = malloc(sizeof(elect_thread_args_type));
    bzero(elect_thread_args, sizeof(elect_thread_args_type));
    elect_thread_args->bdb_state = bdb_state;
    elect_thread_args->op = op;

    Pthread_mutex_lock(&(bdb_state->repinfo->elect_mutex));
    if (bdb_state->repinfo->in_election) {
        logmsg(LOGMSG_INFO, "call_for_election: already in election\n");
        Pthread_mutex_unlock(&(bdb_state->repinfo->elect_mutex));
        return;
    }
    Pthread_mutex_unlock(&(bdb_state->repinfo->elect_mutex));

    logmsg(LOGMSG_INFO, "call_for_election: creating elect thread\n");
    rc = pthread_create(&elect_thr, &(bdb_state->pthread_attr_detach),
                        elect_thread, (void *)elect_thread_args);
    if (rc) {
        logmsg(LOGMSG_ERROR, "call_for_election: can't create election thread: %d\n", rc);
        free(elect_thread_args);
    }
}

void call_for_election(bdb_state_type *bdb_state, const char *func, int line)
{
    logmsg(LOGMSG_USER, "%s line %d called for election\n", func, line);
    call_for_election_int(bdb_state, DONT_LOSE);
}

void call_for_election_and_lose(bdb_state_type *bdb_state, const char *func, int line)
{
    logmsg(LOGMSG_USER, "%s line %d called for election\n", func, line);
    call_for_election_int(bdb_state, LOSE);
}

/*
  i dont expect this to be called outside of here.  we call it in the
   watcher thread when we see the env has been invalidated from a
   replication rollback.
   (bdb_state->rep_handle_dead = 1)
*/
static void bdb_reopen(bdb_state_type *bdb_state, const char *func, int line)
{
    logmsg(LOGMSG_DEBUG, "bdb_reopen called by tid 0x%p\n", (void *)pthread_self());
    logmsg(LOGMSG_USER, "%s line %d called for election (bdb_reopen)\n", func,
           line);
    call_for_election_int(bdb_state, REOPEN_AND_LOSE);
}

static char *print_permslsn(DB_LSN lsn, char str[])
{
    int *lognum;
    int *seqnum;
    int *iptr;

    iptr = (int *)&lsn;
    lognum = iptr + 0;
    seqnum = iptr + 1;

    sprintf(str, "%d %d", *lognum, *seqnum);

    return str;
}

int gbl_write_dummy_trace;
static void *add_thread_int(bdb_state_type *bdb_state, int add_delay)
{
    if (add_delay)
        sleep(bdb_state->attr->new_master_dummy_add_delay);

    BDB_READLOCK("dummy_add_thread");

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost) {
        logmsg(LOGMSG_USER, "%s: not-adding: master-hode=%s myhost=%s\n",
               __func__, bdb_state->repinfo->master_host,
               bdb_state->repinfo->myhost);
        goto done;
    } else if (gbl_is_physical_replicant == 1) {
	logmsg(LOGMSG_USER, "physrep: %s:%d: Not inserting a dummy record\n", __func__, __LINE__);
        goto done;
    }

    if (gbl_write_dummy_trace) {
        logmsg(LOGMSG_USER,
               "%s: adding dummy record for master %s, host %s\n", __func__,
               bdb_state->repinfo->master_host, bdb_state->repinfo->myhost);
    }

    add_dummy(bdb_state);

done:
    BDB_RELLOCK();

    return NULL;
}

static void *dummy_add_thread_int(void *arg, int add_delay)
{
    bdb_state_type *bdb_state = arg;
    thread_started("dummy add");
    bdb_thread_event(bdb_state, 1);
    add_thread_int(bdb_state, add_delay);
    bdb_thread_event(bdb_state, 0);
    return NULL;
}

void *dummy_add_thread_nodelay(void *arg)
{
    dummy_add_thread_int(arg, 0 /* add_delay */);
    return NULL;
}

void *dummy_add_thread(void *arg)
{
    dummy_add_thread_int(arg, 1 /* add_delay */);
    return NULL;
}

/* Only allow one at a time */
void *rep_catchup_add_thread(void *arg)
{
    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
    static int rep_catchup_add_running = 0;
    Pthread_mutex_lock(&lk);
    if (rep_catchup_add_running) {
        Pthread_mutex_unlock(&lk);
        return NULL;
    }
    rep_catchup_add_running = 1;
    Pthread_mutex_unlock(&lk);
    bdb_state_type *bdb_state = arg;
    thread_started("rep_catchup_add");
    bdb_thread_event(bdb_state, 1);
    add_thread_int(bdb_state, 1);
    bdb_thread_event(bdb_state, 0);
    Pthread_mutex_lock(&lk);
    rep_catchup_add_running = 0;
    Pthread_mutex_unlock(&lk);
    return NULL;
}

/*
First is the net wire header:
typedef struct
{
   char fromhost[16];
   int fromport;
   int fromnode;
   char tohost[16];
   int toport;
   int tonode;
   int type;
} wire_header_type;
We care about WIRE_HEADER_USER_MSG: it should be 5.

Next will be the send-message header:
typedef struct net_send_message_header
{
    int usertype;
    int seqnum;
    int waitforack;
    int datalen;
} net_send_message_header;
We care about usertype == USER_TYPE_BERKDB_REP: it should be 1.

Next this:
   bufsz =
    sizeof(int) +      seqnum
    sizeof(int) +      recsz
    sizeof(int) +      reccrc
    rec->size +        recbuf
    sizeof(int) +      controlsz
    sizeof(int) +      controlcrc
    control->size +    controlbuf
    16;                some fluff
We care about the 'rectype': this will be the first word of the
recbuf (after the crc).  We want rectype = REP_LOG: it should be 7.

If all of this is good, then retrieve the LSN from the rep_control:
typedef struct __rep_control {
        u_int32_t	rep_version;
        u_int32_t	log_version;

        DB_LSN		lsn;
        u_int32_t	rectype;
        u_int32_t	gen;
        u_int32_t	flags;
} REP_CONTROL;
*/

int net_get_lsn_rectype(const void *buf, int buflen, DB_LSN *lsn, int *myrectype)
{
    int wire_header_type, usertype, recsize, rectype;
    uint8_t *p_buf;
    const uint8_t *p_buf_end;

    p_buf = (uint8_t *)buf;
    p_buf_end = p_buf + buflen;

    /* Skip net wire-header up to the 'type'.  16 + 4 + 4 + 16 + 4 + 4 */
    if (!(p_buf = buf_skip(48, p_buf, p_buf_end)))
        return -1;

    /* Wire_header_type isn't endianized until later */
    if (!(p_buf = (uint8_t *)buf_no_net_get(
              &(wire_header_type), sizeof(wire_header_type), p_buf, p_buf_end)))
        return -1;

    /* Check for WIRE_HEADER_USER_MSG */
    if (wire_header_type != 5)
        return -1;

    /* Usertype is next */
    if (!(p_buf = (uint8_t *)buf_get(&(usertype), sizeof(usertype), p_buf,
                                     p_buf_end)))
        return -1;

    /* Check for USER_TYPE_BERKDB_REP */
    if (usertype != 1)
        return -1;

    /* Skip seqnum, waitforack, datalen, & seqnum */
    if (!(p_buf = buf_skip(16, p_buf, p_buf_end)))
        return -1;

    if (!(p_buf = (uint8_t *)buf_get(&(recsize), sizeof(recsize), p_buf,
                                     p_buf_end)))
        return -1;

    /* 4 + recsize + 4 + 4 + 4 + 4*/
    if (!(p_buf = buf_skip(recsize + 20, p_buf, p_buf_end)))
        return -1;

    /* Get the file */
    if (!(p_buf = (uint8_t *)buf_get(&(lsn->file), sizeof(lsn->file), p_buf,
                                     p_buf_end)))
        return -1;

    /* Get the offset */
    if (!(p_buf = (uint8_t *)buf_get(&(lsn->offset), sizeof(lsn->offset), p_buf,
                                     p_buf_end)))
        return -1;

    /* Rectype */
    if (!(p_buf = (uint8_t *)buf_get(&(rectype), sizeof(rectype), p_buf,
                                     p_buf_end)))
        return -1;

    /* Copyout rectype */
    if (myrectype)
        *myrectype = rectype;

    return 0;
}

static int net_getlsn_rectype(netinfo_type *netinfo_ptr, void *record, int len,
                              int *file, int *offset, int *rectype)
{
    DB_LSN lsn;
    int myrectype;
    if ((net_get_lsn_rectype(record, len, &lsn, &myrectype)) != 0) return -1;
    if (file) *file = lsn.file;
    if (offset) *offset = lsn.offset;
    if (rectype) *rectype = myrectype;
    return 0;
}

int net_getlsn_rtn(netinfo_type *netinfo_ptr, void *record, int len, int *file,
                   int *offset)
{
    int rectype;
    if ((net_getlsn_rectype(netinfo_ptr, record, len, file, offset, &rectype) ==
         0) &&
        (rectype == 7)) {
        return 0;
    }
    return -1;
}

/* Given two outgoing net buffers, which one is lower */
int net_cmplsn_rtn(netinfo_type *netinfo_ptr, void *x, int xlen, void *y,
                   int ylen)
{
    int rc;
    DB_LSN xlsn, ylsn;

    /* Do not tolerate malformed buffers.  I am inserting x with the inorder
     * flag.  It has to be correct. */
    if ((rc = net_get_lsn_rectype(x, xlen, &xlsn, NULL)) != 0)
        abort();

    if ((rc = net_get_lsn_rectype(y, ylen, &ylsn, NULL)) != 0)
        return -1;

    return log_compare(&xlsn, &ylsn);
}

void net_newnode_rtn(netinfo_type *netinfo_ptr, char *hostname, int portnum)
{
    pthread_t tid;
    bdb_state_type *bdb_state;

    /* get a pointer back to our bdb_state */
    bdb_state = net_get_usrptr(netinfo_ptr);

    /* if we're the master, treat it as incoherent till proven wrong */
    if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost) {
        Pthread_mutex_lock(&(bdb_state->coherent_state_lock));

        set_coherent_state(bdb_state, hostname, STATE_INCOHERENT_WAIT, __func__,
                           __LINE__);
        Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));

        /* Colease thread will do this */
        if (!bdb_state->attr->coherency_lease) {
            pthread_create(&tid, &(bdb_state->pthread_attr_detach),
                           dummy_add_thread, bdb_state);
        }

        bdb_zap_lsn_waitlist(bdb_state, hostname);
    }
}

/* Timestamp of when our coherency lease expires on replicant */
static uint64_t coherency_timestamp = 0;
int gbl_dump_zero_coherency_timestamp;

char coherency_master[128] = {0};

/* Don't let anything commit on the master until after this */
static uint64_t coherency_commit_timestamp = 0;

uint64_t next_commit_timestamp(void)
{
    return coherency_commit_timestamp;
}

/* Make sure that nothing commits before the timestamp set here.
 * This is called when a node changes to from STATE_COHERENT to
 * any other state.  The coherent_state_lock will be held. */
static inline void defer_commits_int(bdb_state_type *bdb_state,
                                     const char *host, const char *func,
                                     int forupgrade)
{
    int colease = bdb_state->attr->coherency_lease;
    int defer = bdb_state->attr->additional_deferms;
    int upgrade = (forupgrade ? (2 * bdb_state->attr->master_lease) : 0);
    time_t cosec, coms;
    struct tm r;
    if (!colease)
        return;
    coherency_commit_timestamp =
        (gettimeofday_ms() + colease + upgrade + defer + 1);
    cosec = (coherency_commit_timestamp / 1000);
    coms = (coherency_commit_timestamp % 1000);
    localtime_r(&cosec, &r);
    logmsg(LOGMSG_INFO,
           "%s node %s deferred commits until %02d:%02d:%02d.%03ld\n", func,
           host ? host : "<all>", r.tm_hour, r.tm_min, r.tm_sec, coms);
}

static inline void defer_commits(bdb_state_type *bdb_state, const char *host,
                                 const char *func)
{
    defer_commits_int(bdb_state, host, func, 0);
}

void defer_commits_for_upgrade(bdb_state_type *bdb_state, const char *host,
                               const char *func)
{
    defer_commits_int(bdb_state, host, func, 1);
}

typedef struct {
    bdb_state_type *bdb_state;
    char *host;
} hostdown_type;

int gbl_reset_on_unelectable_cluster = 1;

void *hostdown_thread(void *arg)
{
    bdb_state_type *bdb_state;
    hostdown_type *hostdown_buf;
    char *host;
    char *master_host;

    thread_started("bdb hostdown");

    hostdown_buf = (hostdown_type *)arg;
    bdb_state = hostdown_buf->bdb_state;
    bdb_thread_event(bdb_state, 1);
    host = hostdown_buf->host;
    free(arg);

    if (bdb_state->callback->nodedown_rtn) {
        bdb_state->callback->nodedown_rtn(host);
    }

    /* XXX dont do this.  its nice to see what seqnum a disconnected
       node was up to - we can gauge how long it will take it to catch
       up */
    /* clear his seqnum */
    /*
    bzero(&(bdb_state->seqnum_info->seqnums[hostdown_buf->node]),
       sizeof(seqnum_type));
       */

    /*
      if we are the master, check to see if we are connected to less
      than half of the nodes.  if so, downgrade ourselves
    */

    BDB_READLOCK("hostdown_thread");

    master_host = bdb_state->repinfo->master_host;

    print(bdb_state, "master is %s we are %s\n", master_host,
          bdb_state->repinfo->myhost);

    if (gbl_reset_on_unelectable_cluster) {
        int num_up, num_connected, electable;

        print(bdb_state, "xxx master is %s we are %s\n", master_host,
              bdb_state->repinfo->myhost);
        electable = is_electable(bdb_state, &num_up, &num_connected);
        print(bdb_state, "connected to %d out of %d up nodes\n", num_connected,
              num_up);

        if (!electable) {

            /* dont bother holding an election,  WE JUST SAID CLUSTER IS
               UNELECTABLE */
            if (master_host == bdb_state->repinfo->myhost) {
                logmsg(LOGMSG_WARN, "cluster is unelectable, downgrading\n");
                bdb_downgrade_noelect(bdb_state);
            } else {
                logmsg(LOGMSG_WARN, "cluster is unelectable, reseting the master\n");
                bdb_state->repinfo->master_host = db_eid_invalid;
            }
        }
    }

/*
   see if the host that went down is marked down
   - if so, decomission it.  if it crashed, but isnt marked down
   yet, thats ok - we'll decom it later when we fail sending to
   it and see that he's marked down
*/

#ifdef DECOM_LOGIC
    if (bdb_state->callback->nodeup_rtn)
        if (!(bdb_state->callback->nodeup_rtn(bdb_state, node))) {
            fprintf(stderr, "decomissioning node %d\n", node);
            net_decom_node(bdb_state->repinfo->netinfo, node);
        }
#endif

    /* see if the down host was the master - if so, call for election */
    /*fprintf(stderr, "got HOSTDOWN for node %d\n", node);*/

    if (host == master_host) {
        if (!bdb_state->exiting) {
            logmsg(LOGMSG_WARN, "net_hostdown_rtn: HOSTDOWN was the master, "
                            "calling for election\n");

            call_for_election(bdb_state, __func__, __LINE__);
        }
    }

    BDB_RELLOCK();

    bdb_thread_event(bdb_state, 0);

    return NULL;
}

int net_hostdown_rtn(netinfo_type *netinfo_ptr, char *host)
{
    bdb_state_type *bdb_state;
    pthread_t tid;
    hostdown_type *hostdown_buf;
    int rc;
    char *master_host;

    /* get a pointer back to our bdb_state */
    bdb_state = net_get_usrptr(netinfo_ptr);

    if (bdb_state->exiting)
        return 0;

    /* I don't think you need the bdb lock here */
    /*BDB_READLOCK("hostdown_rtn");*/
    master_host = bdb_state->repinfo->master_host;

    print(bdb_state, "net_hostdown_rtn: called for %s\n", host);

    /* if we're the master */
    if (master_host == bdb_state->repinfo->myhost) {
        /* clobber his state blindly.  we have no lsn here, just keep the last
           one in place.  */
        Pthread_mutex_lock(&(bdb_state->coherent_state_lock));

        if (bdb_state->coherent_state[nodeix(host)] == STATE_COHERENT) {
            /*
             * We defer waits, making sure the coherency lease expires for
             * the disconnected replicant;  the node needs to be incoherent,
             * no need to wait for it.
             * Once the node reconnects, master will switch the state
             * to STATE_INCOHERENT_WAIT, and master will wait for it again;
             * replicant will run recovery to catch up
             */
            defer_commits(bdb_state, host, __func__);
            set_coherent_state(bdb_state, host, STATE_INCOHERENT, __func__,
                               __LINE__);
        }

        /* hostdown can defer commits */
        bdb_state->last_downgrade_time[nodeix(host)] = gettimeofday_ms();
        Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));
        trigger_unregister_node(host);
    } 
    /*BDB_RELLOCK();*/

    if (host == master_host) {
        logmsg(LOGMSG_WARN, "net_hostdown_rtn: HOSTDOWN was the master, calling "
                        "for election\n");

        /* this is replicant, we are running election followed by recovery */

        call_for_election(bdb_state, __func__, __LINE__);
    }

    /* wake up anyone who might be waiting for a seqnum so that
     * they can stop waiting from this node - it ain't gonna happen! */

    Pthread_mutex_lock(&bdb_state->pending_broadcast_lock);
    bdb_state->pending_seqnum_broadcast = 1;
    Pthread_mutex_unlock(&bdb_state->pending_broadcast_lock);

    hostdown_buf = malloc(sizeof(hostdown_type));
    hostdown_buf->bdb_state = bdb_state;
    hostdown_buf->host = host;

    pthread_attr_t attr;
    Pthread_attr_init(&attr);
    Pthread_attr_setstacksize(&attr, 128 * 1024);

    rc = pthread_create(&tid, &attr, hostdown_thread, hostdown_buf);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "%s: pthread_create hostdown_thread: %d %s\n", __func__,
                rc, strerror(rc));
        exit(1);
    }
    rc = pthread_detach(tid);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "%s: pthread_detach hostdown_thread: %d %s\n", __func__,
                rc, strerror(rc));
        exit(1);
    }
    Pthread_attr_destroy(&attr);
    return 0;
}

void bdb_all_incoherent(bdb_state_type *bdb_state)
{
    int i;
    if (gbl_set_coherent_state_trace) {
        logmsg(LOGMSG_USER, "%s line %d setting all nodes to INCOHERENT_WAIT\n",
               __func__, __LINE__);
    }
    Pthread_mutex_lock(&(bdb_state->coherent_state_lock));
    for (i = 0; i < MAXNODES; i++) {
        bdb_state->coherent_state[i] = STATE_INCOHERENT_WAIT;
    }

    set_coherent_state(bdb_state, bdb_state->repinfo->myhost, STATE_COHERENT,
                       __func__, __LINE__);

    Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));
}

int bdb_get_num_notcoherent(bdb_state_type *bdb_state)
{
    int count;
    const char *hostlist[REPMAX];
    int num_skipped;
    int i;

    count = net_get_all_nodes_connected(bdb_state->repinfo->netinfo, hostlist);

    num_skipped = 0;

    for (i = 0; i < count; i++)
        if (is_incoherent(bdb_state, hostlist[i]))
            num_skipped++;

    return num_skipped;
}

/* NO LOCK.  this is only intended to be used for the incoherent report */
/*
 * Get the list of incoherent peers
 * nodes_list should point to an array of [max_nodes] ints to be populated
 * with node numbers.
 * *num_notcoherent will be set to the number of incoherent nodes (which
 * coul be more than max_nodes)
 * *since_epoch will be set to the epoch time at which nodes first became
 * incoherent.
 */
void bdb_get_notcoherent_list(bdb_state_type *bdb_state,
                              const char *nodes_list[REPMAX], size_t max_nodes,
                              int *num_notcoherent, int *since_epoch)
{
    int count;
    const char *hostlist[REPMAX];
    int i;

    count = net_get_all_nodes_connected(bdb_state->repinfo->netinfo, hostlist);

    *num_notcoherent = 0;
    for (i = 0; i < count; i++) {
        if (is_incoherent(bdb_state, hostlist[i])) {
            if ((*num_notcoherent) < max_nodes)
                nodes_list[*num_notcoherent] = hostlist[i];
            (*num_notcoherent)++;
        }
        *since_epoch = bdb_state->repinfo->skipsinceepoch;
    }
}

void bdb_disable_replication_time_tracking(bdb_state_type *bdb_state)
{
    Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));

    for (int node = 0; node < MAXNODES; node++) {
        if (bdb_state->seqnum_info->waitlist[node]) {
            struct wait_for_lsn *waitforlsn;
            waitforlsn = listc_rtl(bdb_state->seqnum_info->waitlist[node]);
            while (waitforlsn) {
                pool_relablk(bdb_state->seqnum_info->trackpool, waitforlsn);
                waitforlsn = listc_rtl(bdb_state->seqnum_info->waitlist[node]);
            }

            if (bdb_state->seqnum_info->time_10seconds) {
                averager_purge_old(bdb_state->seqnum_info->time_10seconds[node],
                                   INT_MAX);
                averager_purge_old(bdb_state->seqnum_info->time_minute[node],
                                   INT_MAX);
            }
        }
    }

    Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
}

/* when packet is udp, increase counter
 * when it is tcp, do work to find udp loss rate
 */
inline static void update_node_acks(bdb_state_type *bdb_state, char *host,
                                    int is_tcp)
{
    int node_ix = nodeix(host);
    if (is_tcp == 0) {
        bdb_state->seqnum_info->incomming_udp_count[node_ix]++;
        return;
    }

    if (++bdb_state->seqnum_info->udp_average_counter[node_ix] <
        bdb_state->attr->udp_average_over_epochs) {
        return;
    }

    float delta = (bdb_state->seqnum_info->expected_udp_count[node_ix] -
                   bdb_state->seqnum_info->incomming_udp_count[node_ix]);
    float rate =
        100 * delta / bdb_state->seqnum_info->expected_udp_count[node_ix];

    if (bdb_state->seqnum_info->expected_udp_count[node_ix] > 1 &&
        delta > bdb_state->attr->udp_drop_delta_threshold &&
        rate > bdb_state->attr->udp_drop_warn_percent) {
        logmsg(LOGMSG_USER,
               "update_node_acks: host %s, expected_udp_count = %d, delta = "
               "%.1f, loss = %f percent\n",
               host, bdb_state->seqnum_info->expected_udp_count[node_ix], delta,
               rate);
    }
    bdb_state->seqnum_info->incomming_udp_count[node_ix] = 0;
    bdb_state->seqnum_info->expected_udp_count[node_ix] = 0;
    bdb_state->seqnum_info->udp_average_counter[node_ix] = 0;
}

static int lsncmp(const void *lsn1, const void *lsn2)
{
    return (log_compare((DB_LSN *)lsn1, (DB_LSN *)lsn2));
}

extern int bdb_latest_commit(bdb_state_type *bdb_state, DB_LSN *latest_lsn,
                             uint32_t *latest_gen);

uint32_t bdb_get_rep_gen(bdb_state_type *bdb_state)
{
    uint32_t mygen;
    bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &mygen);
    return mygen;
}

int bdb_recoverlk_blocked(bdb_state_type *bdb_state)
{
    return bdb_state->dbenv->wrlock_recovery_blocked(bdb_state->dbenv);
}

void send_newmaster(bdb_state_type *bdb_state, int online)
{
    bdb_state->dbenv->rep_start(bdb_state->dbenv, NULL, 0, DB_REP_MASTER);
    /* Online recovery can wait-for-seqnum */
    bdb_add_dummy_llmeta_wait(online);
}

/* Called by the master to periodically broadcast the durable lsn.  The
 * algorithm: sort lsns of all nodes (including master's).  The durable lsn will
 * be in the (n/2)th spot.  We can only make claims about durability for things
 * in our own generation.  Discard everything else. 
 * NOTE: this will sometimes give a lsn which is less than the actual durable
 * lsn, but it will never return a value which is greater.
 */
static void calculate_durable_lsn(bdb_state_type *bdb_state, DB_LSN *dlsn,
                                  uint32_t *gen, uint32_t flags)
{
    extern int gbl_durable_calc_trace;
    const char *nodelist[REPMAX];
    DB_LSN nodelsns[REPMAX];
    uint32_t nodegens[REPMAX], mygen;
    int nodecount, index = 0, j, selix;

    bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &mygen);
    bdb_latest_commit(bdb_state, &nodelsns[index], &nodegens[index]);

    /* It's only durable if i have written it */
    if (nodegens[index] == mygen)
        index++;

    // This won't include the master
    nodecount =
        net_get_all_commissioned_nodes(bdb_state->repinfo->netinfo, nodelist);
    if (!nodecount) {
        (*dlsn) = nodelsns[0];
        (*gen) = nodegens[0];
        if ((*dlsn).file == 0) {
            logmsg(LOGMSG_FATAL, "%s line %d: aborting on insane durable lsn\n", 
                    __func__, __LINE__);
            abort();
        }
        return;
    }

    Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
    for (j = 0; j < nodecount; j++) {
        int node_ix = nodeix(nodelist[j]);
        memcpy(&nodelsns[index], &bdb_state->seqnum_info->seqnums[node_ix].lsn,
               sizeof(DB_LSN));
        /* Consider only generation matches that aren't in catch-up mode */
        if ((mygen ==
             (nodegens[index] =
                  bdb_state->seqnum_info->seqnums[node_ix].generation)) &&
            (bdb_state->seqnum_info->seqnums[node_ix].lsn.file != 2147483647)) {
            index++;
        }
    }
    Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));

    qsort(nodelsns, index, sizeof(DB_LSN), lsncmp);

    /* If there is an odd number of nodes, you want the middle element (so
     * index 2 if there are 5).  If there an even number, you want the leftmost
     * (smaller) of the two middle elements (so index 1 of an array of 4).
     * We can speak about the durability of the current generation, so
     * downshift the index by the difference of those seqnums which are from a
     * different generation.  */

    /* Nodecount + 1 is total nodes.  nodecount doesn't include the master, so
     * it effectively subtracts one from the total nodes. */
    selix = nodecount / 2;

    /* Shift for nodes which were reporting a different generation & didn't make
     * it into this array. */
    selix -= ((nodecount + 1) - index);
    if (selix < 0)
        selix = 0;

    if (gbl_durable_calc_trace || flags) {
        logmsg(LOGMSG_USER, "%s: ", __func__);
        if (!index) {
            logmsg(LOGMSG_USER, "(No nodes found)");
        }
        for (j = 0; j < index; j++) {
            if (j == selix)
                logmsg(LOGMSG_USER, "*");
            logmsg(LOGMSG_USER, "[%d][%d]", nodelsns[j].file, nodelsns[j].offset);
            if (j == selix)
                logmsg(LOGMSG_USER, "*");
            logmsg(LOGMSG_USER, " ");
        }
        logmsg(LOGMSG_USER, "\n");
    }

    /* Just return the last durable generation */
    if (!index) {
        bdb_state->dbenv->get_durable_lsn(bdb_state->dbenv, dlsn, gen);
    } else {
        (*dlsn) = nodelsns[selix];
        (*gen) = nodegens[selix];
    }
}

int verify_master_leases_int(bdb_state_type *bdb_state, const char **comlist,
                             int comcount, const char *func, uint32_t line)
{
    int total_nodes = (comcount + 1), current_leases = 1, i;
    int verify_trace = bdb_state->attr->verify_master_lease_trace;
    static time_t lastpr = 0;
    static int last_rc = 0;
    time_t now = 0;
    uint64_t ctime = gettimeofday_ms();
    static uint64_t bad_count = 0;

    if (!bdb_state->attr->enable_seqnum_generations) {
        if (verify_trace && (now = time(NULL)) != lastpr) {
            logmsg(LOGMSG_USER,
                   "%s master-lease return good because "
                   "seqnum-generations is disabled\n",
                   __func__);
            lastpr = now;
        }
        return 1;
    }

    Pthread_mutex_lock(&(bdb_state->master_lease_lk));
    for (i = 0; i < comcount; i++) {
        if (ctime < bdb_state->master_lease[nodeix(comlist[i])])
            current_leases++;
    }
    Pthread_mutex_unlock(&(bdb_state->master_lease_lk));

    if (current_leases >= ((total_nodes / 2) + 1)) {
        if (verify_trace && (last_rc == 0 || (now = time(NULL)) != lastpr)) {
            logmsg(LOGMSG_USER,
                   "%s line %d verify_master_leases SUCCEEDED: we have %u "
                   "current leases from %u nodes epoch=%ld\n",
                   func, line, current_leases, total_nodes, time(NULL));
            lastpr = now;
        }

        last_rc = 1;
        return 1;
    }

    bad_count++;

    if (verify_trace && (last_rc == 1 || (now = time(NULL)) != lastpr)) {
        logmsg(LOGMSG_USER,
               "%s line %d verify_master_leases failed: we have %u "
               "current leases from %u nodes epoch=%ld\n",
               func, line, current_leases, total_nodes, time(NULL));
        lastpr = now;
    }

    last_rc = 0;

    return 0;
}

int verify_master_leases(bdb_state_type *bdb_state, const char *func,
                         uint32_t line)
{
    const char *comlist[REPMAX];
    int comcount;

    comcount =
        net_get_all_commissioned_nodes(bdb_state->repinfo->netinfo, comlist);
    return verify_master_leases_int(bdb_state, comlist, comcount, func, line);
}

int gbl_catchup_window_trace = 0;
extern int gbl_set_seqnum_trace;

static inline int should_copy_seqnum(bdb_state_type *bdb_state, seqnum_type *seqnum,
                              seqnum_type *last_seqnum)
{
    int trace = bdb_state->attr->wait_for_seqnum_trace, now;
    static int lastpr = 0;

    if (seqnum->generation > last_seqnum->generation) {
        return 1;
    }

    if (bdb_state->attr->enable_seqnum_generations &&
        seqnum->generation < last_seqnum->generation) {
        if (trace && (now = time(NULL)) > lastpr) {
            logmsg(LOGMSG_USER,
                   "seqnum-generation %d < last_generation %d, not"
                   " copying\n",
                   seqnum->generation, last_seqnum->generation);
            lastpr = now;
        }
        return 0;
    }

    if (last_seqnum->lsn.file == INT_MAX) {
        return 1;
    }

    if (log_compare(&last_seqnum->lsn, &seqnum->lsn) > 0) {
        if (trace && (now = time(NULL)) > lastpr) {
            logmsg(LOGMSG_USER,
                   "seqnum-lsn [%d][%d] < last_lsn [%d][%d], not "
                   "copying\n",
                   seqnum->lsn.file, seqnum->lsn.offset, last_seqnum->lsn.file,
                   last_seqnum->lsn.offset);
            lastpr = now;
        }
        return 0;
    }

    return 1;
}

static void got_new_seqnum_from_node(bdb_state_type *bdb_state,
                                     seqnum_type *seqnum, char *host,
                                     uint8_t is_tcp)
{
    char str[100];
    int catchup_window = bdb_state->attr->catchup_window;
    uint32_t mygen;
    int downgrade_penalty = bdb_state->attr->downgrade_penalty;
    int change_coherency;
    seqnum_type zero_seq;
    DB_LSN *masterlsn;
    int rc;
    unsigned long long cntbytes;
    struct waiting_for_lsn *waitforlsn = NULL;
    int now;
    int track_times;
    int node_ix = nodeix(host);
    int seqnum_trace = bdb_state->attr->wait_for_seqnum_trace;

    track_times = bdb_state->attr->track_replication_times;

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &mygen);

    /* if the generation number here is not equal to ours, ignore it .. if we
     * are master, we should downgrade ..*/
    if (bdb_state->attr->enable_seqnum_generations) {
        uint64_t issue_time, base_ts, lease_time;

        // maybe we're getting alot of these?
        if (seqnum->generation < mygen) {
            static time_t lastpr = 0;
            time_t now;
            static unsigned long long count = 0;
            count++;

            if (seqnum_trace && (now = time(NULL)) > lastpr) {
                logmsg(LOGMSG_USER,
                       "%s: rejecting seqnum from %s because gen is "
                       "%u (low), i want %u, count=%llu\n",
                       __func__, host, seqnum->generation, mygen, count);
                lastpr = now;
            }
            return;
        }

        if (seqnum->generation > mygen) {
            static unsigned long long count = 0;
            static time_t lastpr = 0;
            count++;

            if (seqnum_trace && (now = time(NULL)) > lastpr) {
                logmsg(LOGMSG_USER,
                       "%s: rejecting seqnum from %s because gen is "
                       "%u (high), i want %u, count=%llu\n",
                       __func__, host, seqnum->generation, mygen, count);
                lastpr = now;
            }

            if (gbl_is_physical_replicant == 1) {
                logmsg(LOGMSG_USER,
                       "%s: got seqnum from %s of gen %u (high), i want %u, count=%llu\n",
                       __func__, host, seqnum->generation, mygen, count);
            } else
            if (bdb_state->attr->downgrade_on_seqnum_gen_mismatch &&
                bdb_state->repinfo->master_host == bdb_state->repinfo->myhost)
                call_for_election(bdb_state, __func__, __LINE__);
            return;
        }

        memcpy(&issue_time, seqnum->issue_time, sizeof(issue_time));
        if (bdb_state->attr->master_lease &&
            bdb_state->repinfo->master_host == bdb_state->repinfo->myhost &&
            issue_time) {
            static time_t lastpr = 0;
            time_t now;

            if ((base_ts = gettimeofday_ms()) < issue_time)
                lease_time = base_ts + seqnum->lease_ms;
            else
                lease_time = issue_time + seqnum->lease_ms;
            Pthread_mutex_lock(&(bdb_state->master_lease_lk));
            bdb_state->master_lease[node_ix] = lease_time;
            Pthread_mutex_unlock(&(bdb_state->master_lease_lk));

            if (bdb_state->attr->master_lease_set_trace && (now = time(NULL)) > lastpr)
            {
                logmsg(LOGMSG_USER,
                       "%s: setting lease time for %s to %" PRIu64 ", current "
                       "time is %" PRIu64 " issue time is %" PRIu64
                       " epoch is %ld\n",
                       __func__, host, lease_time, base_ts, issue_time,
                       time(NULL));
                lastpr = now;
            }
        }

        change_coherency = 1;
        int elect_highest = 0;
        char *unused;

        rc = bdb_berkdb_get_attr(bdb_state, "elect_highest_committed_gen",
                                 &unused, &elect_highest);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "Error getting elect_highest_committed_gen, "
                   "%d\n",
                   rc);
        } else if (elect_highest > 0) {
            change_coherency = (seqnum->commit_generation == mygen);
        }
    } else
        change_coherency = 1;

    if (gbl_is_physical_replicant == 1) {
        change_coherency = 1;
    }

    /* if a node is incoherent_slow and we haven't seen any packets for a while,
     * make it plain old incoherent.  Need to
     * give nodes a chance to dig themselves out if there's no activity (eg: if
     * they went incoherent because of a
     * read spike, but there's nomore reads or writes). */
    if (change_coherency &&
        bdb_state->coherent_state[node_ix] == STATE_INCOHERENT_SLOW) {
        Pthread_mutex_lock(&slow_node_check_lk);
        if ((comdb2_time_epochms() - last_slow_node_check_time) >
                bdb_state->attr->slowrep_inactive_timeout &&
            bdb_state->coherent_state[node_ix] == STATE_INCOHERENT_SLOW) {
            Pthread_mutex_lock(&bdb_state->coherent_state_lock);
            if (bdb_state->coherent_state[node_ix] == STATE_INCOHERENT_SLOW) {
                set_coherent_state(bdb_state, host, STATE_INCOHERENT, __func__,
                                   __LINE__);
            }
            Pthread_mutex_unlock(&bdb_state->coherent_state_lock);
            last_slow_node_check_time = comdb2_time_epochms();
        }
        Pthread_mutex_unlock(&slow_node_check_lk);
    }

    bzero(&zero_seq, sizeof(seqnum_type));

    /* make a note of the first time we see a seqnum for a node */
    if (memcmp(&(bdb_state->seqnum_info->seqnums[node_ix]), &zero_seq,
               sizeof(seqnum_type)) == 0) {
        logmsg(LOGMSG_INFO, "got first seqnum from host %s: <%s>\n", host,
               lsn_to_str(str, &(seqnum->lsn)));
    }

    if (track_times)
        now = comdb2_time_epochms();

    /* save the seqnum that we recived */
    Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));

    /* Completely possible .. it just means that the durable lsn will trail a
     * bit */
    if (bdb_state->attr->wait_for_seqnum_trace &&
        log_compare(&bdb_state->seqnum_info->seqnums[node_ix].lsn,
                    &seqnum->lsn) > 0) {
        logmsg(LOGMSG_USER,
               "%s seqnum from %s moving backwards from [%d][%d] gen %d to "
               "[%d][%d] gen %d\n",
               __func__, host,
               bdb_state->seqnum_info->seqnums[node_ix].lsn.file,
               bdb_state->seqnum_info->seqnums[node_ix].lsn.offset,
               bdb_state->seqnum_info->seqnums[node_ix].generation,
               seqnum->lsn.file, seqnum->lsn.offset, seqnum->generation);
    } else if (bdb_state->attr->wait_for_seqnum_trace) {
        logmsg(LOGMSG_USER,
               "%s seqnum from %s moving from [%d][%d] gen %d to "
               "[%d][%d] gen %d commit_gen %d mygen %d change_coherency %d\n",
               __func__, host,
               bdb_state->seqnum_info->seqnums[node_ix].lsn.file,
               bdb_state->seqnum_info->seqnums[node_ix].lsn.offset,
               bdb_state->seqnum_info->seqnums[node_ix].generation,
               seqnum->lsn.file, seqnum->lsn.offset, seqnum->generation,
               seqnum->commit_generation, mygen, change_coherency);
    }

    if (should_copy_seqnum(bdb_state, seqnum,
                &bdb_state->seqnum_info->seqnums[node_ix])) {
        memcpy(&(bdb_state->seqnum_info->seqnums[node_ix]), seqnum,
               sizeof(seqnum_type));
    }

    if (gbl_set_seqnum_trace) {
        logmsg(LOGMSG_USER, "%s line %d set %s seqnum to %d:%d\n", __func__,
               __LINE__, host, seqnum->lsn.file, seqnum->lsn.offset);
    }

    if (change_coherency && track_times) {
        if (bdb_state->seqnum_info->time_10seconds[node_ix] == NULL) {
            if (bdb_state->seqnum_info->waitlist[node_ix] == NULL) {
                bdb_state->seqnum_info->waitlist[node_ix] =
                    malloc(sizeof(wait_for_lsn_list));
                listc_init(bdb_state->seqnum_info->waitlist[node_ix],
                           offsetof(struct waiting_for_lsn, lnk));
            }

            bdb_state->seqnum_info->time_10seconds[node_ix] =
                averager_new(10000, 100000);
            bdb_state->seqnum_info->time_minute[node_ix] =
                averager_new(60000, 100000);
        }
        waitforlsn =
            (struct waiting_for_lsn *)bdb_state->seqnum_info->waitlist[node_ix]
                ->top;
        while (waitforlsn) {
            if (log_compare(&seqnum->lsn, &waitforlsn->lsn) >= 0) {
                struct waiting_for_lsn *next;
                int diff;

                diff = now - waitforlsn->start;
                next = waitforlsn->lnk.next;

                listc_rfl(bdb_state->seqnum_info->waitlist[node_ix],
                          waitforlsn);

                averager_add(bdb_state->seqnum_info->time_10seconds[node_ix],
                             diff, now);
                averager_add(bdb_state->seqnum_info->time_minute[node_ix], diff,
                             now);

                pool_relablk(bdb_state->seqnum_info->trackpool, waitforlsn);
                waitforlsn = next;
            } else
                waitforlsn = waitforlsn->lnk.next;
        }
    }

    if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost)
        update_node_acks(bdb_state, host, is_tcp);

    Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));

    if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost) {
        /* we're done here if we're not the master */
        return;
    }

    /* if this was a node in startup advertising INT_MAX, we're done here */
    if (seqnum->lsn.file == INT_MAX)
        return;

    /* wake up anyone who might be waiting to see this seqnum */
    Pthread_cond_broadcast(&(bdb_state->seqnum_info->cond));

    /* new LSN from node: we may need to make the node coherent */
    Pthread_mutex_lock(&(bdb_state->coherent_state_lock));

    if (change_coherency) {
        if (bdb_state->coherent_state[node_ix] == STATE_INCOHERENT ||
            bdb_state->coherent_state[node_ix] == STATE_INCOHERENT_WAIT) {
            if (bdb_state->callback->nodeup_rtn) {
                if ((bdb_state->callback->nodeup_rtn(bdb_state, host))) {
                    rc = bdb_wait_for_seqnum_from_node_nowait_int(
                        bdb_state, &(bdb_state->seqnum_info->seqnums[nodeix(
                                       bdb_state->repinfo->master_host)]),
                        host);
                    if (rc == 0) {
                        /* prevent a node from becoming coherent for at least
                         * downgrade_penalty seconds after an event that would
                         * delay commits (the last downgrade) */
                        if (downgrade_penalty &&
                            (gettimeofday_ms() -
                             bdb_state->last_downgrade_time[node_ix]) <=
                                downgrade_penalty) {
                            set_coherent_state(bdb_state, host,
                                               STATE_INCOHERENT_WAIT, __func__,
                                               __LINE__);
                        } else {
                            /* dont send here under lock */
                            set_coherent_state(bdb_state, host, STATE_COHERENT,
                                               __func__, __LINE__);
                            uint32_t gen;
                            bdb_state->dbenv->get_rep_gen(bdb_state->dbenv,
                                                          &gen);
                            ctrace("%s %d setting node %s to COHERENT lsn "
                                   "[%d][%u] seqnum-generation=%u mygen=%d\n",
                                   __FILE__, __LINE__, host, seqnum->lsn.file,
                                   seqnum->lsn.offset, seqnum->generation, gen);
                            logmsg(LOGMSG_INFO, "host %s became coherent, lsn "
                                            "[%d][%d] gen=%d mygen=%d master "
                                            "is %s removing skip\n",
                                    host, seqnum->lsn.file,
                                    seqnum->lsn.offset, seqnum->generation, gen,
                                    bdb_state->repinfo->master_host);

                            bdb_zap_lsn_waitlist(bdb_state, host);
                        }
                    }

                    /* INCOHERENT_WAIT if this node is within the catchup_window
                     */
                    if (catchup_window && bdb_state->coherent_state[node_ix] ==
                                              STATE_INCOHERENT) {
                        masterlsn = &(bdb_state->seqnum_info
                                          ->seqnums[nodeix(
                                              bdb_state->repinfo->master_host)]
                                          .lsn);
                        cntbytes =
                            subtract_lsn(bdb_state, masterlsn, &seqnum->lsn);
                        if (cntbytes < catchup_window) {
                            set_coherent_state(bdb_state, host,
                                               STATE_INCOHERENT_WAIT, __func__,
                                               __LINE__);
                        }
                    }
                }
            }
        }
    }

    Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));
}

/* returns -999 on timeout */
static int bdb_wait_for_seqnum_from_node_nowait_int(bdb_state_type *bdb_state,
                                                    seqnum_type *master_seqnum,
                                                    char *host)
{
    seqnum_type *host_seqnum;
    Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
    host_seqnum = &bdb_state->seqnum_info->seqnums[nodeix(host)];

    /*fprintf(stderr, "calling bdb_seqnum_compare\n");*/
    if (bdb_seqnum_compare(bdb_state, host_seqnum, master_seqnum) >= 0) {
        /*fprintf(stderr, "compared >=, returning\n");*/
        if (gbl_set_coherent_state_trace) {
            logmsg(LOGMSG_USER,
                   "%s line %d returning COHERENT for %s, "
                   "master_seqnum=%d:%d generation %d ptr %p, incoming "
                   "seqnum=%d:%d generation %d\n",
                   __func__, __LINE__, host, master_seqnum->lsn.file,
                   master_seqnum->lsn.offset, master_seqnum->generation,
                   master_seqnum, host_seqnum->lsn.file,
                   host_seqnum->lsn.offset, host_seqnum->generation);
        }
        Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
        return 0;
    }
    Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
    return -999;
}

static void bdb_zap_lsn_waitlist(bdb_state_type *bdb_state, const char *host) {
    if (bdb_state == NULL)
        return;

    /* clear statistics */
    Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
    int node_ix = nodeix(host);
    if (bdb_state->seqnum_info->time_minute[node_ix])
        averager_clear(bdb_state->seqnum_info->time_minute[node_ix]);
    if (bdb_state->seqnum_info->time_10seconds[node_ix])
        averager_clear(bdb_state->seqnum_info->time_10seconds[node_ix]);

    /* clear any lsns we were waiting for */
    struct waiting_for_lsn *waitforlsn;
    if (bdb_state->seqnum_info->waitlist[node_ix]) {
        waitforlsn = (struct waiting_for_lsn *)listc_rtl(
            bdb_state->seqnum_info->waitlist[node_ix]);
        while (waitforlsn) {
            pool_relablk(bdb_state->seqnum_info->trackpool, waitforlsn);
            waitforlsn = (struct waiting_for_lsn *)listc_rtl(
                bdb_state->seqnum_info->waitlist[node_ix]);
        }
    }

    Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
}

static void bdb_slow_replicant_check(bdb_state_type *bdb_state,
                                     seqnum_type *seqnum)
{
    double *proctime;
    const char *worst_node = NULL, *second_worst_node = NULL;
    int numnodes;
    const char *hosts[REPMAX];
    int state;
    int print_message;
    int made_incoherent_slow = 0;

    /* this used to be allocated on stack, but that can overflow if called from
     * the appsock thread */
    proctime = malloc(sizeof(double) * MAXNODES);
    if (proctime == NULL)
        return;

    proctime[0] = 0;

    numnodes =
        net_get_all_commissioned_nodes(bdb_state->repinfo->netinfo, hosts);
 
    if (numnodes < 2) {
        free(proctime);
        return;
    }

    Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
    double worst_time = 0;
    /* find the slowest and second slowest nodes */
    for (int i = 0; i < numnodes; i++) {
        const char *host = hosts[i];
        int node_ix = nodeix(host);

        if (bdb_state->seqnum_info->time_minute[node_ix])
            proctime[node_ix] =
                averager_avg(bdb_state->seqnum_info->time_minute[node_ix]);
        else
            proctime[node_ix] = 0;

        /* We're just checking, not checking & setting */
        state = bdb_state->coherent_state[node_ix];

        if (state != STATE_COHERENT)
            continue;

        if (proctime[node_ix] > worst_time) {
            worst_time = proctime[node_ix];
            worst_node = host;
        }
    }
    double second_worst_time = 0;
    for (int i = 0; i < numnodes; i++) {
        const char *host = hosts[i];
        int node_ix = nodeix(host);
        state = bdb_state->coherent_state[node_ix];
        if (state != STATE_COHERENT)
            continue;

        if (proctime[node_ix] > second_worst_time && host != worst_node) {
            second_worst_node = host;
            second_worst_time = proctime[node_ix];
        }
    }
    Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));

#if 0
    printf("bdb_slow_replicant_check worst is %d at %.2fms, second worst is %d at %.2fms, going to start marking incoherent at %.2fs\n",
            worst_node, proctime[worst_node], second_worst_node, proctime[second_worst_node], 
            proctime[second_worst_node] * bdb_state->attr->slowrep_incoherent_factor + bdb_state->attr->slowrep_incoherent_mintime);
#endif
    print_message = 0;
    if (worst_node && second_worst_node && worst_node != second_worst_node) {
        /* weigh time, to account for inter-datacenter delays */
        Pthread_mutex_lock(&(bdb_state->coherent_state_lock));
        int worst_node_ix = nodeix(worst_node);
        state = bdb_state->coherent_state[worst_node_ix];

        if (state == STATE_COHERENT &&
                    worst_time >
                    (second_worst_time *
                    bdb_state->attr->slowrep_incoherent_factor +
                    bdb_state->attr->slowrep_incoherent_mintime)) {
            /* if a node is worse then twice slower than other nodes, mark it
             * incoherent */
            if (bdb_state->attr->warn_slow_replicants ||
                    bdb_state->attr->make_slow_replicants_incoherent) {
                print_message = 1;
                if (bdb_state->attr->make_slow_replicants_incoherent) {
                    if (bdb_state->coherent_state[worst_node_ix] ==
                        STATE_COHERENT)
                        defer_commits(bdb_state, worst_node, __func__);
                    set_coherent_state(bdb_state, worst_node, STATE_INCOHERENT_SLOW,
                            __func__, __LINE__);
                    bdb_state->last_downgrade_time[worst_node_ix] =
                        gettimeofday_ms();
                    made_incoherent_slow = 1;
                }
            }
        }
        Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));
    }

    if (print_message) {
        logmsg(LOGMSG_USER, "replication time for %s (%.2fms) is much worse than "
                        "second-worst node %s (%.2fms)\n",
                worst_node, worst_time, second_worst_node,
                second_worst_time);
    }

    /* TODO: if we ever disable make_slow_replicants_incoherent and have
     * replicants in this state, make them incoherent immediately */
    print_message = 0;
    if (!made_incoherent_slow && worst_node) {
        /* If any nodes were incoherent_slow, and are now within normal bounds,
         * make them "classically" incoherent.  They get to
         * become coherent the same way as everyone else - by announcing that
         * they are up to the master's LSN. */
        for (int i = 0; i < numnodes; i++) {
            const char *host = hosts[i];
            int node_ix = nodeix(host);
            if (proctime[node_ix] == 0)
                continue;
            if (bdb_state->coherent_state[node_ix] != STATE_INCOHERENT_SLOW)
                continue;
            Pthread_mutex_lock(&(bdb_state->coherent_state_lock));
            if (bdb_state->coherent_state[node_ix] == STATE_INCOHERENT_SLOW &&
                (proctime[node_ix] <
                 (worst_time * bdb_state->attr->slowrep_incoherent_factor +
                  bdb_state->attr->slowrep_incoherent_mintime))) {
                print_message = 1;
                set_coherent_state(bdb_state, host, STATE_INCOHERENT, __func__,
                                   __LINE__);
            }
            Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));
            if (print_message)
                logmsg(LOGMSG_USER,
                       "replication time for %s (%.2fms) is within "
                       "bounds of second-worst node %s (%.2fms)\n",
                       host, proctime[node_ix], worst_node, worst_time);
        }
    }
    free(proctime);
}

/* expects seqnum_info lock held */
static int bdb_track_replication_time(bdb_state_type *bdb_state,
                                      seqnum_type *seqnum, const char *host)
{
    if (!bdb_state->attr->track_replication_times)
        return 0;

    struct waiting_for_lsn *waitforlsn;
    int node_ix = nodeix(host);
    if (bdb_state->seqnum_info->waitlist[node_ix] == NULL) {
        bdb_state->seqnum_info->waitlist[node_ix] =
            malloc(sizeof(wait_for_lsn_list));
        listc_init(bdb_state->seqnum_info->waitlist[node_ix],
                   offsetof(struct waiting_for_lsn, lnk));
    }

    if (bdb_state->seqnum_info->waitlist[node_ix]->count <
        bdb_state->attr->track_replication_times_max_lsns) {
        waitforlsn = pool_getablk(bdb_state->seqnum_info->trackpool);
        waitforlsn->lsn = seqnum->lsn;
        waitforlsn->start = comdb2_time_epochms();
        /* printf("waiting for %s\n", lsn_to_str(str, &seqnum->lsn)); */
        listc_abl(bdb_state->seqnum_info->waitlist[node_ix], waitforlsn);
    }

    return 0;
}

static inline int wait_for_seqnum_remove_node(bdb_state_type *bdb_state, int rc)
{
    switch (rc) {
    case 1:
    case -2:
    case -10:
    case -1:
        return 1;
        break;
    default:
        return 0;
        break;
    }
}

/*
 * Return values:
 *    GOOD RETURN CODE
 *    0 - node has caught up
 *
 *    NORMAL TIMEOUT
 *    -999 - the caller will mark incoherent
 *
 *    SPECIAL CASES: DON'T WAIT ANYMORE
 *    1 - node is not coherent / newly online and catching up
 *   -2 - node is rtcpu'd- marked incoherent inline
 *  -10 - node generation is higher than what we are waiting on
 *   -1 - node has been decommissioned
 *
 *   Any of the SPECIAL CASES warrants removing that node from the list of nodes
 *   we wait for.  This counts against durability.
 */
static int bdb_wait_for_seqnum_from_node_int(bdb_state_type *bdb_state,
                                             seqnum_type *seqnum,
                                             const char *host, int timeoutms, int lineno,
                                             int fakeincoherent)
{
    int rc, reset_ts = 1, wakecnt = 0, remaining = timeoutms;
    int seqnum_wait_interval = bdb_state->attr->seqnum_wait_interval;
    struct timespec waittime;
    int i, coherent_state;
    int node_is_rtcpu = 0;
    DB_LSN got_lsn;
    uint32_t got_gen;
    int node_ix = nodeix(host);
    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (fakeincoherent) {
        node_is_rtcpu = 1;
    }
    if (bdb_state->callback->nodeup_rtn)
        if (!(bdb_state->callback->nodeup_rtn(bdb_state, host)))
            node_is_rtcpu = 1;

    /* dont wait if it's in a skipped state */
    Pthread_mutex_lock(&(bdb_state->coherent_state_lock));
    if ((coherent_state = bdb_state->coherent_state[node_ix]) ==
        STATE_INCOHERENT) {
        Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));
        if (bdb_state->attr->wait_for_seqnum_trace) {
            logmsg(LOGMSG_USER, PR_LSN " %s is incoherent, not waiting\n",
                   PARM_LSN(seqnum->lsn), host);
        }
        return 1;
    }

    Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));

    /* node is rtcpued off:  we may need to make the node incoherent */
    if (node_is_rtcpu) {
        Pthread_mutex_lock(&(bdb_state->coherent_state_lock));
        if (bdb_state->coherent_state[node_ix] == STATE_COHERENT ||
            bdb_state->coherent_state[node_ix] == STATE_INCOHERENT_WAIT) {
            if (bdb_state->coherent_state[node_ix] == STATE_COHERENT)
                defer_commits(bdb_state, host, __func__);
            bdb_state->last_downgrade_time[node_ix] = gettimeofday_ms();
            set_coherent_state(bdb_state, host, STATE_INCOHERENT, __func__,
                               __LINE__);
            bdb_state->repinfo->skipsinceepoch = comdb2_time_epoch();
        }

        Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));

        if (bdb_state->attr->wait_for_seqnum_trace) {
            logmsg(LOGMSG_USER, PR_LSN " %s became incoherent, not waiting\n",
                   PARM_LSN(seqnum->lsn), host);
        }
        return -2;
    }

    Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));

    if (gbl_udp)
        bdb_state->seqnum_info->expected_udp_count[node_ix]++;

again:

    if (bdb_state->seqnum_info->seqnums[node_ix].lsn.file == INT_MAX ||
        bdb_lock_desired(bdb_state)) {
        /* add 1 ms of latency if we have someone catching up */
        poll(NULL, 0, 1);
        Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));

        if (bdb_state->attr->wait_for_seqnum_trace) {
            logmsg(LOGMSG_USER, PR_LSN " %s is catching up, not waiting\n",
                   PARM_LSN(seqnum->lsn), host);
        }
        return 1;
    }

    uint32_t gen = bdb_state->seqnum_info->seqnums[node_ix].generation;
    if (bdb_state->attr->enable_seqnum_generations &&
        gen > seqnum->generation) {
        static unsigned long long higher_generation_reject = 0;
        static time_t pr = 0;
        time_t now;

        Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
        higher_generation_reject++;
        // TODO - trace on switch
        if (bdb_state->attr->wait_for_seqnum_trace && ((now = time(NULL)) > pr)) {
            logmsg(LOGMSG_USER, "%s: rejecting: seqnum for %s=%u - it is greater than this "
                   "commit seqnum %u, count=%llu \n",
                   __func__, host, gen, seqnum->generation,
                   higher_generation_reject);
            pr = now;
        }
        return -10;
    }

    if (bdb_state->attr->enable_seqnum_generations &&
        gen < seqnum->generation) {
        static time_t pr = 0;
        time_t now;

        if (bdb_state->attr->wait_for_seqnum_trace && ((now = time(NULL)) > pr)) {
            logmsg(LOGMSG_USER, "%s: generation too low: gen==%u, i want %u\n", __func__,
                   gen, seqnum->generation);
            pr = now;
        }
    }

    got_gen = gen;
    got_lsn = bdb_state->seqnum_info->seqnums[node_ix].lsn;

    if (bdb_seqnum_compare(bdb_state,
                           &(bdb_state->seqnum_info->seqnums[node_ix]),
                           seqnum) >= 0) {
        Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
        if (bdb_state->attr->wait_for_seqnum_trace) {
            logmsg(LOGMSG_USER, "%s line %d called from %d %s good rcode mach-gen %u mach_lsn %d:%d waiting for %u %d:%d\n", 
                    __func__, __LINE__, lineno, host, got_gen, got_lsn.file, got_lsn.offset,
                    seqnum->generation, seqnum->lsn.file, seqnum->lsn.offset);
        }
        return 0;
    }
    /* this node may have been decommissioned, in which case we
     * get woken up.  Check that this node still exists. */
    if (!bdb_state->attr->repalwayswait) {
        const char *nodes[REPMAX];
        int count;
        count = net_get_all_nodes_connected(bdb_state->repinfo->netinfo, nodes);
        for (i = 0; i < count; i++)
            if (nodes[i] == host)
                break;
        if (i == count) {
            /* no longer connected to this node */
            Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
            trigger_unregister_node(host);
            if (bdb_state->attr->wait_for_seqnum_trace) {
                logmsg(LOGMSG_USER,
                       PR_LSN " err waiting for seqnum: host %s no "
                              "longer connected\n",
                       PARM_LSN(seqnum->lsn), host);
            }
            return -1;
        }
    }

    /* Set timespec for first run and timeouts */
    if (reset_ts) {
        if (seqnum_wait_interval <= 50 || remaining < seqnum_wait_interval) {
            setup_waittime(&waittime, remaining);
            remaining = 0;
        } else {
            setup_waittime(&waittime, seqnum_wait_interval);
            remaining -= seqnum_wait_interval;
        }
        reset_ts = 0;
    }

    rc = pthread_cond_timedwait(&(bdb_state->seqnum_info->cond),
                                &(bdb_state->seqnum_info->lock), &waittime);

    /* Keep track of the number of wakeups */
    wakecnt++;

    /* Come up to check lock-desired */
    if (rc == ETIMEDOUT && remaining > 0) {
        reset_ts = 1;
    }
    /* Timeout */
    else if (rc == ETIMEDOUT && remaining <= 0) {
        Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
        if (bdb_state->attr->wait_for_seqnum_trace) {
            logmsg(LOGMSG_USER, "%s line %d called from %d %s timed out, mach-gen %u mach_lsn %d:%d waiting for %u %d:%d\n", 
                    __func__, __LINE__, lineno, host, got_gen, got_lsn.file, got_lsn.offset,
                    seqnum->generation, seqnum->lsn.file, seqnum->lsn.offset);
        }
        return -999;
    }

    else if (rc != ETIMEDOUT && rc != 0) {
        logmsg(LOGMSG_FATAL, "err from pthread_cond_timedwait\n");
        exit(1);
    }

    goto again;
}

int bdb_wait_for_seqnum_from_node(bdb_state_type *bdb_state,
                                  seqnum_type *seqnum, const char *host)
{
    int timeoutms = bdb_state->attr->reptimeout * MILLISEC;
    return bdb_wait_for_seqnum_from_node_int(bdb_state, seqnum, host,
                                             timeoutms, __LINE__, 0);
}

int bdb_wait_for_seqnum_from_node_timeout(bdb_state_type *bdb_state,
                                          seqnum_type *seqnum, const char *host,
                                          int timeoutms)
{
    return bdb_wait_for_seqnum_from_node_int(bdb_state, seqnum, host,
                                             timeoutms, __LINE__, 0);
}

/* inside bdb_commit(), we get a seqnum from the log file,
   then wait for it if we are running with waitforacks set */
int bdb_wait_for_seqnum_from_room(bdb_state_type *bdb_state,
                                  seqnum_type *seqnum)
{
    int i;
    const char *nodelist[REPMAX];
    int numnodes;
    int our_room = 0;

    if (bdb_state->attr->repalwayswait)
        numnodes = net_get_all_nodes(bdb_state->repinfo->netinfo, nodelist);
    else
        numnodes =
            net_get_all_nodes_connected(bdb_state->repinfo->netinfo, nodelist);

    if (bdb_state->callback->getroom_rtn)
        our_room = (bdb_state->callback->getroom_rtn(
            bdb_state, bdb_state->repinfo->myhost));

    for (i = 0; i < numnodes; i++) {
        int rc = 0;
        if (bdb_state->callback->getroom_rtn) {
            if ((bdb_state->callback->getroom_rtn(bdb_state, nodelist[i])) ==
                our_room)
                rc = bdb_wait_for_seqnum_from_node(bdb_state, seqnum,
                                                   nodelist[i]);
        } else {
            rc = bdb_wait_for_seqnum_from_node(bdb_state, seqnum, nodelist[i]);
        }
        if (rc)
            logmsg(LOGMSG_DEBUG,
                   "%s:bdb_wait_for_seqnum_from_node %s returned rc=%d\n",
                   __func__, nodelist[i], rc);
    }

    return 0;
}

static int node_in_list(int node, int list[], int listsz)
{
    int i;

    for (i = 0; i < listsz; i++) {
        if (list[i] == node)
            return 1;
    }

    return 0;
}

/*
  inside bdb_commit(), we get a seqnum from the log file,
  then wait for it if we are running with waitforacks set

  if we are passed an "outbad" array, return a list of all nodes that were
  either SKIPPED (not connected) or TIMED OUT.

  we return -1 to indicate some sort of error condition only if outbad is
  provided.  for historical reasons, in other cases we always return 0
  no matter what, as historically there was nothing you could do anyway.

  */

/* ripped out ALL SUPPORT FOR ALL BROKEN CRAP MODES, aside from "newcoh" */

int gbl_replicant_retry_on_not_durable = 0;
static int bdb_wait_for_seqnum_from_all_int(bdb_state_type *bdb_state,
                                            seqnum_type *seqnum, int *timeoutms,
                                            uint64_t txnsize, int newcoh)
{
    int i, now, cntbytes;
    const char *nodelist[REPMAX];
    const char *connlist[REPMAX];
    int durable_lsns;
    int catchup_window;
    int do_slow_node_check = 0;
    DB_LSN *masterlsn;
    int numnodes;
    int numwait;
    int rc;
    int waitms;
    int numskip;
    int numfailed = 0;
    int outrc;
    int num_incoh = 0;

    int begin_time, end_time;
    int we_used = 0;
    const char *base_node = NULL;
    char str[80];
    int track_once = 1;
    DB_LSN nodelsn;
    uint32_t nodegen;
    int num_successfully_acked = 0;
    int total_commissioned;
    int lock_desired = 0;
    int fake_incoherent = 0;

    /* if we were passed a child, find his parent */
    assert(!bdb_state->parent);
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /* Dereference from parent */
    durable_lsns = (bdb_state->attr->durable_lsns || gbl_replicant_retry_on_not_durable);
    catchup_window = bdb_state->attr->catchup_window;

    /* short ciruit if we are waiting on lsn 0:0  */
    if ((seqnum->lsn.file == 0) && (seqnum->lsn.offset == 0))
        return 0;

    /*
    logmsg(LOGMSG_DEBUG, "%s waiting for %s\n", __func__,
           lsn_to_str(str, &(seqnum->lsn)));
    */

    begin_time = comdb2_time_epochms();

    /* lame, i know.  go into a loop polling once per second to see if
       anyone is coherent yet.  don't wait forever - this must timeout
       eventually or we can end up hung for hours in some pathalogical
       situations. */
    do {
        numnodes = 0;
        numskip = 0;
        numwait = 0;

        if ((total_commissioned = net_get_all_commissioned_nodes(
                 bdb_state->repinfo->netinfo, connlist)) == 0) {
          goto done_wait;
        }

        if (track_once && bdb_state->attr->track_replication_times) {
            track_once = 0;

            Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
            for (int i = 0; i < total_commissioned; i++)
                bdb_track_replication_time(bdb_state, seqnum, connlist[i]);
            Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));

            /* once a second, see if we have any slow replicants */
            now = comdb2_time_epochms();
            Pthread_mutex_lock(&slow_node_check_lk);
            if (now - last_slow_node_check_time > 1000) {
                if (bdb_state->attr->track_replication_times) {
                    last_slow_node_check_time = now;
                    do_slow_node_check = 1;
                }
            }
            Pthread_mutex_unlock(&slow_node_check_lk);

            /* do the slow replicant check - only if we need to ... */
            if (do_slow_node_check &&
                bdb_state->attr->track_replication_times &&
                (bdb_state->attr->warn_slow_replicants ||
                 bdb_state->attr->make_slow_replicants_incoherent)) {
                bdb_slow_replicant_check(bdb_state, seqnum);
            }
        }

        for (i = 0; i < total_commissioned; i++) {
            int wait = 0;
            /* is_incoherent returns 0 for COHERENT & INCOHERENT_WAIT */
            if (!(is_incoherent_complete(bdb_state, connlist[i], &wait))) {
                nodelist[numnodes] = connlist[i];
                numnodes++;
                if (wait)
                    numwait++;
            } else {
                numskip++;
                num_incoh++;
            }
        }

        if (numnodes == 0) {
            goto done_wait;
        }

        if ((debug_switch_all_incoherent() && (rand() % 2))) {
            fake_incoherent = 1;
        }

        for (i = 0; i < numnodes; i++) {
            if (bdb_state->rep_trace)
                logmsg(LOGMSG_USER,
                       "waiting for initial NEWSEQ from node %s of >= <%s>\n",
                       nodelist[i], lsn_to_str(str, &(seqnum->lsn)));

            rc = bdb_wait_for_seqnum_from_node_int(bdb_state, seqnum,
                    nodelist[i], 1000, __LINE__, fake_incoherent);

            if (bdb_lock_desired(bdb_state)) {
                logmsg(LOGMSG_ERROR,
                       "%s line %d early exit because lock-is-desired\n",
                       __func__, __LINE__);
                return (durable_lsns ? BDBERR_NOT_DURABLE : -1);
            }

            if (wait_for_seqnum_remove_node(bdb_state, rc)) {
                nodelist[i] = nodelist[numnodes - 1];
                numnodes--;
                if (numnodes <= 0)
                    goto done_wait;
                i--;
                assert(rc != 0);
            }

            if (rc == 0) {
                base_node = nodelist[i];
                num_successfully_acked++;

                end_time = comdb2_time_epochms();
                we_used = end_time - begin_time;

                /* lets make up a number for how many more ms we should wait
                   based on how long we had to wait for one guy */
                waitms = (we_used * bdb_state->attr->rep_timeout_lag) / 100;

                if (waitms < bdb_state->attr->rep_timeout_minms)
                    waitms = bdb_state->attr->rep_timeout_minms;

                if (bdb_state->rep_trace)
                    logmsg(LOGMSG_USER,
                           "fastest node to <%s> was %dms, will wait another %dms for remainder\n",
                            lsn_to_str(str, &(seqnum->lsn)), we_used, waitms);

                goto got_ack;
            }
        }
    } while (comdb2_time_epochms() - begin_time <
                 bdb_state->attr->rep_timeout_maxms &&
             !(lock_desired = bdb_lock_desired(bdb_state)));

    /* if we get here then we timed out without finding even one good node.
     * allow a waitms of ZERO for the remaining nodes - we've run out of
     * patience!  Note that I *do* want to go into the loop below so that we
     * mark the stragglers incoherent.  The do { } while loop above gaurantees
     * that nodelist is correctly set up. */
    end_time = comdb2_time_epochms();
    we_used = end_time - begin_time;
    waitms =
        bdb_state->attr->rep_timeout_minms - bdb_state->attr->rep_timeout_maxms;
    if (waitms < 0)
        waitms = 0;
    if(!lock_desired)
        logmsg(LOGMSG_WARN, "timed out waiting for initial replication of <%s>\n",
               lsn_to_str(str, &(seqnum->lsn)));
    else
        logmsg(LOGMSG_WARN,
               "lock desired, not waiting for initial replication of <%s>\n",
               lsn_to_str(str, &(seqnum->lsn)));

got_ack:

    /* always wait at least waitms */
    if (waitms < bdb_state->attr->rep_timeout_minms)
        waitms = bdb_state->attr->rep_timeout_minms;

    /* Pass back the total timeout which we are allowing */
    *timeoutms = (we_used + waitms);

    for (i = 0; i < numnodes; i++) {
        if (nodelist[i] == base_node)
            continue;

        if (waitms <= 0)
            waitms = 0;

        /* always wait at least waitms */
        if (waitms < bdb_state->attr->rep_timeout_minms)
            waitms = bdb_state->attr->rep_timeout_minms;

        begin_time = comdb2_time_epochms();

        if (bdb_state->rep_trace)
            logmsg(LOGMSG_USER,
                   "waiting for NEWSEQ from node %s of >= <%s> timeout %d\n",
                   nodelist[i], lsn_to_str(str, &(seqnum->lsn)), waitms);

        rc = bdb_wait_for_seqnum_from_node_int(bdb_state, seqnum, nodelist[i],
                                               waitms, __LINE__, fake_incoherent);

        if (bdb_lock_desired(bdb_state)) {
            logmsg(LOGMSG_ERROR,
                   "%s line %d early exit because lock-is-desired\n", __func__,
                   __LINE__);

            return (durable_lsns ? BDBERR_NOT_DURABLE : -1);
        }

        if (rc == -999) {
            logmsg(LOGMSG_WARN, "replication timeout to node %s (%d ms), base node "
                            "was %s with %d ms\n",
                    nodelist[i], waitms, base_node, we_used);
            numfailed++;
        }

        else if (rc == 0)
            num_successfully_acked++;

        else if (rc == 1)
            rc = 0;

        end_time = comdb2_time_epochms();

        /* take away the amount of time we've used so far */
        waitms -= (end_time - begin_time);

        /* replication timeout: we may need to make the node incoherent */
        if (rc != 0 && rc != -2) {
            int node_ix = nodeix(nodelist[i]);
            // Extract seqnum
            Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
            nodegen = bdb_state->seqnum_info->seqnums[node_ix].generation;
            nodelsn = bdb_state->seqnum_info->seqnums[node_ix].lsn;
            Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));

            Pthread_mutex_lock(&(bdb_state->coherent_state_lock));

            if (nodegen <= seqnum->generation && log_compare(&(seqnum->lsn), &nodelsn) >= 0) {
                /* Only sleep on the change from COHERENT (the point where we
                 * decide to stop sending leases).  For role-change the new
                 * master marks every node as INCOHERENT_WAIT and then sleeps
                 * for the lease interval. */
                if (bdb_state->coherent_state[node_ix] == STATE_COHERENT)
                    defer_commits(bdb_state, nodelist[i], __func__);

                /* Change to INCOHERENT_WAIT if we allow catchup on commit */
                if (bdb_state->attr->catchup_on_commit && catchup_window) {
                    masterlsn =
                        &(bdb_state->seqnum_info
                              ->seqnums[nodeix(bdb_state->repinfo->master_host)]
                              .lsn);
                    cntbytes = subtract_lsn(bdb_state, masterlsn, &nodelsn);

                    set_coherent_state(bdb_state, nodelist[i],
                                       (cntbytes < catchup_window)
                                           ? STATE_INCOHERENT_WAIT
                                           : STATE_INCOHERENT,
                                       __func__, __LINE__);
                } else
                    set_coherent_state(bdb_state, nodelist[i], STATE_INCOHERENT,
                                       __func__, __LINE__);

                /* Record the downgrade time */
                bdb_state->last_downgrade_time[node_ix] = gettimeofday_ms();

                bdb_state->repinfo->skipsinceepoch = comdb2_time_epoch();
            }

            Pthread_mutex_unlock(&(bdb_state->coherent_state_lock));
        }
    }

done_wait:

    outrc = 0;

    if (!numfailed && !numskip && !numwait &&
        bdb_state->attr->remove_commitdelay_on_coherent_cluster &&
        bdb_state->attr->commitdelay) {
        logmsg(LOGMSG_INFO, "Cluster is in sync, removing commitdelay\n");
        bdb_state->attr->commitdelay = 0;
    }

    if (numfailed) {
        outrc = -1;
    }

    if (durable_lsns) {
        uint32_t cur_gen;
        static uint32_t not_durable_count;
        static uint32_t durable_count;
        extern int gbl_durable_wait_seqnum_test;

        int istest = 0;
        int was_durable = 0;

        uint32_t cluster_size = total_commissioned + 1;
        uint32_t number_with_this_update = num_successfully_acked + 1;
        uint32_t durable_target = (cluster_size / 2) + 1;

        if ((number_with_this_update < durable_target) ||
            (gbl_durable_wait_seqnum_test && (istest = (0 == (rand() % 20))))) {
            if (istest)
                logmsg(LOGMSG_USER, 
                        "%s return not durable for durable wait seqnum test\n", __func__);
            outrc = BDBERR_NOT_DURABLE;
            not_durable_count++;
            was_durable = 0;
        } else {
            /* We've released the bdb lock at this point- the master could have
             * changed while
             * we were waiting for this to propogate.  The simple fix: get
             * rep_gen & return
             * not durable if it's changed */
            BDB_READLOCK("wait_for_seqnum");
            bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &cur_gen);
            BDB_RELLOCK();

            if (cur_gen != seqnum->generation) {
                outrc = BDBERR_NOT_DURABLE;
                not_durable_count++;
                was_durable = 0;
            } else {
                Pthread_mutex_lock(&bdb_state->durable_lsn_lk);
                bdb_state->dbenv->set_durable_lsn(bdb_state->dbenv,
                                                  &seqnum->lsn, cur_gen);
                if (seqnum->lsn.file == 0) {
                    logmsg(LOGMSG_FATAL, "%s line %d: aborting on insane durable lsn\n",
                            __func__, __LINE__);
                    abort();
                }
                Pthread_mutex_unlock(&bdb_state->durable_lsn_lk);
                durable_count++;
                was_durable = 1;
            }
        }

        if (bdb_state->attr->wait_for_seqnum_trace) {
            DB_LSN calc_lsn;
            uint32_t calc_gen;
            calculate_durable_lsn(bdb_state, &calc_lsn, &calc_gen, 1);
            /* This is actually okay- do_ack and the thread which broadcasts
             * seqnums can race against each other.  If we got a majority of 
             * these during the commit we are okay */
            if (was_durable && log_compare(&calc_lsn, &seqnum->lsn) < 0) {
                logmsg(LOGMSG_USER,
                       "ERROR: calculate_durable_lsn trails seqnum, "
                       "but this is durable (%d:%d vs %d:%d)?\n",
                       calc_lsn.file, calc_lsn.offset, seqnum->lsn.file,
                       seqnum->lsn.offset);
            }
            logmsg(LOGMSG_USER, 
                "Last txn was %s, tot_connected=%d tot_acked=%d, "
                "durable-commit-count=%u not-durable-commit-count=%u "
                "commit-lsn=[%d][%d] commit-gen=%u calc-durable-lsn=[%d][%d] "
                "calc-durable-gen=%u\n",
                was_durable ? "durable" : "not-durable", total_commissioned,
                num_successfully_acked, durable_count, not_durable_count,
                seqnum->lsn.file, seqnum->lsn.offset, seqnum->generation,
                calc_lsn.file, calc_lsn.offset, calc_gen);
        }
    }

    return outrc;
}

int bdb_wait_for_seqnum_from_all(bdb_state_type *bdb_state, seqnum_type *seqnum)
{
    int timeoutms = bdb_state->attr->reptimeout * MILLISEC;
    return bdb_wait_for_seqnum_from_all_int(bdb_state, seqnum, &timeoutms, 0,
                                            0);
}

int bdb_wait_for_seqnum_from_all_timeout(bdb_state_type *bdb_state,
                                         seqnum_type *seqnum, int timeoutms)
{
    return bdb_wait_for_seqnum_from_all_int(bdb_state, seqnum, &timeoutms, 0,
                                            0);
}

int bdb_wait_for_seqnum_from_all_adaptive(bdb_state_type *bdb_state,
                                          seqnum_type *seqnum, uint64_t txnsize,
                                          int *timeoutms)
{
    *timeoutms = -1;
    return bdb_wait_for_seqnum_from_all_int(bdb_state, seqnum, timeoutms,
                                            txnsize, 0);
}

/*
  these routines enable the "new coherency logic"
   - when we time out to a node, mark it bad (set "skip flag")
     and notify it to go into reject requests mode
     (do this sync with ack, inline here).
   - each time we make it to the end of bdb_wait_for_seqnum_from_all_int(),
     check if anyone marked skipped (we didnt wait for it) is actually
     coherent.  if it is, tell it to get out of reject requests mode,
     and mark it as not skipped.
*/

int bdb_wait_for_seqnum_from_all_newcoh(bdb_state_type *bdb_state,
                                        seqnum_type *seqnum)
{
    int timeoutms = bdb_state->attr->reptimeout * MILLISEC;
    return bdb_wait_for_seqnum_from_all_int(bdb_state, seqnum, &timeoutms, 0,
                                            1);
}

int bdb_wait_for_seqnum_from_all_timeout_newcoh(bdb_state_type *bdb_state,
                                                seqnum_type *seqnum,
                                                int timeoutms)
{
    return bdb_wait_for_seqnum_from_all_int(bdb_state, seqnum, &timeoutms, 0,
                                            1);
}

int bdb_wait_for_seqnum_from_all_adaptive_newcoh(bdb_state_type *bdb_state,
                                                 seqnum_type *seqnum,
                                                 uint64_t txnsize,
                                                 int *timeoutms)
{
    *timeoutms = -1;
    return bdb_wait_for_seqnum_from_all_int(bdb_state, seqnum, timeoutms,
                                            txnsize, 1);
}

/* let everyone know what logfile we are currently using */
void send_filenum_to_all(bdb_state_type *bdb_state, int filenum, int nodelay)
{
    if (bdb_state->exiting)
        return;
    if (!bdb_state->caught_up)
        filenum = 0;
    filenum = htonl(filenum);
    void *data[] = {&filenum};
    int sz[] = {sizeof(filenum)};
    int type[] = {USER_TYPE_BERKDB_FILENUM};
    int flag[] = {nodelay};
    int rc = net_send_all(bdb_state->repinfo->netinfo, 1, data, sz, type, flag);
    if (rc)
        logmsg(LOGMSG_WARN, "%s:net_send returned rc=%d\n", __func__, rc);
}

int bdb_get_myseqnum(bdb_state_type *bdb_state, seqnum_type *seqnum)
{
    if ((!bdb_state->caught_up) || (bdb_state->exiting)) {
        bzero(seqnum, sizeof(seqnum_type));
    } else {
        uint32_t rep_gen;

        if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost) {
            bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &rep_gen);
        } else {
            /* Replicant generation is updated after verify-match */
            bdb_state->dbenv->replicant_generation(bdb_state->dbenv, &rep_gen);
        }

        Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));

        int myhost_ix = nodeix(bdb_state->repinfo->myhost);
        memcpy(seqnum, &(bdb_state->seqnum_info->seqnums[myhost_ix]),
               sizeof(seqnum_type));

        Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
        seqnum->generation = rep_gen;
    }
    return (seqnum->lsn.file > 0);
}

int get_myseqnum(bdb_state_type *bdb_state, uint8_t *p_net_seqnum)
{
    seqnum_type seqnum;
    DB_LSN our_lsn;
    int rc = 0;
    uint64_t issue_time;

    uint8_t *p_buf, *p_buf_end;

    if ((!bdb_state->caught_up) || (bdb_state->exiting)) {
        make_lsn(&our_lsn, INT_MAX, INT_MAX);
        bzero(&seqnum, sizeof(seqnum_type));
        memcpy(&seqnum.lsn, &our_lsn, sizeof(DB_LSN));
    } else {
        Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));

        int myhost_ix = nodeix(bdb_state->repinfo->myhost);
        memcpy(&seqnum, &(bdb_state->seqnum_info->seqnums[myhost_ix]),
               sizeof(seqnum_type));

        Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));

        if ((bdb_state->attr->enable_seqnum_generations &&
             seqnum.generation == 0) ||
            seqnum.lsn.file == 0)
            rc = -1;
    }

    /* Set master-lease information */
    issue_time = gettimeofday_ms();
    memcpy(&seqnum.issue_time, &issue_time, sizeof(issue_time));
    seqnum.lease_ms = bdb_state->attr->master_lease;

    p_buf = p_net_seqnum;
    p_buf_end = p_net_seqnum + BDB_SEQNUM_TYPE_LEN;
    rep_berkdb_seqnum_type_put(&seqnum, p_buf, p_buf_end);

    return rc;
}

int request_copydelay(void *bdb_state_in)
{
    int rc;
    bdb_state_type *bdb_state = (bdb_state_type *)bdb_state_in;
    rc = net_send_flags(bdb_state->repinfo->netinfo,
                        bdb_state->repinfo->master_host, USER_TYPE_COMMITDELAYTIMED, NULL,
                        0, NET_SEND_NODROP);
    return rc;
}

int send_myseqnum_to_master(bdb_state_type *bdb_state, int nodelay)
{
    uint8_t p_net_seqnum[BDB_SEQNUM_TYPE_LEN];
    int rc = 0;

    if (0 == (rc = get_myseqnum(bdb_state, p_net_seqnum))) {
        rc = net_send_nodrop(bdb_state->repinfo->netinfo,
                             bdb_state->repinfo->master_host,
                             USER_TYPE_BERKDB_NEWSEQ, &p_net_seqnum,
                             sizeof(seqnum_type), nodelay);
    } else {
        static time_t lastpr = 0;
        time_t now;
        static uint64_t count = 0;

        count++;
        if ((now = time(NULL)) > lastpr && !bdb_state->attr->createdbs) {
            logmsg(LOGMSG_WARN,
                   "%s: get_myseqnum returned non-0, count=%" PRIu64 "\n",
                   __func__, count);
            lastpr = now;
        }
    }
    return rc;
}

void send_myseqnum_to_all(bdb_state_type *bdb_state, int nodelay)
{
    uint8_t seqnum[BDB_SEQNUM_TYPE_LEN];
    if (get_myseqnum(bdb_state, seqnum) != 0)
        return;
    void *data[] = {seqnum};
    int sz[] = {BDB_SEQNUM_TYPE_LEN};
    int type[] = {USER_TYPE_BERKDB_NEWSEQ};
    int flag[] = {nodelay | NET_SEND_NODROP};
    int rc = net_send_all(bdb_state->repinfo->netinfo, 1, data, sz, type, flag);
    if (rc) {
        logmsg(LOGMSG_ERROR, "0x%p %s:%d net_send rc=%d\n", (void *)pthread_self(), __func__, __LINE__, rc);
    }
}

void bdb_exiting(bdb_state_type *bdb_state)
{
    if (!bdb_state) return;
    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    bdb_state->exiting = 1;
    MEMORY_SYNC;
}

int gbl_last_locked_seqnum = 1;

void bdb_set_seqnum(void *in_bdb_state)
{
    bdb_state_type *bdb_state = (bdb_state_type *)in_bdb_state;
    static int lastpr = 0;
    int now;
    DB_LSN lastlsn;
    uint32_t mygen;

    bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &mygen);

    /* Always only use get_last_locked.  Leave the other in until we are sure
     * that this code works. */
    if (gbl_last_locked_seqnum &&
        bdb_state->repinfo->master_host != bdb_state->repinfo->myhost)
        bdb_state->dbenv->get_last_locked(bdb_state->dbenv, &lastlsn);
    else
        __log_txn_lsn(bdb_state->dbenv, &lastlsn, NULL, NULL);

    if (lastlsn.file > 0) {
        Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
        int myhost_ix = nodeix(bdb_state->repinfo->myhost);
        bdb_state->seqnum_info->seqnums[myhost_ix].lsn = lastlsn;
        bdb_state->seqnum_info->seqnums[myhost_ix].generation = mygen;

        if (gbl_set_seqnum_trace && (now = time(NULL)) - lastpr) {
            logmsg(LOGMSG_USER, "%s line %d set %s seqnum to %d:%d gen %d\n",
                   __func__, __LINE__, bdb_state->repinfo->myhost, lastlsn.file,
                   lastlsn.offset, mygen);
            lastpr = now;
        }
        Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
    }
}

int gbl_online_recovery = 1;

static pthread_mutex_t rep_mon_lk = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t rep_mon_cd = PTHREAD_COND_INITIALIZER;
static pthread_t rep_mon_td;

struct rep_mon {
    pid_t tid;
    char *host;
    int starttime;
    int type;
    int added;
};
static hash_t *rep_mon_hash = NULL;

int gbl_rep_mon_threshold = 0;

static int process_rep_mon_hash(void *obj, void *arg)
{
    struct rep_mon *rm = (struct rep_mon *)obj;
    int now = comdb2_time_epoch(), threshold = gbl_rep_mon_threshold; 
    if (threshold > 0 && now - rm->starttime > threshold) {
        fprintf(stderr, "Thread %u host %s type %d hung for %d seconds\n",
            rm->tid, rm->host, rm->type, now - rm->starttime);
        pstack_self();
#ifndef _LINUX_SOURCE
        return 1;
#endif
    }
    return 0;
}

static void *rep_mon(void *thd)
{
    Pthread_mutex_lock(&rep_mon_lk);
    if (rep_mon_hash) {
        Pthread_mutex_unlock(&rep_mon_lk);
        return NULL;
    }
    rep_mon_td = pthread_self();
    rep_mon_hash = hash_init(sizeof(pthread_t));
    logmsg(LOGMSG_INFO, "Starting replication monitor thread\n");
    while(gbl_rep_mon_threshold > 0) {
        struct timespec waittime;
        clock_gettime(CLOCK_REALTIME, &waittime);
        waittime.tv_sec += 1;
        pthread_cond_timedwait(&rep_mon_cd, &rep_mon_lk, &waittime);
        if (gbl_rep_mon_threshold > 0) {
            hash_for(rep_mon_hash, process_rep_mon_hash, NULL);
        }
    }
    hash_free(rep_mon_hash);
    rep_mon_hash = NULL;
    logmsg(LOGMSG_INFO, "Exiting replication monitor thread\n");
    Pthread_mutex_unlock(&rep_mon_lk);
    return NULL;
}

static void add_rep_mon(struct rep_mon *rm)
{
    if (gbl_rep_mon_threshold <= 0)
        return;
    Pthread_mutex_lock(&rep_mon_lk);
    if (rep_mon_hash) {
        hash_add(rep_mon_hash, rm);
        rm->added = 1;
    } else {
        pthread_t t;
        pthread_create(&t, NULL, rep_mon, NULL);
    }
    Pthread_mutex_unlock(&rep_mon_lk);
}

static void rem_rep_mon(struct rep_mon *rm)
{
    if (!rm->added)
        return;
    Pthread_mutex_lock(&rep_mon_lk);
    if (rep_mon_hash) {
        hash_del(rep_mon_hash, rm);
    }
    Pthread_mutex_unlock(&rep_mon_lk);
}

static int process_berkdb(bdb_state_type *bdb_state, char *host, DBT *control,
                          DBT *rec)
{
    int rc;
    int r;
    char *master;
    uint32_t gen, egen;
    DB_LSN permlsn;
    uint32_t generation, commit_generation;
    int outrc;
    int time1, time2;
    int online = gbl_online_recovery;
    char *oldmaster = NULL;
    int force_election = 0;
    int rectype;
    int got_writelock = 0;
    int got_vote2lock = 0;
    int done = 0;
    int master_confused = 0;
    struct rep_mon rm = {0};

    /* don't give it to berkeley db if we havent started rep yet */
    if (!bdb_state->rep_started || control == NULL) {
        return 0;
    }

    rep_control_type *rep_control;
    rep_control = control->data;

    rectype = ntohl(rep_control->rectype);
    generation = ntohl(rep_control->gen);


    outrc = 0;

    /* there appears to be a bug in berkdb where sometimes we get a
       REP_MASTER_REQ, call rep_process_message, and are given back a 0,
       so we do nothing.  this gets us in states where all nodes "know who
       the master is" except for the master. */
    if (rectype == REP_MASTER_REQ) {
        force_election = 1;
    }

    bdb_reset_thread_stats();

    /* give it to berkeley db */
    time1 = comdb2_time_epoch();

#ifdef __APPLE__
    uint64_t id;
    pthread_threadid_np(pthread_self(), &id);
    rm.tid = id;
#elif defined _LINUX_SOURCE
    rm.tid = syscall(__NR_gettid);
#else
    rm.tid = getpid();
#endif
    rm.host = host;
    rm.starttime = time1;
    rm.type = rectype;
    add_rep_mon(&rm);

    bdb_state->repinfo->repstats.rep_process_message++;

    /* Rep_verify can set the recovery flag, which causes the code ignores
       locks.
       Grab the bdb_writelock here rather than inside of berkdb so that we avoid
       racing against a rep_start. */
    if (!online && rectype == REP_VERIFY &&
        bdb_state->dbenv->rep_verify_will_recover(bdb_state->dbenv, control,
                                                  rec)) {
        BDB_WRITELOCK_REP("bdb_rep_verify");
        got_writelock = 1;
    }

    static pthread_mutex_t vote2_lock = PTHREAD_MUTEX_INITIALIZER;

    if (rectype == REP_VOTE2 || rectype == REP_GEN_VOTE2 ||
        rectype == REP_NEWMASTER) {
        Pthread_mutex_lock(&vote2_lock);
        got_vote2lock = 1;
    }

    bdb_state->repinfo->in_rep_process_message = 1;

    bdb_state->repinfo->rep_process_message_start_time = comdb2_time_epoch();

    if (debug_switch_rep_delay())
        sleep(2);

    if (gbl_debug_drop_nth_rep_message > 0 &&
        (bdb_state->repinfo->repstats.rep_process_message %
         gbl_debug_drop_nth_rep_message) == 0) {
        logmsg(LOGMSG_INFO, "%s:%d dropping message!\n", __func__, __LINE__);
        r = 0;
    } else {
        r = bdb_state->dbenv->rep_process_message(bdb_state->dbenv, control, rec,
                                                  &host, &permlsn,
                                                  &commit_generation, online);
    }

    if (got_vote2lock) {
        if (bdb_get_rep_master(bdb_state, &master, &gen, &egen) != 0) {
            abort();
        }
        Pthread_mutex_unlock(&vote2_lock);
    }

    /*
    fprintf(stderr, "%s line %d permlsn = <%d:%d> rectype = %d\n", __FILE__,
    __LINE__,
          permlsn.file, permlsn.offset, rectype);
     */

    bdb_state->repinfo->rep_process_message_start_time = 0;
    bdb_state->repinfo->in_rep_process_message = 0;

    if (got_writelock) {
        BDB_RELLOCK();
    }

    if (bdb_state->attr->repsleep)
        poll(NULL, 0, (bdb_state->attr->repsleep));

    if (bdb_state->attr->rep_debug_delay > 0)
        usleep(bdb_state->attr->rep_debug_delay * 1000);

    rem_rep_mon(&rm);
    time2 = comdb2_time_epoch();

    if ((time2 - time1) > bdb_state->attr->rep_longreq) {
        const struct berkdb_thread_stats *t = bdb_get_thread_stats();
        logmsg(LOGMSG_WARN, "LONG rep_process_message: %d seconds, type:%d r:%d host:%s\n",
                time2 - time1, rep_control->rectype, r, host);
        bdb_fprintf_stats(t, "  ", stderr);
    }

    if (bdb_state->rep_trace)
        logmsg(LOGMSG_USER, "after rep_process_message() got %d from %s\n", r, host);

    /* force a high lsn if we are starting or stopping */
    if ((!bdb_state->caught_up) || (bdb_state->exiting))
        make_lsn(&permlsn, INT_MAX, INT_MAX);

    if ((force_election) && (bdb_state->caught_up) &&
        (host == bdb_state->repinfo->master_host)) {
        logmsg(LOGMSG_WARN, "master %s requested election\n", host);
        r = DB_REP_HOLDELECTION;
        master_confused = 1;
    }

    /* now do what berkeley db tells us */
    switch (r) {
    case 0:
        bdb_state->repinfo->repstats.rep_zerorc++;
        bdb_set_seqnum(bdb_state);
        break;

    case DB_REP_NEWSITE:
        bdb_state->repinfo->repstats.rep_newsite++;
        /* Don't ctrace in election codepath (see comment above). */
        send_myseqnum_to_all(bdb_state, 1);
        break;

    case DB_REP_HOLDELECTION:
        bdb_state->repinfo->repstats.rep_holdelection++;

        /* Don't ctrace in election codepath (see comment above) */
        logmsg(LOGMSG_WARN, "process_berkdb: DB_REP_HOLDELECTION from %s\n", host);

        /* master shouldnt respond to this with calling for election?
           source - original demo program (ex_rq_util.c) and more recent
           berkdb docs */
        if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost)
            break;

        /* if we are connected to the master, dont respond to this call
           for election - unless it is confused and calling for
           the election itself! */
        if (net_is_connected(bdb_state->repinfo->netinfo,
                             bdb_state->repinfo->master_host) &&
            !master_confused)
            break;

        call_for_election(bdb_state, __func__, __LINE__);

        /* send a hello msg to the node who called for an election.  the reason
         * for this is it may have incomplete knowledge of the topology */
        net_send_hello(bdb_state->repinfo->netinfo, host);

        break;

    case DB_HAS_MAJORITY:
        logmsg(LOGMSG_WARN, "%s: received another vote for leader %s\n", __func__, host);
        break;

    case DB_REP_NEWMASTER:
        bdb_state->repinfo->repstats.rep_newmaster++;

        if (!got_vote2lock) {
            logmsg(LOGMSG_WARN,
                   "process_berkdb: got NEWMASTER with no votelock2\n");
            abort();
            bdb_get_rep_master(bdb_state, &master, &gen, &egen);
        }

        logmsg(LOGMSG_WARN,
               "process_berkdb: DB_REP_NEWMASTER %s time=%ld upgraded to "
               "gen=%u egen=%d\n",
               host, time(NULL), gen, egen);

        /* Check if it's us. */
        if (host == bdb_state->repinfo->myhost) {
            assert(got_vote2lock);
            logmsg(LOGMSG_WARN, "NEWMASTER is ME for GENERATION %d\n", egen);

            /* I'm upgrading and this thread could be holding logical locks:
             * abort sql threads waiting on logical locks */
            BDB_WRITELOCK_REP("upgrade");
            /* we need to upgrade */
            rc = bdb_upgrade(bdb_state, egen, &done);

            BDB_RELLOCK();

            if (rc != 0) {
                /* why did upgrade fail?  lets exit */
                logmsg(LOGMSG_FATAL, "upgrade failed rcode %d %d\n", rc,
                        bdb_state->repinfo->upgrade_allowed);
                exit(1);
            }

        } else {
            /* it's not us, but we were master - we need to downgrade */
            if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost) {
                rc = bdb_downgrade(bdb_state, egen, &done);
            } else
                done = 1;
        }
        if (!done) {
            logmsg(LOGMSG_INFO, "%s:%d DB_REP_NEWMASTER during startup, ignoring\n",
                    __FILE__, __LINE__);
        } else
            bdb_setmaster(bdb_state, host);

        if (gbl_dump_zero_coherency_timestamp) {
            logmsg(LOGMSG_ERROR, "%s line %d zero'ing coherency timestamp\n",
                   __func__, __LINE__);
        }
        coherency_timestamp = 0;
        break;

    case DB_REP_DUPMASTER:
        oldmaster = bdb_state->repinfo->master_host;
        bdb_state->repinfo->repstats.rep_dupmaster++;
        logmsg(LOGMSG_WARN, "rep_process_message: got DUPMASTER from %s, "
                "I think master is %s.  dowgrading and calling for election\n",
                host, oldmaster);
        rc = bdb_downgrade(bdb_state, 0, NULL);
        break;

    case DB_REP_ISPERM: {
        bdb_state->repinfo->repstats.rep_isperm++;

        Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
        int myhost_ix = nodeix(bdb_state->repinfo->myhost);
        bdb_state->seqnum_info->seqnums[myhost_ix].lsn = permlsn;
        bdb_state->seqnum_info->seqnums[myhost_ix].generation = generation;
        Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));

        /*
        fprintf(stderr, "%s line %d DB_REP_ISPERM lastlsn = <%d:%d>\n",
        __FILE__, __LINE__,
              permlsn.file, permlsn.offset);
         */

        if (!gbl_early) {
            rc = do_ack(bdb_state, permlsn, generation);
        }

        break;
    }

    case DB_REP_NOTPERM: {
        bdb_state->repinfo->repstats.rep_notperm++;

        /* during recovery, pretend these are ok so we dont hold
           up the cluster */
        if ((!bdb_state->caught_up) || (bdb_state->exiting)) {
            uint32_t gen;
            bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &gen);
            rc = do_ack(bdb_state, permlsn, gen);
        } else {
            /* fprintf(stderr, "got a NOTPERM\n"); */
        }
    } break;

    case DB_REP_OUTDATED: {
        bdb_state->repinfo->repstats.rep_outdated++;

        if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost) {
            logmsg(LOGMSG_FATAL, 
                    "this database needs to be hot copied with copycomdb2\n");
            logmsg(LOGMSG_FATAL, "exiting now\n");
            exit(1);
        } else {
            logmsg(LOGMSG_ERROR, "got outdated log request from %s\n", host);
        }

        break;
    }

    case DB_REP_STALEMASTER:
        bdb_state->repinfo->repstats.rep_other++;
        logmsg(LOGMSG_ERROR, "rep_process_message: from %s got %d "
                        "control->size=%d rec->size=%d\n",
                host, r, control->size, rec->size);
        outrc = 0;
        break;

    default:
        bdb_state->repinfo->repstats.rep_other++;
        logmsg(LOGMSG_ERROR, "rep_process_message: from %s got %d "
                        "control->size=%d rec->size=%d\n",
                host, r, control->size, rec->size);
        /* for the future, just in case we decide __rep_process_message
           will return new codes that are not handled here, propagate them
           higher */
        outrc = r;
        /* Actually, changed my mind; this message is lost since the receive
           master routine
           "returns" void.  This can be a failed TXN_APPLY for a commit, and
           we're corrupting
           database (losing transactions).  A checkpoint following this will
           make the
           corruption undetectable upon restart until the page is accessed. */
        if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost) {
            /* force this node to resync its log */
            if (bdb_state->attr->elect_on_mismatched_master) {
                logmsg(LOGMSG_ERROR,
                       "Call for election on strange msgtype %d on replicant\n",
                       r);
                call_for_election(bdb_state, __func__, __LINE__);
            } else
                abort();
        }
        break;
    }

    return outrc;
}

int gbl_force_incoherent = 0;
int gbl_ignore_coherency = 0;

static int bdb_am_i_coherent_int(bdb_state_type *bdb_state)
{
    /*master can't be incoherent*/
    if (bdb_amimaster(bdb_state))
        return 1;

    /* force_incoherent overrides ignore_coherency */
    if (gbl_force_incoherent) {
        static time_t lastpr = 0;
        time_t now = time(NULL);
        if (now - lastpr) {
            logmsg(LOGMSG_WARN,
                   "%s returning INCOHERENT on force_incoherent = true\n",
                   __func__);
            lastpr = now;
        }
        return 0;
    }

    /* if we are a rtcpued off replicant, we cant be coherent */
    if (bdb_state->callback->nodeup_rtn) {
        if (!(bdb_state->callback->nodeup_rtn(bdb_state,
                                              bdb_state->repinfo->myhost))) {
            return 0;
        }
    }

    if (gbl_ignore_coherency) {
        static time_t lastpr = 0;
        time_t now = time(NULL);
        if (now - lastpr) {
            logmsg(LOGMSG_WARN,
                   "%s ignoring coherency on 'ignore_coherency' = true\n",
                   __func__);
            lastpr = now;
        }
        return 1;
    }

    int ret = (gettimeofday_ms() <= get_coherency_timestamp());
    if (!ret)
        logmsg(LOGMSG_DEBUG, "%s returning INCOHERENT \n", __func__);
    return ret;
}

int bdb_valid_lease(void *in_bdb_state)
{
    int x;
    bdb_state_type *bdb_state = (bdb_state_type *)in_bdb_state;
    BDB_READLOCK("bdb_am_i_coherent");
    x = (gettimeofday_ms() <= get_coherency_timestamp());
    BDB_RELLOCK();
    return x;
}

int bdb_am_i_coherent(bdb_state_type *bdb_state)
{
    int x;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    BDB_READLOCK("bdb_am_i_coherent");

    x = bdb_am_i_coherent_int(bdb_state);

    BDB_RELLOCK();

    return x;
}

/* called when the master tells us we're not coherent */
void bdb_set_notcoherent(bdb_state_type *bdb_state, int notcoherent)
{

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    BDB_READLOCK("bdb_set_notcoherent");

    /* We're getting a not-coherent msg before we're ready.
    ** Friendly reminder over net? */
    if (!bdb_state->passed_dbenv_open && notcoherent) {
        BDB_RELLOCK();
        return;
    }

    bdb_state->not_coherent = notcoherent;
    bdb_state->not_coherent_time = comdb2_time_epoch();

    BDB_RELLOCK();
}

const uint8_t *colease_type_get(colease_t *p_colease_type, const uint8_t *p_buf,
                                const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || COLEASE_TYPE_LEN > p_buf_end - p_buf)
        return NULL;
    p_buf = buf_get(&(p_colease_type->issue_time),
                    sizeof(p_colease_type->issue_time), p_buf, p_buf_end);
    p_buf = buf_get(&(p_colease_type->lease_ms),
                    sizeof(p_colease_type->lease_ms), p_buf, p_buf_end);
    p_buf =
        buf_skip(sizeof(p_colease_type->fluff), (uint8_t *)p_buf, p_buf_end);
    return p_buf;
}

uint8_t *colease_type_put(const colease_t *p_colease_type, uint8_t *p_buf,
                          uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || COLEASE_TYPE_LEN > p_buf_end - p_buf)
        return NULL;
    p_buf = buf_put(&(p_colease_type->issue_time),
                    sizeof(p_colease_type->issue_time), p_buf, p_buf_end);
    p_buf = buf_put(&(p_colease_type->lease_ms),
                    sizeof(p_colease_type->lease_ms), p_buf, p_buf_end);
    p_buf = buf_zero_put(sizeof(p_colease_type->fluff), p_buf, p_buf_end);
    return p_buf;
}

uint64_t get_coherency_timestamp(void)
{
    uint64_t x = coherency_timestamp;
    if (x == 0) {
        static uint32_t lastpr;
        static uint32_t zero_ts_count = 0;
        uint32_t now;

        zero_ts_count++;
        if (gbl_dump_zero_coherency_timestamp &&
            ((now = time(NULL)) - lastpr)) {
            logmsg(LOGMSG_ERROR,
                   "%s returning 0 coherency_timestamp, count=%u\n", __func__,
                   zero_ts_count);
            lastpr = now;
        }
    }
    return x;
}

typedef struct start_lsn_response {
    uint32_t gen;
    DB_LSN lsn;
} start_lsn_response_t;

enum { START_LSN_RESPONSE_TYPE_LEN = 4 + 8 };
BB_COMPILE_TIME_ASSERT(start_lsn_response, sizeof(start_lsn_response_t) ==
        START_LSN_RESPONSE_TYPE_LEN);

static uint8_t *start_lsn_response_type_put(const start_lsn_response_t 
        *p_start_lsn_response_type, uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || START_LSN_RESPONSE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_put(&(p_start_lsn_response_type->gen),
            sizeof(p_start_lsn_response_type->gen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_start_lsn_response_type->lsn.file),
            sizeof(p_start_lsn_response_type->lsn.file), p_buf, p_buf_end);
    p_buf = buf_put(&(p_start_lsn_response_type->lsn.offset),
            sizeof(p_start_lsn_response_type->lsn.offset), p_buf, p_buf_end);
    return p_buf;
}

static const uint8_t *start_lsn_response_type_get(start_lsn_response_t
        *p_start_lsn_response_type, const uint8_t *p_buf, const uint8_t 
        *p_buf_end)
{
    if (p_buf_end < p_buf || START_LSN_RESPONSE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;
    p_buf = buf_get(&(p_start_lsn_response_type->gen),
            sizeof(p_start_lsn_response_type->gen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_start_lsn_response_type->lsn.file),
            sizeof(p_start_lsn_response_type->lsn.file), p_buf, p_buf_end);
    p_buf = buf_get(&(p_start_lsn_response_type->lsn.offset),
            sizeof(p_start_lsn_response_type->lsn.offset), p_buf, p_buf_end);
    return p_buf;
}

void receive_start_lsn_request(void *ack_handle, void *usr_ptr, char *from_host,
                             int usertype, void *dta, int dtalen,
                             uint8_t is_tcp)
{
    start_lsn_response_t start_lsn = {0};
    uint8_t buf[START_LSN_RESPONSE_TYPE_LEN];
    uint8_t *p_buf, *p_buf_end;
    uint32_t current_gen;
    bdb_state_type *bdb_state = usr_ptr;
    repinfo_type *repinfo = bdb_state->repinfo;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (repinfo->master_host != repinfo->myhost) {
        logmsg(LOGMSG_ERROR, "%s returning bad rcode because i am not master\n", 
                __func__);
        net_ack_message(ack_handle, 1);
        return;
    }

    if (bdb_state->attr->master_lease && 
            !verify_master_leases(bdb_state, __func__, __LINE__)) {
        logmsg(LOGMSG_ERROR, "%s returning bad rcode because i don't have enough "
                "leases\n", __func__);
        net_ack_message(ack_handle, 2);
        return;
    }

    bdb_state->dbenv->get_durable_lsn(bdb_state->dbenv, &start_lsn.lsn, 
            &start_lsn.gen);

    bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &current_gen);

    if (start_lsn.gen != current_gen) {
        logmsg(LOGMSG_ERROR, "%s line %d generation-mismatch: current_gen=%d, "
                             "durable_gen=%d\n",
               __func__, __LINE__, current_gen, start_lsn.gen);
        net_ack_message(ack_handle, 3);
        return;
    }

    if (start_lsn.lsn.file == 2147483647) {
        logmsg(LOGMSG_FATAL, "Huh? Durable lsn is 2147483647???\n");
        abort();
    }


    p_buf = buf;
    p_buf_end = p_buf + START_LSN_RESPONSE_TYPE_LEN;

    start_lsn_response_type_put(&start_lsn, p_buf, p_buf_end);

    if (bdb_state->attr->receive_start_lsn_request_trace) {
        logmsg(LOGMSG_USER, "%s returning gen %d lsn[%d][%d]\n", __func__, 
                start_lsn.gen, start_lsn.lsn.file, start_lsn.lsn.offset);
    }

    net_ack_message_payload(ack_handle, 0, buf, START_LSN_RESPONSE_TYPE_LEN);
    return;
}

void receive_coherency_lease(void *ack_handle, void *usr_ptr, char *from_host,
                             int usertype, void *dta, int dtalen,
                             uint8_t is_tcp)
{
    uint8_t *p_buf, *p_buf_end;
    uint64_t base_ts;
    char *master_host;
    int receive_trace;
    bdb_state_type *bdb_state;
    colease_t colease = {0};

    assert(usertype == USER_TYPE_COHERENCY_LEASE);
    p_buf = (uint8_t *)dta;
    p_buf_end = (uint8_t *)(dta + dtalen);

    if (!(colease_type_get(&colease, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s: corrupt colease packet from %s, len=%d\n",
                __func__, from_host, dtalen);
        return;
    }

    bdb_state = usr_ptr;

    receive_trace = bdb_state->attr->receive_coherency_lease_trace;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    master_host = bdb_state->repinfo->master_host;
    if (from_host != master_host) {
        static time_t lastpr = 0;
        time_t now;
        if (receive_trace && ((now = time(NULL)) > lastpr)) {
            logmsg(LOGMSG_WARN, "%s: ignoring coherency lease from non-master %s "
                            "(master is %s)\n",
                    __func__, from_host, master_host);
            lastpr = now;
        }
        return;
    }

    strncpy0(coherency_master, from_host, sizeof(coherency_master));

    /* Choose most conservative possible expiration: the lessor of
     * 'mytime + leasetime' and 'mastertime + leasetime' */
    if ((base_ts = gettimeofday_ms()) < colease.issue_time)
        coherency_timestamp = base_ts + colease.lease_ms;
    else
        coherency_timestamp = colease.issue_time + colease.lease_ms;

    if (coherency_timestamp < base_ts) {
        static uint64_t no_lease_count = 0;
        static int lastpr = 0;
        time_t now;

        no_lease_count++;

        // Useless leases suggest intolerable time-skew..
        if ((now = time(NULL)) > lastpr) {
            logmsg(LOGMSG_WARN,
                   "%s: got useless lease: lease_base=%" PRIu64 " "
                   "my_base=%" PRIu64 " lease_time=%u diff=%" PRIu64
                   " total-useless=%" PRIu64 "\n",
                   __func__, colease.issue_time, base_ts, colease.lease_ms,
                   (base_ts - coherency_timestamp), no_lease_count);
            lastpr = now;
        }
    }
}

/****** btree page compact routines BEGIN ******/

/* The expected node utilization is lg(2), approx. 0.693
   [Yao. On random 2-3 trees. Acta Informatica, 1978].
   If the database is known to have few write activities,
   we can probably make the value higher. */
double gbl_pg_compact_target_ff = 0.693;

/* thread pool runtine */
static void pg_compact_do_work(struct thdpool *pool, void *work, void *thddata)
{
    extern double gbl_pg_compact_thresh;
    pgcomp_rcv_t *arg;
    DB_ENV *dbenv;
    int32_t fileid;
    DBT dbt;

    arg = (pgcomp_rcv_t *)work;
    dbenv = arg->bdb_state->dbenv;
    fileid = arg->id;

    memset(&dbt, 0, sizeof(dbt));
    dbt.data = arg->data;
    dbt.size = arg->size;

    __dbenv_pgcompact(dbenv, fileid, &dbt, gbl_pg_compact_thresh,
                      gbl_pg_compact_target_ff);
}

/* thread pool work function */
static void pg_compact_do_work_pp(struct thdpool *pool, void *work,
                                  void *thddata, int op)
{
    switch (op) {
    case THD_RUN:
        pg_compact_do_work(pool, work, thddata);
        break;
    }
    free(work);
}

/* enqueue a page compact work */
int enqueue_pg_compact_work(bdb_state_type *bdb_state, int32_t fileid,
                            uint32_t size, const void *data)
{
    pgcomp_rcv_t *rcv;
    int rc;

    if (size > PGCOMPMAXLEN || ((sizeof(pgcomp_rcv_t) + size) < size)) {
        logmsg(LOGMSG_WARN, "%s %d: page compaction invalid size: %u.\n",
               __FILE__, __LINE__, size);
        return E2BIG;
    }

    rcv = malloc(sizeof(pgcomp_rcv_t) + size);
    if (rcv == NULL)
        rc = ENOMEM;
    else {
        rcv->bdb_state = bdb_state;
        rcv->id = fileid;
        rcv->size = size;
        memcpy(rcv->data, data, size);

        rc = thdpool_enqueue(gbl_pgcompact_thdpool, pg_compact_do_work_pp, rcv,
                             0, NULL, 0);

        if (rc != 0) {
            logmsg(LOGMSG_ERROR, "%s %d: failed to thdpool_enqueue rc = %d.\n",
                    __FILE__, __LINE__, rc);
            free(rcv);
        }
    }

    return rc;
}

/* page compact thread pool */
struct thdpool *gbl_pgcompact_thdpool;

int pgcompact_thdpool_init(void)
{
    gbl_pgcompact_thdpool = thdpool_create("pgcompactpool", 0);

    thdpool_set_stack_size(gbl_pgcompact_thdpool, (1 << 20));
    thdpool_set_minthds(gbl_pgcompact_thdpool, 1);
    thdpool_set_maxthds(gbl_pgcompact_thdpool, 1);
    thdpool_set_maxqueue(gbl_pgcompact_thdpool, 1000);
    thdpool_set_linger(gbl_pgcompact_thdpool, 10);
    thdpool_set_longwaitms(gbl_pgcompact_thdpool, 10000);
    return 0;
}
/****** btree page compact routines END ******/

void berkdb_receive_msg(void *ack_handle, void *usr_ptr, char *from_host,
                        int usertype, void *dta, int dtalen, uint8_t is_tcp)
{
    bdb_state_type *bdb_state;
    int node = 0;
    int on_off = 0;
    lsn_cmp_type lsn_cmp;
    int in_rep_process_message;
    DB_LSN cur_lsn;
    DB_LSN trunc_lsn;
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    pgcomp_snd_t pgsnd;
    const uint8_t *pgsnd_pl_pos;

    /* get a pointer back to our bdb_state */
    bdb_state = usr_ptr;

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /*ctrace("berkdb_receive_msg: from %s, ut=%d", from_host, usertype);*/
    switch (usertype) {
    case USER_TYPE_YOUARENOTCOHERENT:
        /* This version of comdb2 shouldn't be getting these messages */
        if (ack_handle) {
            net_ack_message(ack_handle, 0);
        }
        break;

    case USER_TYPE_YOUARECOHERENT:
        /* This version of comdb2 shouldn't be getting these messages */
        if (ack_handle) {
            net_ack_message(ack_handle, 0);
        }
        break;

    case USER_TYPE_ADD:
        if (!dta || dtalen < sizeof(int)) {
            logmsg(LOGMSG_ERROR, "%s add called with invalid args\n", __func__);
        } else {
            p_buf = (uint8_t *)dta;
            p_buf_end = ((uint8_t *)dta + dtalen);
            buf_get(&node, sizeof(int), p_buf, p_buf_end);

            print(bdb_state, "adding node %d to sanctioned list\n", node);
            net_add_to_sanctioned(bdb_state->repinfo->netinfo, hostname(node),
                                  0);
        }
        net_ack_message(ack_handle, 0);
        break;

    case USER_TYPE_ADD_NAME:
        if (!dta || ((char *)dta)[dtalen - 1] != '\0') {
            logmsg(LOGMSG_ERROR, "%s add_name called with invalid args\n",
                   __func__);
        } else {
            print(bdb_state, "adding host %s to sanctioned list\n",
                  (char *)dta);
            net_add_to_sanctioned(bdb_state->repinfo->netinfo,
                                  intern((char *)dta), 0);
        }
        net_ack_message(ack_handle, 0);
        break;

    case USER_TYPE_DEL:
        if (!dta || dtalen < sizeof(int)) {
            logmsg(LOGMSG_ERROR, "%s del called with invalid args\n", __func__);
        } else {
            p_buf = (uint8_t *)dta;
            p_buf_end = ((uint8_t *)dta + dtalen);
            buf_get(&node, sizeof(int), p_buf, p_buf_end);

            print(bdb_state, "removing node %d from sanctioned list\n", node);
            net_del_from_sanctioned(bdb_state->repinfo->netinfo,
                                    hostname(node));
        }
        net_ack_message(ack_handle, 0);
        break;

    case USER_TYPE_DEL_NAME:
        if (!dta || ((char *)dta)[dtalen - 1] != '\0') {
            logmsg(LOGMSG_ERROR, "%s del_name called with invalid args\n",
                   __func__);
        } else {
            print(bdb_state, "removing host %s from sanctioned list\n",
                  (char *)dta);
            net_del_from_sanctioned(bdb_state->repinfo->netinfo,
                                    intern((char *)dta));
        }
        net_ack_message(ack_handle, 0);
        break;

    case USER_TYPE_DECOM_DEPRECATED: {
        char *host;
        if (!dta || dtalen < sizeof(int)) {
            logmsg(LOGMSG_ERROR, "%s decom_dep called with bad args\n",
                   __func__);
        } else {
            p_buf = (uint8_t *)dta;
            p_buf_end = ((uint8_t *)dta + dtalen);
            buf_get(&node, sizeof(int), p_buf, p_buf_end);
            logmsg(LOGMSG_DEBUG, "--- got decom for node %d\n", node);
            logmsg(LOGMSG_DEBUG, "acking message\n");
            net_ack_message(ack_handle, 0);
            host = hostname(node);
            osql_decom_node(host);
            net_decom_node(bdb_state->repinfo->netinfo, host);
        }
        break;
    }

    case USER_TYPE_DECOM_NAME_DEPRECATED: {
        char *host;
        if (!dta || ((char *)dta)[dtalen - 1] != '\0') {
            logmsg(LOGMSG_ERROR, "%s decom_name_dep called with invalid args\n",
                   __func__);
        } else {
            logmsg(LOGMSG_DEBUG, "--- got decom for node %s\n", (char *)dta);
            logmsg(LOGMSG_DEBUG, "acking message\n");
            net_ack_message(ack_handle, 0);
            host = intern((char *)dta);
            osql_decom_node(host);
            net_decom_node(bdb_state->repinfo->netinfo, host);
        }
        break;
    }

    case USER_TYPE_ADD_DUMMY: {
        extern pthread_attr_t gbl_pthread_attr_detached;
        pthread_t tid;
        pthread_create(&tid, &gbl_pthread_attr_detached,
                       dummy_add_thread_nodelay, bdb_state);
        net_ack_message(ack_handle, 0);
        break;
    }

    case USER_TYPE_TRANSFERMASTER:
        /* Prevent race against watcher thread. */
        bdb_state->repinfo->dont_elect_untill_time = comdb2_time_epoch() + 5;

        logmsg(LOGMSG_INFO, "transfer master received\n");
        /* Don't ack this - if we get this message we want an election. */
        break;

    case USER_TYPE_TRANSFERMASTER_NAME:
        /* Prevent race against watcher thread. */
        logmsg(LOGMSG_INFO, "transfer master received\n");
        bdb_state->repinfo->dont_elect_untill_time = comdb2_time_epoch() + 5;
        bdb_state->need_to_upgrade = 1;
        net_ack_message(ack_handle, 0);
        break;

    case USER_TYPE_LSNCMP: {
        uint64_t delta;
        const uint64_t one_file = bdb_state->attr->logfilesize;

        p_buf = (uint8_t *)dta;
        p_buf_end = ((uint8_t *)dta + dtalen);

        get_my_lsn(bdb_state, &cur_lsn);

        if (!dta ||
            (bdb_lsn_cmp_type_get(&lsn_cmp, p_buf, p_buf_end) == NULL)) {
            logmsg(LOGMSG_ERROR, "%s lsncmp called with invalid args\n",
                   __func__);
            net_ack_message(ack_handle, 1);
            return;
        }

        bdb_state->dbenv->rep_flush(bdb_state->dbenv);

        logmsg(LOGMSG_INFO, "USER_TYPE_LSNCMP %d %d    %d %d host:%s\n", lsn_cmp.lsn.file,
                cur_lsn.file, lsn_cmp.lsn.offset, cur_lsn.offset, from_host);

        /* if he's ahead he's good */
        if (log_compare(&lsn_cmp.lsn, &cur_lsn) >= 0) {
            net_ack_message(ack_handle, 0);
            break;
        }

        /* we're ahead */
        if (cur_lsn.file == lsn_cmp.lsn.file) {
            delta = cur_lsn.offset - lsn_cmp.lsn.offset;
        } else {
            /* different file */
            delta = (cur_lsn.file - lsn_cmp.lsn.file - 1) * one_file;
            delta += cur_lsn.offset;
            delta += (one_file - lsn_cmp.lsn.offset);
        }

        if (delta > lsn_cmp.delta) {
            net_ack_message(ack_handle, 1);
        } else {
            net_ack_message(ack_handle, 0);
        }
        break;
    }

    case USER_TYPE_REPTRC:
        if (!dta || dtalen < sizeof(int)) {
            logmsg(LOGMSG_ERROR, "%s reptrc called with bad args\n", __func__);
        } else {
            p_buf = (uint8_t *)dta;
            p_buf_end = ((uint8_t *)dta + dtalen);
            buf_get(&on_off, sizeof(int), p_buf, p_buf_end);

            logmsg(LOGMSG_USER, "node %s told me to set rep trace to %d\n",
                   from_host, on_off);

            bdb_state->rep_trace = on_off;
        }

        net_ack_message(ack_handle, 0);
        break;

    case USER_TYPE_INPROCMSG:
        in_rep_process_message = bdb_state->repinfo->in_rep_process_message;
        logmsg(LOGMSG_INFO, "got USER_TYPE_INPROCMSG in_rep_process_message=%d\n",
                in_rep_process_message);

        if (in_rep_process_message)
            net_ack_message(ack_handle, 1);
        else
            net_ack_message(ack_handle, 0);

        break;

    case USER_TYPE_DOWNGRADEANDLOSE: {
        if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost) {
            logmsg(LOGMSG_WARN, "i was told to downgrade and lose\n");
            bdb_state->need_to_downgrade_and_lose = 1;
        }

        net_ack_message(ack_handle, 0);
    } break;

    case USER_TYPE_TCP_TIMESTAMP:
        if (!dta || dtalen < sizeof(ack_info)) {
            logmsg(LOGMSG_ERROR, "%s tcp_timestamp called with bad args\n",
                   __func__);
            return;
        }
        handle_tcp_timestamp(bdb_state, dta, from_host);
        break;

    case USER_TYPE_TCP_TIMESTAMP_ACK:
        if (!dta || dtalen < sizeof(ack_info)) {
            logmsg(LOGMSG_ERROR, "%s tcp_timestamp_ack called with bad args\n",
                   __func__);
            return;
        }
        handle_tcp_timestamp_ack(bdb_state, dta);
        break;

    case USER_TYPE_PING_TIMESTAMP:
        if (!dta || dtalen < sizeof(ack_info)) {
            logmsg(LOGMSG_ERROR, "%s tcp_timestamp_ack called with bad args\n",
                   __func__);
            return;
        }
        handle_ping_timestamp(bdb_state, dta, from_host);
        break;

    case USER_TYPE_ANALYZED_TBL: {
        char tblname[MAXTABLELEN + 1] = {0};
        memcpy(tblname, dta, MIN(dtalen, (sizeof(tblname) - 1)));
        ctrace("MASTER received notification, tbl %s was analyzed\n", tblname);
        reset_aa_counter(tblname);
    } break;

    case USER_TYPE_PAGE_COMPACT:
        p_buf = (uint8_t *)dta;
        p_buf_end = ((uint8_t *)dta + dtalen);
        if (!dta || ((pgsnd_pl_pos = pgcomp_snd_type_get(&pgsnd, p_buf,
                                                         p_buf_end)) == NULL)) {
            logmsg(LOGMSG_ERROR,
                   "%s user_type_lsncmp called with invalid "
                   "args\n",
                   __func__);
            return;
        }
        enqueue_pg_compact_work(bdb_state, pgsnd.id, pgsnd.size, pgsnd_pl_pos);
        break;

    case USER_TYPE_TRUNCATE_LOG:
        if (gbl_physrep_debug == 1)
            logmsg(LOGMSG_USER, "%s:%d processing truncate log message\n", __func__, __LINE__);

        p_buf = (uint8_t *)dta;
        p_buf_end = ((uint8_t *)dta + dtalen);
        if ((db_lsn_type_get(&trunc_lsn, p_buf, p_buf_end)) == NULL) {
            logmsg(LOGMSG_ERROR, "%s %d: failed to get trunc-lsn\n", __func__,
                   __LINE__);
        } else {
            logmsg(LOGMSG_INFO, "Truncating log to %d:%d\n", trunc_lsn.file,
                   trunc_lsn.offset);
            truncate_log_lock(bdb_state, trunc_lsn.file, trunc_lsn.offset, 0);
        }
        net_ack_message(ack_handle, 0);
        break;

    default:
#if 0 
        fprintf(stderr,"%s: unknown message: %d (0x%08X)\n",__func__,usertype, 
                usertype);
#endif
        break;
    }
}

void berkdb_receive_test(void *ack_handle, void *usr_ptr, char *from_host,
                         int usertype, void *dta, int dtalen)
{
    logmsg(LOGMSG_USER, "got req from %s\n", from_host);

    net_ack_message(ack_handle, 0);
}

static void udppfault_do_work_pp(struct thdpool *pool, void *work,
                                 void *thddata, int op);

static void udppfault_do_work(struct thdpool *pool, void *work, void *thddata)
{
    bdb_state_type *bdb_state;
    int32_t fileid;
    unsigned int pgno;
    DB *file_dbp = NULL;
    DB_MPOOLFILE *mpf;
    int ret;

    udppf_rq_t *req = (udppf_rq_t *)work;
    bdb_state = req->bdb_state;
    fileid = req->fileid;
    pgno = req->pgno;

    if ((ret = __dbreg_id_to_db_prefault(bdb_state->dbenv, NULL, &file_dbp,
                                         fileid, 1)) != 0)
        return;

    mpf = file_dbp->mpf;

    touch_page(mpf, pgno);
    if (gbl_prefault_latency > 0)
        sleep(gbl_prefault_latency);

    __dbreg_prefault_complete(bdb_state->dbenv, fileid);
}

void touch_page(DB_MPOOLFILE *mpf, db_pgno_t pgno)
{

    PAGE *pagep;
    int ret = 0;
    if ((ret = __memp_fget(mpf, &pgno, DB_MPOOL_PFGET, &pagep)) != 0) {
        // fprintf(stderr, "touch page: __mem_fget failed with ret: %d\n", ret);
        goto out;
    }

    if ((ret = __memp_fput(mpf, pagep, DB_MPOOL_PFPUT)) != 0) {
        // fprintf(stderr, "touch page: __mem_fput failed with ret: %d\n", ret);
        goto out;
    }

out:
    return;
}

static void touch_page_pp(struct thdpool *pool, void *work, void *thddata,
                          int op)
{

    DB_MPOOLFILE *mpf = ((touch_pg *)work)->mpf;
    db_pgno_t pgno = ((touch_pg *)work)->pgno;

    switch (op) {
    case THD_RUN:
        touch_page(mpf, pgno);
        break;
    }
    free(work);
}

int enqueue_touch_page(DB_MPOOLFILE *mpf, db_pgno_t pgno)
{
    int rc;
    touch_pg *work = (touch_pg *)malloc(sizeof(touch_pg));
    work->mpf = mpf;
    work->pgno = pgno;
    rc =
        thdpool_enqueue(gbl_udppfault_thdpool, touch_page_pp, work, 0, NULL, 0);
    return rc;
}

static void udppfault_do_work_pp(struct thdpool *pool, void *work,
                                 void *thddata, int op)
{
    udppf_rq_t *req = (udppf_rq_t *)work;

    switch (op) {
    case THD_RUN:
        udppfault_do_work(pool, work, thddata);
        break;
    }
    free(req);
}

int enque_udppfault_filepage(bdb_state_type *bdb_state, unsigned int fileid,
                             unsigned int pgno)
{
    udppf_rq_t *qdata = NULL;
    int rc;

    qdata = calloc(1, sizeof(udppf_rq_t));
    if (qdata == NULL) {
        logmsg(LOGMSG_FATAL, "failed to malloc udp prefault request\n");
        exit(1);
    }

    qdata->bdb_state = bdb_state;
    qdata->fileid = fileid;
    qdata->pgno = pgno;

    rc = thdpool_enqueue(gbl_udppfault_thdpool, udppfault_do_work_pp, qdata, 0,
                         NULL, 0);

    if (rc != 0) {
        free(qdata);
    }
    return rc;
}

#define MIN(a, b) (((a) < (b)) ? (a) : (b))

int gbl_commit_delay_copy_ms = 0;
int gbl_commit_delay_timeout = 10;
static int64_t commit_delay_timeout = 0;

int bdb_commitdelay(void *arg)
{
    bdb_state_type *bdb_state = arg;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (bdb_state->attr->commitdelaymax <= 0)
        return 0;

    if (bdb_state->attr->commitdelay > 0)
        return MIN(bdb_state->attr->commitdelay, bdb_state->attr->commitdelaymax);

    if (time(NULL) < commit_delay_timeout)
        return MIN(gbl_commit_delay_copy_ms, bdb_state->attr->commitdelaymax);

    return 0;
}

static int berkdb_receive_rtn_int(void *ack_handle, void *usr_ptr,
                                  char *from_node, int usertype, void *dta,
                                  int dtalen, uint8_t is_tcp)
{
    char *recbuf;
    char *controlbuf;
    uint8_t *p_buf, *p_buf_end;
    int recbufsz;
    int controlbufsz;
    int recbufcrc;
    int controlbufcrc;
    struct rep_type_berkdb_rep_ctrlbuf_hdr p_rep_type_berkdb_rep_ctrlbuf_hdr ={0};
    struct rep_type_berkdb_rep_buf_hdr p_rep_type_berkdb_rep_buf_hdr = {0};
    struct rep_type_berkdb_rep_seqnum p_rep_type_berkdb_rep_seqnum ={0};
    DBT rec;
    DBT control;
    bdb_state_type *bdb_state;
    int rc;
    int seqnum;
    int outrc = 0;
    seqnum_type berkdb_seqnum;
    int filenum = 0;
    unsigned long long master_cmpcontext;

    outrc = 0;

    /* get a pointer back to our bdb_state */
    bdb_state = usr_ptr;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /* this DOES NOT NEED TO BE LOCKED, dont get scared.  for testing,
       its much easier if i serialize things, so i keep this commented
       lock/unlock pair around in the code */
    /*Pthread_mutex_lock(&bdb_state->repinfo->receive_lock);*/

    /*fprintf( stderr, "%s:%d received %d\n", __FILE__, __LINE__, usertype);*/

    switch (usertype) {
    case USER_TYPE_BERKDB_REP:

        p_buf = dta;
        p_buf_end = ((uint8_t *)dta + dtalen);

        p_buf = (uint8_t *)rep_type_berkdb_rep_seqnum_get(
            &p_rep_type_berkdb_rep_seqnum, p_buf, p_buf_end);

        seqnum = p_rep_type_berkdb_rep_seqnum.seqnum;

        p_buf = (uint8_t *)rep_type_berkdb_rep_buf_hdr_get(
            &p_rep_type_berkdb_rep_buf_hdr, p_buf, p_buf_end);

        recbufsz = p_rep_type_berkdb_rep_buf_hdr.recbufsz;
        recbufcrc = p_rep_type_berkdb_rep_buf_hdr.recbufcrc;

        recbuf = (char *)p_buf;
        p_buf += recbufsz;

        p_buf = (uint8_t *)rep_type_berkdb_rep_ctrlbuf_hdr_get(
            &p_rep_type_berkdb_rep_ctrlbuf_hdr, p_buf, p_buf_end);

        controlbufsz = p_rep_type_berkdb_rep_ctrlbuf_hdr.controlbufsz;
        controlbufcrc = p_rep_type_berkdb_rep_ctrlbuf_hdr.controlbufcrc;

        controlbuf = (char *)p_buf;
        p_buf += controlbufsz;

        /* perform some reasonable sanity checks on the stuff that
           came from the network */

        if (p_buf - ((uint8_t *)dta) > dtalen) {
            logmsg(LOGMSG_ERROR, "buf-dta != dtalen\n");
            logmsg(LOGMSG_ERROR, "%p %p %d\n", p_buf, dta, dtalen);
            return -1;
        }

        if ((controlbufsz + recbufsz) > dtalen) {
            logmsg(LOGMSG_ERROR, "controlbufsz+recbufsz too big\n");
            return -1;
        }

        if (bdb_state->rep_trace) {
            DB_LSN tmp;

            tmp.file = ntohl(((rep_control_type *)controlbuf)->lsn.file);
            tmp.offset = ntohl(((rep_control_type *)controlbuf)->lsn.offset);

            logmsg(LOGMSG_USER, "--- berkdb_receive_routine: received seq %d from "
                            "%s (rectype %d lsn %d:%d)\n",
                    seqnum, from_node,
                    ntohl(((rep_control_type *)controlbuf)->rectype), tmp.file,
                    tmp.offset);
        }

        bzero(&rec, sizeof(DBT));
        bzero(&control, sizeof(DBT));

        rec.size = recbufsz;
        rec.data = recbuf;

        control.size = controlbufsz;
        control.data = controlbuf;

        if (bdb_state->attr->repchecksum) {
            /*fprintf(stderr, "2) repchecksum\n");*/

            if (crc32c(rec.data, rec.size) != recbufcrc) {
                logmsg(LOGMSG_ERROR, "CRC MISMATCH on rec\n");
                return -1;
            }

            if (crc32c(control.data, control.size) != controlbufcrc) {
                logmsg(LOGMSG_ERROR, "CRC MISMATCH on control\n");
                return -1;
            }
        }

        rc = process_berkdb(bdb_state, from_node, &control, &rec);
        if (rc > 0) {
            outrc = rc;
            logmsg(LOGMSG_ERROR, "bad rc %d from process_berkdb\n", rc);
        } else {
            outrc = 0;
        }

        break;

    case USER_TYPE_BERKDB_FILENUM:
        p_buf = dta;
        p_buf_end = ((uint8_t *)dta + dtalen);

        p_buf = (uint8_t *)buf_get(&filenum, sizeof(filenum), p_buf, p_buf_end);
        bdb_state->seqnum_info->filenum[nodeix(from_node)] = filenum;
        break;

    case USER_TYPE_BERKDB_NEWSEQ:
        p_buf = dta;
        p_buf_end = ((uint8_t *)dta + dtalen);
        p_buf = (uint8_t *)rep_berkdb_seqnum_type_get(&berkdb_seqnum, p_buf,
                                                      p_buf_end);

        got_new_seqnum_from_node(bdb_state, &berkdb_seqnum, from_node, is_tcp);
        break;

    case USER_TYPE_COMMITDELAYTIMED:
        commit_delay_timeout = time(NULL) + gbl_commit_delay_timeout;
        logmsg(LOGMSG_WARN, "--- got commitdelaytimed req from node %s.  timeout=%" PRId64 "\n", from_node,
               commit_delay_timeout);
        break;

    case USER_TYPE_COMMITDELAYMORE:
        if (bdb_state->attr->commitdelay == 0)
            bdb_state->attr->commitdelay = 1;
        else
            bdb_state->attr->commitdelay *= 2;

        if (bdb_state->attr->commitdelay > bdb_state->attr->commitdelaymax)
            bdb_state->attr->commitdelay = bdb_state->attr->commitdelaymax;

        logmsg(LOGMSG_WARN, "--- got commitdelaymore req from node %s.  now %d\n",
                from_node, bdb_state->attr->commitdelay);

        break;

    case USER_TYPE_COMMITDELAYNONE:
        logmsg(LOGMSG_WARN, "--- got commitdelaynone req from node %s\n",
                from_node);

        bdb_state->attr->commitdelay = 0;
        send_context_to_all(bdb_state);
        logmsg(gbl_commit_delay_trace ? LOGMSG_USER : LOGMSG_WARN,
               "-- setting commitdelay to 0 on COMMITDELAYNONE\n");
        break;

    case USER_TYPE_GETCONTEXT:
        logmsg(LOGMSG_DEBUG, "USER_TYPE_GETCONTEXT\n");
        break;

    case USER_TYPE_HEREISCONTEXT:
        logmsg(LOGMSG_DEBUG, "USER_TYPE_HEREISCONTEXT\n");
        break;

    case USER_TYPE_GBLCONTEXT:
        if (dtalen >= sizeof(unsigned long long)) {
            memcpy(&master_cmpcontext, dta, sizeof(unsigned long long));
            if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost) {
                bdb_state->got_gblcontext = 1;
                set_gblcontext(bdb_state, master_cmpcontext);
            }
        }
        break;

    case USER_TYPE_MASTERCMPCONTEXTLIST:
        break;

    default:
        logmsg(LOGMSG_ERROR, "berkdb_receive_rtn_int: unknown type %d\n", usertype);
        break;
    }

    /*Pthread_mutex_unlock(&bdb_state->repinfo->receive_lock);*/
    return outrc;
}

void berkdb_receive_rtn(void *ack_handle, void *usr_ptr, char *from_host,
                        int usertype, void *dta, int dtalen, uint8_t is_tcp)
{
    bdb_state_type *bdb_state;
    int rc;

    /* get a pointer back to our bdb_state */
    bdb_state = usr_ptr;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

again:
    BDB_READLOCK("berkdb_receive_rtn");

    rc = berkdb_receive_rtn_int(ack_handle, usr_ptr, from_host, usertype, dta,
                                dtalen, is_tcp);
    /* If we get a deadlock from processing an update, release the lock,
     * poll (to give the watcher time to process a re-open if the DEADLOCK
     * was really a REP_HANDLE_DEAD), then try again */
    if (rc == DB_LOCK_DEADLOCK) {
        BDB_RELLOCK();
        poll(NULL, 0, 100);
        goto again;
    }

    BDB_RELLOCK();
}

void send_downgrade_and_lose(bdb_state_type *bdb_state)
{
    int rc;

    rc = 0;

    rc = net_send_message(
        bdb_state->repinfo->netinfo, bdb_state->repinfo->master_host,
        USER_TYPE_DOWNGRADEANDLOSE, &rc, sizeof(int), 1, 10 * 1000);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "send_downgrade_and_lose rc %d\n", rc);
    }
}

extern int gbl_dump_locks_on_repwait;
extern int gbl_dump_sql_on_repwait_sec;
extern int gbl_lock_get_list_start;
int bdb_clean_pglogs_queues(bdb_state_type *bdb_state, DB_LSN lsn,
                            int truncate);

int request_delaymore(void *bdb_state_in)
{
    int rc;
    bdb_state_type *bdb_state = (bdb_state_type *)bdb_state_in;
    rc = net_send_flags(bdb_state->repinfo->netinfo,
                        bdb_state->repinfo->master_host,
                        USER_TYPE_COMMITDELAYMORE, NULL, 0, NET_SEND_NODROP | NET_SEND_NODELAY);
    return rc;
}

int gbl_rep_wait_core_ms = 0;

#include <signal.h>

static void abort_stalled_exit(int signum)
{
    logmsg(LOGMSG_WARN, "Aborting stalled watchdog alarm\n");
    raise(SIGABRT);
    _exit(1);
}

void bdb_dump_threads_and_maybe_abort(bdb_state_type *bdb_state, int watchdog,
                                      int fatal)
{
    if (watchdog || fatal) {
        if (fatal) {
            signal(SIGALRM, abort_stalled_exit);
        }
        alarm(60);
        logmsg(LOGMSG_FATAL, "Getting ready to die, printing useful debug info.\n");
    }
    if (bdb_state->callback->threaddump_rtn) (bdb_state->callback->threaddump_rtn)();
    lock_info_lockers(stderr, bdb_state);
    thd_dump();
    pstack_self();
    if (fatal) abort();
}

void *watcher_thread(void *arg)
{
    bdb_state_type *bdb_state;
    extern int gbl_rep_lock_time_ms;
    extern int gbl_truncating_log;
    char *master_host = db_eid_invalid;
    int stopped_count = 0;
    int i;
    int j;
    int time_now, time_then;
    int rc;
    int master_is_bad = 0;
    int done = 0;
    char *rep_master = 0;
    int list_start;

    gbl_watcher_thread_ran = comdb2_time_epoch();

    thrman_register(THRTYPE_WATCHDOG);
    thread_started("bdb watcher");

    /* hold off on "watching" for a little bit during startup */
    sleep(5);

    bdb_state = (bdb_state_type *)arg;
    bdb_thread_event(bdb_state, BDBTHR_EVENT_START_RDONLY);

    /* if we were passed a child, find his parent */
    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    print(bdb_state, "watcher_thread started as 0x%"PRIxPTR"\n",
          (intptr_t)pthread_self());

    poll(NULL, 0, (rand() % 100) + 1000);

    i = 0;
    j = 0;

    bdb_state->repinfo->disable_watcher = 0;

    while (!db_is_stopped()) {
        time_now = comdb2_time_epoch();
        time_then = bdb_state->repinfo->disable_watcher;

        if (time_now < time_then) {
            int diff;
            diff = time_then - time_now;

            /* safeguard - no way are we gonna pause more than a minute here */
            if (diff > 60)
                diff = 60;
            logmsg(LOGMSG_WARN, "watcher thread pausing for %d second\n", diff);
            sleep(diff);
        }

        if (gbl_truncating_log) {
            stopped_count++;
            if (stopped_count > gbl_long_log_truncation_abort_thresh_sec) {
                logmsg(LOGMSG_FATAL,
                       "%s: truncating log for %d seconds, aborting\n",
                       __func__, stopped_count);
                abort();
            }
            if (stopped_count > gbl_long_log_truncation_warn_thresh_sec) {
                logmsg(LOGMSG_WARN, "%s: truncating log for %d seconds\n",
                       __func__, stopped_count);
            }
            sleep(1);
            gbl_watcher_thread_ran = comdb2_time_epoch();
            continue;
        }
        stopped_count = 0;

        i++;
        j++;

        BDB_READLOCK("watcher_thread");

        if (!gbl_new_snapisol_asof) {
            DB_LSN lsn = {0};
            bdb_clean_pglogs_queues(bdb_state, lsn, 0);
        }

        if (bdb_state->attr->coherency_lease &&
            !bdb_state->coherency_lease_thread) {
            create_coherency_lease_thread(bdb_state);
        }

        if (bdb_state->attr->master_lease && !bdb_state->master_lease_thread) {
            create_master_lease_thread(bdb_state);
        }

        int rep_lock_wait_time_ms = gbl_rep_lock_time_ms;
        int rep_wait_core_ms = gbl_rep_wait_core_ms;
        int elapsed;

        if (rep_wait_core_ms && rep_lock_wait_time_ms &&
            (elapsed = (comdb2_time_epochms() - rep_lock_wait_time_ms)) >
                rep_wait_core_ms) {
            logmsg(LOGMSG_FATAL,
                   "%s: coring, rep thread blocked too long (%d ms)\n",
                   __func__, elapsed);
            bdb_dump_threads_and_maybe_abort(bdb_state, 0, 1);
        }

        /* are we incoherent?  see how we're doing, let's send commitdelay
           if we are falling far behind */
        if (!bdb_am_i_coherent_int(bdb_state)) {
            int last_behind = INT_MAX;
            int num_times_behind = 0;
            DB_LSN my_lsn = {0}, master_lsn = {0};
            get_my_lsn(bdb_state, &my_lsn);
            get_master_lsn(bdb_state, &master_lsn);
            int behind = subtract_lsn(bdb_state, &master_lsn, &my_lsn);
            if (behind > bdb_state->attr->commitdelaybehindthresh) {
                if (behind > last_behind) /* we are falling further behind */
                    num_times_behind++;
                else
                    num_times_behind = 0;

                if (num_times_behind > bdb_state->attr->numtimesbehind) {
                    logmsg(LOGMSG_WARN, "i am incoherent and falling behind\n");

                    if (bdb_state->attr->enable_incoherent_delaymore) {
                        int rc = request_delaymore(bdb_state);
                        if (rc != 0) {
                            logmsg(LOGMSG_ERROR,
                                   "failed to send COMMITDELAYMORE to %s\n",
                                   bdb_state->repinfo->master_host);
                        } else if (gbl_commit_delay_trace) {
                            logmsg(LOGMSG_USER,
                                   "%s line %d requested COMMITDELAYMORE\n",
                                   __func__, __LINE__);
                        }
                    }

                    num_times_behind = 0;
                }

                last_behind = behind;
            }
        }

        /* are we the master?  do we have lots of incoherent nodes? */
        if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost) {
            int count;
            const char *hostlist[REPMAX];
            int num_skipped;

            /*
             * Don't mess with VOODOO:  rep_flush grabs the last logfile
             * message,
             * and broadcasts it to all nodes
             */
            bdb_state->dbenv->rep_flush(bdb_state->dbenv);

            num_skipped = 0;

            count = net_get_all_nodes_connected(bdb_state->repinfo->netinfo,
                                                hostlist);

            for (i = 0; i < count; i++)
                if (is_incoherent(bdb_state, hostlist[i]))
                    num_skipped++;

            if (num_skipped >= bdb_state->attr->toomanyskipped) {
                /* too many guys being skipped, let's take drastic measures!
                 * delay ourselves */
                if (bdb_state->attr->commitdelay <
                    bdb_state->attr->skipdelaybase) {
                    bdb_state->attr->commitdelay =
                        bdb_state->attr->skipdelaybase;
                    if (bdb_state->attr->commitdelay > bdb_state->attr->commitdelaymax)
                        bdb_state->attr->commitdelay = bdb_state->attr->commitdelaymax;
                    if (gbl_commit_delay_trace) {
                        logmsg(LOGMSG_USER,
                               "%s line %d setting commitdelay to "
                               "skipdelaybase %d\n",
                               __func__, __LINE__,
                               bdb_state->attr->skipdelaybase);
                    }
                }
            }

            if (bdb_state->attr->track_replication_times) {
                int now;
                now = comdb2_time_epochms();
                Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
                for (i = 0; i < count; i++) {
                    if (bdb_state->seqnum_info
                            ->time_10seconds[nodeix(hostlist[i])])
                        averager_purge_old(
                            bdb_state->seqnum_info
                                ->time_10seconds[nodeix(hostlist[i])],
                            now);
                    if (bdb_state->seqnum_info
                            ->time_minute[nodeix(hostlist[i])])
                        averager_purge_old(
                            bdb_state->seqnum_info
                                ->time_minute[nodeix(hostlist[i])],
                            now);
                }
                Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));
            }
        }

        net_timeout_watchlist(bdb_state->repinfo->netinfo);

        if ((bdb_state->passed_dbenv_open) &&
            (bdb_state->repinfo->rep_process_message_start_time)) {
            if (comdb2_time_epoch() - bdb_state->repinfo->rep_process_message_start_time > 10) {
                logmsg(LOGMSG_WARN, "rep_process_message running for 10 seconds, dumping thread pool to trc.c\n");
                gbl_logmsg_ctrace = 1;
                bdb_dump_threads_and_maybe_abort(bdb_state, 0, 0);
                gbl_logmsg_ctrace = 0;
                bdb_state->repinfo->rep_process_message_start_time = 0;
            }
            if ((comdb2_time_epoch() - bdb_state->repinfo->rep_process_message_start_time) > gbl_dump_sql_on_repwait_sec) {
                comdb2_dump_blockers(bdb_state->dbenv);
            }
        }

        list_start = gbl_lock_get_list_start;
        if (gbl_dump_locks_on_repwait && list_start > 0 &&
            (time(NULL) - list_start) >= 3) {
            logmsg(LOGMSG_USER, "Long wait on replicant getting locks:\n");
            bdb_dump_threads_and_maybe_abort(bdb_state, 0, 0);
        }

        if (bdb_state->exiting) {
            print(bdb_state, "watcher_thread: exiting\n");

            BDB_RELLOCK();

            bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDONLY);
            pthread_exit(NULL);
        }

        master_host = bdb_state->repinfo->master_host;
        bdb_get_rep_master(bdb_state, &rep_master, NULL, NULL);

        if (bdb_state->caught_up) {
            /* periodically send info too all nodes about our curresnt LSN and
               the current logfile we are on */
            send_myseqnum_to_all(bdb_state, 0);
        }

        BDB_RELLOCK();

        /*
           this is the logic.  i dont think there is a "right" solution.

           1) if you are marked down, try to yield master.
                UNLESS all the other nodes are marked down.

           2) if you are marked up, AND you notice that the master is marked
              down, tell the master to downgrade and lose the election.

           */

        /* if we are rtcpued off and are master, downgrade ourselves */
        if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost) {
            if (rep_master == bdb_state->repinfo->myhost) {
                if (bdb_state->callback->nodeup_rtn) {
                    if (!(bdb_state->callback->nodeup_rtn(
                            bdb_state, bdb_state->repinfo->myhost))) {
                        /* don't downgrade if not in a cluster as then we will
                           needlessly flip in and out of read-only state until
                           the node is routed on again.

                           For this case, if there's not another node available,
                           this node will stay master. */
                        int num = net_count_nodes(bdb_state->repinfo->netinfo);
                        if (num > 1 &&
                            bdb_state->attr->allow_offline_upgrades) {
                            int count;
                            const char *hostlist[REPMAX];

                            /* Check all nodes.  Transfer master if one is
                             * online. */
                            count = net_get_all_nodes_connected(
                                bdb_state->repinfo->netinfo, hostlist);

                            for (i = 0; i < count; i++) {
                                if ((bdb_state->callback->nodeup_rtn)(
                                        bdb_state, hostlist[i])) {
                                    logmsg(LOGMSG_WARN, 
                                        "transfering master because im rtcpued"
                                        "off and another node is available\n");
                                    bdb_transfermaster(bdb_state);

                                    break;
                                }
                            }
                        }
                        /* If there's no other node available, transfermaster
                         * will call
                         * for an election.  */
                        else if (num > 1) {
                            logmsg(LOGMSG_WARN, 
                                   "transfering master because im rtcpued off\n");
                            bdb_transfermaster(bdb_state);
                        } else {
                            /* Stay master if you are a single node.  Local
                               processes
                               will still be able to write.  */
                        }
                    }
                }
            } else {
                /* mismatch between master_host and rep_master*/
                bdb_downgrade(bdb_state, 0, NULL);
            }
        }

        /* if the master is marked down, and we are not, try to become master */
        if (bdb_state->callback->nodeup_rtn) {
            char *mynode;

            mynode = bdb_state->repinfo->myhost;

            /*
               IF
                 there is a master      AND
                 it is not us           AND
                 it is marked down      AND
                 we are marked up
               THEN
                 tell it to yield.
             */

            master_host = bdb_state->repinfo->master_host;

            if ((master_host != mynode) &&
                (!(bdb_state->callback->nodeup_rtn)(bdb_state, master_host)) &&
                ((bdb_state->callback->nodeup_rtn)(bdb_state, mynode))) {
                master_is_bad++;

                logmsg(LOGMSG_WARN,
                       "master %s is marked down and i am up telling him to "
                       "yield\n",
                       master_host);
                send_downgrade_and_lose(bdb_state);
                /* Don't call for election- the other node will transfer
                 * master. */
            } else {
                master_is_bad = 0;
            }
        }

        /* This is for the test-only upgrade codepath */
        if (bdb_state->need_to_upgrade) {

            /* if we're already master, like if the election thread promoted us,
               skip this step */
            if (master_host != bdb_state->repinfo->myhost) {
                logmsg(LOGMSG_INFO, "calling bdb_upgrade because we were told to\n");

                rc = bdb_upgrade(bdb_state, 0, &done);
                if (rc != 0) {
                    logmsg(LOGMSG_ERROR, "got %d from bdb_upgrade%s\n", rc,
                            (!done) ? " (nop)" : "");
                } else {
                    bdb_setmaster(bdb_state, bdb_state->repinfo->myhost);
                }
            } else {
                logmsg(LOGMSG_INFO, "%s:%d skipping upgrade since we're already the master\n",
                    __FILE__, __LINE__);
            }

            bdb_state->need_to_upgrade = 0;
        }

        if (bdb_state->need_to_downgrade_and_lose) {
            bdb_state->need_to_downgrade_and_lose = 0;

            /*
               signal to db layer we are rt-ed off
               this is extremely helpful for canceling
               long sql (blocksql) requests
             */
            if (bdb_state->signal_rtoff) {
                bdb_state->signal_rtoff();
            }

            /* if we're already downgraded, like if we've detected that
               we're routed off before getting traps from neighbours,
             calling bdb_transfermaster */
            if (master_host == bdb_state->repinfo->myhost) {
                logmsg(LOGMSG_WARN, "transfering master because i was told to "
                                "downgrade and lose\n");
                bdb_transfermaster(bdb_state);
            } else {
                logmsg(LOGMSG_INFO, "%s:%d skipping master transfer, we've already "
                                "downgraded\n",
                        __FILE__, __LINE__);
            }
        }

        /* downgrade ourselves if we are in a dupmaster situation */
        if (master_host == bdb_master_dupe) {
            print(bdb_state, "calling bdb_downgrade\n");
            bdb_downgrade(bdb_state, 0, NULL);
            print(bdb_state, "back from bdb_downgrade\n");
        }

        /* call for an election if we don't have a master */
        if (master_host == db_eid_invalid) {

            /* we want to alert if NO MASTER for a "significant" number of
             * seconds */
            if (!gbl_lost_master_time) {
                gbl_lost_master_time = time(NULL);
                gbl_ignore_lost_master_time =
                    0; /* new loss, reactivate trace */
            } else {
                if (!gbl_ignore_lost_master_time) {
                    time_t crt = time(NULL);
                    if (crt >= (gbl_lost_master_time +
                                bdb_state->attr->nomaster_alert_seconds)) {
                        logmsg(LOGMSG_WARN, "NOMASTER FOR %ld seconds\n",
                               crt - gbl_lost_master_time);
                        gbl_ignore_lost_master_time = 1;
                    }
                }
            }

            /* try to re-establish connections to everyone after a few failures */
            if (i > 10) {
                connect_to_all(bdb_state->repinfo->netinfo);
                i = 0;
            }
            if (!bdb_state->repinfo->in_election) {
                call_for_election(bdb_state, __func__, __LINE__);
            }
        }

        if (bdb_state->rep_handle_dead) {
            logmsg(LOGMSG_WARN, "watcher found rep_handle_dead");

            bdb_state->rep_handle_dead = 0;
            bdb_reopen(bdb_state, __func__, __LINE__);
        }

#if 0   /* we just did this above */
        /* check if the master is db_eid_invalid and call election is so */
        if (!bdb_state->repinfo->in_election) {
            if (rep_master == db_eid_invalid)
                call_for_election(bdb_state, __func__, __LINE__);
        }
#endif

        /* check if some thread has called close_hostnode
           and got a pending_seqnum_broadcast, in which case
           we broadcast here */
        Pthread_mutex_lock(&bdb_state->pending_broadcast_lock);
        if (bdb_state->pending_seqnum_broadcast) {
            Pthread_mutex_lock(&(bdb_state->seqnum_info->lock));
            Pthread_cond_broadcast(&(bdb_state->seqnum_info->cond));
            Pthread_mutex_unlock(&(bdb_state->seqnum_info->lock));

            bdb_state->pending_seqnum_broadcast = 0;
        }
        Pthread_mutex_unlock(&bdb_state->pending_broadcast_lock);

        int log_delete_is_stopped(void);
        if (log_delete_is_stopped()) {
            logmsg(LOGMSG_WARN, "Logdelete stopped, requesting copy commit-delay\n");
            if (rep_master == bdb_state->repinfo->myhost) {
                commit_delay_timeout = time(NULL) + gbl_commit_delay_timeout;
                logmsg(LOGMSG_WARN, "--- delaying commits for copy on master.  timeout=%"PRId64"\n",
                    commit_delay_timeout);
            } else {
                request_copydelay(bdb_state);
            }
        }

        /* check if we have connections to everyone in the sanc list */
        bdb_state->sanc_ok =
            net_sanctioned_list_ok(bdb_state->repinfo->netinfo);

        gbl_watcher_thread_ran = comdb2_time_epoch();

        if (db_is_exiting())
            break;

        /* sleep for somewhere between 1-2 seconds */
        poll(NULL, 0, (rand() % 1000) + 1000);

        send_context_to_all(bdb_state);
    }

    bdb_thread_event(bdb_state, BDBTHR_EVENT_DONE_RDONLY);
    return NULL;
}

int bdb_wait_for_seqnum_from_n(bdb_state_type *bdb_state, seqnum_type *seqnum,
                               int n)
{
    int num_acks = 0;
    const char *connlist[REPMAX];
    int numnodes;
    int i;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    while (num_acks < n) {
        num_acks = 0;
        numnodes =
            net_get_all_nodes_connected(bdb_state->repinfo->netinfo, connlist);
        Pthread_mutex_lock(&bdb_state->seqnum_info->lock);
        for (i = 0; i < numnodes; i++) {
            if (bdb_seqnum_compare(
                    bdb_state,
                    &bdb_state->seqnum_info->seqnums[nodeix(connlist[i])],
                    seqnum) >= 0) {
                num_acks++;
            }
        }
        if (num_acks < n)
            Pthread_cond_wait(&bdb_state->seqnum_info->cond,
                              &bdb_state->seqnum_info->lock);
        Pthread_mutex_unlock(&bdb_state->seqnum_info->lock);
    }
    return 0;
}

void bdb_set_rep_handle_dead(bdb_state_type *bdb_state)
{
    bdb_state->rep_handle_dead = 1;
}

int bdb_master_should_reject(bdb_state_type *bdb_state)
{
    int time_now;
    int should_reject;

    if (!bdb_state->attr->master_reject_requests)
        return 0;

    BDB_READLOCK("bdb_master_should_reject");

    if (bdb_state->repinfo->master_host != bdb_state->repinfo->myhost) {
        BDB_RELLOCK();
        return 0;
    }

    time_now = comdb2_time_epoch();

    if ((time_now - bdb_state->repinfo->should_reject_timestamp) > 10) {
        int count;
        const char *hostlist[REPMAX];
        int total;
        int i;

        bdb_state->repinfo->should_reject_timestamp = time_now;

        if (bdb_state->attr->master_reject_sql_ignore_sanc) {
            count = net_get_all_nodes_connected(bdb_state->repinfo->netinfo,
                                                hostlist);
        } else {
            count = net_sanctioned_and_connected_nodes(
                bdb_state->repinfo->netinfo,
                sizeof(hostlist) / sizeof(hostlist[0]), hostlist);
        }

        total = count;

        for (i = 0; i < count; i++)
            if (is_incoherent(bdb_state, hostlist[i]))
                total--;

        /* if we have someone else who can handle this request */
        if (total > 0)
            bdb_state->repinfo->should_reject = 1;
        else
            bdb_state->repinfo->should_reject = 0;
    }

    should_reject = bdb_state->repinfo->should_reject;

    BDB_RELLOCK();

    return should_reject;
}

int bdb_debug_logreq(bdb_state_type *bdb_state, int file, int offset)
{
    DB_LSN lsn;
    lsn.file = file;
    lsn.offset = offset;

    __rep_send_message(bdb_state->dbenv, bdb_state->repinfo->master_host,
                       REP_LOG_REQ, &lsn, NULL, 0, NULL);

    return 0;
}

// Piggy-backing durable LSN requests doesn't work: each thread must make a
// SEPARATE request for a durable LSN.  Here's the counter-example:
//
// 1. Thread A makes a request for a durable LSN from the master - it goes to
//    the master and is stalled on it's way back to the replicant
// 2. Thread B writes a record durably
// 3. Thread C make a request for a durable LSN- instead of going to the master
//    directly, it gloms onto the already outstanding durable LSN request, and
//    retrieves the previous durable LSN
//
// .. Because Thread C started AFTER Thread B, it should see a durable LSN
//    corresponding to B's writes
//

int request_durable_lsn_from_master(bdb_state_type *bdb_state, 
        uint32_t *durable_file, uint32_t *durable_offset, 
        uint32_t *durable_gen) {

    const uint8_t *p_buf, *p_buf_end;
    DB_LSN durable_lsn;
    uint8_t *buf = NULL;
    uint32_t current_gen;
    int data = 0, buflen = 0, waitms = bdb_state->attr->durable_lsn_request_waitms, rc;
    int request_durable_lsn_trace = bdb_state->attr->request_durable_lsn_trace;
    start_lsn_response_t start_lsn;
    static time_t lastpr = 0;
    time_t now;
    uint64_t start_time, end_time;
    static uint32_t goodcount = 0, badcount = 0;

    if (waitms <= 0) 
        waitms = 1000;

    if (bdb_state->repinfo->master_host == bdb_state->repinfo->myhost) {
        if (bdb_state->attr->master_lease && !verify_master_leases(bdb_state, 
                    __func__, __LINE__)) {
            logmsg(LOGMSG_ERROR, "%s line %d failed verifying master leases\n", 
                    __func__, __LINE__);
            badcount++;
            return -2;
        }

        bdb_state->dbenv->get_durable_lsn(bdb_state->dbenv, &durable_lsn, durable_gen);
        bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &current_gen);

        if (current_gen != *durable_gen) {
            logmsg(LOGMSG_ERROR, "%s line %d master generation-mismatch: "
                                 "current_gen=%d, durable_gen=%d\n",
                   __func__, __LINE__, current_gen, *durable_gen);
            badcount++;
            return -3;
        }

        *durable_file = durable_lsn.file;
        *durable_offset = durable_lsn.offset;

        if (request_durable_lsn_trace && ((now = time(NULL)) > lastpr)) {
            logmsg(LOGMSG_USER, "%s executed on local-machine, durable lsn is gen %d [%d][%d] "
                    "good-count=%u bad-count=%u\n", __func__, *durable_gen, *durable_file, 
                    *durable_offset, goodcount, badcount);
            lastpr = now;
        }

        return 0;
    }

    start_time = gettimeofday_ms();
    if ((rc = net_send_message_payload_ack(
             bdb_state->repinfo->netinfo, bdb_state->repinfo->master_host,
             USER_TYPE_REQ_START_LSN, (void *)&data, sizeof(data),
             (uint8_t **)&buf, &buflen, 1, waitms)) != 0) {
        end_time = gettimeofday_ms();
        if (rc == NET_SEND_FAIL_TIMEOUT) {
            logmsg(LOGMSG_WARN,
                   "%s line %d: timed out waiting for start_lsn from master %s "
                   "after %" PRIu64 " ms\n",
                   __func__, __LINE__, bdb_state->repinfo->master_host,
                   end_time - start_time);
        }
        else {
            logmsg(LOGMSG_USER, "%s line %d: net_send_message_payload_ack returns %d\n",
                    __func__, __LINE__, rc);
        }
        badcount++;
        return rc;
    }

    if (buflen < START_LSN_RESPONSE_TYPE_LEN) {
        logmsg(LOGMSG_ERROR, "%s line %d: payload size to small: len is %d, i want"
                " at least %d\n", __func__, __LINE__, buflen, 
                START_LSN_RESPONSE_TYPE_LEN);
        if (buf)
            free(buf);
        badcount++;
        return -1;
    }

    p_buf = buf;
    p_buf_end = p_buf + buflen;

    if (!(p_buf = start_lsn_response_type_get(&start_lsn, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s line %d error unpacking start_lsn\n", __func__, 
                __LINE__);
        if (buf)
            free(buf);
        badcount++;
        return -2;
    }

    (*durable_file) = start_lsn.lsn.file;
    (*durable_offset) = start_lsn.lsn.offset;
    (*durable_gen) = start_lsn.gen;
    free(buf);

    goodcount++;
    if (request_durable_lsn_trace && ((now = time(NULL)) > lastpr)) {
        logmsg(LOGMSG_USER, "%s returning a good rcode, durable lsn is gen %d [%d][%d] "
                "good-count=%u bad-count=%u\n", __func__, *durable_gen, *durable_file, 
                *durable_offset, goodcount, badcount);
        lastpr = now;
    }

    return 0;
}

int bdb_num_connected_nodes(bdb_state_type *bdb_state)
{
    const char *hostlist[REPMAX];

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;
    return net_get_all_nodes_connected(bdb_state->repinfo->netinfo, hostlist);
}
