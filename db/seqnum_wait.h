#ifndef INCLUDED_SEQNUM_WAIT_H
#define INCLUDED_SEQNUM_WAIT_H

#include <pthread.h>
#include <stdint.h>

#include "comdb2.h"
#include "errstat.h"
#include "list.h"
#include "bdb_int.h"

/*
 * Asynchronous "distributed commit" (a.k.a. wait-for-seqnum).
 *
 * Normally the block processor thread that committed a transaction goes on to
 * block in bdb_wait_for_seqnum_from_all_int() until every replicant has acked
 * the commit LSN.  With gbl_async_dist_commit on, the block processor instead
 * hands the commit seqnum to a single background thread and returns to the
 * pool immediately; that thread polls the acks for all outstanding commits and
 * signals each waiting sql thread when its commit is durable.
 *
 * The client still waits -- only the block processor thread is freed.
 */

enum seqnum_wait_state {
    SEQNUM_WAIT_INIT,
    SEQNUM_WAIT_FIRST_ACK,
    SEQNUM_WAIT_GOT_FIRST_ACK,
    SEQNUM_WAIT_DONE,
    SEQNUM_WAIT_COMMIT,
    SEQNUM_WAIT_FREE,
};

struct seqnum_wait {
    LINKC_T(struct seqnum_wait) lsn_lnk;
    LINKC_T(struct seqnum_wait) absolute_ts_lnk;

    enum seqnum_wait_state cur_state;
    bdb_state_type *bdb_state;
    seqnum_type seqnum;

    struct interned_string *nodelist[REPMAX];
    struct interned_string *connlist[REPMAX];
    int total_connected;
    int numnodes;
    int numwait;
    int numskip;
    int numfailed;
    int num_successfully_acked;
    int durable_lsns;
    int catchup_window;

    int waitms;
    int start_time; /* when we started waiting for the first ack */
    int end_time;   /* when the first ack arrived (or we gave up on it) */
    int we_used;
    int next_ts; /* absolute ms timestamp when this item wants attention */

    struct interned_string *base_node;
    int outrc;

    /* Everything we need to signal the originating sql thread once the commit
     * is durable.  Copied by value because `iq' is recycled as soon as the
     * block processor returns. */
    osql_target_t target;
    unsigned long long rqid;
    uuid_t uuid;
    snap_uid_t *snap;
    int nops;
    int rcout;
    errstat_t errstat;
};

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    LISTC_T(struct seqnum_wait) lsn_list;         /* ordered by commit LSN */
    LISTC_T(struct seqnum_wait) absolute_ts_list; /* ordered by next_ts */
    uint64_t next_commit_timestamp;
} seqnum_wait_queue;

/* Returns 1 if the commit was handed off to the waiter thread, 0 if the caller
 * must wait inline (queue full, or not initialised). */
int add_to_seqnum_wait_queue(bdb_state_type *bdb_state, seqnum_type *seqnum, struct ireq *iq);

int seqnum_wait_gbl_mem_init(void);

#endif
