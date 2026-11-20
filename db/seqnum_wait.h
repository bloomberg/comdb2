#ifndef INCLUDED_SEQNUM_WAIT_H
#define INCLUDED_SEQNUM_WAIT_H

#include "comdb2.h"

/*
 * Asynchronous "distributed commit" (a.k.a. wait-for-seqnum).
 *
 * Normally the block processor thread that committed a transaction goes on to
 * block in bdb_wait_for_seqnum_from_all_int() until every replicant has acked
 * the commit LSN.  With gbl_async_dist_commit on, the block processor instead
 * hands the whole request to a single background thread and returns to the
 * pool immediately; that thread polls the acks for all outstanding commits,
 * then replies to each request and finishes it once its wait is over.
 *
 * The client still waits -- only the block processor thread is freed.
 */

struct seqnum_wait;

/* Called by trans_commit_int() right after the commit, instead of waiting for
 * the acks: reserve a place for this commit's wait.  Returns NULL if the
 * caller must wait inline (queue full, or not initialized). */
struct seqnum_wait *seqnum_wait_prepare(bdb_state_type *bdb_state, db_seqnum_type *seqnum, struct ireq *iq);
/* toblock()'s rc, for the reply */
void seqnum_wait_set_rc(struct seqnum_wait *swait, int block_rc);
/* Called by the block processor where it would recycle the request: the
 * waiter owns the request and its logger from here on. */
void seqnum_wait_start(struct seqnum_wait *swait);
/* The rep-time and timeout stats a finished ack wait adds to dbenv. */
void seqnum_wait_count_reptime(struct ireq *iq);

void seqnum_wait_gbl_mem_init(void);
/* On clean exit: wait (bounded) for the requests handed to the waiter. */
void seqnum_wait_drain(void);

#endif
