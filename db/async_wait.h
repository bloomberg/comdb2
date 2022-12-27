#ifndef INCLUDED_SEQNUM_WAIT_H
#define INCLUDED_SEQNUM_WAIT_H
#include<comdb2.h>
#include<errstat.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <strings.h>
#include <poll.h>
#include <alloca.h>
#include <limits.h>
#include <math.h>

#include <epochlib.h>
#include <build/db.h>
#include <rtcpu.h>
#include "debug_switches.h"

#include <cheapstack.h>
#include "net.h"
#include "bdb_int.h"
#include "locks.h"
#include "locks_wrap.h"
#include "list.h"
#include <endian_core.h>

#include <time.h>

#include "memory_sync.h"
#include "compile_time_assert.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#include "ctrace.h"
#include "nodemap.h"
#include "util.h"
#include "crc32c.h"
#include "gettimeofday_ms.h"

#include <build/db_int.h>
#include "dbinc/db_page.h"
#include "dbinc/db_swap.h"
#include "dbinc/db_shash.h"
#include "dbinc/btree.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include <trigger.h>
#include "printformats.h"
#include <llog_auto.h>
#include "phys_rep_lsn.h"
#include "logmsg.h"
#include <compat.h>
#include "str0.h"

#include <inttypes.h>

/* The asynchronous wait for distributed commit can be captured
   by a state machine involving the following states : 
   - INIT               ->
   - WAIT_FOR_FIRST_ACK ->
   - WAIT_FOR_ALL_ACK   ->
   - WAIT_FINISH        -> 
   - DONE               -> 
 */
enum async_wait_state{
    INIT,
    WAIT_FOR_FIRST_ACK,
    WAIT_FOR_ALL_ACK,
    WAIT_FINISH,
    WAIT_NEXT_COMMIT_TIMESTAMP,
    WAIT_NOTIFY,
    DONE
};
#endif

/*
	This struct encapsulates the progress of 'distributed commit' as 
	a work item part of a larger work queue.
*/
struct async_wait_node{
	LINKC_T(struct async_wait_node) lsn_lnk;
    LINKC_T(struct async_wait_node) absolute_ts_lnk;
    enum async_wait_state cur_state; 
    const char *nodelist[REPMAX];
    const char *connlist[REPMAX];
    int numnodes;
    int numwait;
    int waitms;
    int numskip;
    int numfailed;
    int outrc;
    errstat_t errstat;
    int num_incoh;
    uint64_t next_ts;              // timestamp in the future when this item has to be "worked" on
    uint64_t start_time , end_time;
    uint64_t enqueue_time;
    /* following times are mainly for debugging */
    uint64_t wait_init_start_time;
    uint64_t wait_init_end_time;
    uint64_t wait_wait_for_first_ack_start_time;
    uint64_t wait_wait_for_first_ack_end_time;
    uint64_t wait_wait_for_all_ack_start_time;
    uint64_t wait_wait_for_all_ack_end_time;
    uint64_t wait_wait_finish_start_time;
    uint64_t wait_wait_finish_end_time;
    uint64_t wait_wait_next_commit_timestamp_start_time;
    uint64_t wait_wait_next_commit_timestamp_end_time;
    uint64_t wait_wait_notify_start_time;
    uint64_t wait_wait_notify_end_time;
    int we_used;
    const char *base_node;
    int num_commissioned;
    int num_successfully_acked;
    int num_nodes;
    bdb_state_type *bdb_state;
    seqnum_type seqnum;
    int track_once;
    int durable_lsns;
    struct ireq *iq;
};
typedef struct async_wait_node async_wait_node;

typedef struct{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int size;
    LISTC_T(struct async_wait_node) lsn_list;
    LISTC_T(struct async_wait_node) absolute_ts_list;
    uint64_t next_commit_timestamp;
}async_wait_queue;

int async_wait_init();
void async_wait_cleanup();
int add_to_async_wait_queue(struct ireq *iq, int rc);
