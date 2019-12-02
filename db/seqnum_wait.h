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

// Copied this #define from rep.c. Make sure to mirror changes
#define MILLISEC 1000
enum seqnum_wait_state{
    INIT,
    FIRST_ACK,
    GOT_FIRST_ACK,
    DONE_WAIT,
    COMMIT,
    FREE
};
struct seqnum_wait{
    LINKC_T(struct seqnum_wait) lsn_lnk;
    LINKC_T(struct seqnum_wait) absolute_ts_lnk;
    enum seqnum_wait_state cur_state;           // Cur state of the work item designating progress made on this work item. 
    int  now, cntbytes;
    const char *nodelist[REPMAX];
    const char *connlist[REPMAX];
    int do_slow_node_check;
    DB_LSN *masterlsn;
    int numnodes;
    int numwait;
    int waitms;
    int numskip;
    int numfailed;
    int outrc;
    int num_incoh;
    int next_ts;              // timestamp in the future when this item has to be "worked" on
    struct timespec wait_time;
    int start_time , end_time;
    int we_used;
    const char *base_node;
    char str[80];
    DB_LSN nodelsn;
    errstat_t errstat;
    sorese_info_t sorese;
    uint32_t nodegen;
    int num_successfully_acked;
    int total_connected;
    int lock_desired;
    bdb_state_type *bdb_state;
    struct dbenv *dbenv;
    seqnum_type seqnum;
    int got_ack_from_atleast_one_node;
};

typedef struct{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int size;
    LISTC_T(struct seqnum_wait) lsn_list;
    LISTC_T(struct seqnum_wait) absolute_ts_list;
    uint64_t next_commit_timestamp;
}seqnum_wait_queue;
// Add work item to seqnum_wait_queue.
int add_to_seqnum_wait_queue(bdb_state_type* bdb_state,seqnum_type *seqnum,struct dbenv *dbenv,sorese_info_t *sorese, errstat_t *errstat,int rc);
int seqnum_wait_gbl_mem_init();
void seqnum_wait_cleanup();

#endif
