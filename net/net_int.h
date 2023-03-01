/*
   Copyright 2015 Bloomberg Finance L.P.

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

/*
 * Comdb2 network library internal header file.
 * I want to remove the dependencies between net and bdb where possible so
 * we have a clean distinction between public and private interfaces.
 */

#ifndef INCLUDED__NET_INT_H
#define INCLUDED__NET_INT_H

#include <netinet/in.h>
#include <pthread.h>

#include "list.h"
#include "compile_time_assert.h"
#include "thread_util.h"
#include "mem.h"
#include "net_types.h"
#include "cdb2_constants.h"
#include "logmsg.h"
#include "quantize.h"
#include "perf.h"

enum {
    /* Flags for write_list() */
    WRITE_MSG_HEAD = 1,
    WRITE_MSG_NODELAY = 2,
    WRITE_MSG_NOHELLOCHECK = 4,
    WRITE_MSG_NODUPE = 8,
    WRITE_MSG_NOLIMIT = 16,
    WRITE_MSG_INORDER = 32
};

#define HOSTNAME_LEN 16

typedef struct {
    char fromhost[HOSTNAME_LEN];
    int fromport;
    int fromnode;
    char tohost[HOSTNAME_LEN];
    int toport;
    int tonode;
    int type;
} wire_header_type;

enum { NET_WIRE_HEADER_TYPE_LEN = 16 + 4 + 4 + 16 + 4 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(net_write_header_type,
                       sizeof(wire_header_type) == NET_WIRE_HEADER_TYPE_LEN);

/* We can't change the on-wire protocol easily.  So it
 * retains node numbers, but they're unused for now */
/* type 0 is internal connect message.
   type >0 is for applications */
typedef struct {
    char to_hostname[HOSTNAME_LEN];
    int to_portnum;
    int flags; /* was `int to_nodenum` */
    char from_hostname[HOSTNAME_LEN];
    int from_portnum;
    int from_nodenum;
} connect_message_type;

/* flags for connect_message_typs */
#define CONNECT_MSG_SSL 0x80000000
#define CONNECT_MSG_TONODE 0x0000ffff /* backwards compatible */

enum {
    NET_CONNECT_MESSAGE_TYPE_LEN = HOSTNAME_LEN + sizeof(int) + sizeof(int) +
                                   HOSTNAME_LEN + sizeof(int) + sizeof(int)
};

BB_COMPILE_TIME_ASSERT(net_connect_message_type,
                       sizeof(connect_message_type) ==
                           NET_CONNECT_MESSAGE_TYPE_LEN);
typedef struct write_node_data {
    int flags;
    int enque_time;
    struct write_node_data *next;
    struct write_node_data *prev;
    size_t len;
    /* Must be last thing in struct; payload immediately follows header */
    union {
        wire_header_type header;
        char raw[1];
    } payload;
} write_data;

typedef struct seq_node_data {
    struct seq_node_data *next;
    int seqnum;
    int ack;
    void *payload;
    int payloadlen;
    int outrc;
    int timestamp;
} seq_data;

struct host_node_tag;
struct netinfo_struct;
struct watchlist_node_tag;

typedef struct watchlist_node_tag {
    char magic[4]; /* should be "WLST" */

    SBUF2 *sb;
    sbuf2readfn readfn;
    sbuf2writefn writefn;

    int write_age;
    int read_age;

    int write_timeout;
    int read_timeout;
    /* if read_timeout is 0, this is a warning mechanism */
    int read_warning_timeout;
    void *read_warning_arg;
    int (*read_warning_func)(void *arg, int timeout, int current);

    struct netinfo_struct *netinfo_ptr;

    int in_watchlist;

    struct sockaddr_in addr;

    LINKC_T(struct watchlist_node_tag) lnk;
} watchlist_node_type;

/* lockless its just stats */
typedef struct {
    unsigned long long bytes_written;
    unsigned long long bytes_read;
    unsigned long long throttle_waits;
    unsigned long long reorders;
} stats_type;

typedef struct net_send_message_header {
    int usertype;
    int seqnum;
    int waitforack;
    int datalen;
} net_send_message_header;
enum { NET_SEND_MESSAGE_HEADER_LEN = 4 + 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(net_send_message_header,
        sizeof(net_send_message_header) == NET_SEND_MESSAGE_HEADER_LEN);

typedef struct net_ack_message_type {
    int seqnum;
    int outrc;
} net_ack_message_type;
enum { NET_ACK_MESSAGE_TYPE_LEN = 4 + 4 };
BB_COMPILE_TIME_ASSERT(net_ack_message_type,
        sizeof(net_ack_message_type) == NET_ACK_MESSAGE_TYPE_LEN);

typedef struct net_ack_message_payload_type {
    int seqnum;
    int outrc;
    int paylen;
} net_ack_message_payload_type;
enum { NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN = 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(net_ack_message_payload_type,
        sizeof(net_ack_message_payload_type) == NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN);

struct event_info;
struct host_node_tag {
    struct event_info *event_info;
    int fd;
    SBUF2 *sb;
    char *host;
    int hostname_len;
    char subnet[HOSTNAME_LEN];
    int port;
    struct host_node_tag *next;
    int have_connect_thread;
    int have_reader_thread;
    int have_writer_thread;
    int decom_flag;
    pthread_t connect_thread_id;
    pthread_t reader_thread_id;
    pthread_t writer_thread_id;
    arch_tid connect_thread_arch_tid;
    arch_tid reader_thread_arch_tid;
    arch_tid writer_thread_arch_tid;
    write_data *write_head;
    write_data *write_tail;
    seq_data *wait_list;
    pthread_mutex_t lock;
    pthread_mutex_t enquelk;
    pthread_cond_t ack_wakeup;
    pthread_mutex_t wait_mutex;
    int timestamp;
    pthread_mutex_t write_lock;
    pthread_cond_t write_wakeup;
    int got_hello;
    int running_user_func; /* This is a count of how many are running */
    int closed;
    int really_closed;

    unsigned enque_count; /* number of items currently
                             enqueued for writing */
    unsigned peak_enque_count;
    unsigned peak_enque_count_time;

    unsigned num_queue_full; /* how often we hit queue full issue */
    unsigned last_queue_full_time;

    unsigned enque_bytes;
    unsigned peak_enque_bytes;
    unsigned peak_enque_bytes_time;

    unsigned dedupe_count;

    struct in_addr addr;
    int distress; /* if this is set, do not report any errors, we know we're
                    looping trying to get a successful read_message_header

                    used as a counter to see how many times I have created
                    a failed connect process (connect, reader, writer, ...)
                  */

    int rej_up_cnt; /* number of connections rejected because the node is
                       already up */
    watchlist_node_type *watchlist_ptr;
    struct netinfo_struct *netinfo_ptr;
    stats_type stats; /* useful per host */

#ifdef PER_THREAD_MALLOC
    comdb2ma msp;
#endif

    void *user_data_buf;

    HostInfo udp_info;
    int num_sends;
    unsigned long long num_flushes;
    pthread_mutex_t timestamp_lock; /* no more premature session killing */

    /* Number of waiters. This includes number of throttle waiters and connect
       thread waiter. A host can't be safely removed unless its nwaiters is 0
       and its reader or writer has exited. */
    int nwaiters;
    pthread_mutex_t waiter_lock;
    pthread_cond_t waiter_wakeup;
    int last_queue_dump;
    int last_print_queue_time;
    int interval_max_queue_count;
    int interval_max_queue_bytes;
    void *qstat;
    struct time_metric *metric_queue_size;
};

/* Cut down data structure used for storing the sanc list. */
struct sanc_node_tag {
    const char *host;
    int port;
    int timestamp;
    struct sanc_node_tag *next;
};

typedef struct decom_struct {
    int node;
    int timestamp;
    struct decom_struct *next;
} decom_type;

typedef struct userfunc_info {
    NETFP *func;
    char *name;
    int64_t count;
    int64_t totus;
} userfunc_t;

struct netinfo_struct {
    host_node_type *head;
    sanc_node_type *sanctioned_list;
    int numhosts;
    /*
    upping to 32 to prevents myhostname_other from bleeding into myport on our
    new linux machines.  This change does not affect the wire protocol.
    */
    char *myhostname;
    int myhostname_len;
    char myhostname_other[32];
    int myport;
    int myfd;
    char app[16];
    char service[16];
    char instance[MAX_DBNAME_LENGTH];
    unsigned int seqnum;

    /* child nets - only parent listens on a port, forwards connections
     * meant for other nets to their accept callbacks */
    int num_child_nets;
    struct netinfo_struct *parent;
    int netnum;
    netinfo_type **child_nets;
    int ischild;
    int accept_on_child;

    userfunc_t userfuncs[USER_TYPE_MAX];
    decom_type *decomhead;
    pthread_mutex_t stop_thread_callback_lock;
    pthread_rwlock_t lock;
    pthread_mutex_t watchlk;
    pthread_mutex_t sanclk;
    pthread_t accept_thread_id;
    pthread_t heartbeat_send_thread_id;
    pthread_t heartbeat_check_thread_id;

    /* get the archthreads for each */
    arch_tid accept_thread_arch_tid;
    arch_tid heartbeat_send_thread_arch_tid;
    arch_tid heartbeat_check_thread_arch_tid;

    int fake;     /* 1 if this is set, then we don't ever send or receieve */
    void *usrptr; /* pointer to user supplied data */
    HOSTDOWNFP *hostdown_rtn; /* user supplied routine called when host
                                 gets disconnected */
    NEWNODEFP *new_node_rtn;
    pthread_attr_t pthread_attr_detach;
    APPSOCKFP *appsock_rtn;
    APPSOCKFP *admin_appsock_rtn;
    HELLOFP *hello_rtn;
    int accept_thread_created;
    int heartbeat_send_time;
    int heartbeat_check_time;
    int decom_time;
    char *name;
    stats_type stats;
    NETTHROTTLEFP *throttle_rtn;
    NETALLOWFP *allow_rtn;
    void *callback_data;
    void (*start_thread_callback)(void *);
    void (*stop_thread_callback)(void *);

    int bufsz;

    LISTC_T(struct watchlist_node_tag) watchlist;

    /* it proves that the sql offload net has slightly
       different requirements than replication net
       (for example, we would like the protocol to correctly
       report back errors when packets are lost due to queue-full
       this bit mark the difference
     */
    int offload;
    uint32_t max_queue;
    uint64_t max_bytes;
    int exiting;
    int trace;

    int pool_size;

    int user_data_buf_size;
    int net_test;

    host_node_type *last_used_node_ptr;
    unsigned int last_used_node_hit_cntr;
    unsigned int last_used_node_miss_cntr;

    int netpoll;
    void *connpool;
    pthread_mutex_t connlk;

    int enque_flush_interval;

    int throttle_percent;
    NETCMPFP *netcmp_rtn;
    int enque_reorder_lookahead;
    int portmux_register_interval;
    int portmux_register_time;
    int port_from_lrl;

    int use_getservbyname;
    int hellofd;
    GETLSNFP *getlsn_rtn;
    QSTATINITFP *qstat_init_rtn;
    QSTATREADERFP *qstat_reader_rtn;
    QSTATENQUEFP *qstat_enque_rtn;
    QSTATCLEARFP *qstat_clear_rtn;
    QSTATDUMPFP *qstat_dump_rtn;
    QSTATFREEFP *qstat_free_rtn;

    struct quantize *conntime_all;
    struct quantize *conntime_periodic;
    int64_t num_accepts;
    int64_t num_accept_timeouts;
    int conntime_dump_period;


    /* An appsock routine may or may not close the connection.
       Therefore we can only reliably keep track of non-appsock connections. */
    int num_current_non_appsock_accepts;
};

typedef struct ack_state_struct {
    int seqnum;
    int needack; /* detect when someone acks if we werent asking for it */
    char *fromhost;
    netinfo_type *netinfo;
} ack_state_type;

/* Trace functions */
void host_node_printf(loglvl lvl, host_node_type *host_node_ptr, const char *fmt, ...);
void host_node_errf(loglvl lvl, host_node_type *host_node_ptr, const char *fmt, ...);

/* To verify replicant database name. */
extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern int gbl_nid_dbname;

const uint8_t *net_ack_message_payload_type_get(net_ack_message_payload_type *, const uint8_t *, const uint8_t *);
const uint8_t *net_ack_message_type_get(net_ack_message_type *, const uint8_t *, const uint8_t *);
const uint8_t *net_connect_message_get(connect_message_type *, const uint8_t *, const uint8_t *);
const uint8_t *net_send_message_header_get(net_send_message_header *, const uint8_t *, const uint8_t *);
uint8_t *net_send_message_header_put(const net_send_message_header *, uint8_t *, const uint8_t *);
const uint8_t *net_wire_header_get(wire_header_type *, const uint8_t *, const uint8_t *);
uint8_t *net_wire_header_put(const wire_header_type *, uint8_t *, const uint8_t *);

void add_host(host_node_type *);
void decom(char *);
void do_appsock(netinfo_type *, struct sockaddr_in *, SBUF2 *, uint8_t);
int findpeer(int, char *, int);
int get_dedicated_conhost(host_node_type *, struct in_addr *);
host_node_type *get_host_node_by_name_ll(netinfo_type *, const char *);
host_node_type *add_to_netinfo_ll(netinfo_type *, const char hostname[], int portnum);
int net_flush_evbuffer(host_node_type *);
int net_send_all_evbuffer(netinfo_type *, int, void **, int *, int *, int *);
int write_connect_message(netinfo_type *, host_node_type *, SBUF2 *);
int write_connect_message_evbuffer(host_node_type *, const struct iovec *, int);
int write_decom(netinfo_type *, host_node_type *, const char *, int, const char *);
int write_heartbeat(netinfo_type *, host_node_type *);
int write_hello(netinfo_type *, host_node_type *);
int write_hello_reply(netinfo_type *, host_node_type *);
int write_list_evbuffer(host_node_type *, int, const struct iovec *, int, int);
int net_send_evbuffer(netinfo_type *, const char *, int, void *, int, int, void **, int *, int);

int get_hosts_evbuffer(int n, host_node_type **);

int should_reject_request(void);

#endif /* INCLUDED__NET_INT_H */
