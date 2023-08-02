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
 * This is the public API for the comdb2 network library.
 */

#ifndef __NET_H__
#define __NET_H__

#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>

#include <cdb2_constants.h>
#include <perf.h>
#include <sbuf2.h>
#include <event2/buffer.h>

#ifndef HOST_NAME_MAX
#   ifdef MAXHOSTNAMELEN
#       define HOST_NAME_MAX MAXHOSTNAMELEN
#   else
#       define HOST_NAME_MAX 64
#   endif
#endif

extern int gbl_libevent;
extern int gbl_libevent_appsock;
extern int gbl_libevent_rte_only;
extern int gbl_net_maxconn;

/* Public structures and typedefs */
struct netinfo_struct;
struct host_node_tag;
struct sanc_node_tag;

typedef struct netinfo_struct netinfo_type;
typedef struct host_node_tag host_node_type;
typedef struct sanc_node_tag sanc_node_type;

typedef void HELLOFP(struct netinfo_struct *netinfo, char name[]);

typedef void APPSOCKFP(struct netinfo_struct *netinfo, SBUF2 *sb);

typedef void NETFP(void *ack_handle, void *usr_ptr, char *fromhost,
                   int usertype, void *dta, int dtalen, uint8_t is_tcp);

typedef int HOSTDOWNFP(netinfo_type *netinfo, char *host);

/* Return -1 if we should enque before this item, 1 if we should insert after
 * this item */
typedef int NETCMPFP(struct netinfo_struct *netinfo, void *insert_item,
                     int insert_item_len, void *current_item,
                     int current_item_len);

typedef int GETLSNFP(struct netinfo_struct *netinfo, void *record, int len,
                     int *file, int *offset);
typedef int NEWNODEFP(struct netinfo_struct *netinfo, char hostname[],
                      int portnum);

typedef void *QSTATINITFP(struct netinfo_struct *netinfo, const char *nettype,
                          const char hostname[]);
typedef void QSTATREADERFP(struct netinfo_struct *netinfo, void *netstat);
typedef void QSTATCLEARFP(struct netinfo_struct *netinfo, void *netstat);
typedef void QSTATENQUEFP(struct netinfo_struct *netinfo, void *netstat,
                          void *rec, int len);
typedef void QSTATFREEFP(struct netinfo_struct *netinfo, void *netstat);

struct net_get_records;
struct net_queue_stat;
typedef void QSTATITERFP(struct net_get_records *, struct net_queue_stat*);

typedef void QSTATDUMPFP(struct netinfo_struct *netinfo, void *netstat,
                         FILE *f);
typedef void UFUNCITERFP(struct netinfo_struct *netinfo, void *arg,
                         char *service, char *userfunc, int64_t count,
                         int64_t totus);

typedef int NETALLOWFP(struct netinfo_struct *netinfo, const char *hostname);

typedef int NETTHROTTLEFP(struct netinfo_struct *netinfo, const char *hostname);

void net_setbufsz(netinfo_type *info, int bufsz);

void net_set_callback_data(netinfo_type *info, void *data);
void net_register_start_thread_callback(netinfo_type *info, void (*)(void *));
void net_register_stop_thread_callback(netinfo_type *info, void (*)(void *));

int net_is_connected(netinfo_type *netinfo_ptr, const char *hostname);
int net_close_connection(netinfo_type *net, const char *hostname);

enum {
    NET_SEND_NODELAY = 0x00000001,
    NET_SEND_NODROP = 0x00000002,
    NET_SEND_INORDER = 0x00000004,
    NET_SEND_TRACE = 0x00000008,
    NET_SEND_LOGPUT = 0x00000010
};

enum {
    NET_SEND_FAIL_INVALIDNODE = -1,
    NET_SEND_FAIL_NOSOCK = -2,
    NET_SEND_FAIL_SENDTOME = -3,
    NET_SEND_FAIL_WRITEFAIL = -4,
    NET_SEND_FAIL_INTERNAL = -5,
    NET_SEND_FAIL_TIMEOUT = -6,
    NET_SEND_FAIL_INVALIDACKRC = -7,
    NET_SEND_FAIL_QUEUE_FULL = -8,
    NET_SEND_FAIL_MALLOC_FAIL = -9,
};

/*
   net_send_message returns 0 on success.
   a negative return is a system level failure
   a return > 0 is an application level failure
*/
int net_send_message(netinfo_type *netinfo,
                     const char *to_host, /* send to this node number */
                     int usertype, void *dta, int dtalen, int waitforack,
                     int waitms);

/* Returns an ack payload */
int net_send_message_payload_ack(netinfo_type *netinfo_ptr, const char *to_host,
                     int usertype, void *data, int datalen, uint8_t **payloadptr, 
                     int *payloadlen, int waitforack, int waitms);

int net_send_flags(netinfo_type *netinfo,
                   const char *to_host, /* send to this node number */
                   int usertype, void *dta, int dtalen, uint32_t flags);

int net_send(netinfo_type *netinfo,
             const char *to_host, /* send to this node number */
             int usertype, void *dta, int dtalen, int nodelay);

int net_send_nodrop(netinfo_type *netinfo, const char *to_host, int usertype,
                    void *dta, int dtalen, int nodelay);

int net_send_inorder_nodrop(netinfo_type *netinfo, const char *to_host,
                            int usertype, void *dta, int dtalen, int nodelay);

int net_send_inorder(netinfo_type *netinfo,
                     const char *to_host, /* send to this node number */
                     /*host_node_type *host_node, */
                     int usertype, void *dta, int dtalen, int nodelay);

/* register your callback routine that will be called when
   user messages of type "usertype" are recieved */
int net_register_handler(netinfo_type *netinfo_ptr, int usertype,
                         char *name, NETFP func);

/* register your callback routine that will be called when a
   disconnect happens for a node */
int net_register_hostdown(netinfo_type *netinfo_ptr, HOSTDOWNFP func);

int net_register_getlsn(netinfo_type *netinfo_ptr, GETLSNFP func);

int net_register_queue_stat(netinfo_type *netinfo_ptr, QSTATINITFP *qinit,
                            QSTATREADERFP *reader, QSTATENQUEFP *enque,
                            QSTATDUMPFP *dump, QSTATCLEARFP *qclear,
                            QSTATFREEFP *qfree);

/* register a callback that you can compare the order of things
   already on the write queue. */
int net_register_netcmp(netinfo_type *netinfo_ptr, NETCMPFP func);

/* register a callback routine that will be called when a
   new node is dynamically added */
int net_register_newnode(netinfo_type *netinfo_ptr, NEWNODEFP func);

int net_register_appsock(netinfo_type *netinfo_ptr, APPSOCKFP func);

/* callback to disable logputs if a node is too far behind */
int net_register_throttle(netinfo_type *netinfo_ptr, NETTHROTTLEFP func);

int net_register_admin_appsock(netinfo_type *netinfo_ptr, APPSOCKFP func);

/* register a callback routine that will be called to find out if net
 * connections should be allowed from a given node.  the callback
 * should return non-zero if the connection should be allowed. */
int net_register_allow(netinfo_type *netinfo_ptr, NETALLOWFP func);

/* for debugging only */
void netinfo_lock(netinfo_type *netinfo_ptr, int seconds);

/*
  acknowlege a message that you recieved with the needack argument set
  you can send back a return code that will be passed back to the caller
  of net_send_message().
  return 0 if no error.  return >0 to inidicate application level
  failure.  all return codes >0 are available for use.
  return codes <0 are reserved by system.
*/
int net_ack_message(void *ack_handle, int outrc);
int net_ack_message_payload(void *ack_handle, int outrc, void *payload, 
        int payloadlen);

netinfo_type *create_netinfo(char myhostname[], int myportnum, int myfd,
                             char app[], char service[], char instance[],
                             int fake, int offload, int ischild,
                             int use_getservbyname);

host_node_type *add_to_netinfo(netinfo_type *netinfo_ptr, const char hostname[],
                               int portnum);

/* convenience routines - a "fake" netinfo can be used programatically
   like a regular netinfo, but will short circuit on
   sending/recieving data.  it will fail the "is_real_netinfo" test */
netinfo_type *create_netinfo_fake(void);
int is_real_netinfo(netinfo_type *netinfo);
int is_offload_netinfo(netinfo_type *netinfo);

void net_inc_recv_cnt_from(netinfo_type *netinfo_ptr, char *host);
void net_reset_udp_stat(netinfo_type *netinfo_ptr);
void print_all_udp_stat(netinfo_type *netinfo_ptr);
void print_node_udp_stat(char *prefix, netinfo_type *netinfo_ptr,
                         const char *host);
ssize_t net_udp_send(int udp_fd, netinfo_type *netinfo_ptr, const char *host,
                     size_t len, void *info);

/*
  routines to send messages of type WIRE_HEADER_DECOM_NAME.
*/

/* send a decom message about node "decom_node" to all nodes */
int net_send_decom_all(netinfo_type *netinfo_ptr, char *decom_host);

int net_send_authcheck_all(netinfo_type *netinfo_ptr);

/* start the network */
int net_init(netinfo_type *netinfo_ptr);

/* return a list of all nodes (YOUR NODE IS NOT INCLUDED IN LIST) */
int net_get_all_nodes(netinfo_type *netinfo_ptr, const char *hostlist[REPMAX]);

/* "all" (registered) nodes, minus the decomissioned nodes */
int net_get_all_commissioned_nodes(netinfo_type *netinfo_ptr, const char* hostlist[REPMAX]);

/* return a list of all connected nodes (YOUR NODE IS NOT INCLUDED IN LIST) */
int net_get_all_nodes_connected(netinfo_type *netinfo_ptr,
                                const char *hostlist[REPMAX]);

/* count all connected nodes, including your node */
int net_count_connected_nodes(netinfo_type *netinfo_ptr);

/* count all nodes, including your node */
int net_count_nodes(netinfo_type *netinfo);

/* combines net_count_nodes and net_count_connected_nodes */
void net_count_nodes_ex(netinfo_type *netinfo_ptr, int *total_ptr,
                        int *connected_ptr);

/* make sure that all sanctioned nodes are ok */
int net_sanctioned_list_ok(netinfo_type *netinfo_ptr);

sanc_node_type *net_add_to_sanctioned(netinfo_type *netinfo_ptr,
                                      char hostname[], int portnum);

int net_is_single_sanctioned_node(netinfo_type *netinfo_ptr);
int net_get_sanctioned_node_list(netinfo_type *netinfo_ptr, int max_nodes,
                                 const char *nodes[REPMAX]);
int net_get_sanctioned_replicants(netinfo_type *netinfo_ptr, int max_nodes,
                                 const char *nodes[REPMAX]);

/* return the list of sanc nodes that are connected */
int net_sanctioned_and_connected_nodes(netinfo_type *netinfo_ptr, int max_nodes,
                                       const char *nodes[REPMAX]);

/* Remove a node from the sanctioned list.  Returns 0 on success, -1 if
 * node was not in sanctioned list. */
int net_del_from_sanctioned(netinfo_type *netinfo_ptr, char *host);

/* Caller supplies either the hostname(preferred) or the IP (when wire protocol
 * messages don't give us a choice) */
void net_decom_node(netinfo_type *netinfo_ptr, const char *host);

/* send a hello msg to a node.  this sends him our current table and
   causes him to try to connect to anyone in the table that he is missing */
int net_send_hello(netinfo_type *netinfo_ptr, const char *tohost);

int net_set_pool_size(netinfo_type *netinfo_ptr, int size);

int net_set_heartbeat_send_time(netinfo_type *netinfo_ptr, int time);
int net_get_heartbeat_send_time(netinfo_type *netinfo_ptr);
int net_set_heartbeat_check_time(netinfo_type *netinfo_ptr, int time);
int net_set_decom_time(netinfo_type *netinfo_ptr, int time);

int net_register_name(netinfo_type *netinfo_ptr, char name[]);
int net_register_hello(netinfo_type *netinfo_ptr, HELLOFP func);

/* For berkdb_rep.c */
void connect_to_all(netinfo_type *netinfo_ptr);

/* This appears to be unused -- Sam J 03/24/05 */
void print_netinfo(netinfo_type *netinfo_ptr);

/* offloading support */
/*
  same as net_send, but accept a tail after the payload
*/
int net_send_tail(netinfo_type *netinfo,
                  const char *host, /* send to this node */
                  int usertype, void *dta, int dtalen, int nodelay, void *tail,
                  int tailen);

int net_send_tails(netinfo_type *netinfo_ptr, const char *host, int usertype,
                   void *data, int datalen, int nodelay, int numtails,
                   void **tails, int *taillens);

/* pick a sibling for sql offloading */
char *net_get_osql_node(netinfo_type *netinfo_ptr);

/* netinfo getters and setters so that we don't have tomake the entire
 * netinfo struct public. */
char *net_get_mynode(netinfo_type *netinfo_ptr);
void *net_get_usrptr(netinfo_type *netinfo_ptr);
void net_set_usrptr(netinfo_type *netinfo_ptr, void *usrptr);

/* grab and hold the netlock for some number for seconds */
void net_sleep_with_lock(netinfo_type *netinfo_ptr, int nseconds);

void net_timeout_watchlist(netinfo_type *netinfo_ptr);
void net_add_watch(SBUF2 *sb, int read_timeout, int write_timeout);
void net_set_writefn(SBUF2 *, sbuf2writefn);
void net_end_appsock(SBUF2 *sb);

/* get information about our network nodes.  fills in up to max_nodes array
 * elements and then returns the number of nodes in total (which could be
 * more than max_nodes) */
struct host_node_info {
    int fd;
    char *host;
    int port;
};
int net_get_nodes_info(netinfo_type *netinfo_ptr, int max_nodes,
                       struct host_node_info *out_nodes);

struct net_host_stats {
    int queue_size;
};
int net_get_host_stats(netinfo_type *netinfo_ptr, const char *host, struct net_host_stats *stat);

struct net_stats {
    int num_drops;
};
int net_get_stats(netinfo_type *netinfo_ptr, struct net_stats *stat);

void net_cmd(netinfo_type *netinfo_ptr, char *line, int lline, int st, int op1);

int net_set_max_queue(netinfo_type *netinfo_ptr, int x);
int net_set_max_bytes(netinfo_type *netinfo_ptr, uint64_t x);

int net_get_host_network_usage(netinfo_type *netinfo_ptr, const char *host,
                               unsigned long long *written,
                               unsigned long long *read,
                               unsigned long long *throttle_waits,
                               unsigned long long *reorders);

int net_get_network_usage(netinfo_type *netinfo_ptr,
                          unsigned long long *written, unsigned long long *read,
                          unsigned long long *throttle_waits,
                          unsigned long long *reorders);

int net_get_queue_size(netinfo_type *netinfo_type, const char *host, int *limit,
                       int *usage);

void net_exiting(netinfo_type *netinfo_ptr);
int net_is_exiting(netinfo_type *netinfo_ptr);

void net_trace(netinfo_type *netinfo_ptr, int on);

enum { NET_TEST_NONE = 0, NET_TEST_QUEUE_FULL = 1, NET_TEST_MAX = 2 };

/* simple way to enable/disable testing of net logic for cdb2tcm */
void net_enable_test(netinfo_type *netinfo_ptr, int testno);
void net_disable_test(netinfo_type *netinfo_ptr);

/* used by comdb2 to add subnet suffices for replication */
int net_add_nondedicated_subnet(void *, void *);
int net_add_to_subnets(const char *suffix, const char *lrlname);
void net_cleanup();
void net_cleanup_netinfo(netinfo_type *netinfo_ptr);

/* Maximum time accept will wait for a identifying byte from a socket.
   This defaults to 100ms */
void net_set_poll(netinfo_type *netinfo_ptr, int polltm);
int net_get_poll(netinfo_type *netinfo_ptr);

void net_set_enque_flush_interval(netinfo_type *, int x);
void net_set_enque_reorder_lookahead(netinfo_type *, int x);

int get_host_port(netinfo_type *);

/*****************************/
/* Support for bdb/bdb_net.c */
/*****************************/

typedef struct {
    uint64_t sent;
    uint64_t recv;
} HostInfo;

void print_netdelay(void);
void net_delay(const char *host);

/* print memory usage on netinfo & hostnodes */
void print_net_memstat(int human_readable);

void net_add_watch_warning(SBUF2 *sb, int read_warning_timeout,
                           int write_timeout, void *arg,
                           int (*callback)(void *, int, int));
int net_appsock_get_addr(SBUF2 *db, struct sockaddr_in *addr);
int net_listen(int port);

void net_set_throttle_percent(netinfo_type *netinfo_ptr, int x);
void net_set_portmux_register_interval(netinfo_type *netinfo_ptr, int x);

void net_queue_stat_iterate(netinfo_type *, QSTATITERFP, struct net_get_records *);
void net_queue_stat_iterate_evbuffer(netinfo_type *, QSTATITERFP, struct net_get_records *);
void net_userfunc_iterate(netinfo_type *netinfo_ptr, UFUNCITERFP *uf_iter, void *arg);

int do_appsock_evbuffer(struct evbuffer *buf, struct sockaddr_in *ss, int fd, int is_readonly);

/* Blocks until the net-queue is X% full or less */
int net_throttle_wait(netinfo_type *netinfo_ptr);

void net_enable_explicit_flush_trace(void);
void net_disable_explicit_flush_trace(void);

void kill_subnet(const char *subnet);
void net_clipper(const char *subnet, int onoff);
void net_subnet_status();

void net_register_child_net(netinfo_type *netinfo_ptr,
                            netinfo_type *netinfo_child, int netnum,
                            int accept);

void net_disable_getservbyname(netinfo_type *netinfo_ptr);
int net_get_port_by_service(const char *dbname);

int64_t net_get_num_accepts(netinfo_type *netinfo_ptr);
int64_t net_get_num_current_non_appsock_accepts(netinfo_type *netinfo_ptr);
int64_t net_get_num_accept_timeouts(netinfo_type *netinfo_ptr);
void net_set_conntime_dump_period(netinfo_type *netinfo_ptr, int value);
int net_get_conntime_dump_period(netinfo_type *netinfo_ptr);
int net_send_all(netinfo_type *, int, void **, int *, int *, int *);
void update_host_net_queue_stats(host_node_type *, size_t, size_t);
int db_is_stopped(void);
int db_is_exiting(void);
void stop_event_net(void);
int sync_state_to_protobuf(int);
void increase_net_buf(void);

#endif
