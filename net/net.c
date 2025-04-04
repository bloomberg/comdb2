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

#define NODELAY
#define NOLINGER
#ifdef __linux__
#define TCPBUFSZ
#endif

#include <errno.h>
#include <netdb.h>
#include <unistd.h>
#include <signal.h>

#ifdef __sun
#include <siginfo.h>
#endif

#include <fcntl.h>
#ifdef _AIX
#include <sys/socket.h>
#include <sys/socketvar.h>
#endif
#include <netinet/tcp.h>
#include <utime.h>
#include <sys/time.h>
#include <poll.h>

#include <bb_oscompat.h>
#include <compat.h>
#include <pool.h>
#include <assert.h>
#include <crc32c.h>

#include "sys_wrap.h"
#include "net.h"
#include "net_int.h"

/* rtcpu.h breaks dbx on sun */
#ifndef NET_DEBUG
#include <rtcpu.h>
#endif

#include <endian_core.h>
#include <compile_time_assert.h>
#include <portmuxapi.h>
#include <epochlib.h>
#include <str0.h>

#include <util.h>
#include "intern_strings.h"

#include "rtcpu.h"
#include "mem_net.h"
#include "mem_override.h"
#include <bdb_net.h>

#include "debug_switches.h"
#include "thrman.h"
#include "thread_util.h"
#include <timer_util.h>
#include <comdb2_atomic.h>

#ifdef UDP_DEBUG
static int curr_udp_cnt = 0;
#endif

#define MILLION 1000000
#define BILLION 1000000000

extern int gbl_pmux_route_enabled;
extern int gbl_exit;
extern int gbl_accept_on_child_nets;
extern int gbl_server_admin_mode;

extern void myfree(void *ptr);
extern int db_is_exiting(void);

int gbl_verbose_net = 0;
int gbl_dump_net_queue_on_partial_write = 0;
int gbl_debug_partial_write = 0;
int subnet_blackout_timems = 5000;
int gbl_net_maxconn = 0;
int gbl_heartbeat_check = 5;

static int net_flush(host_node_type *host_node_ptr)
{
    return net_flush_evbuffer(host_node_ptr);
}

void net_set_subnet_blackout(int ms)
{
    if (ms >= 0) {
        subnet_blackout_timems = ms;
    }
}

static unsigned long long gettmms(void)
{
    struct timeval tm;

    gettimeofday(&tm, NULL);

    return 1000 * ((unsigned long long)tm.tv_sec) +
           ((unsigned long long)tm.tv_usec) / 1000;
}

static int sbuf2read_wrapper(SBUF2 *sb, char *buf, int nbytes)
{
    if (debug_switch_verbose_sbuf())
        logmsg(LOGMSG_USER, "reading, reading %llu\n", gettmms());

    return sbuf2unbufferedread(sb, buf, nbytes);
}

static int sbuf2write_wrapper(SBUF2 *sb, const char *buf, int nbytes)
{
    if (debug_switch_verbose_sbuf())
        logmsg(LOGMSG_USER, "writing, writing %llu\n", gettmms());

    return sbuf2unbufferedwrite(sb, buf, nbytes);
}

/* Help me build the test program... - Sam J */
#ifdef TEST
static void myfree(void *ptr)
{
    if (ptr)
        free(ptr);
}
#endif

static sanc_node_type *add_to_sanctioned_nolock(netinfo_type *netinfo_ptr,
                                                const char hostname[],
                                                int portnum);

static int net_writes(SBUF2 *sb, const char *buf, int nbytes);
static int net_reads(SBUF2 *sb, char *buf, int nbytes);

static watchlist_node_type *get_watchlist_node(SBUF2 *, const char *funcname);

int sbuf2ungetc(char c, SBUF2 *sb);

/* Endian manipulation routines */
static uint8_t *net_connect_message_put(const connect_message_type *msg_ptr,
                                        uint8_t *p_buf,
                                        const uint8_t *p_buf_end)
{
    int node = 0;
    if (p_buf_end < p_buf || NET_CONNECT_MESSAGE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(msg_ptr->to_hostname),
                           sizeof(msg_ptr->to_hostname), p_buf, p_buf_end);
    p_buf = buf_put(&(msg_ptr->to_portnum), sizeof(msg_ptr->to_portnum), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&(msg_ptr->flags), sizeof(msg_ptr->flags), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(msg_ptr->from_hostname),
                           sizeof(msg_ptr->from_hostname), p_buf, p_buf_end);
    p_buf = buf_put(&(msg_ptr->from_portnum), sizeof(msg_ptr->from_portnum), p_buf,
                    p_buf_end);
    p_buf = buf_put(&node, sizeof(msg_ptr->from_nodenum), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *net_connect_message_get(connect_message_type *msg_ptr,
                                       const uint8_t *p_buf,
                                       const uint8_t *p_buf_end)
{
    int node = 0;
    if (p_buf_end < p_buf || NET_CONNECT_MESSAGE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(msg_ptr->to_hostname),
                           sizeof(msg_ptr->to_hostname), p_buf, p_buf_end);
    p_buf = buf_get(&(msg_ptr->to_portnum), sizeof(msg_ptr->to_portnum), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&(msg_ptr->flags), sizeof(msg_ptr->flags), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(msg_ptr->from_hostname),
                           sizeof(msg_ptr->from_hostname), p_buf, p_buf_end);
    p_buf = buf_get(&(msg_ptr->from_portnum), sizeof(msg_ptr->from_portnum), p_buf,
                    p_buf_end);
    p_buf = buf_get(&node, sizeof(msg_ptr->from_nodenum), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *net_wire_header_put(const wire_header_type *header_ptr, uint8_t *p_buf,
                             const uint8_t *p_buf_end)
{
    int node = 0;
    if (p_buf_end < p_buf || NET_WIRE_HEADER_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(header_ptr->fromhost),
                           sizeof(header_ptr->fromhost), p_buf, p_buf_end);
    p_buf = buf_put(&(header_ptr->fromport), sizeof(header_ptr->fromport),
                    p_buf, p_buf_end);
    p_buf = buf_put(&node, sizeof(header_ptr->fromnode), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(header_ptr->tohost), sizeof(header_ptr->tohost),
                           p_buf, p_buf_end);
    p_buf = buf_put(&(header_ptr->toport), sizeof(header_ptr->toport), p_buf,
                    p_buf_end);
    p_buf = buf_put(&node, sizeof(header_ptr->tonode), p_buf, p_buf_end);
    p_buf = buf_put(&(header_ptr->type), sizeof(header_ptr->type), p_buf,
                    p_buf_end);

    return p_buf;
}

const uint8_t *net_wire_header_get(wire_header_type *header_ptr,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    int node = 0;
    if (p_buf_end < p_buf || NET_WIRE_HEADER_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(header_ptr->fromhost),
                           sizeof(header_ptr->fromhost), p_buf, p_buf_end);
    p_buf = buf_get(&(header_ptr->fromport), sizeof(header_ptr->fromport),
                    p_buf, p_buf_end);
    p_buf = buf_get(&node, sizeof(header_ptr->fromnode), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(header_ptr->tohost), sizeof(header_ptr->tohost),
                           p_buf, p_buf_end);
    p_buf = buf_get(&(header_ptr->toport), sizeof(header_ptr->toport), p_buf,
                    p_buf_end);
    p_buf = buf_get(&node, sizeof(header_ptr->tonode), p_buf, p_buf_end);
    p_buf = buf_get(&(header_ptr->type), sizeof(header_ptr->type), p_buf,
                    p_buf_end);

    return p_buf;
}

uint8_t *net_send_message_header_put(const net_send_message_header *header_ptr,
                                     uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || NET_SEND_MESSAGE_HEADER_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(header_ptr->usertype), sizeof(header_ptr->usertype),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(header_ptr->seqnum), sizeof(header_ptr->seqnum), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(header_ptr->waitforack), sizeof(header_ptr->waitforack),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(header_ptr->datalen), sizeof(header_ptr->datalen), p_buf,
                    p_buf_end);

    return p_buf;
}

const uint8_t *net_send_message_header_get(net_send_message_header *header_ptr,
                                           const uint8_t *p_buf,
                                           const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || NET_SEND_MESSAGE_HEADER_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(header_ptr->usertype), sizeof(header_ptr->usertype),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(header_ptr->seqnum), sizeof(header_ptr->seqnum), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(header_ptr->waitforack), sizeof(header_ptr->waitforack),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(header_ptr->datalen), sizeof(header_ptr->datalen), p_buf,
                    p_buf_end);

    return p_buf;
}

static uint8_t *net_ack_message_payload_type_put(
    const net_ack_message_payload_type *payload_type_ptr, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || (NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN +
                              payload_type_ptr->paylen) > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(payload_type_ptr->seqnum),
                    sizeof(payload_type_ptr->seqnum), p_buf, p_buf_end);
    p_buf = buf_put(&(payload_type_ptr->outrc), sizeof(payload_type_ptr->outrc),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(payload_type_ptr->paylen),
                    sizeof(payload_type_ptr->paylen), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
net_ack_message_payload_type_get(net_ack_message_payload_type *payload_type_ptr,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(payload_type_ptr->seqnum),
                    sizeof(payload_type_ptr->seqnum), p_buf, p_buf_end);
    p_buf = buf_get(&(payload_type_ptr->outrc), sizeof(payload_type_ptr->outrc),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(payload_type_ptr->paylen),
                    sizeof(payload_type_ptr->paylen), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
net_ack_message_type_put(const net_ack_message_type *p_net_ack_message_type,
                         uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || NET_ACK_MESSAGE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_net_ack_message_type->seqnum),
                    sizeof(p_net_ack_message_type->seqnum), p_buf, p_buf_end);
    p_buf = buf_put(&(p_net_ack_message_type->outrc),
                    sizeof(p_net_ack_message_type->outrc), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
net_ack_message_type_get(net_ack_message_type *p_net_ack_message_type,
                         const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || NET_ACK_MESSAGE_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_net_ack_message_type->seqnum),
                    sizeof(p_net_ack_message_type->seqnum), p_buf, p_buf_end);
    p_buf = buf_get(&(p_net_ack_message_type->outrc),
                    sizeof(p_net_ack_message_type->outrc), p_buf, p_buf_end);

    return p_buf;
}

void update_host_net_queue_stats(host_node_type *host_node_ptr, size_t count, size_t bytes) {
    host_node_ptr->enque_count += count;
    if (host_node_ptr->enque_count > host_node_ptr->peak_enque_count) {
        host_node_ptr->peak_enque_count = host_node_ptr->enque_count;
        host_node_ptr->peak_enque_count_time = comdb2_time_epoch();
    }
    host_node_ptr->enque_bytes += bytes;
    if (host_node_ptr->enque_bytes > host_node_ptr->peak_enque_bytes) {
        host_node_ptr->peak_enque_bytes = host_node_ptr->enque_bytes;
        host_node_ptr->peak_enque_bytes_time = comdb2_time_epoch();
    }
}

/* Enque a net message consisting of a header and some optional data.
 * The caller should hold the enque lock.
 * Note that dataptr1==NULL => datasz1==0 and dataptr2==NULL => datasz2==0
 */
static int write_list(netinfo_type *netinfo_ptr, host_node_type *host_node_ptr,
                      const wire_header_type *headptr, const struct iovec *iov,
                      int iovcount, int flags)
{
    return write_list_evbuffer(host_node_ptr, headptr->type, iov, iovcount, flags);
}

/*
 * Retrieve the host_node_type by name.
 * Caller should be holding netinfo_ptr->lock.
 */
host_node_type *get_host_node_by_name_ll(netinfo_type *netinfo_ptr,
                                         const char name[])
{
    if (!isinterned(name))
        abort();

    host_node_type *ptr = NULL;
    ptr = netinfo_ptr->head;
    while (ptr != NULL && ptr->host != name)
        ptr = ptr->next;

    return ptr;
}

static uint64_t net_delayed = 0;

// 10000 * 0.1 ms = 1s
#define net_delay_mult 10000

// don't delay > 5s
#define net_delay_max (5 * net_delay_mult)

void print_netdelay(void)
{
    int d = debug_switch_net_delay();
    double delay = (double)d / 10; // 0.1ms -> ms
    const char *status = "no";
    if (d && delay <= net_delay_max)
        status = "yes";
    logmsg(LOGMSG_USER,
           "netdelay=> delay:%.1fms delayed:%" PRIu64 " delaying:%s\n", delay,
           net_delayed, status);
}

static void timeval_to_timespec(struct timeval *tv, struct timespec *ts)
{
    ts->tv_sec = tv->tv_sec;
    ts->tv_nsec = tv->tv_usec * 1000;
}

static void timespec_to_timeval(struct timespec *ts, struct timeval *tv)
{
    tv->tv_sec = ts->tv_sec;
    tv->tv_usec = ts->tv_nsec / 1000;
}

static int timeval_cmp(struct timeval *x, struct timeval *y)
{
    if (x->tv_sec > y->tv_sec)
        return 1;
    if (x->tv_sec < y->tv_sec)
        return -1;
    if (x->tv_usec > y->tv_usec)
        return 1;
    if (x->tv_usec < y->tv_usec)
        return -1;
    return 0;
}

void comdb2_nanosleep(struct timespec *req)
{
    struct timeval before, now, need, elapsed;
    timespec_to_timeval(req, &need);
    gettimeofday(&before, NULL);
    do {
        sched_yield();
        gettimeofday(&now, NULL);
        timersub(&now, &before, &elapsed);
    } while (timeval_cmp(&elapsed, &need) < 0);
}

void net_delay(const char *host)
{
    if (!host)
        return;
    int delay = debug_switch_net_delay();
    if (delay) {
        if (delay > net_delay_max)
            return;
        int other_room;
        struct timespec req;
        time_t sec;
        other_room = getroom_callback(NULL, host);
        if (gbl_myroom == other_room)
            return;
        sec = 0;
        if (delay >= net_delay_mult) {
            sec = delay / net_delay_mult;
            delay = delay % net_delay_mult;
        }
        req.tv_sec = sec;
        req.tv_nsec = delay * 100000; // 0.1 ms -> ns

#ifdef _LINUX_SOURCE
        // spin for delay < 10ms
        if (delay < 100)
            comdb2_nanosleep(&req);
        else
#endif
            nanosleep(&req, NULL);
        ++net_delayed;
    }
}

extern ssl_mode gbl_rep_ssl_mode;
extern SSL_CTX *gbl_ssl_ctx;

int write_connect_message(netinfo_type *netinfo_ptr,
                          host_node_type *host_node_ptr, SBUF2 *sb)
{
    connect_message_type connect_message;
    uint8_t conndata[NET_CONNECT_MESSAGE_TYPE_LEN] = {0}, *p_buf, *p_buf_end;
    char type = 0;
    int append_to = 0, append_from = 0;


    memset(&connect_message, 0, sizeof(connect_message_type));

    if (host_node_ptr->hostname_len > HOSTNAME_LEN) {
        snprintf(connect_message.to_hostname,
                 sizeof(connect_message.to_hostname), ".%d",
                 host_node_ptr->hostname_len);
        append_to = 1;
    } else {
        strncpy0(connect_message.to_hostname, host_node_ptr->host,
                 sizeof(connect_message.to_hostname));
    }
    connect_message.to_portnum = host_node_ptr->port;
    /* It was `to_nodenum`. */
    connect_message.flags = 0;
    if (SSL_IS_REQUIRED(gbl_rep_ssl_mode))
        connect_message.flags |= CONNECT_MSG_SSL;

    if (netinfo_ptr->myhostname_len > HOSTNAME_LEN) {
        snprintf(connect_message.from_hostname,
                 sizeof(connect_message.from_hostname), ".%d",
                 netinfo_ptr->myhostname_len);
        append_from = 1;
    } else {
        strncpy0(connect_message.from_hostname, netinfo_ptr->myhostname,
                 sizeof(connect_message.from_hostname));
    }

    if (gbl_accept_on_child_nets || !netinfo_ptr->ischild) {
        connect_message.from_portnum = netinfo_ptr->myport;
    } else {
        connect_message.from_portnum =
            netinfo_ptr->parent->myport | (netinfo_ptr->netnum << 16);
    }

    connect_message.from_nodenum = 0;

    p_buf = conndata;
    p_buf_end = (conndata + sizeof(conndata));

    net_connect_message_put(&connect_message, p_buf, p_buf_end);

    int i = 0;
    int n = 2 + append_from + append_to;
    struct iovec iov[n];

    iov[i].iov_base = &type;
    iov[i].iov_len = sizeof(type);
    ++i;

    iov[i].iov_base = &conndata;
    iov[i].iov_len = NET_CONNECT_MESSAGE_TYPE_LEN;
    ++i;

    if (append_from) {
        iov[i].iov_base = netinfo_ptr->myhostname;
        iov[i].iov_len = netinfo_ptr->myhostname_len;
        ++i;
    }

    if (append_to) {
        iov[i].iov_base = host_node_ptr->host;
        iov[i].iov_len = host_node_ptr->hostname_len;
        ++i;
    }

    return write_connect_message_evbuffer(host_node_ptr, iov, n);
}

/* To reduce double buffering and other daftness this has evolved a sort of
 * writev style interface with data1 and data2. */
static int write_message_int(netinfo_type *netinfo_ptr,
                             host_node_type *host_node_ptr, int type,
                             const struct iovec *iov, int iovcount, int flags)
{
    wire_header_type wire_header;
    int rc;

    if ((flags & WRITE_MSG_NOHELLOCHECK) == 0) {
        if (!host_node_ptr->got_hello) {
            return -9;
        }
    }

    /* The writer thread will fill in these details later.. for now, we don't
     * necessarily know the correct details anyway. */
    /*
    strncpy0(wire_header.fromhost, netinfo_ptr->myhostname,
       sizeof(wire_header.fromhost));
    wire_header.fromport = netinfo_ptr->myport;
    wire_header.fromnode = netinfo_ptr->mynode;
    strncpy0(wire_header.tohost, host_node_ptr->host,
       sizeof(wire_header.tohost));
    wire_header.toport = host_node_ptr->port;
    wire_header.tonode = host_node_ptr->node;
    */

    wire_header.type = type;

    /* Add this message to our linked list to send. */
    rc = write_list(netinfo_ptr, host_node_ptr, &wire_header, iov, iovcount, flags);
    if (rc < 0) {
        if (rc == -1) {
            logmsg(LOGMSG_ERROR, "%s: got reallybad failure?\n", __func__);
            return 2;
        } else {
            return rc;
        }
    }
    return 0;
}

static int write_message_checkhello(netinfo_type *netinfo_ptr,
                                    host_node_type *host_node_ptr, int type,
                                    const struct iovec *iov, int iovcount,
                                    int nodelay, int nodrop, int inorder)
{
    return write_message_int(netinfo_ptr, host_node_ptr, type, iov, iovcount,
                             (nodelay ? WRITE_MSG_NODELAY : 0) |
                                 (nodrop ? WRITE_MSG_NOLIMIT : 0) |
                                 (inorder ? WRITE_MSG_INORDER : 0));
}

static int write_message_nohello(netinfo_type *netinfo_ptr,
                                 host_node_type *host_node_ptr, int type,
                                 const void *data, size_t datalen)
{
    struct iovec iov = {(void *)data, datalen};
    return write_message_int(netinfo_ptr, host_node_ptr, type, &iov, 1,
                             WRITE_MSG_NODELAY | WRITE_MSG_NOHELLOCHECK);
}

static int write_message(netinfo_type *netinfo_ptr,
                         host_node_type *host_node_ptr, int type,
                         const void *data, size_t datalen)
{
    struct iovec iov = {(void *)data, datalen};
    return write_message_int(netinfo_ptr, host_node_ptr, type, &iov, 1,
                             WRITE_MSG_NODELAY);
}

int write_heartbeat(netinfo_type *netinfo_ptr, host_node_type *host_node_ptr)
{
    /* heartbeats always jump to the head */
    int flags = WRITE_MSG_HEAD | WRITE_MSG_NODUPE | WRITE_MSG_NODELAY | WRITE_MSG_NOLIMIT;
    return write_message_int(netinfo_ptr, host_node_ptr, WIRE_HEADER_HEARTBEAT, NULL, 0, flags);
}

typedef int (hello_func)(netinfo_type *, host_node_type *, int type,
                          const void *, size_t);

/*
  this is the protocol where each node advertises all the other nodes
  they know about so that eventually (quickly) every node know about
  every other nodes
*/
static int write_hello_int(netinfo_type *netinfo_ptr, host_node_type *host_node_ptr, int type, hello_func *func)
{
    int rc;
    int numhosts;
    char *data;
    uint8_t *p_buf, *p_buf_end;
    host_node_type *tmp_host_ptr;
    int datasz;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    numhosts = 0;
    for (tmp_host_ptr = netinfo_ptr->head; tmp_host_ptr != NULL;
         tmp_host_ptr = tmp_host_ptr->next)
        numhosts++;

    datasz = sizeof(int) + sizeof(int) + /* int numhosts */
             (HOSTNAME_LEN * numhosts) + /* char host[16]... ( 1 per host ) */
             (sizeof(int) * numhosts)  + /* int port...      ( 1 per host ) */
             (sizeof(int) * numhosts);   /* int node...      ( 1 per host ) */

    /* write long hostnames */
    for (tmp_host_ptr = netinfo_ptr->head; tmp_host_ptr != NULL;
         tmp_host_ptr = tmp_host_ptr->next) {
        if (tmp_host_ptr->hostname_len > HOSTNAME_LEN)
            datasz += tmp_host_ptr->hostname_len;
    }
    data = malloc(datasz);
    memset(data, 0, datasz);

    p_buf = (uint8_t *)data;
    p_buf_end = (uint8_t *)(data + datasz);

    p_buf = buf_put(&datasz, sizeof(int), p_buf, p_buf_end);

    /* fill in numhosts */
    p_buf = buf_put(&numhosts, sizeof(int), p_buf, p_buf_end);

    /* fill in hostnames */
    for (tmp_host_ptr = netinfo_ptr->head; tmp_host_ptr != NULL;
         tmp_host_ptr = tmp_host_ptr->next) {
        if (tmp_host_ptr->hostname_len > HOSTNAME_LEN) {
            char lenstr[HOSTNAME_LEN] = {0};
            snprintf(lenstr, sizeof(lenstr), ".%d", tmp_host_ptr->hostname_len);
            lenstr[HOSTNAME_LEN - 1] = 0;
            p_buf = buf_no_net_put(lenstr, HOSTNAME_LEN - 1, p_buf, p_buf_end);
        } else {
            char lenstr[HOSTNAME_LEN] = {0};
            memcpy(lenstr, tmp_host_ptr->host, tmp_host_ptr->hostname_len);
            lenstr[HOSTNAME_LEN - 1] = 0;
            p_buf = buf_no_net_put(lenstr, HOSTNAME_LEN - 1, p_buf, p_buf_end);
        }
        /* null terminate */
        p_buf = buf_zero_put(sizeof(char), p_buf, p_buf_end);
    }

    for (tmp_host_ptr = netinfo_ptr->head; tmp_host_ptr != NULL;
         tmp_host_ptr = tmp_host_ptr->next) {
        p_buf = buf_put(&tmp_host_ptr->port, sizeof(int), p_buf, p_buf_end);
    }

    for (tmp_host_ptr = netinfo_ptr->head; tmp_host_ptr != NULL;
         tmp_host_ptr = tmp_host_ptr->next) {
        int node = machine_num(tmp_host_ptr->host);
        p_buf = buf_put(&node, sizeof(int), p_buf, p_buf_end);
    }
    /* write long hostnames */
    for (tmp_host_ptr = netinfo_ptr->head; tmp_host_ptr != NULL;
         tmp_host_ptr = tmp_host_ptr->next) {
        if (tmp_host_ptr->hostname_len > HOSTNAME_LEN) {
            p_buf =
                buf_no_net_put(tmp_host_ptr->host, tmp_host_ptr->hostname_len,
                               p_buf, p_buf_end);
        }
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    rc = func(netinfo_ptr, host_node_ptr, type, data, datasz);

    free(data);

    return rc;
}

int write_hello(netinfo_type *netinfo_ptr, host_node_type *host_node_ptr)
{
    return write_hello_int(netinfo_ptr, host_node_ptr, WIRE_HEADER_HELLO,
                           write_message_nohello);
}

int write_hello_reply(netinfo_type *netinfo_ptr, host_node_type *host_node_ptr)
{
    return write_hello_int(netinfo_ptr, host_node_ptr, WIRE_HEADER_HELLO_REPLY,
                           write_message);
}

static void add_millisecs_to_timespec(struct timespec *orig, int millisecs)
{
    int nanosecs = orig->tv_nsec;
    int secs = orig->tv_sec;

    secs += (millisecs / 1000);
    millisecs = (millisecs % 1000);

    nanosecs += (millisecs * MILLION);
    secs += (nanosecs / BILLION);
    nanosecs = (nanosecs % BILLION);
    orig->tv_sec = secs;
    orig->tv_nsec = nanosecs;
    return;
}

static seq_data *add_seqnum_to_waitlist(host_node_type *host_node_ptr,
                                        int seqnum)
{
    seq_data *new_seq_node, *seq_list_ptr;
    new_seq_node = malloc(sizeof(seq_data));
    memset(new_seq_node, 0, sizeof(seq_data));
    new_seq_node->seqnum = seqnum;
    new_seq_node->timestamp = time(NULL);
    /* always add to the end of the list. */
    /* only remove from the beginning, and then,
       only if the "ack" has occurred */
    if (host_node_ptr->wait_list == NULL)
        host_node_ptr->wait_list = new_seq_node;
    else {
        seq_list_ptr = host_node_ptr->wait_list;
        while (seq_list_ptr->next != NULL)
            seq_list_ptr = seq_list_ptr->next;
        seq_list_ptr->next = new_seq_node;
    }
    return new_seq_node;
}

/* already under lock */
static int remove_seqnum_from_waitlist(host_node_type *host_node_ptr,
                                       void **payloadptr, int *payloadlen,
                                       int seqnum)
{
    seq_data *seq_list_ptr, *back;
    int outrc;

    back = seq_list_ptr = host_node_ptr->wait_list;

    while (seq_list_ptr != NULL && seq_list_ptr->seqnum != seqnum) {
        back = seq_list_ptr;
        seq_list_ptr = seq_list_ptr->next;
    }
    if (seq_list_ptr == NULL) {
        /*fprintf(stderr,"cant find seq num %d in waitlist\n", seqnum);*/
        return -1;
    }
    if (seq_list_ptr == host_node_ptr->wait_list)
        host_node_ptr->wait_list = host_node_ptr->wait_list->next;
    else
        back->next = seq_list_ptr->next;

    outrc = seq_list_ptr->outrc;
    if (payloadptr) {
        (*payloadptr) = seq_list_ptr->payload;
        seq_list_ptr->payload = NULL;
        (*payloadlen) = seq_list_ptr->payloadlen;
    }
    if (seq_list_ptr->payload)
        free(seq_list_ptr->payload);
    free(seq_list_ptr);

    return outrc;
}


int net_send_message_payload_ack(netinfo_type *netinfo_ptr, const char *to_host,
                                 int usertype, void *data, int datalen,
                                 uint8_t **payloadptr, int *payloadlen,
                                 int waitforack, int waitms)
{
    net_send_message_header tmphd, msghd;
    uint8_t *p_buf, *p_buf_end;
    seq_data *seq_ptr;
    host_node_type *host_node_ptr;
    int rc;
    struct timespec waittime;
#ifndef HAS_CLOCK_GETTIME
    struct timeval tv;
#endif
    struct iovec iov[2];

    rc = 0;

    /* do nothing if we have a fake netinfo */
    if (netinfo_ptr->fake)
        return 0;
    if (to_host == NULL)
        abort();

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));
    host_node_ptr = get_host_node_by_name_ll(netinfo_ptr, to_host);
    if (host_node_ptr == NULL) {
        Pthread_rwlock_unlock(&(netinfo_ptr->lock));
        return NET_SEND_FAIL_INVALIDNODE;
    }

    if (host_node_ptr->host == netinfo_ptr->myhostname) {
        rc = NET_SEND_FAIL_SENDTOME;
        goto end;
    }

    /* fail if we don't have a socket */
    if (host_node_ptr->fd == -1 || host_node_ptr->closed) {
        rc = NET_SEND_FAIL_NOSOCK;
        goto end;
    }

    msghd.usertype = usertype;
    msghd.seqnum = ATOMIC_ADD32(netinfo_ptr->seqnum, 1);
    msghd.waitforack = waitforack;
    msghd.datalen = datalen;

    p_buf = (uint8_t *)&tmphd;
    p_buf_end = ((uint8_t *)&tmphd + sizeof(net_send_message_header));

    net_send_message_header_put(&msghd, p_buf, p_buf_end);

    iov[0].iov_base = (int8_t *)&tmphd;
    iov[0].iov_len = sizeof(tmphd);
    iov[1].iov_base = data;
    iov[1].iov_len = datalen;

    Pthread_mutex_lock(&(host_node_ptr->wait_mutex));

    if (waitforack) {
        seq_ptr = add_seqnum_to_waitlist(host_node_ptr, msghd.seqnum);
        seq_ptr->ack = 0;
    } else
        seq_ptr = NULL;

    rc = write_message_checkhello(netinfo_ptr, host_node_ptr,
                                  WIRE_HEADER_USER_MSG, iov, 2, 1 /*nodelay*/,
                                  0, 0);

    if (rc != 0) {
        if (seq_ptr)
            remove_seqnum_from_waitlist(host_node_ptr, (void**) payloadptr, 
                                        payloadlen, seq_ptr->seqnum);
        Pthread_mutex_unlock(&(host_node_ptr->wait_mutex));

        rc = NET_SEND_FAIL_WRITEFAIL;
        goto end;
    }

    if (!waitforack) {
        Pthread_mutex_unlock(&(host_node_ptr->wait_mutex));
        rc = 0;
        goto end;
    }

#ifdef HAS_CLOCK_GETTIME
    rc = clock_gettime(CLOCK_REALTIME, &waittime);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "clock_gettime err %d %s\n", errno, strerror(errno));
        remove_seqnum_from_waitlist(host_node_ptr, payloadptr, payloadlen,
                                    seq_ptr->seqnum);
        Pthread_mutex_unlock(&(host_node_ptr->wait_mutex));

        rc = NET_SEND_FAIL_INTERNAL;
        goto end;
    }
#else
    rc = gettimeofday(&tv, NULL);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "gettimeofday failed\n");
        remove_seqnum_from_waitlist(host_node_ptr, (void**) payloadptr, payloadlen, 
                                    seq_ptr->seqnum);
        Pthread_mutex_unlock(&(host_node_ptr->wait_mutex));

        rc = NET_SEND_FAIL_INTERNAL;
        goto end;
    }

    timeval_to_timespec(&tv, &waittime);
#endif

    add_millisecs_to_timespec(&waittime, waitms);

    rc = 0;
    while (1) {
        if (seq_ptr->ack == 1) {
            rc = remove_seqnum_from_waitlist(host_node_ptr, (void**) payloadptr,
                                             payloadlen, seq_ptr->seqnum);
            /* user is only allowed to return >=0 */
            if (rc < 0)
                rc = NET_SEND_FAIL_INVALIDACKRC;

            Pthread_mutex_unlock(&(host_node_ptr->wait_mutex));

            if (rc == ETIMEDOUT) {
                logmsg(LOGMSG_ERROR, "timeout, but found reply afterwards??\n");
            }
            goto end;
        }

        if (rc == ETIMEDOUT) {
            remove_seqnum_from_waitlist(host_node_ptr, (void**) payloadptr,
                                        payloadlen, seq_ptr->seqnum);
            logmsg(LOGMSG_ERROR, "net_send_message: timeout to %s\n",
                    host_node_ptr->host);

            Pthread_mutex_unlock(&(host_node_ptr->wait_mutex));

            rc = NET_SEND_FAIL_TIMEOUT;
            goto end;
        } else if (rc != 0) {
            remove_seqnum_from_waitlist(host_node_ptr, (void**) payloadptr,
                                        payloadlen, seq_ptr->seqnum);
            Pthread_mutex_unlock(&(host_node_ptr->wait_mutex));
            logmsg(LOGMSG_ERROR,
                   "net_send_message: host %s, "
                   "got rc = %d from pthread_cond_timedwait\n",
                   host_node_ptr->host, rc);

            rc = NET_SEND_FAIL_INTERNAL;
            goto end;
        }

        Pthread_rwlock_unlock(&(netinfo_ptr->lock));
        rc = pthread_cond_timedwait(&(host_node_ptr->ack_wakeup), &(host_node_ptr->wait_mutex), &waittime);
        if (rc == EINVAL) return rc;
        Pthread_rwlock_rdlock(&(netinfo_ptr->lock));
    }

end:
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
    return rc;
}

int net_send_message(netinfo_type *netinfo_ptr, const char *to_host,
                     int usertype, void *data, int datalen, int waitforack,
                     int waitms)
{
    return net_send_message_payload_ack(netinfo_ptr, to_host, usertype, data,
                                        datalen, NULL, NULL, waitforack,
                                        waitms);
}

static int net_send_int(netinfo_type *netinfo_ptr, const char *host,
                        int usertype, void *data, int datalen, int nodelay,
                        int numtails, void **tails, int *taillens, int nodrop,
                        int inorder, int trace)
{
    int f = 0;
    if (nodelay) f |= NET_SEND_NODELAY;
    if (nodrop) f |= NET_SEND_NODROP;
    return net_send_evbuffer(netinfo_ptr, host, usertype, data, datalen, numtails, tails, taillens, f);
}

int net_send_authcheck_all(netinfo_type *netinfo_ptr)
{
    int rc, count = 0, i;
    const char *nodes[REPMAX];
    int outrc = 0;

    count = net_get_all_nodes(netinfo_ptr, nodes);

    for (i = 0; i < count; i++) {
        rc = net_send_message(netinfo_ptr, nodes[i], NET_AUTHENTICATION_CHECK,
                              NULL, 0, 0, 5000);
        if (rc < 0) {
            logmsg(LOGMSG_ERROR,
                   "Sending Auth Check failed for node %s rc=%d\n", nodes[i],
                   rc);
            outrc++;
        }
    }
    return outrc;
}

int net_send_flags(netinfo_type *netinfo_ptr, const char *host, int usertype,
                   void *data, int datalen, uint32_t flags)
{
    return net_send_int(netinfo_ptr, host, usertype, data, datalen,
                        (flags & NET_SEND_NODELAY), 0, NULL, 0,
                        (flags & NET_SEND_NODROP), (flags & NET_SEND_INORDER),
                        (flags & NET_SEND_TRACE));
}

int net_send(netinfo_type *netinfo_ptr, const char *host, int usertype,
             void *data, int datalen, int nodelay)
{

    return net_send_int(netinfo_ptr, host, usertype, data, datalen, nodelay, 0,
                        NULL, 0, 0, 0, 0);
}

int net_send_nodrop(netinfo_type *netinfo_ptr, const char *host, int usertype,
                    void *data, int datalen, int nodelay)
{

    return net_send_int(netinfo_ptr, host, usertype, data, datalen, nodelay, 0,
                        NULL, 0, 1, 0, 0);
}

int net_send_tails(netinfo_type *netinfo_ptr, const char *host, int usertype,
                   void *data, int datalen, int nodelay, int numtails,
                   void **tails, int *taillens)
{

    return net_send_int(netinfo_ptr, host, usertype, data, datalen, nodelay,
                        numtails, tails, taillens, 0, 0, 0);
}

int net_send_tail(netinfo_type *netinfo_ptr, const char *host, int usertype,
                  void *data, int datalen, int nodelay, void *tail, int tailen)
{

#ifdef _BLOCKSQL_DBG
    int i = 0;
    printf("Sending data [%d]:\n", datalen);
    for (i = 0; i < datalen; i++)
        printf("%02x ", ((char *)data)[i]);
    printf("\n");

    printf("Sending tail[%d]:\n", tailen);
    for (i = 0; i < tailen; i++)
        printf("%02x ", ((char *)tail)[i]);
    printf("\n");
#endif
    return net_send_int(netinfo_ptr, host, usertype, data, datalen, nodelay, 1,
                        &tail, &tailen, 0, 0, 0);
}

/* returns all nodes MINUS you */
int net_get_all_nodes(netinfo_type *netinfo_ptr, const char *hostlist[REPMAX])
{
    host_node_type *ptr;
    int count = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        /* dont send to yourself */
        if (ptr->host == netinfo_ptr->myhostname)
            continue;

        hostlist[count++] = ptr->host;
        if (count >= REPMAX)
            break;
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return count;
}

int net_get_all_nodes_interned(netinfo_type *netinfo_ptr, struct interned_string *hostlist[REPMAX])
{
    host_node_type *ptr;
    int count = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        /* dont send to yourself */
        if (ptr->host == netinfo_ptr->myhostname)
            continue;

        hostlist[count++] = ptr->host_interned;
        if (count >= REPMAX)
            break;
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return count;
}



int net_get_all_commissioned_nodes_interned(netinfo_type *netinfo_ptr, struct interned_string *hostlist[REPMAX])
{
    host_node_type *ptr;
    int count = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        /* dont send to yourself */
        if (ptr->host == netinfo_ptr->myhostname)
            continue;

        if (!ptr->decom_flag) {
            hostlist[count++] = ptr->host_interned;
            if (count >= REPMAX)
                break;
        }
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return count;
}

int net_get_all_commissioned_nodes(netinfo_type *netinfo_ptr,
                                   const char *hostlist[REPMAX])
{
    host_node_type *ptr;
    int count = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        /* dont send to yourself */
        if (ptr->host == netinfo_ptr->myhostname)
            continue;

        if (!ptr->decom_flag) {
            hostlist[count++] = ptr->host;
            if (count >= REPMAX)
                break;
        }
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return count;
}

int net_get_all_nodes_connected_interned(netinfo_type *netinfo_ptr, struct interned_string *hostlist[REPMAX])
{
    host_node_type *ptr;
    int count = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        /* dont send to yourself */
        if (ptr->host == netinfo_ptr->myhostname)
            continue;

        /* dont count disconected guys */
        if (ptr->fd <= 0)
            continue;

        /* dont count guys that didnt hello us */
        if (!ptr->got_hello)
            continue;

        hostlist[count++] = ptr->host_interned;
        if (count >= REPMAX)
            break;
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return count;
}

int net_get_all_nodes_connected(netinfo_type *netinfo_ptr,
                                const char *hostlist[REPMAX])
{
    host_node_type *ptr;
    int count = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        /* dont send to yourself */
        if (ptr->host == netinfo_ptr->myhostname)
            continue;

        /* dont count disconected guys */
        if (ptr->fd <= 0)
            continue;

        /* dont count guys that didnt hello us */
        if (!ptr->got_hello)
            continue;

        hostlist[count++] = ptr->host;
        if (count >= REPMAX)
            break;
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return count;
}

int net_register_queue_stat(netinfo_type *netinfo_ptr, QSTATINITFP *qinit,
                            QSTATREADERFP *reader, QSTATENQUEFP *enque,
                            QSTATDUMPFP *dump, QSTATCLEARFP *qclear,
                            QSTATFREEFP *qfree)
{
    host_node_type *tmp_host_ptr;

    /* Set qstat for each existing node */
    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (tmp_host_ptr = netinfo_ptr->head; tmp_host_ptr != NULL;
         tmp_host_ptr = tmp_host_ptr->next) {
        if (strcmp(tmp_host_ptr->host, netinfo_ptr->myhostname) != 0) {
            tmp_host_ptr->qstat =
                qinit(netinfo_ptr, netinfo_ptr->service, tmp_host_ptr->host);
        }
    }

    netinfo_ptr->qstat_free_rtn = qfree;
    netinfo_ptr->qstat_init_rtn = qinit;
    netinfo_ptr->qstat_reader_rtn = reader;
    netinfo_ptr->qstat_enque_rtn = enque;
    netinfo_ptr->qstat_dump_rtn = dump;
    netinfo_ptr->qstat_clear_rtn = qclear;
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return 0;
}

void net_userfunc_iterate(netinfo_type *netinfo_ptr, UFUNCITERFP *uf_iter,
                          void *arg)
{
    for (int i = 0; i < USER_TYPE_MAX; i++) {
        if (netinfo_ptr->userfuncs[i].func) {
            uf_iter(netinfo_ptr, arg, netinfo_ptr->service,
                    netinfo_ptr->userfuncs[i].name,
                    netinfo_ptr->userfuncs[i].count,
                    netinfo_ptr->userfuncs[i].totus);
        }
    }
}

void net_queue_stat_iterate(netinfo_type *netinfo_ptr, QSTATITERFP qs_iter, struct net_get_records *arg)
{
    net_queue_stat_iterate_evbuffer(netinfo_ptr, qs_iter, arg);
}

int net_register_hostdown(netinfo_type *netinfo_ptr, HOSTDOWNFP *func)
{
    netinfo_ptr->hostdown_rtn = func;

    return 0;
}

int net_register_name(netinfo_type *netinfo_ptr, char name[])
{
    netinfo_ptr->name = strdup(name);

    return 0;
}

int net_register_handler(netinfo_type *netinfo_ptr, int usertype,
                         char *name, NETFP func)
{
    if (usertype <= USER_TYPE_MIN || usertype >= USER_TYPE_MAX)
        return -1;
    netinfo_ptr->userfuncs[usertype].func = func;
    netinfo_ptr->userfuncs[usertype].name = name;
    netinfo_ptr->userfuncs[usertype].totus = 0;
    netinfo_ptr->userfuncs[usertype].count = 0;

    return 0;
}

int is_real_netinfo(netinfo_type *netinfo_ptr)
{
    if (!netinfo_ptr->fake)
        return 1;
    else
        return 0;
}

/* This function needs to be called with netinfo_ptr->lock held.
 *
 * Will get netinfo for node, check cache first:
 * if not the last used node then do a linear search in list
 */
static inline host_node_type *get_host_node_cache_ll(netinfo_type *netinfo_ptr,
                                                     const char *host)
{
    host_node_type *host_node_ptr = netinfo_ptr->last_used_node_ptr;
    if (!host_node_ptr || host_node_ptr->host != host) {
        host_node_ptr = get_host_node_by_name_ll(netinfo_ptr, host);
        netinfo_ptr->last_used_node_ptr = host_node_ptr;
        netinfo_ptr->last_used_node_miss_cntr++;
    } else {
        netinfo_ptr->last_used_node_hit_cntr++;
    }
    return host_node_ptr;
}


void net_inc_recv_cnt_from(netinfo_type *netinfo_ptr, char *host)
{
    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));
    host_node_type *host_node_ptr = get_host_node_cache_ll(netinfo_ptr, host);
    if (!host_node_ptr) {
        Pthread_rwlock_unlock(&(netinfo_ptr->lock));
        logmsg(LOGMSG_ERROR, "%s: node not found %s\n", __func__, host);
        return;
    }

    ++host_node_ptr->udp_info.recv;
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
}

void net_reset_udp_stat(netinfo_type *netinfo_ptr)
{
    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (host_node_type *ptr = netinfo_ptr->head; ptr != NULL;
         ptr = ptr->next) {
        ptr->udp_info.sent = 0;
        ptr->udp_info.recv = 0;
    }

    netinfo_ptr->last_used_node_hit_cntr = 0;
    netinfo_ptr->last_used_node_miss_cntr = 0;
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
}

void print_all_udp_stat(netinfo_type *netinfo_ptr)
{
    if (!netinfo_ptr) return;
    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (host_node_type *ptr = netinfo_ptr->head; ptr != NULL;
         ptr = ptr->next) {
        struct sockaddr_in sin;
        sin.sin_addr = ptr->addr;
        sin.sin_family = AF_INET;
        sin.sin_port = htons(ptr->port);
        int port = ptr->port;
        uint64_t sent = ptr->udp_info.sent;
        char buf1[256];
#ifdef UDP_DEBUG
        uint64_t recv = ptr->udp_info.recv;
        printf("node:%s port:%5d recv:%7llu sent:%7lu %s\n", ptr->host, port,
               recv, sent, print_addr(&sin, buf1));
#else
        logmsg(LOGMSG_USER, "node:%s port:%5d sent:%7" PRIu64 " %s\n",
               ptr->host, port, sent, print_addr(&sin, buf1));
#endif
    }
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
    logmsg(LOGMSG_USER, "netinfo udp cache hits: %u misses: %u lastnode: %s\n",
           netinfo_ptr->last_used_node_hit_cntr,
           netinfo_ptr->last_used_node_miss_cntr,
           (netinfo_ptr->last_used_node_ptr
                ? netinfo_ptr->last_used_node_ptr->host
                : NULL));
}

void print_node_udp_stat(char *prefix, netinfo_type *netinfo_ptr,
                         const char *host)
{
    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));
    host_node_type *host_node_ptr = get_host_node_cache_ll(netinfo_ptr, host);
    if (!host_node_ptr) {
        Pthread_rwlock_unlock(&(netinfo_ptr->lock));
        logmsg(LOGMSG_ERROR, "%s: node not found %s\n", __func__, host);
        return;
    }

    int port = host_node_ptr->port;
    uint64_t sent = host_node_ptr->udp_info.sent;
    uint64_t recv = host_node_ptr->udp_info.recv;
    struct in_addr addr = host_node_ptr->addr;
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    logmsg(LOGMSG_USER,
           "%snode:%s port:%5d recv:%7" PRIu64 " sent:%7" PRIu64 " [%s]\n",
           prefix, host, port, recv, sent, inet_ntoa(addr));
}

ssize_t net_udp_send(int udp_fd, netinfo_type *netinfo_ptr, const char *host,
                     size_t len, void *info)
{
    struct sockaddr_in paddr;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));
    host_node_type *host_node_ptr = get_host_node_cache_ll(netinfo_ptr, host);

    if (!host_node_ptr) {
        Pthread_rwlock_unlock(&(netinfo_ptr->lock));
        extern const char *db_eid_invalid;
        if (strcmp(host, db_eid_invalid) == 0)
            return -999;
        logmsg(LOGMSG_ERROR, "%s: node not found %s\n", __func__, host);
        return -1;
    }
    ++host_node_ptr->udp_info.sent;
    paddr.sin_addr = host_node_ptr->addr;
    paddr.sin_port = htons(host_node_ptr->port);
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    paddr.sin_family = AF_INET;
    socklen_t addrlen = sizeof(paddr);

#ifdef UDP_DEBUG
    __atomic_add_fetch(&curr_udp_cnt, 1, __ATOMIC_SEQ_CST);
#endif

    net_delay(host);
    ssize_t nsent =
        sendto(udp_fd, info, len, 0, (struct sockaddr *)&paddr, addrlen);

    if (nsent < 0) {
        logmsgperror("net_udp_send:sendto");
        logmsg(LOGMSG_USER, "dest=%s, addr=%s\n", host,
               inet_ntoa(paddr.sin_addr));
    }

    return nsent;
}

host_node_type *add_to_netinfo_ll(netinfo_type *netinfo_ptr, const char hostname[], int portnum)
{
    int count = 0;
    host_node_type *ptr;
    /* check to see if the node already exists */
    ptr = netinfo_ptr->head;
    while (ptr != NULL && count < REPMAX && ptr->host != hostname) {
        ptr = ptr->next;
        count++;
    }
    if (ptr != NULL) {
        return ptr;
    }

    if (count==REPMAX) {
        logmsg(LOGMSG_ERROR, "Cannot add more than REPMAX(%d) number of nodes\n", REPMAX);
        return NULL;
    }

    ptr = calloc(1, sizeof(host_node_type));
    if (!ptr) {
        logmsg(LOGMSG_FATAL, "Can't allocate memory for netinfo\n");
        abort();
    }

    ptr->netinfo_ptr = netinfo_ptr;
    ptr->closed = 1;
    ptr->fd = -1;

    ptr->next = netinfo_ptr->head;
    ptr->host_interned = intern_ptr(hostname);
    ptr->host = ptr->host_interned->str;
    ptr->hostname_len = strlen(ptr->host) + 1;
    /* ptr->addr will be set by connect_thread() */
    ptr->port = portnum;

    Pthread_mutex_init(&(ptr->wait_mutex), NULL);
    Pthread_cond_init(&(ptr->ack_wakeup), NULL);

    if (netinfo_ptr->qstat_init_rtn) {
        ptr->qstat = (netinfo_ptr->qstat_init_rtn)(
            netinfo_ptr, netinfo_ptr->service, hostname);
    } else {
        ptr->qstat = NULL;
    }

    netinfo_ptr->head = ptr;

    char *metric_name = comdb2_asprintf("queue_size_%s", hostname);
    ptr->metric_queue_size = time_metric_new(metric_name);
    free(metric_name);

    return ptr;
}

host_node_type *add_to_netinfo(netinfo_type *netinfo_ptr, const char hostname[],
                               int portnum)
{
    host_node_type *ptr;

#ifdef DEBUGNET
    fprintf(stderr, "%s: adding %s\n", __func__, hostname);
#endif

    if (!isinterned(hostname))
        abort();

    /*override with smaller timeout*/
    portmux_set_default_timeout(100);

    /* don't add disallowed nodes */
    if (netinfo_ptr->allow_rtn &&
        !netinfo_ptr->allow_rtn(netinfo_ptr, hostname)) {
        logmsg(LOGMSG_ERROR, "%s: not allowed to add %s\n", __func__, hostname);
        return NULL;
    }

    /* we need to lock the netinfo to prevent creating too many connect threads
     */
    Pthread_rwlock_wrlock(&(netinfo_ptr->lock));

    ptr = add_to_netinfo_ll(netinfo_ptr, hostname, portnum);

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
    return ptr;
}

/* for debugging only */
void netinfo_lock(netinfo_type *netinfo_ptr, int seconds)
{
    logmsg(LOGMSG_USER, "grabbing exclusive access to netinfo lock\n");
    Pthread_rwlock_wrlock(&(netinfo_ptr->lock));
    logmsg(LOGMSG_USER, "sleeping for %d seconds\n", seconds);
    sleep(seconds);
    logmsg(LOGMSG_USER, "releasing netinfo lock\n");
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
}

sanc_node_type *net_add_to_sanctioned(netinfo_type *netinfo_ptr,
                                      char hostname[], int portnum)
{
    sanc_node_type *ptr;

    /* don't add disallowed nodes */
    if (netinfo_ptr->allow_rtn &&
        !netinfo_ptr->allow_rtn(netinfo_ptr, hostname)) {
        logmsg(LOGMSG_ERROR, "net_add_to_sanctioned: not allowed to add %s\n",
                hostname);
        return NULL;
    }

    logmsg(LOGMSG_INFO, "net_add_to_sanctioned %s\n", hostname);

    Pthread_mutex_lock(&(netinfo_ptr->sanclk));

    ptr = add_to_sanctioned_nolock(netinfo_ptr, hostname, portnum);

    Pthread_mutex_unlock(&(netinfo_ptr->sanclk));

    return ptr;
}

int net_is_single_sanctioned_node(netinfo_type *netinfo_ptr)
{
    int single_node = 0 ;
    sanc_node_type *ptr;

    Pthread_mutex_lock(&(netinfo_ptr->sanclk));

    ptr = netinfo_ptr->sanctioned_list;

    if (ptr && !ptr->next && !strcmp(ptr->host->str, netinfo_ptr->myhostname))
        single_node = 1;

    Pthread_mutex_unlock(&(netinfo_ptr->sanclk));

    return single_node;
}


static int net_get_sanctioned_interned_int(netinfo_type *netinfo_ptr, int max_nodes,
                                 struct interned_string *hosts[REPMAX], int include_self)
{
    int count = 0;
    sanc_node_type *ptr;

    Pthread_mutex_lock(&(netinfo_ptr->sanclk));

    for (ptr = netinfo_ptr->sanctioned_list; ptr != NULL; ptr = ptr->next) {
        if (ptr->host->str == netinfo_ptr->myhostname && !include_self)
            continue;

        if (count < max_nodes) {
            hosts[count] = ptr->host;
        }
        count++;
    }
    Pthread_mutex_unlock(&(netinfo_ptr->sanclk));

    return count;
}

static int net_get_sanctioned_int(netinfo_type *netinfo_ptr, int max_nodes,
                                 const char *hosts[REPMAX], int include_self)
{
    int count = 0;
    sanc_node_type *ptr;

    Pthread_mutex_lock(&(netinfo_ptr->sanclk));

    for (ptr = netinfo_ptr->sanctioned_list; ptr != NULL; ptr = ptr->next) {
        if (ptr->host->str == netinfo_ptr->myhostname && !include_self)
            continue;

        if (count < max_nodes) {
            hosts[count] = ptr->host->str;
        }
        count++;
    }
    Pthread_mutex_unlock(&(netinfo_ptr->sanclk));

    return count;
}

int net_get_sanctioned_replicants_interned(netinfo_type *netinfo_ptr, int max_nodes,
                                           struct interned_string *hosts[REPMAX])
{
    return net_get_sanctioned_interned_int(netinfo_ptr, max_nodes, hosts, 0);
}

int net_get_sanctioned_node_list_interned(netinfo_type *netinfo_ptr, int max_nodes,
                                          struct interned_string *hosts[REPMAX])
{
    return net_get_sanctioned_interned_int(netinfo_ptr, max_nodes, hosts, 1);
}

int net_get_sanctioned_node_list(netinfo_type *netinfo_ptr, int max_nodes,
                                 const char *hosts[REPMAX])
{
    return net_get_sanctioned_int(netinfo_ptr, max_nodes, hosts, 1);
}

int net_get_sanctioned_replicants(netinfo_type *netinfo_ptr, int max_nodes,
                                 const char *hosts[REPMAX])
{
    return net_get_sanctioned_int(netinfo_ptr, max_nodes, hosts, 0);
}

int net_sanctioned_and_connected_nodes_intern(netinfo_type *netinfo_ptr, int max_nodes,
                                       struct interned_string *hosts[REPMAX])
{
    host_node_type *ptr;
    sanc_node_type *ptr_sanc;
    int count = 0;
    int is_sanc = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        /* dont send to yourself */
        if (ptr->host == netinfo_ptr->myhostname)
            continue;

        /* dont count disconected guys */
        if (ptr->fd <= 0)
            continue;

        /* dont count guys that didnt hello us */
        if (!ptr->got_hello)
            continue;

        is_sanc = 0;
        Pthread_mutex_lock(&(netinfo_ptr->sanclk));
        for (ptr_sanc = netinfo_ptr->sanctioned_list; ptr_sanc != NULL;
             ptr_sanc = ptr_sanc->next) {
            if (strcmp(ptr_sanc->host->str, ptr->host) == 0
                /*&& ptr_sanc->port == ptr->port*/) {
                is_sanc = 1;
                break;
            }
        }
        Pthread_mutex_unlock(&(netinfo_ptr->sanclk));

        if (is_sanc) {
            hosts[count++] = ptr->host_interned;
            if (count >= REPMAX)
                break;
        }
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return count;
}

int net_sanctioned_and_connected_nodes(netinfo_type *netinfo_ptr, int max_nodes,
                                       const char *hosts[REPMAX])
{
    host_node_type *ptr;
    sanc_node_type *ptr_sanc;
    int count = 0;
    int is_sanc = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        /* dont send to yourself */
        if (ptr->host == netinfo_ptr->myhostname)
            continue;

        /* dont count disconected guys */
        if (ptr->fd <= 0)
            continue;

        /* dont count guys that didnt hello us */
        if (!ptr->got_hello)
            continue;

        is_sanc = 0;
        Pthread_mutex_lock(&(netinfo_ptr->sanclk));
        for (ptr_sanc = netinfo_ptr->sanctioned_list; ptr_sanc != NULL;
             ptr_sanc = ptr_sanc->next) {
            if (strcmp(ptr_sanc->host->str, ptr->host) == 0) {
                is_sanc = 1;
                break;
            }
        }
        Pthread_mutex_unlock(&(netinfo_ptr->sanclk));

        if (is_sanc) {
            hosts[count++] = ptr->host;
            if (count >= REPMAX)
                break;
        }
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return count;
}

/* This just pulls the given node out of the linked list and frees it.
 * We assume that the thread ids and pointers to other memory will all
 * be NULL and so we don't have to stop any threads/free other memory. */
int net_del_from_sanctioned(netinfo_type *netinfo_ptr, char *host)
{
    sanc_node_type *ptr, *last;

    Pthread_mutex_lock(&(netinfo_ptr->sanclk));

    ptr = netinfo_ptr->sanctioned_list;

    last = NULL;
    while (ptr != NULL && ptr->host->str != host) {
        last = ptr;
        ptr = ptr->next;
    }

    if (ptr != NULL) {
        if (last)
            last->next = ptr->next;
        else
            netinfo_ptr->sanctioned_list = ptr->next;
    }

    Pthread_mutex_unlock(&(netinfo_ptr->sanclk));

    if (ptr) {
        logmsg(LOGMSG_INFO, "net_del_from_sanctioned %s\n", host);
        myfree(ptr);
        return 0;
    } else {
        logmsg(LOGMSG_INFO, "net_del_from_sanctioned %s - not in sanc list\n",
                host);
        return -1;
    }
}

typedef struct netinfo_node {
    LINKC_T(struct netinfo_node) lnk;
    netinfo_type *netinfo_ptr;
} netinfo_node_t;
static LISTC_T(netinfo_node_t) nets_list;
static pthread_mutex_t nets_list_lk = PTHREAD_MUTEX_INITIALIZER;

netinfo_type *create_netinfo(char myhostname[], int myportnum, int myfd,
                             char app[], char service[], char instance[],
                             int fake, int offload, int ischild,
                             int use_getservbyname)
{
    netinfo_type *netinfo_ptr;
    host_node_type *host_node_ptr;
    netinfo_node_t *netinfo_node;

    netinfo_ptr = calloc(1, sizeof(netinfo_type));
    if (!netinfo_ptr) {
        logmsg(LOGMSG_FATAL, "Can't allocate memory for netinfo entry\n");
        abort();
    }

    listc_init(&(netinfo_ptr->watchlist), offsetof(watchlist_node_type, lnk));


    netinfo_ptr->heartbeat_check_time = gbl_heartbeat_check;
    netinfo_ptr->ischild = ischild;
    netinfo_ptr->use_getservbyname = use_getservbyname;

    if (myportnum > 0 && !ischild) {
        /* manually specified port in lrl */
        netinfo_ptr->port_from_lrl = 1;
    }

    if (myportnum <= 0 && !fake && (!ischild || gbl_accept_on_child_nets)) {
        if (netinfo_ptr->use_getservbyname) {
            myportnum = net_get_port_by_service(instance);
        }
        if (myportnum <= 0) {
            myportnum = portmux_register(app, service, instance);
            if (myportnum == -1) {
                logmsg(LOGMSG_FATAL, "couldnt register port\n");
                exit(1);
            }
            logmsg(LOGMSG_INFO, "i registered port %d for %s\n", myportnum,
                    service);
        } else {
            portmux_use(app, service, instance, myportnum);
        }
    }

    Pthread_rwlock_init(&(netinfo_ptr->lock), NULL);
    Pthread_mutex_init(&(netinfo_ptr->watchlk), NULL);
    Pthread_mutex_init(&(netinfo_ptr->sanclk), NULL);

    netinfo_ptr->seqnum = ((unsigned int)getpid()) * 65537;
    netinfo_ptr->myport = myportnum;
    netinfo_ptr->myhost_interned = intern_ptr(myhostname);
    netinfo_ptr->myhostname = netinfo_ptr->myhost_interned->str;
    netinfo_ptr->myhostname_len = strlen(netinfo_ptr->myhostname) + 1;

    netinfo_ptr->fake = fake;

    strncpy0(netinfo_ptr->app, app, sizeof(netinfo_ptr->app));
    strncpy0(netinfo_ptr->service, service, sizeof(netinfo_ptr->service));
    strncpy0(netinfo_ptr->instance, instance, sizeof(netinfo_ptr->instance));

    host_node_ptr = add_to_netinfo(netinfo_ptr, myhostname, myportnum);
    if (host_node_ptr == NULL) {
        logmsg(LOGMSG_ERROR, "create_netinfo: couldn't add self to netinfo\n");
        goto fail;
    }
    netinfo_ptr->myfd = myfd;

    netinfo_node = malloc(sizeof(netinfo_node_t));
    if (netinfo_node == NULL) {
        logmsg(LOGMSG_ERROR, "create_netinfo: malloc failed. memstat on this "
                        "netinfo will not be tracked\n");
    } else {
        netinfo_node->netinfo_ptr = netinfo_ptr;
        Pthread_mutex_lock(&nets_list_lk);
        listc_atl(&nets_list, netinfo_node);
        Pthread_mutex_unlock(&nets_list_lk);
    }

    netinfo_ptr->conntime_all = quantize_new(1, 100, "ms");
    netinfo_ptr->conntime_periodic = quantize_new(1, 100, "ms");
    netinfo_ptr->conntime_dump_period = 10 * 60;

    return netinfo_ptr;

fail:
    free(netinfo_ptr);
    return NULL;
}

netinfo_type *create_netinfo_fake(void)
{
    return create_netinfo(intern("fakehost"), -1, -1, "fakeapp", "fakeservice", "fakeinstance", 1, 0, 0, 0);
}

void net_count_nodes_ex(netinfo_type *netinfo_ptr, int *total_ptr,
                        int *connected_ptr)
{
    host_node_type *ptr;
    int total = 0, connected = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        total++;
        if (ptr->got_hello)
            connected++;
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    if (total_ptr)
        *total_ptr = total;
    if (connected_ptr)
        *connected_ptr = connected + 1; /* because I won't have had a hello
                                           from myself */
}

inline int net_count_nodes(netinfo_type *netinfo_ptr)
{
    if (!netinfo_ptr) return 0;
    int total;
    net_count_nodes_ex(netinfo_ptr, &total, NULL);
    return total;
}

inline int net_count_connected_nodes(netinfo_type *netinfo_ptr)
{
    int connected;
    net_count_nodes_ex(netinfo_ptr, NULL, &connected);
    return connected;
}

/* This appears to be unused -- Sam J 03/24/05 */
void print_netinfo(netinfo_type *netinfo_ptr)
{
    host_node_type *ptr;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        logmsg(LOGMSG_USER, "%s:%d fd=%d host=%s\n", ptr->host, ptr->port, ptr->fd,
                ptr->host);
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
}

int net_ack_message_payload(void *handle, int outrc, void *payload,
                            int payloadlen)
{
    uint8_t *ack_buf, *p_buf, *p_buf_end;
    net_ack_message_payload_type p_net_ack_payload_message;
    host_node_type *host_node_ptr;
    int rc = 0;
    ack_state_type *ack_state = handle;

    int sz = NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN + payloadlen;

    if (ack_state->needack) {
        Pthread_rwlock_rdlock(&(ack_state->netinfo->lock));

        host_node_ptr =
            get_host_node_by_name_ll(ack_state->netinfo, ack_state->fromhost);

        if (host_node_ptr == NULL) {
            Pthread_rwlock_unlock(&(ack_state->netinfo->lock));
            return -1;
        }

        p_net_ack_payload_message.seqnum = ack_state->seqnum;
        p_net_ack_payload_message.outrc = outrc;
        p_net_ack_payload_message.paylen = payloadlen;

        ack_buf = alloca(sz);
        p_buf = ack_buf;
        p_buf_end = ack_buf + sz;

        p_buf = net_ack_message_payload_type_put(&p_net_ack_payload_message,
                                                 p_buf, p_buf_end);
        p_buf = buf_no_net_put(payload, payloadlen, p_buf, p_buf_end);

        /*fprintf(stderr, "net_ack_message: sending to %d\n",
          ack_state->from_node);*/

        rc = write_message(ack_state->netinfo, host_node_ptr,
                           WIRE_HEADER_ACK_PAYLOAD, ack_buf, sz);

        Pthread_rwlock_unlock(&(ack_state->netinfo->lock));
    }
    return rc;
}

int net_ack_message(void *handle, int outrc)
{
    uint8_t ack_buf[NET_ACK_MESSAGE_TYPE_LEN], *p_buf, *p_buf_end;
    net_ack_message_type p_net_ack_message;
    host_node_type *host_node_ptr;
    int rc = 0;
    ack_state_type *ack_state = handle;

    if (ack_state->needack) {
        Pthread_rwlock_rdlock(&(ack_state->netinfo->lock));

        host_node_ptr =
            get_host_node_by_name_ll(ack_state->netinfo, ack_state->fromhost);

        if (host_node_ptr == NULL) {
            Pthread_rwlock_unlock(&(ack_state->netinfo->lock));
            return -1;
        }

        p_net_ack_message.seqnum = ack_state->seqnum;
        p_net_ack_message.outrc = outrc;

        p_buf = ack_buf;
        p_buf_end = ack_buf + NET_ACK_MESSAGE_TYPE_LEN;

        net_ack_message_type_put(&p_net_ack_message, p_buf, p_buf_end);

        /*fprintf(stderr, "net_ack_message: sending to %d\n",
          ack_state->from_node);*/

        rc = write_message(ack_state->netinfo, host_node_ptr, WIRE_HEADER_ACK,
                           ack_buf, sizeof(ack_buf));

        Pthread_rwlock_unlock(&(ack_state->netinfo->lock));
    }
    return rc;
}

/* remove node from the netinfo list */
void net_decom_node(netinfo_type *netinfo_ptr, const char *host)
{
    if (host && netinfo_ptr->myhostname == host) {
        return;
    }

    Pthread_rwlock_wrlock(&(netinfo_ptr->lock));

    /* remove the host node from the netinfo list */
    host_node_type *host_ptr, *host_back;
    host_back = host_ptr = netinfo_ptr->head;
    while (host_ptr != NULL && host_ptr->host != host) {
        host_back = host_ptr;
        host_ptr = host_ptr->next;
    }
    if (host_ptr != NULL) {
        if (host_ptr == netinfo_ptr->head)
            netinfo_ptr->head = host_ptr->next;
        else
            host_back->next = host_ptr->next;

        host_ptr->decom_flag = 1;

        if (host_ptr == netinfo_ptr->last_used_node_ptr)
            netinfo_ptr->last_used_node_ptr = NULL; // clear last_used_node_ptr
    }

    /* we can't free the host node pointer memory -
       let the connect thread do that */
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
}

int write_decom_msg(struct netinfo_struct *n, struct host_node_tag *h, int type,
                    void *a, int alen, void *b, int blen)
{
    struct iovec iov[] = {{.iov_base = a, .iov_len = alen},
                          {.iov_base = b, .iov_len = blen}};
    return write_message_int(n, h, type, iov, b ? 2 : 1,
                             WRITE_MSG_NOLIMIT | WRITE_MSG_NOHELLOCHECK |
                                 WRITE_MSG_NODELAY | WRITE_MSG_HEAD);
}

static int write_decom_hostname(netinfo_type *netinfo_ptr,
                                host_node_type *host_node_ptr,
                                const char *decom_host, int decom_hostlen,
                                const char *to_host)
{
    int a = htonl(decom_hostlen);
    return write_decom_msg(netinfo_ptr, host_node_ptr, WIRE_HEADER_DECOM_NAME,
                           &a, sizeof(a), (void *)decom_host, decom_hostlen);
}

static decom_writer *write_decom_impl = write_decom_hostname;
void set_decom_writer(decom_writer *impl)
{
    write_decom_impl = impl;
}

/* write decom message to to_host */
int write_decom(netinfo_type *netinfo_ptr, host_node_type *host_node_ptr,
                const char *decom_host, int decom_hostlen, const char *to_host)
{
    return write_decom_impl(netinfo_ptr, host_node_ptr, decom_host,
                            decom_hostlen, to_host);
}


/* send a decom message about node "decom_host" to node "to_host" */
static int net_send_decom(netinfo_type *netinfo_ptr, const char *decom_host,
                          const char *to_host)
{
    host_node_type *host_node_ptr;
#ifdef DEBUG
    fprintf(stderr, "net_send_decom [%s] to_node=%s decom_node=%s\n",
            netinfo_ptr->service, to_host, decom_host);
#endif

    /* grab the host-node ptr for the to_host */
    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    host_node_ptr = netinfo_ptr->head;
    while (host_node_ptr != NULL && host_node_ptr->host != to_host)
        host_node_ptr = host_node_ptr->next;

    if (host_node_ptr == NULL) {
        Pthread_rwlock_unlock(&(netinfo_ptr->lock));
#ifdef DEBUG
        fprintf(stderr, "net_send_decom [%s] not found %s\n",
                netinfo_ptr->service, to_host);
#endif
        return -1;
    }

    int decom_hostlen = strlen(decom_host) + 1;
    int rc = write_decom(netinfo_ptr, host_node_ptr, decom_host, 
                         decom_hostlen, to_host);
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return rc;
}

int net_send_decom_all(netinfo_type *netinfo_ptr, char *decom_host)
{
    int outrc = 0;
    const char *nodes[REPMAX];
    int count = net_get_all_nodes(netinfo_ptr, nodes);
    for (int i = 0; i < count; i++) {
        logmsg(LOGMSG_INFO, "%s: [%s] decom:%s to:%s\n", __func__,
               netinfo_ptr->service, decom_host, nodes[i]);
        int rc = net_send_decom(netinfo_ptr, decom_host, nodes[i]);
        if (rc != 0) {
            outrc++;
            logmsg(LOGMSG_ERROR, "rc=%d sending decom to node %s\n", rc,
                   nodes[i]);
        }
    }
    dispatch_decom(decom_host);
    return outrc;
}


int net_is_connected(netinfo_type *netinfo_ptr, const char *host)
{
    host_node_type *host_node_ptr;
    int rc = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    host_node_ptr = netinfo_ptr->head;

    while (host_node_ptr != NULL && host_node_ptr->host != host)
        host_node_ptr = host_node_ptr->next;

    if (host_node_ptr && (host_node_ptr->fd != -1))
        rc = 1;

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return rc;
}


int net_send_hello(netinfo_type *netinfo_ptr, const char *tohost)
{
    host_node_type *host_node_ptr;
    int rc = -1;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));
    host_node_ptr = netinfo_ptr->head;
    while (host_node_ptr != NULL && host_node_ptr->host != tohost)
        host_node_ptr = host_node_ptr->next;
    if (host_node_ptr)
        rc = write_hello(netinfo_ptr, host_node_ptr);
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return rc;
}

void kill_subnet(const char *subnet)
{
    host_node_type *ptr;
    int len = strlen(subnet) + 1;
    netinfo_node_t *curpos, *tmppos;

    Pthread_mutex_lock(&nets_list_lk);
    LISTC_FOR_EACH_SAFE(&nets_list, curpos, tmppos, lnk)
    {
        netinfo_type *netinfo_ptr = curpos->netinfo_ptr;
        ptr = netinfo_ptr->head;

        while (ptr != NULL) {
            if (!strncmp(ptr->subnet, subnet, len)) {
                if (!ptr->closed) {
                    logmsg(LOGMSG_INFO, "Shutting down socket for %s %s\n",
                           ptr->host, ptr->netinfo_ptr->service);
                } else {
                    logmsg(LOGMSG_INFO, "Already closed socket for %s %s\n",
                           ptr->host, ptr->netinfo_ptr->service);
                }
            }
            ptr = ptr->next;
        }
    }
    Pthread_mutex_unlock(&nets_list_lk);
}

#define MAXSUBNETS 15
// MAXSUBNETS + Slot for the Non-dedicated net
static char *subnet_suffices[MAXSUBNETS + 1] = {0};
static uint8_t num_dedicated_subnets = 0;
static time_t subnet_disabled[MAXSUBNETS + 1] = {0};
static int last_bad_subnet_idx = -1;
static time_t last_bad_subnet_time = 0;
static pthread_mutex_t subnet_mtx = PTHREAD_MUTEX_INITIALIZER;
uint8_t _non_dedicated_subnet = 0;

int net_check_bad_subnet_lk(int ii)
{
    int rc = 0;

    if (subnet_disabled[ii])
        return 1;

    if (!last_bad_subnet_time) {
        if (gbl_verbose_net)
            logmsg(LOGMSG_USER, "%p %s Not set %d %s\n", (void *)pthread_self(),
                   __func__, ii, subnet_suffices[ii]);
        goto out;
    }

    if (last_bad_subnet_time + subnet_blackout_timems < comdb2_time_epochms()) {
        if (gbl_verbose_net)
            logmsg(LOGMSG_USER, "%p %s Clearing out net %d %s\n",
                   (void *)pthread_self(), __func__, ii, subnet_suffices[ii]);
        last_bad_subnet_time = 0;
        goto out;
    }

    if (ii == last_bad_subnet_idx) {
        if (gbl_verbose_net)
            logmsg(LOGMSG_USER, "%p %s Bad net %d %s\n", (void *)pthread_self(),
                   __func__, ii, subnet_suffices[ii]);
        rc = 1;
    }
out:
    return rc;
}

void net_subnet_status()
{
    int i = 0;
    Pthread_mutex_lock(&subnet_mtx);
    char my_buf[30];
    for (i = 0; i < num_dedicated_subnets; i++) {
        logmsg(LOGMSG_USER, "Subnet %s %s%s%s", subnet_suffices[i],
               subnet_disabled[i] ? "disabled" : "enabled\n",
               subnet_disabled[i] ? " at " : "",
               subnet_disabled[i] ? ctime_r(&subnet_disabled[i], my_buf) : "");
    }
    Pthread_mutex_unlock(&subnet_mtx);
}

void net_set_bad_subnet(const char *subnet)
{
    int i = 0;
    Pthread_mutex_lock(&subnet_mtx);
    for (i = 0; i < num_dedicated_subnets; i++) {
        if (subnet_suffices[i][0] &&
            strncmp(subnet, subnet_suffices[i], strlen(subnet) + 1) == 0) {
            last_bad_subnet_time = comdb2_time_epochms();
            last_bad_subnet_idx = i;
            if (gbl_verbose_net)
                logmsg(LOGMSG_USER, "%p %s Marking %s bad, idx %d time %d\n",
                       (void *)pthread_self(), __func__, subnet_suffices[i],
                       last_bad_subnet_idx, (int)last_bad_subnet_time);
        }
    }
    Pthread_mutex_unlock(&subnet_mtx);
}

static void net_clipper_lockless(const char *subnet, int is_disable)
{
    int i = 0;
    time_t now;
    for (i = 0; i < num_dedicated_subnets; i++) {
        if (subnet_suffices[i][0] &&
            strncmp(subnet, subnet_suffices[i], strlen(subnet) + 1) == 0) {
            extern int gbl_ready;
            if (gbl_ready)
                now = comdb2_time_epoch();
            else
                time(&now);
            if (gbl_verbose_net)
                logmsg(LOGMSG_USER, "0x%p %s subnet %s time %ld\n",
                       (void *)pthread_self(),
                       (is_disable) ? "Disabling" : "Enabling",
                       subnet_suffices[i], now);

            if (is_disable == 0) {
                subnet_disabled[i] = 0;
            } else {
                subnet_disabled[i] = now;
                kill_subnet(subnet);
            }
        }
    }
}

void net_clipper(const char *subnet, int is_disable)
{
    Pthread_mutex_lock(&subnet_mtx);
    net_clipper_lockless(subnet, is_disable);
    Pthread_mutex_unlock(&subnet_mtx);
}

int net_subnet_disabled(const char *subnet)
{
    int i = 0;
    int rc = 0;
    Pthread_mutex_lock(&subnet_mtx);
    for (i = 0; i < num_dedicated_subnets; i++) {
        if (subnet_suffices[i][0] &&
            strncmp(subnet, subnet_suffices[i], strlen(subnet) + 1) == 0) {
            rc = (subnet_disabled[i] != 0);
            break;
        }
    }
    Pthread_mutex_unlock(&subnet_mtx);
    return rc;
}

int net_add_nondedicated_subnet(void *context, void *value)
{
    Pthread_mutex_lock(&subnet_mtx);
    // increment num_dedicated_subnets only once for non dedicated subnet
    if (0 == _non_dedicated_subnet) {
        _non_dedicated_subnet = 1;

        /* Disable all subnets */
        for (int i = 0; i != num_dedicated_subnets; ++i)
            net_clipper_lockless(subnet_suffices[i], 1);

        /* Add the non-dedicated network */
        subnet_suffices[num_dedicated_subnets] = strdup("");
        num_dedicated_subnets++;
        Pthread_mutex_unlock(&subnet_mtx);
    } else {
        /* Disable all subnets */
        for (int i = 0; i != num_dedicated_subnets; ++i)
            net_clipper_lockless(subnet_suffices[i], 1);

        /* Enable the non-dedicated network */
        net_clipper_lockless("", 0);
    }
    Pthread_mutex_unlock(&subnet_mtx);
    return 0;
}

int net_add_to_subnets(const char *suffix, const char *lrlname)
{
#ifdef DEBUG
    printf("net_add_to_subnets subnet '%s'\n", suffix);
#endif

    Pthread_mutex_lock(&subnet_mtx);
    if (num_dedicated_subnets >= MAXSUBNETS) {
        logmsg(LOGMSG_ERROR, "too many subnet suffices (max=%d) in lrl %s\n",
               MAXSUBNETS, lrlname);
        Pthread_mutex_unlock(&subnet_mtx);
        return -1;
    }
    subnet_suffices[num_dedicated_subnets] = strdup(suffix);
    num_dedicated_subnets++;
    Pthread_mutex_unlock(&subnet_mtx);
    return 0;
}


void net_cleanup()
{
    Pthread_mutex_lock(&subnet_mtx);
    for (uint8_t i = 0; i < num_dedicated_subnets; i++) {
        if (subnet_suffices[i]) {
            free(subnet_suffices[i]);
            subnet_suffices[i] = NULL;
        }
    }
    Pthread_mutex_unlock(&subnet_mtx);
}

/* Dedicated subnets are specified in the lrl file:
 * If option is left out, we use the normal subnet.
 * If more than one is specified, we use a counter to rotate
 * between the available dedicated subnets
 * When trying to connect, if the subnet is down
 * we will try to connect to the next one until we succeed.
 */
int get_dedicated_conhost(host_node_type *host_node_ptr, struct in_addr *addr)
{
    static unsigned int counter = 0xffff;
    uint8_t ii = 0; // do the loop no more that max subnets

    Pthread_mutex_lock(&subnet_mtx);
    if (num_dedicated_subnets == 0) {
#ifdef DEBUG
        host_node_printf(LOGMSG_USER, host_node_ptr,
                         "Connecting to default hostname/subnet '%s'\n",
                         host_node_ptr->host);
#endif
        Pthread_mutex_unlock(&subnet_mtx);
        int rc = comdb2_gethostbyname(&host_node_ptr->host, addr);
        if (rc == 0 && addr != &host_node_ptr->addr) {
            host_node_ptr->addr = *addr;
        }
        return rc;
    }

    if (counter == 0xffff) // start with a random subnet
        counter = rand() % num_dedicated_subnets;

    int rc = 0;
    while (ii < num_dedicated_subnets) {
        counter++;
        ii++;

        const char *subnet = subnet_suffices[counter % num_dedicated_subnets];

        /* skip last bad network, if we have a choice */
        if (num_dedicated_subnets > 1) {
            if (net_check_bad_subnet_lk(counter % num_dedicated_subnets))
                continue;
        }

        int loc_len = strlen(host_node_ptr->host) + strlen(subnet);
        char tmp_hostname[loc_len + 1];
        char *rephostname = tmp_hostname;
        strcpy(rephostname, host_node_ptr->host);
        strcat(rephostname, subnet);
        strncpy0(host_node_ptr->subnet, subnet, HOSTNAME_LEN);

#ifdef DEBUG
        host_node_printf(LOGMSG_USER, host_node_ptr, "Connecting to %s dedicated hostname/subnet '%s' counter=%d\n",
                         (subnet[0] == '\0') ? "NON" : "", rephostname, counter);
#endif
        rc = comdb2_gethostbyname(&rephostname, addr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%d) %s(): ERROR gethostbyname '%s' FAILED\n",
                    ii, __func__, rephostname);
        } else {
            if (gbl_verbose_net) {
                host_node_printf(LOGMSG_USER, host_node_ptr,
                                 "'%s': gethostbyname '%s' addr %x\n", __func__,
                                 rephostname, (unsigned)addr->s_addr);
            }
            if (addr != &host_node_ptr->addr) {
                host_node_ptr->addr = *addr;
            }
            break;
        }
    }
    Pthread_mutex_unlock(&subnet_mtx);
    return rc;
}

int net_get_port_by_service(const char *dbname)
{
    short port = 0;
    comdb2_getservbyname(dbname, "tcp", &port);
    return ntohs(port);
}

int gbl_waitalive_iterations = 3;

#if defined _SUN_SOURCE
void wait_alive(int fd)
{
    int iter = gbl_waitalive_iterations, i;
    for (i = 0; i < iter; i++) {
        int error = 0;
        socklen_t len = sizeof(error);
        int retval = getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len);
        if (retval == 0 && error == 0) {
            if (i > 0) {
                logmsg(LOGMSG_ERROR, "%s returning after %d iterations %dms\n",
                       __func__, i, i * 10);
            }
            return;
        }
        poll(NULL, 0, 10);
    }
}
#endif

static int get_subnet_incomming_syn(host_node_type *host_node_ptr)
{
    struct sockaddr_in lcl_addr_inet;
    socklen_t lcl_len = sizeof(lcl_addr_inet);
    if (getsockname(host_node_ptr->fd, (struct sockaddr *)&lcl_addr_inet,
                    &lcl_len)) {
        logmsg(LOGMSG_ERROR, "Failed to getsockname() for fd=%d\n",
               host_node_ptr->fd);
        return 0;
    }

    char host[NI_MAXHOST], service[NI_MAXSERV];
    /* get our host name for local _into_ address of connection */
    int s = getnameinfo((struct sockaddr *)&lcl_addr_inet, lcl_len, host,
                        NI_MAXHOST, service, NI_MAXSERV, 0);

    if (s != 0) {
        logmsg(LOGMSG_WARN, "Incoming connection into unknown (%s:%u): %s\n",
               inet_ntoa(lcl_addr_inet.sin_addr),
               (unsigned)ntohs(lcl_addr_inet.sin_port), gai_strerror(s));
        return 0;
    }

    /* extract the suffix of subnet ex. '_n3' in name node1_n3 */
    int myh_len = strlen(host_node_ptr->netinfo_ptr->myhostname);
    if (strncmp(host_node_ptr->netinfo_ptr->myhostname, host, myh_len) == 0) {
        assert(myh_len <= sizeof(host));
        char *subnet = &host[myh_len];
        if (subnet[0])
            strncpy0(host_node_ptr->subnet, subnet, HOSTNAME_LEN);
    }

    /* check if the net is disabled */
    if (net_subnet_disabled(host_node_ptr->subnet))
        return 1;

    return 0;
}

/* find the remote peer.  code stolen from sqlinterfaces.c */
int findpeer(int fd, char *addr, int len)
{
    int rc;
    struct sockaddr_in peeraddr;
    socklen_t pl = sizeof(struct sockaddr_in);

    /* find peer ip */
    rc = getpeername(fd, (struct sockaddr *)&peeraddr, &pl);
    if (rc) {
        snprintf(addr, len, "<unknown>");
        return -1;
    }

    /* find hostname */
    if (NULL == inet_ntop(peeraddr.sin_family, &peeraddr.sin_addr, addr, len)) {
        snprintf(addr, len, "<unknown>");
        return -1;
    }

    return 0;
}


void net_register_child_net(netinfo_type *netinfo_ptr,
                            netinfo_type *netinfo_child, int netnum, int accept)
{
    netinfo_type **t;
    Pthread_rwlock_wrlock(&(netinfo_ptr->lock));
    if (netnum >= netinfo_ptr->num_child_nets) {
        t = calloc(netnum + 1, sizeof(netinfo_type *));
        if (t == NULL) {
            logmsg(LOGMSG_FATAL, "Can't allocate memory for child net\n");
            abort();
        }
        for (int i = 0; i < netinfo_ptr->num_child_nets; i++)
            t[i] = netinfo_ptr->child_nets[i];
        netinfo_ptr->child_nets = t;
    }
    netinfo_child->parent = netinfo_ptr;
    netinfo_child->netnum = netnum;
    netinfo_child->accept_on_child = accept;
    netinfo_ptr->child_nets[netinfo_child->netnum] = netinfo_child;
    if (netnum > netinfo_ptr->num_child_nets)
        netinfo_ptr->num_child_nets = netnum + 1;
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));
}

int gbl_forbid_remote_admin = 1;

void do_appsock(netinfo_type *netinfo_ptr, struct sockaddr_in *cliaddr,
                SBUF2 *sb, uint8_t firstbyte)
{
    watchlist_node_type *watchlist_node;
    int new_fd = sbuf2fileno(sb);
    char paddr[64];
    extern struct in_addr gbl_myaddr;

    int admin = 0;
    APPSOCKFP *rtn = NULL;

    if (firstbyte == '@') {
        findpeer(new_fd, paddr, sizeof(paddr));
        if (!gbl_forbid_remote_admin ||
            (cliaddr->sin_addr.s_addr == htonl(INADDR_LOOPBACK)) ||
            (cliaddr->sin_addr.s_addr == gbl_myaddr.s_addr)) {
            admin = 1;
        } else {
            logmsg(LOGMSG_INFO, "Rejecting non-local admin user from %s\n",
                   paddr);
            sbuf2close(sb);
            return;
        }
    } else if (firstbyte != sbuf2ungetc(firstbyte, sb)) {
        logmsg(LOGMSG_ERROR, "sbuf2ungetc failed %s:%d\n", __FILE__, __LINE__);
        sbuf2close(sb);
        return;
    }


    if (gbl_server_admin_mode && !admin) {
        sbuf2close(sb);
        return;
    }

    /* call user specified app routine */
    if (admin && netinfo_ptr->admin_appsock_rtn) {
        rtn = netinfo_ptr->admin_appsock_rtn;
    } else if (netinfo_ptr->appsock_rtn) {
        rtn = netinfo_ptr->appsock_rtn;
    }

    if (rtn) {
        /* set up the watchlist system for this node */
        watchlist_node = calloc(1, sizeof(watchlist_node_type));
        if (!watchlist_node) {
            logmsg(LOGMSG_ERROR, "%s: malloc watchlist_node failed\n",
                   __func__);
            sbuf2close(sb);
            return;
        }
        memcpy(watchlist_node->magic, "WLST", 4);
        watchlist_node->in_watchlist = 0;
        watchlist_node->netinfo_ptr = netinfo_ptr;
        watchlist_node->sb = sb;
        watchlist_node->readfn = sbuf2getr(sb);
        watchlist_node->writefn = sbuf2getw(sb);
        watchlist_node->addr = *cliaddr;
        sbuf2setrw(sb, net_reads, net_writes);
        sbuf2setuserptr(sb, watchlist_node);

        /* this doesn't read- it just farms this off to a thread */
        (rtn)(netinfo_ptr, sb);
    }
}

static watchlist_node_type *get_watchlist_node(SBUF2 *sb, const char *funcname)
{
    watchlist_node_type *watchlist_node = sbuf2getuserptr(sb);
    if (!watchlist_node) {
        logmsg(LOGMSG_ERROR, "%s: sbuf2 %p has no user pointer\n", funcname, sb);
        return NULL;
    } else if (memcmp(watchlist_node->magic, "WLST", 4) != 0) {
        logmsg(LOGMSG_ERROR, "%s: sbuf2 %p user pointer is not a watch list node\n",
                funcname, sb);
        return NULL;
    } else {
        return watchlist_node;
    }
}

static int net_writes(SBUF2 *sb, const char *buf, int nbytes)
{
    int outrc;
    watchlist_node_type *watchlist_node = get_watchlist_node(sb, __func__);
    if (!watchlist_node)
        return -1;
    watchlist_node->write_age = comdb2_time_epoch();
    outrc = watchlist_node->writefn(sb, buf, nbytes);
    watchlist_node->write_age = 0;
    return outrc;
}

static int net_reads(SBUF2 *sb, char *buf, int nbytes)
{
    int outrc;
    watchlist_node_type *watchlist_node = get_watchlist_node(sb, __func__);
    if (!watchlist_node)
        return -1;
    watchlist_node->read_age = comdb2_time_epoch();
    outrc = watchlist_node->readfn(sb, buf, nbytes);
    watchlist_node->read_age = 0;
    return outrc;
}

void net_timeout_watchlist(netinfo_type *netinfo_ptr)
{
    watchlist_node_type *watchlist_ptr;
    SBUF2 *sb;
    int fd;

    Pthread_mutex_lock(&(netinfo_ptr->watchlk));

    LISTC_FOR_EACH(&(netinfo_ptr->watchlist), watchlist_ptr, lnk)
    {
        sb = watchlist_ptr->sb;
        fd = sbuf2fileno(sb);

        int write_age = watchlist_ptr->write_age;
        int read_age = watchlist_ptr->read_age;

        if (((watchlist_ptr->write_timeout) && (write_age) &&
             ((comdb2_time_epoch() - write_age) >
              watchlist_ptr->write_timeout)) ||

            ((watchlist_ptr->read_timeout) && (read_age) &&
             ((comdb2_time_epoch() - read_age) >
              watchlist_ptr->read_timeout))) {
            logmsg(LOGMSG_INFO, "timing out session, closing fd %d read_age %d "
                                "timeout %d write_age %d timeout %d\n",
                   fd, comdb2_time_epoch() - read_age,
                   watchlist_ptr->read_timeout, comdb2_time_epoch() - write_age,
                   watchlist_ptr->write_timeout);
            shutdown(fd, 2);

            watchlist_ptr->write_timeout = 0;
            watchlist_ptr->read_timeout = 0;
        }
        /* warning path */
        else if ((watchlist_ptr->read_warning_timeout) &&
                 (watchlist_ptr->read_warning_arg) &&
                 (watchlist_ptr->read_warning_func) && (read_age)) {
            int gap = comdb2_time_epoch() - read_age;
            if (gap > watchlist_ptr->read_warning_timeout) {
                int rc = watchlist_ptr->read_warning_func(
                    watchlist_ptr->read_warning_arg,
                    watchlist_ptr->read_warning_timeout, gap);
                if (rc < 0) {
                    logmsg(LOGMSG_INFO, "timing out session, closing fd %d\n", fd);
                    shutdown(fd, 2);

                    watchlist_ptr->write_timeout = 0;
                    watchlist_ptr->read_timeout = 0;
                    watchlist_ptr->read_warning_timeout = 0;
                } else if (rc == 1) {
                    watchlist_ptr->read_warning_timeout = 0; /* stop warning */
                }
            }
        }
    }

    Pthread_mutex_unlock(&(netinfo_ptr->watchlk));
}

void net_end_appsock(SBUF2 *sb)
{
    watchlist_node_type *watchlist_node;
    netinfo_type *netinfo_ptr;

    if (!sb)
        return;

    watchlist_node = get_watchlist_node(sb, __func__);
    if (watchlist_node) {
        netinfo_ptr = watchlist_node->netinfo_ptr;

        /* remove from the watch list, if it's on there */
        Pthread_mutex_lock(&(netinfo_ptr->watchlk));
        if (watchlist_node->in_watchlist) {
            listc_rfl(&(netinfo_ptr->watchlist), watchlist_node);
        }

        /* Restore original read/write functions so that if sbuf2close does a
         * flush it won't be trying to update the watchlist node. */
        sbuf2setrw(sb, watchlist_node->readfn, watchlist_node->writefn);

        free(watchlist_node);
        Pthread_mutex_unlock(&(netinfo_ptr->watchlk));
    }

    sbuf2close(sb);
}

/* call this under netinfo_ptr lock ! */
static int is_ok(netinfo_type *netinfo_ptr, const char *host)
{
    host_node_type *host_node_ptr;
    if (netinfo_ptr->myhostname == host)
        return 1;
    for (host_node_ptr = netinfo_ptr->head; host_node_ptr != NULL;
         host_node_ptr = host_node_ptr->next) {
        if (host_node_ptr->host == host) {
            /* To prevent race conditions we should check this
             * stuff under lock. */
            int ok = 0;
            if (host_node_ptr->fd > 0 && !host_node_ptr->decom_flag && !host_node_ptr->closed) {
                ok = 1;
            }
            return ok;
        }
    }

    return 0;
}

int net_sanctioned_list_ok(netinfo_type *netinfo_ptr)
{
    sanc_node_type *sanc_node_ptr;
    int ok = 1;

    Pthread_mutex_lock(&(netinfo_ptr->sanclk));

    for (sanc_node_ptr = netinfo_ptr->sanctioned_list;
         ok && sanc_node_ptr != NULL; sanc_node_ptr = sanc_node_ptr->next) {
        ok = is_ok(netinfo_ptr, sanc_node_ptr->host->str);
    }

    Pthread_mutex_unlock(&(netinfo_ptr->sanclk));

    return ok;
}

static sanc_node_type *add_to_sanctioned_nolock(netinfo_type *netinfo_ptr,
                                                const char hostname[],
                                                int portnum)
{
    /* scan to see if it's already there */
    int count = 0;
    sanc_node_type *ptr = netinfo_ptr->sanctioned_list;

    while (ptr != NULL && count < REPMAX && ptr->host->str != hostname) {
        ptr = ptr->next;
        count++;
    }

    if (ptr != NULL) {
        return ptr;
    }

    if (count==REPMAX) {
        logmsg(LOGMSG_ERROR, "%s : Cannot add to more than REPMAX(%d) number of nodes\n", __func__, REPMAX);
        return NULL;
    }

    ptr = calloc(1, sizeof(sanc_node_type));
    ptr->next = netinfo_ptr->sanctioned_list;
    ptr->host = intern_ptr(hostname);
    ptr->port = portnum;
    ptr->timestamp = time(NULL);

    netinfo_ptr->sanctioned_list = ptr;

    return ptr;
}

/*
  1) set up a socket bound, and listening on our host/port
  2) create an accept_thread blocked on that socket
  3) create a connect thread for each entry in sites[] array
  4) create a heartbeat thread
*/

int net_init(netinfo_type *netinfo_ptr)
{
    int rc;
    host_node_type *host_node_ptr;

    /* block SIGPIPE */
    signal(SIGPIPE, SIG_IGN);

    /* do nothing if we have a fake netinfo */
    if (netinfo_ptr->fake)
        return 0;

    int num = 0;
    /* add everything we have at this point to the sanctioned list */
    for (host_node_ptr = netinfo_ptr->head; host_node_ptr != NULL;
         host_node_ptr = host_node_ptr->next) {
        add_to_sanctioned_nolock(netinfo_ptr, host_node_ptr->host, host_node_ptr->port);
        add_host(host_node_ptr);
        ++num;
    }

    if (num > 1 && !netinfo_ptr->ischild) {
        struct timeval a, b, c;
        gettimeofday(&a, NULL);
        /* wait up to 1s to connect to siblings */
        const char *hostlist[REPMAX];
        int retry = 100;
        while (--retry >= 0 && (rc = net_get_all_nodes_connected(netinfo_ptr, hostlist)) < (num - 1)) {
            usleep(10 * 1000); //10ms
        }
        gettimeofday(&b, NULL);
        timersub(&b, &a, &c);
        logmsg(LOGMSG_INFO, "%s %s waited:%ldms connected:%d\n", __func__, netinfo_ptr->service, c.tv_sec * 1000 + c.tv_usec / 1000, rc);
    }
    return 0;
}


int net_register_admin_appsock(netinfo_type *netinfo_ptr, APPSOCKFP func)
{
    netinfo_ptr->admin_appsock_rtn = func;
    return 0;
}

int net_register_appsock(netinfo_type *netinfo_ptr, APPSOCKFP func)
{
    netinfo_ptr->appsock_rtn = func;
    return 0;
}

int net_register_throttle(netinfo_type *netinfo_ptr, NETTHROTTLEFP func)
{
    netinfo_ptr->throttle_rtn = func;
    return 0;
}

int net_register_allow(netinfo_type *netinfo_ptr, NETALLOWFP func)
{
    netinfo_ptr->allow_rtn = func;
    return 0;
}

int net_register_newnode(netinfo_type *netinfo_ptr, NEWNODEFP func)
{
    netinfo_ptr->new_node_rtn = func;
    return 0;
}

void net_set_callback_data(netinfo_type *info, void *data)
{
    info->callback_data = data;
}

void net_register_start_thread_callback(netinfo_type *info,
                                        void (*callback)(void *))
{
    info->start_thread_callback = callback;
}

void net_register_stop_thread_callback(netinfo_type *info,
                                       void (*callback)(void *))
{
    info->stop_thread_callback = callback;
}

/* pick a sibling for sql offloading */
char *net_get_osql_node(netinfo_type *netinfo_ptr)
{
    host_node_type *ptr, *nodes[REPMAX]; /* 16 siblings, more than reasonable
                                          * replicated cluster */
    int nnodes = 0;
    int index = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        /* prefering to offload */
        if (ptr->host == netinfo_ptr->myhostname)
            continue;

        /* disconnected already ?*/
        if (ptr->fd <= 0 || !ptr->got_hello)
            continue;

        /* is rtcpu-ed? */
        if (machine_is_up(ptr->host, NULL) != 1)
            continue;

        if (nnodes >= REPMAX)
            break;

        nodes[nnodes++] = ptr;
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    /* no siblings */
    if (!nnodes)
        return NULL;

    {
        /* avoid sending all same sec requests to the same node */
        static int init = 0;
        if (!init) {
            unsigned int t = time(NULL);
            index = init = rand_r(&t) % nnodes; /* let it spread */
        } else {
            init = (init + 1) % nnodes; /* round robin */
            index = init;
        }
    }

    return nodes[index]->host;
}

char *net_get_mynode(netinfo_type *netinfo_ptr)
{
    return netinfo_ptr->myhostname;
}

struct interned_string *net_get_mynode_interned(netinfo_type *netinfo_ptr)
{
    return netinfo_ptr->myhost_interned;
}

void *net_get_usrptr(netinfo_type *netinfo_ptr) { return netinfo_ptr->usrptr; }

void net_set_usrptr(netinfo_type *netinfo_ptr, void *usrptr)
{
    netinfo_ptr->usrptr = usrptr;
}

/* deprecated */
void net_sleep_with_lock(netinfo_type *netinfo_ptr, int nseconds)
{
    logmsg(LOGMSG_ERROR, "%s is deprecated\n", __func__);
}

int net_get_nodes_info(netinfo_type *netinfo_ptr, int max_nodes,
                       struct host_node_info *out_nodes)
{
    host_node_type *ptr;
    int count = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));

    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        if (max_nodes > 0) {
            out_nodes->fd = ptr->fd;
            out_nodes->host = ptr->host;
            out_nodes->host_interned = ptr->host_interned;
            out_nodes->port = ptr->port;

            out_nodes++;
            max_nodes--;
        }
        count++;
    }

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return count;
}

int net_get_host_network_usage(netinfo_type *netinfo_ptr, const char *host,
                               unsigned long long *written,
                               unsigned long long *read,
                               unsigned long long *throttle_waits,
                               unsigned long long *reorders)
{
    host_node_type *ptr;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));
    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        if (ptr->host == host)
            break;
    }
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    if (ptr == NULL)
        return -1;

    *written = ptr->stats.bytes_written;
    *read = ptr->stats.bytes_read;
    *throttle_waits = ptr->stats.throttle_waits;
    *reorders = ptr->stats.reorders;

    return 0;
}

int net_get_network_usage(netinfo_type *netinfo_ptr,
                          unsigned long long *written, unsigned long long *read,
                          unsigned long long *throttle_waits,
                          unsigned long long *reorders)
{
    *written = netinfo_ptr->stats.bytes_written;
    *read = netinfo_ptr->stats.bytes_read;
    *throttle_waits = netinfo_ptr->stats.throttle_waits;
    *reorders = netinfo_ptr->stats.reorders;
    return 0;
}

int get_host_port(netinfo_type *netinfo)
{
    Pthread_rwlock_rdlock(&(netinfo->lock));
    int port = netinfo->myport;
    Pthread_rwlock_unlock(&(netinfo->lock));
    return port;
}

int net_appsock_get_addr(SBUF2 *sb, struct sockaddr_in *addr)
{
    watchlist_node_type *watchlist_node;

    watchlist_node = get_watchlist_node(sb, __func__);
    if (!watchlist_node)
        return 1;

    *addr = watchlist_node->addr;
    return 0;
}

/* return a socket set up for listening on given port */
int net_listen(int port)
{
    struct sockaddr_in sin;
    int listenfd;
    int reuse_addr;
    socklen_t len;
    struct linger linger_data;
    int flag;
    int rc;

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
    sin.sin_port = htons(port);

    /* TODO: make these tunable */

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) {
        logmsg(LOGMSG_ERROR, "%s: socket rc %d %s\n", __func__, errno,
                strerror(errno));
        return -1;
    }

#if defined _SUN_SOURCE
    wait_alive(listenfd);
#endif

#ifdef NODELAY
    flag = 1;
    len = sizeof(flag);
    rc = setsockopt(listenfd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, len);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: couldnt turn off nagel on listenfd %d: %d %s\n",
                __func__, listenfd, errno, strerror(errno));
        return -1;
    }
#endif

#ifdef TCPBUFSZ
    int tcpbfsz = (8 * 1024 * 1024);
    len = sizeof(tcpbfsz);
    rc = setsockopt(listenfd, SOL_SOCKET, SO_SNDBUF, &tcpbfsz, len);
    if (rc < 0) {
        logmsg(LOGMSG_ERROR, 
                "%s: couldnt set tcp sndbuf size on listenfd %d: %d %s\n",
                __func__, listenfd, errno, strerror(errno));
        return -1;
    }

    tcpbfsz = (8 * 1024 * 1024);
    len = sizeof(tcpbfsz);
    rc = setsockopt(listenfd, SOL_SOCKET, SO_RCVBUF, &tcpbfsz, len);
    if (rc < 0) {
        logmsg(LOGMSG_ERROR, 
                "%s: couldnt set tcp rcvbuf size on listenfd %d: %d %s\n",
                __func__, listenfd, errno, strerror(errno));
        return -1;
    }
#endif

    /* allow reuse of local addresses */
    reuse_addr = 1;
    len = sizeof(reuse_addr);
    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse_addr,
                   len) != 0) {
        logmsg(LOGMSG_ERROR, "%s: coun't set reuseaddr %d %s\n", __func__, errno,
                strerror(errno));
        return -1;
    }

#ifdef NOLINGER
    linger_data.l_onoff = 0;
    linger_data.l_linger = 1;
    len = sizeof(linger_data);
    if (setsockopt(listenfd, SOL_SOCKET, SO_LINGER, (char *)&linger_data,
                   len) != 0) {
        logmsg(LOGMSG_ERROR, "%s: coun't set keepalive %d %s\n", __func__, errno,
                strerror(errno));
        return -1;
    }
#endif

    /* enable keepalive timer. */
    int on = 1;
    len = sizeof(on);
    if (setsockopt(listenfd, SOL_SOCKET, SO_KEEPALIVE, (char *)&on, len) != 0) {
        logmsg(LOGMSG_ERROR, "%s: coun't set keepalive %d %s\n", __func__, errno,
                strerror(errno));
        return -1;
    }

    /* bind an address to the socket */
    if (bind(listenfd, (struct sockaddr *)&sin, sizeof(sin)) == -1) {
        logmsg(LOGMSG_ERROR, "%s: FAILED TO BIND to port %d: %d %s\n", __func__,
                port, errno, strerror(errno));
        return -1;
    }

    /* listen for connections on socket */
    if (listen(listenfd, gbl_net_maxconn ? gbl_net_maxconn : SOMAXCONN) < 0) {
        logmsg(LOGMSG_ERROR, "%s: listen rc %d %s\n", __func__, errno,
                strerror(errno));
        return -1;
    }

    return listenfd;
}

void net_set_conntime_dump_period(netinfo_type *netinfo_ptr, int value)  {
    netinfo_ptr->conntime_dump_period = value;
}

int net_get_conntime_dump_period(netinfo_type *netinfo_ptr) {
    return netinfo_ptr->conntime_dump_period;
}

int net_get_stats(netinfo_type *netinfo_ptr, struct net_stats *stat) {
    struct host_node_tag *ptr;

    stat->num_drops = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));
    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next)
        stat->num_drops += ptr->num_queue_full;

    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return 0;
}

int net_get_host_stats(netinfo_type *netinfo_ptr, const char *host, struct net_host_stats *stat) {
    struct host_node_tag *ptr;
    stat->queue_size = 0;

    Pthread_rwlock_rdlock(&(netinfo_ptr->lock));
    for (ptr = netinfo_ptr->head; ptr != NULL; ptr = ptr->next) {
        if (strcmp(host, ptr->host) == 0) {
            stat->queue_size = time_metric_max(ptr->metric_queue_size);
            break;
        }
    }
    Pthread_rwlock_unlock(&(netinfo_ptr->lock));

    return 0;
}

int net_send_all(netinfo_type *netinfo_ptr, int num, void **data, int *sz,
                 int *type, int *flag)
{
    return net_send_all_evbuffer(netinfo_ptr, num, data, sz, type, flag);
}
