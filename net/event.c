/*
   Copyright 2020 Bloomberg Finance L.P.

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

#include <alloca.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/tcp.h>
#include <sys/un.h>
#include <sys/socketvar.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/event_struct.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>

#include <akq.h>
#include <bb_oscompat.h>
#include <comdb2_atomic.h>
#include <compat.h>
#include <dbinc/queue.h>
#include <intern_strings.h>
#include <locks_wrap.h>
#include <logmsg.h>
#include <mem_net.h>
#include <mem_override.h>
#include <net.h>
#include <net_int.h>
#include <plhash.h>
#include <portmuxapi.h>

#define DISTRESS_COUNT 5

#define hprintf_format(a)                                                      \
    LOGMSG_USER, "[%.3s %-8s fd:%-3d %18s] " a, e->service, e->host, e->fd,    \
        __func__

#define distress_logmsg(...)                                                   \
    do {                                                                       \
        if (e->distressed) {                                                   \
            break;                                                             \
        } else if (e->distress_count > DISTRESS_COUNT) {                       \
            logmsg(hprintf_format("ENTERING DISTRESS MODE\n"));                \
            e->distressed = 1;                                                 \
            break;                                                             \
        }                                                                      \
        logmsg(__VA_ARGS__);                                                   \
    } while (0)

#define no_distress_logmsg(...) logmsg(__VA_ARGS__)

#define hprintf(a, ...) distress_logmsg(hprintf_format(a), __VA_ARGS__)
#define hprintf_nd(a, ...) no_distress_logmsg(hprintf_format(a), __VA_ARGS__)
#define hprintf0(a) no_distress_logmsg(hprintf_format(a))
#define hprintf0_nd(a) no_distress_logmsg(hprintf_format(a))

#define HOST_NODE_PTR(x) (x)->host_node_ptr

int gbl_libevent = 1;

extern char *gbl_myhostname;
extern int gbl_create_mode;
extern int gbl_exit;
extern int gbl_fullrecovery;
extern int gbl_netbufsz;
extern int gbl_pmux_route_enabled;

static struct timeval one_sec = {1, 0};

/* Write is dispatched on thread wanting to write. If write would block, it is
 * scheduled on the base thread.
 * Data is read by designated thread and dispatched for processing on akq. */

enum net_thd_policy {
    NET_THD_POLICY_GLOBAL,      /* Read on base thread */
    NET_THD_POLICY_PER_OP,      /* Dedicated read thread */
    NET_THD_POLICY_PER_NET,     /* One read thread per net for all hosts */
    NET_THD_POLICY_PER_HOST,    /* One read thread per host for all nets */
    NET_THD_POLICY_PER_EVENT,   /* One read thread per host, per net (no akq) */
};

static enum net_thd_policy net_thd_policy = NET_THD_POLICY_PER_HOST;

struct policy {
    struct akq *akq;
    pthread_t rdthd;
    struct event_base *rdbase;
};

struct policy global;
struct policy per_op;

static pthread_t base_thd;
static struct event_base *base;

static pthread_t timer_thd;
static struct event_base *timer_base;

#define get_policy()                                                           \
    ({                                                                         \
        struct policy *f = NULL;                                               \
        switch (net_thd_policy) {                                              \
        case NET_THD_POLICY_GLOBAL: f = &global; break;                        \
        case NET_THD_POLICY_PER_OP: f = &per_op; break;                        \
        case NET_THD_POLICY_PER_NET: f = &e->net_info->per_net; break;         \
        case NET_THD_POLICY_PER_HOST: f = &e->host_info->per_host; break;      \
        case NET_THD_POLICY_PER_EVENT: f = &e->per_event; break;               \
        }                                                                      \
        f;                                                                     \
    })

#define rd_thd ({ get_policy()->rdthd; })
#define rd_base ({ get_policy()->rdbase; })
#define get_akq() ({ get_policy()->akq; })

/* Operations on host_list/event_list *must* be dispatched on main base. This
** keeps all such operations single-threaded and avoids race-conditions and
** locking complications present in existing net-lib.
**
** check_base_thd() detects when this is not the case and calls abort() to get
** a core. */

#define check_thd(thd)                                                         \
    if (thd != pthread_self()) {                                               \
        fprintf(stderr, "FATAL ERROR: %s EVENT NOT DISPATCHED on " #thd "\n",  \
                __func__);                                                     \
        abort();                                                               \
    }

#define check_base_thd() check_thd(base_thd)
#define check_timer_thd() check_thd(timer_thd)
#define check_rd_thd() check_thd(rd_thd)

static void *start_stop_callback_data;
static void (*start_callback)(void *);
static void (*stop_callback)(void *);

static pthread_once_t exit_once = PTHREAD_ONCE_INIT;
static pthread_cond_t exit_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t exit_mtx = PTHREAD_MUTEX_INITIALIZER;

struct connect_info;
struct net_info;
static void event_info_free(struct event_info *);
static void flush_when_ready(int, short, void *);
static void host_setup(int, short, void *);
static void pmux_connect(int, short, void *);
static void pmux_reconnect(struct connect_info *c);
static void unix_connect(int, short, void *);
static void user_msg_func(void *);

#define event_once(a, b, c)                                                    \
    ({                                                                         \
        int erc;                                                               \
        if ((erc = event_base_once(a, -1, EV_TIMEOUT, b, c, NULL)) != 0) {     \
            logmsg(LOGMSG_ERROR, "%s:%d event_base_once failed\n", __func__,   \
                   __LINE__);                                                  \
        }                                                                      \
        erc;                                                                   \
    })

static struct timeval reconnect_time(void)
{
    int min = 1;
    int max = 5;
    int s = random() % (max - 1);
    struct timeval t = {min + s, 0};
    return t;
}

static void make_socket_blocking(int fd)
{
    int flags;
    if ((flags = fcntl(fd, F_GETFL, NULL)) < 0) {
        logmsgperror("fcntl:F_GETFL");
        exit(1);
    }
    if (!(flags & O_NONBLOCK)) {
        return;
    }
    flags &= ~O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) == -1) {
        logmsgperror("fcntl:F_SETFL");
        exit(1);
    }
}

static void make_socket_nolinger(int fd)
{
    struct linger lg = {.l_onoff = 0, .l_linger = 0};
    if (setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(struct linger))) {
        logmsg(LOGMSG_ERROR, "%s fd:%d err:%s\n", __func__, fd, strerror(errno));
    }
}

static void make_socket_nodelay(int fd)
{
    int flag = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag))) {
        logmsg(LOGMSG_ERROR, "%s fd:%d err:%s\n", __func__, fd, strerror(errno));
    }
}

static void make_socket_keepalive(int fd)
{
    int flag = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &flag, sizeof(flag))) {
        logmsg(LOGMSG_ERROR, "%s fd:%d err:%s\n", __func__, fd, strerror(errno));
    }
}

static void make_socket_reusable(int fd)
{
    int flag = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag))) {
        logmsg(LOGMSG_ERROR, "%s fd:%d err:%s\n", __func__, fd, strerror(errno));
    }
}

static void set_socket_bufsz(int fd)
{
    size_t tcpbfsz = (8 * 1024 * 1024);
    size_t len = sizeof(tcpbfsz);
    int rc = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &tcpbfsz, len);
    if (rc < 0) {
        logmsg(LOGMSG_ERROR, "%s fd:%d snd err:%s\n", __func__, fd, strerror(errno));
    }
    tcpbfsz = (8 * 1024 * 1024);
    len = sizeof(tcpbfsz);
    rc = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &tcpbfsz, len);
    if (rc < 0) {
        logmsg(LOGMSG_ERROR, "%s fd:%d rcv err:%s\n", __func__, fd, strerror(errno));
    }
}

static void make_socket_nonblocking(int fd)
{
    evutil_make_socket_nonblocking(fd);
    make_socket_nolinger(fd);
    make_socket_nodelay(fd);
    make_socket_keepalive(fd);
    make_socket_reusable(fd);
    //set_socket_bufsz(fd);
}

static int get_nonblocking_socket(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        logmsgperror("get_nonblocking_socket socket");
    } else {
        make_socket_nonblocking(fd);
    }
    return fd;
}

struct host_connected_info;

struct host_info {
    LIST_HEAD(, event_info) event_list;
    LIST_ENTRY(host_info) entry;
    char *host;
    struct policy per_host;
};

struct event_info {
    LIST_HEAD(, host_connected_info) host_connected_list;
    LIST_ENTRY(event_info) host_list_entry;
    LIST_ENTRY(event_info) net_list_entry;
    int fd;
    char *host;
    char *service;
    int wirehdr_len;
    wire_header_type *wirehdr[WIRE_HEADER_MAX]; /* Outgoing */
    host_node_type *host_node_ptr;
    struct host_info *host_info;
    struct net_info *net_info;
    struct event *hb_check_ev;
    struct event *hb_send_ev;
    struct bufferevent *rd_bev;
    time_t recv_at;
    time_t sent_at;
    wire_header_type hdr; /* Incoming */
    int state;
    int distress_count;

    /* Write ops guarded by wr_lk */
    pthread_mutex_t wr_lk;
    struct evbuffer *wr_buf;
    struct event wr_event;
    int wr_full;
    int wr_pending;
    int distressed;
    int skip_drain;
    int rd_full;
    size_t watermark;
    size_t rd_size;
    struct policy per_event;
    net_send_message_header msg;
    net_ack_message_payload_type ack;
};

#define EVENT_HASH_KEY_SZ 128
struct event_hash_entry {
    char key[EVENT_HASH_KEY_SZ];
    struct event_info *event_info;
};
static hash_t *event_hash;
static pthread_mutex_t event_hash_lk = PTHREAD_MUTEX_INITIALIZER;
static void make_event_hash_key(char *key, const char *service, const char *host)
{
    strcpy(key, service);
    strcat(key, host);
}

struct net_dispatch_info {
    const char *who;
    struct event_base *base;
    struct host_info *host_info;
};

struct user_msg_info {
    NETFP *func;
    host_node_type *host_node_ptr;
    net_send_message_header msg;
    struct evbuffer *buf;
};

struct user_event_info {
    void *data;
    struct event *ev;
    event_callback_fn func;
    LIST_ENTRY(user_event_info) entry;
};

//static LIST_HEAD(, user_event_info) user_event_list = LIST_HEAD_INITIALIZER(user_event_list);
static LIST_HEAD(, net_info) net_list = LIST_HEAD_INITIALIZER(net_list);
static LIST_HEAD(, host_info) host_list = LIST_HEAD_INITIALIZER(host_list);

static void host_node_open(host_node_type *host_node_ptr, int fd)
{
    check_base_thd();
    host_node_ptr->fd = fd;
    host_node_ptr->closed = 0;
    host_node_ptr->decom_flag = 0;
    host_node_ptr->distress = 0;
    host_node_ptr->really_closed = 0;
}

static void host_node_close(host_node_type *host_node_ptr)
{
    check_base_thd();
    host_node_ptr->fd = -1;
    host_node_ptr->got_hello = 0;
    host_node_ptr->closed = 1;
    host_node_ptr->distress = 1;
    host_node_ptr->really_closed = 1;
}

static void shutdown_close(int fd)
{
    if (fd <= 2) {
        logmsg(LOGMSG_FATAL, "%s closing fd:%d\n", __func__, fd);
        abort();
    }
    shutdown(fd, SHUT_RDWR);
    close(fd);
}

static void akq_start_callback(void *dummy)
{
    start_callback(start_stop_callback_data);
}

static void akq_stop_callback(void *dummy)
{
    stop_callback(start_stop_callback_data);
}

static void init_base_int(pthread_t *, struct event_base **, void *(*f)(void *), const char *who);
static void net_dispatch(struct net_dispatch_info *);
static void *net_dispatch_host(void *data)
{
    net_dispatch(data);
    printf("DONE -- %s\n", __func__);
    return NULL;
}

static void *net_dispatch_net(void *data)
{
    net_dispatch(data);
    printf("DONE -- %s\n", __func__);
    return NULL;
}

static struct host_info *host_info_new(host_node_type *host_node_ptr)
{
    check_base_thd();
    struct host_info *h = calloc(1, sizeof(struct host_info));
    LIST_INSERT_HEAD(&host_list, h, entry);
    LIST_INIT(&h->event_list);
    h->host = host_node_ptr->host;
    if (net_thd_policy == NET_THD_POLICY_PER_HOST) {
        h->per_host.akq = akq_new(sizeof(struct user_msg_info), user_msg_func, akq_start_callback, akq_stop_callback);
        init_base_int(&h->per_host.rdthd, &h->per_host.rdbase, net_dispatch_host, h->host);
    }
    return h;
}

static void host_info_free(struct host_info *h)
{
    check_base_thd();
    LIST_REMOVE(h, entry);
    struct event_info *e, *tmpe;
    LIST_FOREACH_SAFE(e, &h->event_list, host_list_entry, tmpe) {
        event_info_free(e);
    }
    // event_base_free(h->rd_base);
    logmsg(LOGMSG_INFO, "%s - %s\n", __func__, h->host);
    free(h);
}

static struct host_info *host_info_find(const char *host)
{
    check_base_thd();
    struct host_info *h;
    LIST_FOREACH(h, &host_list, entry) {
        if (strcmp(h->host, host) == 0) {
            break;
        }
    }
    return h;
}

struct net_info {
    LIST_HEAD(, event_info) event_list;
    LIST_ENTRY(net_info) entry;
    netinfo_type *netinfo_ptr;
    struct event *unix_ev;
    struct evbuffer *unix_buf;
    struct evconnlistener *listener;

    struct policy per_net;
};

static struct net_info *net_info_new(netinfo_type *netinfo_ptr)
{
    check_base_thd();
    struct net_info *n = calloc(1, sizeof(struct net_info));
    n->netinfo_ptr = netinfo_ptr;
    if (net_thd_policy == NET_THD_POLICY_PER_NET) {
        n->per_net.akq = akq_new(sizeof(struct user_msg_info), user_msg_func, akq_start_callback, akq_stop_callback);
        init_base_int(&n->per_net.rdthd, &n->per_net.rdbase, net_dispatch_net, netinfo_ptr->service);
    }
    LIST_INSERT_HEAD(&net_list, n, entry);
    LIST_INIT(&n->event_list);
    return n;
}

static void net_info_free(struct net_info *n)
{
    check_base_thd();
    LIST_REMOVE(n, entry);
    n->netinfo_ptr->myfd = -1;
    event_free(n->unix_ev);
    evbuffer_free(n->unix_buf);
    evconnlistener_free(n->listener);
    free(n);
}

static struct net_info *net_info_find(const char *name)
{
    check_base_thd();
    struct net_info *n;
    LIST_FOREACH(n, &net_list, entry) {
        if (strcmp(n->netinfo_ptr->service, name) == 0) {
            break;
        }
    }
    return n;
}
static struct event_info *event_info_find(struct host_info *h, const char *svc)
{
    struct event_info *e;
    LIST_FOREACH(e, &h->event_list, host_list_entry) {
        if (strcmp(e->service, svc) == 0) {
            return e;
        }
    }
    return NULL;
}

static void get_host_event(char *svc, const char *host, struct host_info **hi,
                           struct event_info **ei)
{
    check_base_thd();
    *hi = NULL;
    *ei = NULL;
    struct host_info *h = host_info_find(host);
    if (h == NULL) {
        return;
    }
    *hi = h;
    *ei = event_info_find(h, svc);
}

static void update_wire_hdrs(struct event_info *e)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    for (int i = 1; i < WIRE_HEADER_MAX; ++i) {
        e->wirehdr[i]->toport = htonl(host_node_ptr->port);
    }
}

static void setup_wire_hdrs(struct event_info *e)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    size_t len = NET_WIRE_HEADER_TYPE_LEN;
    if (netinfo_ptr->myhostname_len > HOSTNAME_LEN)
        len += netinfo_ptr->myhostname_len;
    if (host_node_ptr->hostname_len > HOSTNAME_LEN)
        len += host_node_ptr->hostname_len;
    e->wirehdr_len = len;
    for (int i = 1; i < WIRE_HEADER_MAX; ++i) {
        e->wirehdr[i] = calloc(1, len);
        char *name = (char *)e->wirehdr[i] + NET_WIRE_HEADER_TYPE_LEN;
        if (netinfo_ptr->myhostname_len > HOSTNAME_LEN) {
            sprintf(e->wirehdr[i]->fromhost, ".%d", netinfo_ptr->myhostname_len);
            strcpy(name, netinfo_ptr->myhostname);
            name += netinfo_ptr->myhostname_len;
        } else {
            strcpy(e->wirehdr[i]->fromhost, netinfo_ptr->myhostname);
        }
        e->wirehdr[i]->fromport = htonl(netinfo_ptr->myport);
        if (host_node_ptr->hostname_len > HOSTNAME_LEN) {
            sprintf(e->wirehdr[i]->tohost, ".%d", host_node_ptr->hostname_len);
            strcpy(name, host_node_ptr->host);
        } else {
            strcpy(e->wirehdr[i]->tohost, host_node_ptr->host);
        }
        e->wirehdr[i]->toport = htonl(host_node_ptr->port);
        e->wirehdr[i]->type = htonl(i);
    }
}

static struct event_info *event_info_new(host_node_type *host_node_ptr,
                                         struct host_info *h)
{
    check_base_thd();
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    struct net_info *n = net_info_find(netinfo_ptr->service);
    if (n == NULL) {
        n = net_info_new(netinfo_ptr);
    }
    struct event_info *e = calloc(1, sizeof(struct event_info));
    host_node_ptr->event_info = e;
    LIST_INSERT_HEAD(&h->event_list, e, host_list_entry);
    LIST_INSERT_HEAD(&n->event_list, e, net_list_entry);
    LIST_INIT(&e->host_connected_list);
    e->fd = -1;
    e->host = host_node_ptr->host;
    e->service = netinfo_ptr->service;
    e->host_node_ptr = host_node_ptr;
    e->host_info = h;
    e->net_info = n;
    Pthread_mutex_init(&e->wr_lk, NULL);
    setup_wire_hdrs(e);
    if (net_thd_policy == NET_THD_POLICY_PER_EVENT) {
        e->per_event.akq = NULL;
        init_base_int(&e->per_event.rdthd, &e->per_event.rdbase, net_dispatch_host, host_node_ptr->host);
    }
    struct event_hash_entry *entry = malloc(sizeof(struct event_hash_entry));
    make_event_hash_key(entry->key, netinfo_ptr->service, host_node_ptr->host);
    entry->event_info = e;
    Pthread_mutex_lock(&event_hash_lk);
    hash_add(event_hash, entry);
    Pthread_mutex_unlock(&event_hash_lk);
    return e;
}

static void event_info_free(struct event_info *e)
{
    abort();
#   if 0
    check_base_thd();
    LIST_REMOVE(e, host_list_entry);
    LIST_REMOVE(e, net_list_entry);
    Pthread_mutex_lock(&e->wr_lk);
    if (!single_thread && e->rd_bev) {
        bufferevent_free(e->rd_bev);
    }
    if (e->wr_buf) {
        evbuffer_free(e->wr_buf);
    }
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    host_node_close(host_node_ptr);
    rem_from_netinfo(e->host_node_ptr->netinfo_ptr, e->host_node_ptr);
    for (int i = 1; i < WIRE_HEADER_MAX; ++i) {
        printf("********         FREEING WIREHDRs         ***********\n");
        free(e->wirehdr[i]);
    }
    printf("********         FREEING EVENTINFO      ***********\n");
    Pthread_mutex_lock(&event_hash_lk);
    hash_del(event_hash, entry);
    Pthread_mutex_unlock(&event_hash_lk);
    free(entry);
    Pthread_mutex_unlock(&e->wr_lk);
    Pthread_mutex_destroy(&e->wr_lk);
    free(e);
#   endif
}

struct net_msg_data {
    int sz;
    uint32_t ref;
    net_send_message_header msghdr;
    uint8_t buf[0];
};

static struct net_msg_data *net_msg_data_new(netinfo_type *netinfo_ptr,
                                             void *buf, int len, int type)
{
    struct net_msg_data *data = malloc(sizeof(struct net_msg_data) + len);
    if (data == NULL) {
        return NULL;
    }
    memset(data, 0, sizeof(struct net_msg_data));
    data->sz = NET_SEND_MESSAGE_HEADER_LEN + len;
    /* No ack, no seqnum */
    data->ref = 1;
    data->msghdr.datalen = htonl(len);
    data->msghdr.usertype = htonl(type);
    memcpy(&data->buf, buf, len);
    return data;
}

static void net_msg_data_free(const void *unused, size_t datalen, void *extra)
{
    struct net_msg_data *data = extra;
    int ref = ATOMIC_ADD32(data->ref, -1);
    if (ref) {
        return;
    }
    free(data);
}

static void net_msg_data_add_ref(struct net_msg_data *data)
{
    ATOMIC_ADD32(data->ref, 1);
}

struct net_msg {
    int num;
    int flags;
    netinfo_type *netinfo_ptr;
    struct net_msg_data *data[0];
};

static void net_msg_free(struct net_msg *msg)
{
    for (int i = 0; i < msg->num; ++i) {
        struct net_msg_data *data = msg->data[i];
        if (data) {
            net_msg_data_free(&data->msghdr, data->sz, data);
        }
    }
    free(msg);
}

static struct net_msg *net_msg_new(netinfo_type *netinfo_ptr, int n, void **buf,
                                   int *len, int *type, int *flags)
{
    struct net_msg *msg;
    msg = calloc(1, sizeof(struct net_msg) + sizeof(struct net_msg_data *) * n);
    if (msg == NULL) {
        return NULL;
    }
    msg->netinfo_ptr = netinfo_ptr;
    msg->num = n;
    for (int i = 0; i < n; ++i) {
        msg->data[i] = net_msg_data_new(netinfo_ptr, buf[i], len[i], type[i]);
        if (msg->data[i] == NULL) {
            net_msg_free(msg);
            return NULL;
        }
        msg->flags |= flags[i];
    }
    return msg;
}

struct host_connected_info {
    LIST_ENTRY(host_connected_info) entry;
    int fd;
    struct event_info *event_info;
    int connect_msg;
};

static struct host_connected_info *
host_connected_info_new(struct event_info *e, int fd, int connect_msg)
{
    struct host_connected_info *info = calloc(1, sizeof(struct host_connected_info));
    info->event_info = e;
    info->fd = fd;
    info->connect_msg = connect_msg;
    LIST_INSERT_HEAD(&e->host_connected_list, info, entry);
    return info;
}

static void host_connected_info_free(struct host_connected_info *info)
{
    LIST_REMOVE(info, entry);
    free(info);
}

struct accept_info {
    int fd;
    struct evbuffer *buf;
    int to_len;
    char *to_host;
    int from_len;
    char *from_host;
    char *from_host_interned;
    int from_port;
    int need;
    struct sockaddr_in ss;
    connect_message_type c;
    netinfo_type *netinfo_ptr;
};

static struct accept_info *accept_info_new(netinfo_type *netinfo_ptr,
                                           struct sockaddr_in *addr, int fd)
{
    struct accept_info *a = calloc(1, sizeof(struct accept_info));
    a->netinfo_ptr = netinfo_ptr;
    a->ss = *addr;
    a->fd = fd;
    return a;
}

static void accept_info_free(struct accept_info *a)
{
    if (a->fd != -1) {
        shutdown_close(a->fd);
    }
    if (a->buf) {
        evbuffer_free(a->buf);
    }
    free(a->from_host);
    free(a->to_host);
    free(a);
}

struct connect_info {
    int fd;
    struct evbuffer *buf;
    struct event_info *event_info;
};

static struct connect_info *connect_info_new(struct event_info *e)
{
    struct connect_info *c = calloc(1, sizeof(struct connect_info));
    c->fd = -1;
    c->event_info = e;
    c->buf = evbuffer_new();
    return c;
}

static void connect_info_free(struct connect_info *c)
{
    if (c->fd != -1) {
        struct event_info *e = c->event_info;
		hprintf("CLOSING fd:%d\n", c->fd);
        shutdown_close(c->fd);
    }
    if (c->buf) {
        evbuffer_free(c->buf);
    }
    free(c);
}

struct disable_info {
    struct event_info *event_info;
    event_callback_fn func;
};

static void disable_write(int dummyfd, short what, void *data)
{
    struct disable_info *i = data;
    struct event_info *e = i->event_info;
    event_callback_fn func = i->func;
    check_base_thd();
    free(i);
    Pthread_mutex_lock(&e->wr_lk);
    if (e->wr_buf) {
        evbuffer_free(e->wr_buf);
        e->wr_buf = NULL;
    }
    if (e->fd != -1) {
        shutdown_close(e->fd);
        e->fd = -1;
    }
    if (e->wr_pending) {
        event_del(&e->wr_event);
        e->wr_pending = 0;
    }
    Pthread_mutex_unlock(&e->wr_lk);
    event_once(base, func, e);
}

static void disable_read(int dummyfd, short what, void *data)
{
    struct disable_info *i = data;
    struct event_info *e = i->event_info;
    check_rd_thd();
    if (e->rd_bev) {
        bufferevent_free(e->rd_bev);
        e->rd_bev = NULL;
    }
    event_once(base, disable_write, i);
}

static void disable_heartbeats(int dummyfd, short what, void *data)
{
    check_timer_thd();
    struct disable_info *i = data;
    struct event_info *e = i->event_info;
    if (e->hb_check_ev) {
        event_free(e->hb_check_ev);
        e->hb_check_ev = NULL;
    }
    if (e->hb_send_ev) {
        event_free(e->hb_send_ev);
        e->hb_send_ev = NULL;
    }
    event_once(rd_base, disable_read, i);
}

static void do_host_close(int dummyfd, short what, void *data)
{
    struct disable_info *i = data;
    struct event_info *e = i->event_info;
    if (e->fd != -1) {
        hprintf("CLOSING CONNECTION fd:%d\n", e->fd);
    }
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    host_node_close(host_node_ptr);
    event_once(timer_base, disable_heartbeats, i);
}

static void do_close(struct event_info *e, event_callback_fn func)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    if (netinfo_ptr->hostdown_rtn) {
        netinfo_ptr->hostdown_rtn(netinfo_ptr, host_node_ptr->host);
    }
    struct disable_info *i = malloc(sizeof(struct disable_info));
    i->event_info = e;
    i->func = func;
    event_once(base, do_host_close, i);
}

static void do_reconnect(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct event_info *e = data;
    struct timeval t = reconnect_time();
    hprintf("RECONNECT IN %ds\n", (int)t.tv_sec);
    event_base_once(base, -1, EV_TIMEOUT, pmux_connect, e, &t);
}

static void reconnect(struct event_info *e)
{
    if (gbl_exit) {
        return;
    }
    do_close(e, do_reconnect);
}

static void nop(int dummyfd, short what, void *data)
{
    /* EVLOOP_NO_EXIT_ON_EMPTY is not available in LibEvent v2.0. This keeps
     * base around even if there are no events. */

#   if 0
    int nets = 0, hosts = 0;
    struct net_info *n;
    LIST_FOREACH(n, &net_list, entry) {
        ++nets;
    }
    struct host_info *h;
    LIST_FOREACH(h, &host_list, entry) {
        ++hosts;
    }
    int conns[2] = {0};
    int hellos[2] = {0};
    int i = 0;
    LIST_FOREACH(n, &net_list, entry) {
        struct event_info *e;
        LIST_FOREACH(e, &n->event_list, net_list_entry) {
            if (e->host_node_ptr->fd != -1)
                ++conns[i];
            if (e->host_node_ptr->got_hello)
                ++hellos[i];
        }
        ++i;
    }
    printf("%s: nets:%d hosts:%d rep(%d,%d) offload(%d,%d)\n", __func__, nets, hosts, conns[0], hellos[0], conns[1], hellos[1]);
#   endif
}

static void send_decom_all(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    struct net_info *n = net_info_find(netinfo_ptr->service);
    struct event_info *to;
    LIST_FOREACH(to, &n->event_list, net_list_entry) {
        if (to->host_node_ptr == host_node_ptr) {
            continue;
        }
        write_decom(netinfo_ptr, to->host_node_ptr, host_node_ptr->host,
                    host_node_ptr->hostname_len, to->host_node_ptr->host);
    }
    hprintf0_nd("DECOMMED\n");
}

static void do_decom(int dummyfd, short what, void *data)
{
    check_base_thd();
    char *host = data;
    struct host_info *h = host_info_find(host);
    if (h == NULL) {
        return;
    }
    struct event_info *e;
    LIST_FOREACH(e, &h->event_list, host_list_entry) {
        host_node_type *host_node_ptr = HOST_NODE_PTR(e);
        if (host_node_ptr->decom_flag) {
            continue;
        } else {
            hprintf0_nd("SETTING DECOM FLAG\n");
            host_node_ptr->decom_flag = 1;
            do_close(e, send_decom_all);
        }
    }
}

static void net_dispatch(struct net_dispatch_info *n)
{
    struct event *ev = event_new(n->base, -1, EV_PERSIST, nop, n);
    struct timeval ten = {10, 0};
    event_add(ev, &ten);
    start_callback(start_stop_callback_data);
    event_base_dispatch(n->base);
    stop_callback(start_stop_callback_data);
    event_del(ev);
    event_free(ev);
    free(n);

    Pthread_mutex_lock(&exit_mtx);
    Pthread_cond_signal(&exit_cond);
    Pthread_mutex_unlock(&exit_mtx);
}

static void *net_dispatch_rd(void *data)
{
    net_dispatch(data);
    printf("DONE -- %s\n", __func__);
    return NULL;
}

static void *net_dispatch_wr(void *data)
{
    net_dispatch(data);
    printf("DONE -- %s\n", __func__);
    return NULL;
}

static void *net_dispatch_timer(void *data)
{
    net_dispatch(data);
    printf("DONE -- %s\n", __func__);
    return NULL;
}

static void *net_dispatch_main(void *data)
{
    net_dispatch(data);
    printf("DONE -- %s\n", __func__);
    return NULL;
}

static void user_msg_func_int(NETFP *func, host_node_type *host_node_ptr,
                              net_send_message_header *msg, uint8_t *payload)
{
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    char *host = host_node_ptr->host;
    void *usrptr = netinfo_ptr->usrptr;
    ack_state_type ack, *ack_ptr = NULL;
    if (msg->waitforack) {
        ack.seqnum = msg->seqnum;
        ack.needack = msg->waitforack;
        ack.fromhost = host;
        ack.netinfo = netinfo_ptr;
        ack_ptr = &ack;
    }
    func(ack_ptr, usrptr, host, msg->usertype, payload, msg->datalen, 1);
}

static void enable_read(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    check_rd_thd();
    if (e->rd_bev) {
        bufferevent_enable(e->rd_bev, EV_READ);
    }
}

static void user_msg_func(void *work)
{
    struct user_msg_info *info = work;
    host_node_type *host_node_ptr = info->host_node_ptr;
    struct event_info *e = host_node_ptr->event_info;
    struct evbuffer *buf = info->buf;
    size_t len = evbuffer_get_length(buf);
    void *payload = evbuffer_pullup(buf, -1);
    user_msg_func_int(info->func, host_node_ptr, &info->msg, payload);
    len *= -1;
    len = ATOMIC_ADD64(e->rd_size, len);
    if (e->rd_full) {
        netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
        if (len < netinfo_ptr->max_bytes) {
            e->rd_full = 0;
            hprintf0("RESUMING READ\n");
            event_once(rd_base, enable_read, e);
        }
    }
    evbuffer_free(buf);
}

static void user_msg(struct event_info *e, net_send_message_header *msg,
                     uint8_t *payload)
{
    check_rd_thd();
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    NETFP *func = netinfo_ptr->userfuncs[msg->usertype].func;
    if (func == NULL) {
        /* Startup race without accept-on-child-net: Replication net is
         * ready and accepts offload connection but offload callbacks have not
         * registered yet. */
        reconnect(e);
        return;
    }
    struct akq *q = get_akq();
    if (q) {
        struct user_msg_info *info = akq_work_new(q);
        size_t len = e->watermark;
        info->func = func;
        info->host_node_ptr = host_node_ptr;
        info->msg = *msg;
        info->buf = evbuffer_new();
        struct evbuffer *input = bufferevent_get_input(e->rd_bev);
        evbuffer_remove_buffer(input, info->buf, len);
        len = ATOMIC_ADD64(e->rd_size, len);
        if (e->rd_full == 0 && len > netinfo_ptr->max_bytes) {
            bufferevent_disable(e->rd_bev, EV_READ);
            e->rd_full = 1;
            hprintf0("READ BUFFER IS FULL\n");
        }
        e->skip_drain = 1;
        akq_enqueue(q, info); // -> user_msg_func()
    } else {
        user_msg_func_int(func, host_node_ptr, msg, payload);
    }
}

static void user_event_func(int fd, short what, void *data)
{
    struct user_event_info *info = data;
    info->func(fd, what, info->data);
}

static void stop_a_base(struct event_base *b)
{
    Pthread_mutex_lock(&exit_mtx);
    event_base_loopbreak(b);
    /*
    struct timeval t = {0};
    t.tv_sec = 3;
    event_base_loopexit(b, &t);
    */
    Pthread_cond_wait(&exit_cond, &exit_mtx);
    Pthread_mutex_unlock(&exit_mtx);
}

static void stop_main_base()
{
    stop_a_base(base);
}

static void stop_timer_base()
{
    stop_a_base(timer_base);
}

static void stop_rd_base()
{
    //stop_a_base(rd_base(e));
}

static void stop_user_msg_qs()
{
#if 0
    if (single_thread) {
        //akq_stop(akq);
        printf("DONE -- akq\n");
        return;
    }
    struct net_info *n;
    LIST_FOREACH(n, &net_list, entry) {
        //akq_stop(n->akq);
        printf("DONE -- %s akq\n", n->netinfo_ptr->service);
    }
#endif
}

static int net_stop = 1;
static void exit_once_func(void)
{
    /* FIXME TODO go through do_close and teardown everything */
#if 0
    net_stop = 1;
    stop_main_base();
    if (!single_thread) {
        stop_timer_base();
        stop_rd_base();
    }

    stop_user_msg_qs();

    event_base_free(base);
    if (!single_thread) {
        event_base_free(timer_base);
        //event_base_free(rd_base(e));
    }

    struct user_event_info *info, *tmp;
    LIST_FOREACH_SAFE(info, &user_event_list, entry, tmp) {
        event_del(info->ev);
        free(info);
    }

    Pthread_cond_destroy(&exit_cond);
    Pthread_mutex_destroy(&exit_mtx);
#endif
}

static double difftime_now(time_t last)
{
    time_t now = time(NULL);
    return difftime(now, last);
}

static void heartbeat_check(int dummyfd, short what, void *data)
{
    check_timer_thd();
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    double diff = difftime_now(e->recv_at);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    if (e->rd_full) {
        return;
    }
    if (diff > netinfo_ptr->heartbeat_check_time) {
        hprintf("no data in %d seconds\n", (int)diff);
        reconnect(e);
    }
}

static void heartbeat_send(int dummyfd, short what, void *data)
{
    check_timer_thd();
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    double diff = difftime_now(e->sent_at);
    if (diff < netinfo_ptr->heartbeat_send_time) {
        return;
    }
    int rc = write_heartbeat(netinfo_ptr, host_node_ptr);
    diff = difftime_now(e->sent_at);
    if (diff < netinfo_ptr->heartbeat_send_time) {
        return;
    }
    hprintf("write rc:%d after %d seconds\n", rc, (int)diff);
    reconnect(e);
}

static void message_done(struct event_info *e)
{
    e->hdr.type = 0;
    e->watermark = e->wirehdr_len;
}

static void hello_hdr_common(struct event_info *e, uint8_t *payload)
{
    uint32_t n;
    memcpy(&n, payload, sizeof(uint32_t));
    e->watermark = htonl(n) - sizeof(uint32_t);
}

static void hello_msg(struct event_info *e, uint8_t *payload)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    uint32_t n;
    memcpy(&n, payload, sizeof(uint32_t));
    n = htonl(n);
    if (n > REPMAX) {
        reconnect(e);
        return;
    }
    payload += sizeof(int);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    char *hosts = (char *)payload;
    uint32_t *ports = (uint32_t *)(payload + (n * HOSTNAME_LEN));
    uint32_t *nodes = ports + n; /* in payload but ignored */
    char *long_hosts = (char *)(nodes + n);
    for (uint32_t i = 0; i < n; ++i) {
        char h[HOSTNAME_LEN + 1];
        char *host = h;
        uint32_t port;
        memcpy(h, hosts, HOSTNAME_LEN);
        h[HOSTNAME_LEN] = 0;
        hosts += HOSTNAME_LEN;
        memcpy(&port, ports, sizeof(uint32_t));
        port = htonl(port);
        ++ports;
        if (h[0] == '.') {
            uint32_t s = atoi(h + 1);
            if (s > HOST_NAME_MAX) {
                reconnect(e);
                return;
            }
            host = alloca(s + 1);
            memcpy(host, long_hosts, s);
            host[s] = 0;
            long_hosts += s;
        }
        char *ihost = intern(host);
        host_node_type *newhost = add_to_netinfo(netinfo_ptr, ihost, port);
        if (newhost) {
            if (port) {
                newhost->port = port;
            }
            add_host(newhost);
        }
    }
    host_node_ptr->got_hello = 1;
    int count = e->distress_count;
    /* Need to clear before we can print */
    e->distress_count = e->distressed = 0;
    if (count >= DISTRESS_COUNT) {
        hprintf("LEAVING DISTRESS MODE (retries:%d)\n", count);
    }
}

static void process_hello_msg(struct event_info *e, uint8_t *payload)
{
    if (e->state == 0) {
        hello_hdr_common(e, payload);
        return;
    }
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    hello_msg(e, payload);
    message_done(e);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    write_hello_reply(netinfo_ptr, host_node_ptr);
}

static void process_hello_msg_reply(struct event_info *e, uint8_t *payload)
{
    if (e->state == 0) {
        hello_hdr_common(e, payload);
        return;
    }
    hello_msg(e, payload);
    message_done(e);
}

static void process_user_msg(struct event_info *e, uint8_t *payload)
{
    net_send_message_header *msg = &e->msg;
    if (e->state == 0) {
        net_send_message_header_get(msg, payload, payload + NET_SEND_MESSAGE_HEADER_LEN);
        if (msg->usertype <= USER_TYPE_MIN || msg->usertype >= USER_TYPE_MAX) {
            hprintf("bad bad usertype:%d (htonl:%d)\n", msg->usertype, htonl(msg->usertype));
            abort();
        }
        if (msg->datalen) {
            e->watermark = msg->datalen;
            return;
        }
    }
    user_msg(e, msg, payload);
    message_done(e);
}

static void process_ack_no_payload(struct event_info *e, uint8_t *payload)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    net_ack_message_type ack = {0};
    net_ack_message_type_get(&ack, payload, payload + NET_ACK_MESSAGE_TYPE_LEN);
    Pthread_mutex_lock(&host_node_ptr->wait_mutex);
    seq_data *ptr = host_node_ptr->wait_list;
    while (ptr != NULL && (ptr->seqnum != ack.seqnum)) {
        ptr = ptr->next;
    }
    if (ptr) {
        ptr->ack = 1;
        ptr->outrc = ack.outrc;
        Pthread_cond_signal(&host_node_ptr->ack_wakeup);
    }
    Pthread_mutex_unlock(&host_node_ptr->wait_mutex);
    message_done(e);
}

static void process_ack_with_payload(struct event_info *e, uint8_t *payload)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    net_ack_message_payload_type *ack = &e->ack;
    if (e->state == 0) {
        uint8_t *start = payload;
        uint8_t *end = payload + NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN;
        net_ack_message_payload_type_get(ack, start, end);
        e->watermark = ack->paylen;
        return;
    }
    void *p = malloc(ack->paylen);
    memcpy(p, payload, ack->paylen);
    Pthread_mutex_lock(&host_node_ptr->wait_mutex);
    seq_data *ptr = host_node_ptr->wait_list;
    while (ptr != NULL && (ptr->seqnum != ack->seqnum)) {
        ptr = ptr->next;
    }
    if (ptr) {
        ptr->ack = 1;
        ptr->outrc = ack->outrc;
        ptr->payload = p;
        ptr->payloadlen = ack->paylen;
        Pthread_cond_signal(&host_node_ptr->ack_wakeup);
    }
    Pthread_mutex_unlock(&host_node_ptr->wait_mutex);
    if (!ptr) {
        free(p);
    }
    message_done(e);
}

static void process_decom_nodenum(struct event_info *e, uint8_t *payload)
{
    uint32_t n;
    memcpy(&n, payload, sizeof(uint32_t));
    n = htonl(n);
    message_done(e);
    decom(hostname(n));
}

static void process_decom_hostname(struct event_info *e, uint8_t *payload)
{
    if (e->state == 0) {
        uint32_t n;
        memcpy(&n, payload, sizeof(uint32_t));
        n = htonl(n);
        e->watermark = htonl(n);
        return;
    }
    message_done(e);
    decom(intern((char *)payload));
}

static int process_hdr(struct event_info *e, uint8_t *buf)
{
    wire_header_type tmp;
    net_wire_header_get(&tmp, buf, buf + e->watermark);
    e->hdr = tmp;
    e->state = 0;
    switch (tmp.type) {
    case WIRE_HEADER_HEARTBEAT:
        message_done(e);
        break;
    case WIRE_HEADER_HELLO:
        e->watermark = sizeof(uint32_t);
        break;
    case WIRE_HEADER_DECOM:
        e->watermark = sizeof(uint32_t);
        break;
    case WIRE_HEADER_USER_MSG:
        e->watermark = NET_SEND_MESSAGE_HEADER_LEN;
        break;
    case WIRE_HEADER_ACK:
        e->watermark = NET_ACK_MESSAGE_TYPE_LEN;
        break;
    case WIRE_HEADER_HELLO_REPLY:
        e->watermark = sizeof(uint32_t);
        break;
    case WIRE_HEADER_DECOM_NAME:
        e->watermark = sizeof(uint32_t);
        break;
    case WIRE_HEADER_ACK_PAYLOAD:
        e->watermark = NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN;
        break;
    default:
        hprintf("UNKNOWN HDR:%d\n", tmp.type);
        abort();
        return -1;
    }
    return 0;
}

static int process_payload(struct event_info *e, uint8_t *payload)
{
    switch (e->hdr.type) {
    case WIRE_HEADER_HELLO:
        process_hello_msg(e, payload);
        break;
    case WIRE_HEADER_DECOM:
        process_decom_nodenum(e, payload);
        break;
    case WIRE_HEADER_USER_MSG:
        process_user_msg(e, payload);
        break;
    case WIRE_HEADER_ACK:
        process_ack_no_payload(e, payload);
        break;
    case WIRE_HEADER_HELLO_REPLY:
        process_hello_msg_reply(e, payload);
        break;
    case WIRE_HEADER_DECOM_NAME:
        process_decom_hostname(e, payload);
        break;
    case WIRE_HEADER_ACK_PAYLOAD:
        process_ack_with_payload(e, payload);
        break;
    default:
        hprintf("UNKNOWN HDR:%d\n", e->hdr.type);
        abort();
        return -1;
    }
    ++e->state;
    return 0;
}

static void readcb(struct bufferevent *bev, void *data)
{
    struct event_info *e = data;
    check_rd_thd();
    if (e->rd_bev != bev) {
        abort();
    }
    struct evbuffer *input = bufferevent_get_input(bev);
    size_t need = e->watermark;
    uint8_t *buf;
    int rc = 0;
    while ((buf = evbuffer_pullup(input, need)) != NULL) {
            if (e->skip_drain) {
                puts("before this thing");
                abort();
            }
        e->recv_at = time(NULL);
        if (e->hdr.type == 0) {
            rc = process_hdr(e, buf);
            if (e->skip_drain) {
                puts("this thing");
                abort();
            }
        } else {
            rc = process_payload(e, buf);
        }
        if (rc) {
            bufferevent_disable(bev, EV_READ);
            reconnect(e);
            return;
        }
        if (e->skip_drain) {
            e->skip_drain = 0;
        } else {
            evbuffer_drain(input, need);
        }
            if (e->skip_drain) {
                puts("how could it bethis thing");
                abort();
            }
        need = e->watermark;
    }
    bufferevent_setwatermark(bev, EV_READ, need, 0);
}

static void eventcb(struct bufferevent *bev, short events, void *data)
{
    struct event_info *e = data;
    if (e->rd_bev != bev) {
        abort();
    }
    int err = evutil_socket_geterror(bufferevent_getfd(bev));
    hprintf("fd:%d RD:%d WR:%d EOF:%d ERR:%d T'OUT:%d CON'D:%d errno:%d %s\n",
            bufferevent_getfd(bev), !!(events & BEV_EVENT_READING),
            !!(events & BEV_EVENT_WRITING), !!(events & BEV_EVENT_EOF),
            !!(events & BEV_EVENT_ERROR), !!(events & BEV_EVENT_TIMEOUT),
            !!(events & BEV_EVENT_CONNECTED), err, strerror(err));
    bufferevent_disable(bev, EV_READ | EV_WRITE);
    reconnect(e);
}

static void do_queued(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    struct host_connected_info *info = LIST_FIRST(&e->host_connected_list);
    event_once(base, host_setup, info);
}

static void finish_host_setup(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct host_connected_info *info = data;
    struct event_info *e = info->event_info;
    int connect_msg = info->connect_msg;
    host_connected_info_free(info);
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    if (!LIST_EMPTY(&e->host_connected_list)) {
        hprintf0("WORKING ON QUEUED CONNECTION\n");
        do_close(e, do_queued);
    } else if (connect_msg) {
        hprintf0("CONNECTION ESTABLISHED - writing hello\n");
        netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
        write_connect_message(netinfo_ptr, host_node_ptr, NULL);
        write_hello(netinfo_ptr, host_node_ptr);
    } else {
        hprintf0("CONNECTION ESTABLISHED\n");
    }
}

static void setup_heartbeats(int dummyfd, short what, void *data)
{
    check_timer_thd();
    struct host_connected_info *info = data;
    struct event_info *e = info->event_info;
    e->recv_at = e->sent_at = time(NULL);
    if (e->hb_send_ev == NULL) {
        e->hb_send_ev = event_new(timer_base, -1, EV_PERSIST, heartbeat_send, e);
        event_add(e->hb_send_ev, &one_sec);
    }
    if (e->hb_check_ev == NULL) {
        e->hb_check_ev = event_new(timer_base, -1, EV_PERSIST, heartbeat_check, e);
        event_add(e->hb_check_ev, &one_sec);
    }
    event_once(base, finish_host_setup, info);
}

static void setup_rd_bev(int dummyfd, short what, void *data)
{
    struct host_connected_info *info = data;
    struct event_info *e = info->event_info;
    check_rd_thd();
    message_done(e);
    e->rd_full = 0;
    e->skip_drain = 0;
    e->rd_bev = bufferevent_socket_new(rd_base, info->fd, 0);
    bufferevent_enable(e->rd_bev, EV_READ);
    bufferevent_disable(e->rd_bev, EV_WRITE);
    bufferevent_setwatermark(e->rd_bev, EV_READ, e->wirehdr_len, 0);
    bufferevent_setcb(e->rd_bev, readcb, NULL, eventcb, e);
    event_once(timer_base, setup_heartbeats, info);
}

static void setup_wr_buf(int dummyfd, short what, void *data)
{
    struct host_connected_info *info = data;
    struct event_info *e = info->event_info;
    check_base_thd();
    Pthread_mutex_lock(&e->wr_lk);
    e->wr_buf = evbuffer_new();
    e->wr_full = 0;
    e->wr_pending = 0;
    e->fd = info->fd;
    Pthread_mutex_unlock(&e->wr_lk);
    event_once(rd_base, setup_rd_bev, info);
}

static void host_setup(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct host_connected_info *info = data;
    struct event_info *e = info->event_info;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    host_node_open(host_node_ptr, info->fd);
    setup_wr_buf(dummyfd, what, info);
}

static void host_connected(struct event_info *e, int fd, int connect_msg)
{
    check_base_thd();
    int dispatch = LIST_EMPTY(&e->host_connected_list);
    host_connected_info_new(e, fd, connect_msg);
    if (dispatch) {
        hprintf("PROCESSING CONNECTION fd:%d\n", fd);
        do_close(e, do_queued);
    }
}

static netinfo_type *pmux_netinfo(host_node_type *host_node_ptr)
{
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    if (netinfo_ptr->ischild && !netinfo_ptr->accept_on_child) {
        netinfo_ptr = netinfo_ptr->parent;
    }
    return netinfo_ptr;
}

static void comdb2_connected(int fd, short what, void *data)
{
    struct connect_info *c = data;
    struct event_info *e = c->event_info;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    if (host_node_ptr->fd != -1) {
        //hprintf0_nd("ALREADY HAVE CONNECTION\n");
    } else if (!LIST_EMPTY(&e->host_connected_list)) {
        //hprintf0_nd("HAVE PENDING CONNECTION\n");
    } else if (host_node_ptr->decom_flag) {
        //hprintf0_nd("SKIP DECOM NODE\n");
    } else {
        hprintf("MADE NEW CONNECTION fd:%d\n", fd);
        host_connected(e, fd, 1);
        c->fd = -1;
    }
    connect_info_free(c);
}

static void comdb2_connect(struct connect_info *c, int port)
{
    struct event_info *e = c->event_info;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    struct sockaddr_in sin = {0};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(host_node_ptr->port);
    if (get_dedicated_conhost(host_node_ptr, &host_node_ptr->addr)) {
        pmux_reconnect(c);
        return;
    }
    sin.sin_addr = host_node_ptr->addr;
    int fd = get_nonblocking_socket();
    if (fd == -1) {
        pmux_reconnect(c);
        return;
    }
    c->fd = fd;
    socklen_t len = sizeof(sin);
    int rc = connect(fd, (struct sockaddr *)&sin, len);
    if (rc == -1 && errno != EINPROGRESS) {
        pmux_reconnect(c);
        return;
    }
    event_base_once(base, fd, EV_WRITE, comdb2_connected, c, NULL);
}

static void pmux_rte_readcb(int fd, short what, void *data)
{
    struct connect_info *c = data;
    if (evbuffer_read(c->buf, fd, -1) <= 0) {
        pmux_reconnect(c);
        return;
    }
    size_t len;
    char *res = evbuffer_readln(c->buf, &len, EVBUFFER_EOL_ANY);
    if (res == NULL) {
        event_base_once(base, fd, EV_READ, pmux_rte_readcb, c, NULL);
        return;
    }
    int pmux_rc = -1;
    int rc = sscanf(res, "%d", &pmux_rc);
    free(res);
    if (rc != 1 || pmux_rc != 0) {
        pmux_reconnect(c);
        return;
    }
    event_base_once(base, fd, EV_WRITE, comdb2_connected, c, NULL);
}

static void pmux_rte_writecb(int fd, short what, void *data)
{
    struct connect_info *c = data;
    if (evbuffer_write(c->buf, fd) <= 0) {
        pmux_reconnect(c);
        return;
    }
    if (evbuffer_get_length(c->buf) == 0) {
        event_base_once(base, fd, EV_READ, pmux_rte_readcb, c, NULL);
    } else {
        event_base_once(base, fd, EV_WRITE, pmux_rte_writecb, c, NULL);
    }
}

static void pmux_rte(struct connect_info *c)
{
    struct event_info *e = c->event_info;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = pmux_netinfo(host_node_ptr);
    evbuffer_add_printf(c->buf, "rte %s/%s/%s\n",
                        netinfo_ptr->app, netinfo_ptr->service,
                        netinfo_ptr->instance);
    event_base_once(base, c->fd, EV_WRITE, pmux_rte_writecb, c, NULL);
}

static void pmux_get_readcb(int fd, short what, void *data)
{
    struct connect_info *c = data;
    struct event_info *e = c->event_info;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = pmux_netinfo(host_node_ptr);
    if (evbuffer_read(c->buf, fd, -1) <= 0) {
        pmux_reconnect(c);
        return;
    }
    size_t len;
    char *res = evbuffer_readln(c->buf, &len, EVBUFFER_EOL_ANY);
    if (res == NULL) {
        event_base_once(base, fd, EV_READ, pmux_get_readcb, c, NULL);
        return;
    }
    int good = 0;
    int port = -1;
    char *app = NULL, *svc = NULL, *ins = NULL;
    int rc = sscanf(res, "%d %m[^/]/%m[^/]/%ms", &port, &app, &svc, &ins);
    if (rc == 4 && strcmp(netinfo_ptr->app, app) == 0 &&
        strcmp(netinfo_ptr->service, svc) == 0 &&
        strcmp(netinfo_ptr->instance, ins) == 0 &&
        port > 0 && port <= USHRT_MAX
    ){
        good = 1;
    }
    free(res);
    os_free(app);
    os_free(svc);
    os_free(ins);
    if (!good) {
        pmux_reconnect(c);
        return;
    }
    host_node_ptr->port = port;
    if (gbl_pmux_route_enabled) {
        pmux_rte(c);
        return;
    }
    /* Thanks pmux for the port */
    c->fd = -1;
    shutdown_close(fd);
    comdb2_connect(c, port);
}

static void pmux_get_writecb(int fd, short what, void *data)
{
    struct connect_info *c = data;
    if (evbuffer_write(c->buf, fd) <= 0) {
        pmux_reconnect(c);
        return;
    }
    if (evbuffer_get_length(c->buf) == 0) {
        event_base_once(base, fd, EV_READ, pmux_get_readcb, c, NULL);
    } else {
        event_base_once(base, fd, EV_WRITE, pmux_get_writecb, c, NULL);
    }
}

static void pmux_get(struct connect_info *c)
{
    struct event_info *e = c->event_info;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = pmux_netinfo(host_node_ptr);
    evbuffer_add_printf(c->buf, "get /echo %s/%s/%s\n", netinfo_ptr->app,
                        netinfo_ptr->service, netinfo_ptr->instance);
    event_base_once(base, c->fd, EV_WRITE, pmux_get_writecb, c, NULL);
}

static void pmux_reconnect(struct connect_info *c)
{
    struct event_info *e = c->event_info;
    connect_info_free(c);
    struct timeval t = reconnect_time();
    event_base_once(base, -1, EV_TIMEOUT, pmux_connect, e, &t);
}

static void pmux_connect(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    struct connect_info *c = connect_info_new(e);
    ++e->distress_count;
    if (get_dedicated_conhost(host_node_ptr, &host_node_ptr->addr)) {
        hprintf0("get_dedicated_conhost failed\n");
        pmux_reconnect(c);
        return;
    }
    struct sockaddr_in sin = {0};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(get_portmux_port());
    sin.sin_addr = host_node_ptr->addr;
    socklen_t len = sizeof(sin);
    int fd = get_nonblocking_socket();
    if (fd == -1) {
        pmux_reconnect(c);
        return;
    }
    c->fd = fd;
    int rc = connect(fd, (struct sockaddr *)&sin, len);
    if (rc == -1 && errno != EINPROGRESS) {
        pmux_reconnect(c);
        return;
    }
    hprintf("CONNECTING fd:%d\n", c->fd);
    if (gbl_pmux_route_enabled && host_node_ptr->port != 0) {
        pmux_rte(c);
    } else {
        pmux_get(c);
    }
}

static struct timeval ms_to_timeval(int ms)
{
    struct timeval t = {0};
    uint64_t usec = ms * 1000;
    if (usec > 1000000) {
        t.tv_sec = usec / 1000000;
    }
    t.tv_usec = usec % 1000000;
    return t;
}

static int accept_host(struct accept_info *a)
{
    int port = a->from_port;
    char *host = a->from_host_interned;
    netinfo_type *netinfo_ptr = a->netinfo_ptr;
    host_node_type *host_node_ptr = get_host_node_by_name_ll(netinfo_ptr, host);
    if (host_node_ptr == NULL) {
        host_node_ptr = add_to_netinfo(netinfo_ptr, host, port);
    }
    if (host_node_ptr == NULL) {
        return -1;
    }
    host_node_ptr->port = port;
    host_node_ptr->addr = a->ss.sin_addr;
    if (netinfo_ptr->new_node_rtn) {
        netinfo_ptr->new_node_rtn(netinfo_ptr, host, port);
    }
    struct host_info *h = NULL;
    struct event_info *e = NULL;
    get_host_event(intern(netinfo_ptr->service), host_node_ptr->host, &h, &e);
    if (h == NULL) {
        h = host_info_new(host_node_ptr);
    }
    if (e == NULL) {
        e = event_info_new(host_node_ptr, h);
    } else {
        update_wire_hdrs(e);
    }
    hprintf("ACCEPTED NEW CONNECTION fd:%d\n", a->fd);
    host_connected(e, a->fd, 0);
    a->fd = -1;
    accept_info_free(a);
    return 0;
}

static int validate_host(struct accept_info *a)
{
#   if 0
    socklen_t slen = sizeof(a->ss);
    char from[HOST_NAME_MAX];
    socklen_t hlen = sizeof(from);
    int rc = getnameinfo((struct sockaddr *)&a->ss, slen, from, hlen, NULL, 0, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s getnameinfo failed rc:%d\n", __func__, rc);
        return -1;
    }
    char *f = strchr(from, '.');
    if (f) {
        *f = '\0';
    }
    if (strcmp(a->from_host, from) != 0 || strcmp(a->to_host, gbl_myhostname) != 0) {
        logmsg(LOGMSG_WARN, "%s fd:%d invalid names from:%s (exp:%s) to:%s (exp:%s)\n",
               __func__, a->fd, a->from_host, from, a->to_host, gbl_myhostname);
        return -1;
    }
#   endif
    int check_port = 1;
    int port = a->c.my_portnum;
    int netnum = port >> 16;
    a->from_port = port &= 0xffff;
    if (netnum > 0 && netnum < a->netinfo_ptr->num_child_nets) {
        check_port = 0;
        a->netinfo_ptr = a->netinfo_ptr->child_nets[netnum];
    }
    netinfo_type *netinfo_ptr = a->netinfo_ptr;
    if (netinfo_ptr == NULL) {
        logmsg(LOGMSG_ERROR,
               "%s failed node:%d host:%s netnum:%d (%d) failed\n", __func__,
               a->c.my_nodenum, a->from_host, netnum, a->netinfo_ptr->netnum);
        return -1;
    }
    if (check_port && a->c.to_portnum != netinfo_ptr->myport) {
        logmsg(LOGMSG_WARN,
               "%s: fd:%d hostname:%s svc:%s invalid port:%d (exp:%d)\n",
               __func__, a->fd, a->from_host, netinfo_ptr->service,
               a->c.to_portnum, netinfo_ptr->myport);
        return -1;
    }
    char *host = a->from_host_interned = intern(a->from_host);
    if (netinfo_ptr->allow_rtn && !netinfo_ptr->allow_rtn(netinfo_ptr, host)) {
        logmsg(LOGMSG_ERROR, "connection from node:%d host:%s not allowed\n",
               a->c.my_nodenum, host);
        return -1;
    }
    if (a->c.flags & CONNECT_MSG_SSL) {
        abort();
        return -1;
    } else {
        return accept_host(a);
    }
}

static int process_long_hostname(struct accept_info *a)
{
    struct evbuffer *input = a->buf;
    char *buf = (char *)evbuffer_pullup(input, a->need);
    if (a->from_len) {
        a->from_host = strndup(buf, a->from_len);
        buf += a->from_len;
    }
    if (a->to_len) {
        a->to_host = strndup(buf, a->to_len);
    }
    return validate_host(a);
}

static void read_long_hostname(int fd, short what, void *data)
{
    struct accept_info *a = data;
    int need = a->need - evbuffer_get_length(a->buf);
    int n = evbuffer_read(a->buf, fd, need);
    if (n <= 0) {
        accept_info_free(a);
        return;
    }
    int rc;
    if (n == need) {
        rc = process_long_hostname(a);
    } else {
        rc = event_base_once(base, fd, EV_READ, read_long_hostname, a, NULL);
    }
    if (rc) {
        accept_info_free(a);
    }
}

static int read_hostname(char **host, int *len, char *name)
{
    int n = 0;
    if (*name == '.') {
        ++name;
        n = atoi(name);
        if (n < 1 || n > HOST_NAME_MAX) {
            return -1;
        }
        *len = n;
        return 0;
    }
    *host = strndup(name, HOSTNAME_LEN);
    return 0;
}

static int process_connect_message(struct accept_info *a)
{
    struct evbuffer *input = a->buf;
    uint8_t *buf = evbuffer_pullup(input, NET_CONNECT_MESSAGE_TYPE_LEN);
    if (buf == NULL) {
        return -1;
    }
    net_connect_message_get(&a->c, buf, buf + NET_CONNECT_MESSAGE_TYPE_LEN);
    evbuffer_drain(input, NET_CONNECT_MESSAGE_TYPE_LEN);
    if (read_hostname(&a->from_host, &a->from_len, a->c.my_hostname) != 0) {
        return -1;
    }
    if (read_hostname(&a->to_host, &a->to_len, a->c.to_hostname) != 0) {
        return -1;
    }
    int more = a->from_len + a->to_len;
    if (more == 0) {
        return validate_host(a);
    }
    a->need = more;
    int fd = a->fd;
    int n = evbuffer_read(input, fd, more);
    int rc;
    if (n == more) {
        rc = process_long_hostname(a);
    } else {
        rc = event_base_once(base, fd, EV_READ, read_long_hostname, a, NULL);
    }
    return rc;
}

static void read_connect(int fd, short what, void *data)
{
    struct accept_info *a = data;
    int need = a->need - evbuffer_get_length(a->buf);
    int n = evbuffer_read(a->buf, fd, need);
    if (n <= 0) {
        accept_info_free(a);
        return;
    }
    int rc;
    if (n == need) {
        rc = process_connect_message(a);
    } else {
        rc = event_base_once(base, fd, EV_READ, read_connect, a, NULL);
    }
    if (rc) {
        accept_info_free(a);
    }
}

static void do_read(int fd, short what, void *data)
{
    check_base_thd();
    struct accept_info *a = data;
    uint8_t b;
    ssize_t n = recv(fd, &b, 1, 0);
    if (n <= 0) {
        accept_info_free(a);
        return;
    }
    if (b) {
        make_socket_blocking(fd);
        SBUF2 *sb = sbuf2open(fd, 0);
        do_appsock(a->netinfo_ptr, &a->ss, sb, b);
        a->fd = -1;
        accept_info_free(a);
        return;
    }
    a->buf = evbuffer_new();
    a->need = NET_CONNECT_MESSAGE_TYPE_LEN;
    n = evbuffer_read(a->buf, fd, a->need);
    int rc;
    if (n == a->need) {
        rc = process_connect_message(a);
    } else {
        rc = event_base_once(base, fd, EV_READ, read_connect, a, NULL);
    }
    if (rc) {
        accept_info_free(a);
    }
}

static void do_accept(struct evconnlistener *listener, evutil_socket_t fd,
                      struct sockaddr *addr, int len, void *data)
{
    check_base_thd();
    struct net_info *n = data;
    netinfo_type *netinfo_ptr = n->netinfo_ptr;
    netinfo_ptr->num_accepts++;
    struct accept_info *a;
    a = accept_info_new(netinfo_ptr, (struct sockaddr_in *)addr, fd);
    if (event_base_once(base, fd, EV_READ, do_read, a, NULL) != 0) {
        accept_info_free(a);
    }
}

static void reopen_unix(int fd, struct net_info *n)
{
    check_base_thd();
    if (n->unix_ev) {
        event_free(n->unix_ev);
        n->unix_ev = NULL;
    }
    if (n->unix_buf) {
        evbuffer_free(n->unix_buf);
        n->unix_buf = NULL;
    }
    shutdown_close(fd);
    event_base_once(base, -1, EV_TIMEOUT, unix_connect, n, &one_sec);
}

#ifndef __sun
#define is_fd(m)                                                               \
    ((m)->cmsg_len == CMSG_LEN(sizeof(int)) &&                                 \
     (m)->cmsg_level == SOL_SOCKET && (m)->cmsg_type == SCM_RIGHTS)
#endif

static int recvfd(int fd)
{
    int newfd = -1;
    char buf[sizeof("pmux") - 1];
    struct iovec iov = {.iov_base = buf, .iov_len = sizeof(buf)};
#ifdef __sun
    struct msghdr msg = {.msg_iov = &iov,
                         .msg_iovlen = 1,
                         .msg_accrights = (caddr_t)&newfd,
                         .msg_accrightslen = sizeof(newfd)};
    ssize_t rc = recvmsg(fd, &msg, 0);
    if (rc != sizeof(buf)) {
        logmsg(LOGMSG_ERROR, "%s:recvmsg fd:%d rc:%zd expected:%zu (%s)\n",
               __func__, fd, rc, sizeof(buf), strerror(errno));
        return -1;
    }
    if (msg.msg_accrightslen != sizeof(newfd)) {
        return -1;
    }
#else
    struct cmsghdr *cmsgptr = alloca(CMSG_SPACE(sizeof(int)));
    struct msghdr msg = {.msg_iov = &iov,
                         .msg_iovlen = 1,
                         .msg_control = cmsgptr,
                         .msg_controllen = CMSG_SPACE(sizeof(int))};
    ssize_t rc = recvmsg(fd, &msg, 0);
    if (rc != sizeof(buf)) {
        logmsg(LOGMSG_ERROR, "%s:recvmsg fd:%d rc:%zd expected:%zu (%s)\n",
               __func__, fd, rc, sizeof(buf), strerror(errno));
        return -1;
    }
    struct cmsghdr *tmp, *m;
    m = tmp = CMSG_FIRSTHDR(&msg);
    if (CMSG_NXTHDR(&msg, tmp) != NULL) {
        logmsg(LOGMSG_ERROR, "%s:CMSG_NXTHDR unexpected msghdr\n", __func__);
        return -1;
    }
    if (!is_fd(m)) {
        logmsg(LOGMSG_ERROR, "%s: bad msg attributes\n", __func__);
        return -1;
    }
    newfd = *(int *)CMSG_DATA(m);
#endif
    if (memcmp(buf, "pmux", sizeof(buf)) != 0) {
        shutdown_close(newfd);
        logmsg(LOGMSG_ERROR, "%s:recvmsg fd:%d unexpected msg:%.*s\n", __func__,
               fd, (int)sizeof(buf), buf);
        return -1;
    }
    return newfd;
}

static void do_recvfd(int fd, short what, void *data)
{
    check_base_thd();
    struct net_info *n = data;
    int newfd = recvfd(fd);
    if (newfd == -1) {
        reopen_unix(fd, n);
        return;
    }
    make_socket_nonblocking(newfd);
    ssize_t rc = write(newfd, "0\n", 2);
    if (rc != 2) {
        logmsg(LOGMSG_ERROR, "%s:write fd:%d rc:%zd (%s)\n", __func__, fd, rc,
               strerror(errno));
        shutdown_close(newfd);
        return;
    }
    struct sockaddr_in saddr;
    struct sockaddr *addr = (struct sockaddr *)&saddr;
    socklen_t addrlen = sizeof(saddr);
    getpeername(newfd, addr, &addrlen);
    do_accept(NULL, newfd, addr, addrlen, n);
}

static int process_reg_reply(char *res, struct net_info *n)
{
    int port;
    if (sscanf(res, "%d", &port) != 1) {
        return -1;
    }
    netinfo_type *netinfo_ptr = n->netinfo_ptr;
    if (netinfo_ptr->myport == port) {
        return 0;
    }
    logmsg(LOGMSG_ERROR, "%s: PORT CHANGED old:%d new:%d!\n", __func__,
           netinfo_ptr->myport, port);
    evconnlistener_free(n->listener);
    struct sockaddr_in sin = {0};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    socklen_t len = sizeof(sin);
    struct sockaddr *s  = (struct sockaddr *)&sin;
    unsigned flags;
    flags = LEV_OPT_REUSEABLE | LEV_OPT_CLOSE_ON_FREE;
    n->listener = evconnlistener_new_bind(base, do_accept, NULL, flags, SOMAXCONN, s, len);
    if (n->listener == NULL) {
        return -1;
    }
    netinfo_ptr->myfd = evconnlistener_get_fd(n->listener);
    netinfo_ptr->myport = port;
    logmsg(LOGMSG_INFO, "%s svc:%s accepting on port:%d fd:%d\n", __func__,
           netinfo_ptr->service, netinfo_ptr->myport, netinfo_ptr->myfd);
    return 0;
}

static void unix_reg_reply(int fd, short what, void *data)
{
    check_base_thd();
    struct net_info *n = data;
    if ((what & EV_READ) == 0) {
        logmsg(LOGMSG_WARN, "%s fd:%d %s%s%s%s\n", __func__, fd,
               (what & EV_TIMEOUT) ? " timeout" : "",
               (what & EV_READ) ? " read" : "",
               (what & EV_WRITE) ? " write" : "",
               (what & EV_SIGNAL) ? " signal" : "");
        reopen_unix(fd, n);
        return;
    }
    struct evbuffer *buf = n->unix_buf;
    int rc;
    size_t len;
    char *res = NULL;
    while ((rc = evbuffer_read(buf, fd, 1)) == 1) {
        if ((res = evbuffer_readln(buf, &len, EVBUFFER_EOL_ANY)) != NULL)
            break;
        if ((len = evbuffer_get_length(buf)) > sizeof("65535")) {
            logmsg(LOGMSG_ERROR, "%s: bad pmux reply len:%zu max-expected:6\n",
                   __func__, len);
            reopen_unix(fd, n);
            return;
        }
    }
    if (res == NULL) {
        if (rc <= 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                logmsgperror("unix_reg_reply:evbuffer_read");
                reopen_unix(fd, n);
            }
        }
        return;
    }
    if (process_reg_reply(res, n) == 0) {
        netinfo_type *netinfo_ptr = n->netinfo_ptr;
        evbuffer_free(n->unix_buf);
        n->unix_buf = NULL;
        event_free(n->unix_ev);
        n->unix_ev = event_new(base, fd, EV_READ | EV_PERSIST, do_recvfd, n);
        logmsg(LOGMSG_INFO, "%s: svc:%s accepting on unix socket fd:%d\n",
               __func__, netinfo_ptr->service, fd);
        event_add(n->unix_ev, NULL);
    } else {
        reopen_unix(fd, n);
    }
    free(res);
    return;
}

static void unix_reg(int fd, short what, void *data)
{
    check_base_thd();
    struct net_info *n = data;
    if ((what & EV_WRITE) == 0) {
        logmsg(LOGMSG_WARN, "%s fd:%d %s%s%s%s\n", __func__, fd,
               (what & EV_TIMEOUT) ? " timeout" : "",
               (what & EV_READ) ? " read" : "",
               (what & EV_WRITE) ? " write" : "",
               (what & EV_SIGNAL) ? " signal" : "");
        reopen_unix(fd, n);
        return;
    }
    struct evbuffer *buf = n->unix_buf;
    int rc = evbuffer_write(buf, fd);
    if (rc <= 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            logmsgperror("unix_reg:evbuffer_write");
            reopen_unix(fd, n);
        }
        return;
    }
    if (evbuffer_get_length(buf) == 0) {
        struct event *ev = n->unix_ev;
        event_free(ev);
        n->unix_ev = event_new(base, fd, EV_READ | EV_PERSIST, unix_reg_reply, n);
        event_add(n->unix_ev, &one_sec);
    }
}

static void unix_connect(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct net_info *n = data;
    netinfo_type *netinfo_ptr = n->netinfo_ptr;
    if (n->unix_ev) {
        logmsg(LOGMSG_FATAL, "%s: LEFTOVER unix_ev for:%s\n", __func__, netinfo_ptr->service);
        abort();
    }
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    evutil_make_socket_nonblocking(fd);
    logmsg(LOGMSG_USER, "%s unix fd:%d\n", __func__, fd);

    struct sockaddr_un addr = {0};
    socklen_t len = sizeof(addr);
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, get_portmux_bind_path());
    int rc = connect(fd, (struct sockaddr *)&addr, len);
    if (rc == -1 && errno != EINPROGRESS) {
        logmsgperror("unix_connect:connect");
        reopen_unix(fd, n);
        return;
    }
    n->unix_buf = evbuffer_new();
    evbuffer_add_printf(n->unix_buf, "reg %s/%s/%s\n", netinfo_ptr->app,
                        netinfo_ptr->service, netinfo_ptr->instance);
    n->unix_ev = event_new(base, fd, EV_WRITE | EV_PERSIST, unix_reg, n);
    event_add(n->unix_ev, &one_sec);
}

static void net_accept(netinfo_type *netinfo_ptr)
{
    check_base_thd();
    struct net_info *n = net_info_find(netinfo_ptr->service);
    if (n) {
        return;
    }
    n = net_info_new(netinfo_ptr);
    if (netinfo_ptr->ischild && !netinfo_ptr->accept_on_child) {
        return;
    }
    unix_connect(-1, EV_TIMEOUT, n);
    int fd = netinfo_ptr->myfd;
    make_socket_nonblocking(fd);
    unsigned flags = LEV_OPT_LEAVE_SOCKETS_BLOCKING | LEV_OPT_CLOSE_ON_FREE;
    n->listener = evconnlistener_new(base, do_accept, n, flags, SOMAXCONN, fd);
    logmsg(LOGMSG_INFO, "%s svc:%s accepting on port:%d fd:%d\n", __func__,
           netinfo_ptr->service, netinfo_ptr->myport, fd);
}

static void do_add_host(int accept_fd, short what, void *data)
{
    host_node_type *host_node_ptr = data;
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    net_accept(netinfo_ptr);
    if (host_node_ptr->host == netinfo_ptr->myhostname) {
        return;
    }
    struct host_info *h = NULL;
    struct event_info *e = NULL;
    get_host_event(intern(netinfo_ptr->service), host_node_ptr->host, &h, &e);
    if (e) {
        return;
    }
    /* TELL EVERYONE ABOUT IT */
    struct net_info *n = net_info_find(netinfo_ptr->service);
    LIST_FOREACH(e, &n->event_list, net_list_entry) {
        host_node_type *tmph = HOST_NODE_PTR(e);
        if (tmph->got_hello) {
            write_hello(netinfo_ptr, tmph);
        }
    }
    if (h == NULL) {
        h = host_info_new(host_node_ptr);
    }
    e = event_info_new(host_node_ptr, h);
    pmux_connect(-1, 0, e);
}

static void init_base_int(pthread_t *t, struct event_base **bb,
                          void *(*f)(void *), const char *who)
{
    struct net_dispatch_info *info;
    info = calloc(1, sizeof(struct net_dispatch_info));
    struct event_config *cfg = event_config_new();
    event_config_set_flag(cfg, EVENT_BASE_FLAG_EPOLL_USE_CHANGELIST);
    *bb = event_base_new_with_config(cfg);
    event_config_free(cfg);
    info->who = who;
    info->base = *bb;
    Pthread_create(t, NULL, f, info);
    Pthread_detach(*t);
}

static void init_base(void)
{
    init_base_int(&base_thd, &base, net_dispatch_main, "main");
    logmsg(LOGMSG_USER, "Libevent %s with backend method %s\n",
           event_get_version(), event_base_get_method(base));
    if (net_thd_policy == NET_THD_POLICY_GLOBAL) {
        global.akq = akq_new(sizeof(struct user_msg_info), user_msg_func, akq_start_callback, akq_stop_callback);
        timer_thd = global.rdthd = base_thd;
        timer_base = global.rdbase = base;
    } else if (net_thd_policy == NET_THD_POLICY_PER_OP) {
        per_op.akq = akq_new(sizeof(struct user_msg_info), user_msg_func, akq_start_callback, akq_stop_callback);
        init_base_int(&per_op.rdthd,    &per_op.rdbase,    net_dispatch_rd, "read");
    }
    init_base_int(&timer_thd, &timer_base, net_dispatch_timer, "timer");
}

static void init_event_net(netinfo_type *netinfo_ptr)
{
    if (base) {
        return;
    }
    evthread_use_pthreads();
#   ifdef PER_THREAD_MALLOC
    event_set_mem_functions(malloc, realloc, free);
#   endif
    start_stop_callback_data = netinfo_ptr->callback_data;
    start_callback = netinfo_ptr->start_thread_callback;
    stop_callback = netinfo_ptr->stop_thread_callback;
    init_base();
    event_hash = hash_init_str(offsetof(struct event_hash_entry, key));
    net_stop = 0;
}

static void net_send_one(struct event_info *e, struct net_msg *msg)
{
    struct evbuffer *buf = e->wr_buf;
    int rc = 0;
    for (int i = 0; i < msg->num; ++i) {
        struct net_msg_data *data = msg->data[i];
        rc = evbuffer_add_reference(buf, e->wirehdr[WIRE_HEADER_USER_MSG], e->wirehdr_len, NULL, NULL);
        if (rc) break;
        rc = evbuffer_add_reference(buf, &data->msghdr, data->sz, net_msg_data_free, data);
        if (rc) break;
        net_msg_data_add_ref(data);
    }
    if (rc != 0) {
        hprintf("Failed to add ref #msgs:%d rc:%d\n", msg->num, rc);
        reconnect(e);
        return;
    }
}

static void flush_bufferevent(struct event_info *e)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    if (e->fd == -1 || !e->wr_buf) {
        return;
    }
    if (e->wr_pending) {
        return;
    }
    if (evbuffer_write(e->wr_buf, e->fd) >= 0) {
        e->sent_at = time(NULL);
    }
    size_t outstanding = evbuffer_get_length(e->wr_buf);
    if (outstanding) {
        e->wr_pending = 1;
        event_assign(&e->wr_event, base, e->fd, EV_WRITE, flush_when_ready, e);
        event_add(&e->wr_event, NULL);
    }
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    uint64_t max_bytes = netinfo_ptr->max_bytes;
    if (max_bytes) {
       if (outstanding > max_bytes) {
            e->wr_full = 1;
            hprintf0("WRITE BUFFER IS FULL\n");
        } else {
            e->wr_full = 0;
        }
    }
}

static void flush_when_ready(int fd, short what, void *data)
{
    struct event_info *e = data;
    Pthread_mutex_lock(&e->wr_lk);
    if (!e->wr_buf) {
        abort();
    }
    if (fd != e->fd) {
        abort();
    }
    e->wr_pending = 0;
    flush_bufferevent(e);
    Pthread_mutex_unlock(&e->wr_lk);
}

static void do_net_send_all(int dummy_fd, short what, void *data)
{
    check_base_thd();
    struct net_msg *msg = data;
    netinfo_type *netinfo_ptr = msg->netinfo_ptr;
    struct net_info *n = net_info_find(netinfo_ptr->service);
    struct event_info *e;
    int nodrop = msg->flags & NET_SEND_NODROP;
    int nodelay = msg->flags & NET_SEND_NODELAY;
    LIST_FOREACH(e, &n->event_list, net_list_entry) {
        host_node_type *host_node_ptr = HOST_NODE_PTR(e);
        if (strcmp(host_node_ptr->host, gbl_myhostname) == 0 ||
            host_node_ptr->fd == -1 || host_node_ptr->got_hello == 0) {
            continue;
        }
        if (nodrop || e->wr_full == 0) {
            Pthread_mutex_lock(&e->wr_lk);
            if (e->wr_buf) {
                net_send_one(e, msg);
            }
            if (nodelay) {
                flush_bufferevent(e);
            }
            Pthread_mutex_unlock(&e->wr_lk);
        }
    }
    net_msg_free(msg);
}

#if 0
static void add_event(int fd, event_callback_fn func, void *data)
{
    if (net_stop) {
        return;
    }
    struct user_event_info *info = malloc(sizeof(struct user_event_info));
    LIST_INSERT_HEAD(&user_event_list, info, entry);
    info->func = func;
    info->data = data;
    info->ev = event_new(rd_base, fd, EV_READ | EV_PERSIST, user_event_func, info);
    event_add(info->ev, NULL);
}

void add_tcp_event(int fd, event_callback_fn func, void *data)
{
    make_socket_nonblocking(fd);
    add_event(fd, func, data);
}

void add_udp_event(int fd, event_callback_fn func, void *data)
{
    evutil_make_socket_nonblocking(fd);
    add_event(fd, func, data);
}

void add_timer_event(event_callback_fn func, void *data, int ms)
{
    if (net_stop) {
        return;
    }
    struct user_event_info *info = malloc(sizeof(struct user_event_info));
    LIST_INSERT_HEAD(&user_event_list, info, entry);
    info->func = func;
    info->data = data;
    info->ev = event_new(timer_base, -1, EV_PERSIST, user_event_func, info);
    struct timeval t = ms_to_timeval(ms);
    event_add(info->ev, &t);
}
#endif

/* PUBLIC INTERFACE */

void add_host(host_node_type *host_node_ptr)
{
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    int fake = netinfo_ptr->fake;
    if (gbl_create_mode || gbl_fullrecovery || fake || !gbl_libevent) {
        return;
    }
    init_event_net(netinfo_ptr);
    event_once(base, do_add_host, host_node_ptr);
}

void decom(char *host)
{
    if (net_stop) {
        return;
    }
    if (strcmp(host, gbl_myhostname) == 0) {
        return;
    }
    event_once(base, do_decom, host);
}

void stop_event_net(void)
{
    if (net_stop) {
        return;
    }
    Pthread_once(&exit_once, exit_once_func);
}

int write_connect_message_bufferevent(host_node_type *host_node_ptr,
                                      const struct iovec *iov, int n)
{
    if (net_stop) {
        return 0;
    }
    check_base_thd();
    struct event_info *e = host_node_ptr->event_info;
    Pthread_mutex_lock(&e->wr_lk);
    struct evbuffer *buf = e->wr_buf;
    if (buf) {
        for (int i = 0; i < n; ++i) {
            evbuffer_add(buf, iov[i].iov_base, iov[i].iov_len);
        }
    }
    Pthread_mutex_unlock(&e->wr_lk);
    return 0;
}

int write_list_bufferevent(host_node_type *host_node_ptr, int type,
                           const struct iovec *iov, int n, int flags)
{
    if (net_stop) {
        return 0;
    }
    int rc;
    int nodrop = flags & NET_SEND_NODROP;
    int nodelay = flags & NET_SEND_NODELAY;
    struct event_info *e = host_node_ptr->event_info;
    struct evbuffer *buf = evbuffer_new();
    if (buf == NULL) {
        rc = -1;
        goto out;
    }
    rc = evbuffer_add_reference(buf, e->wirehdr[type], e->wirehdr_len, NULL, NULL);
    if (rc) {
        rc = -1;
        goto out;
    }
    for (int i = 0; i < n; ++i) {
        rc = evbuffer_add(buf, iov[i].iov_base, iov[i].iov_len);
        if (rc) {
            rc = -1;
            goto out;
        }
    }
    if (e->wr_full && !nodrop) {
        rc = -2;
        goto out;
    }
    Pthread_mutex_lock(&e->wr_lk);
    if (e->wr_buf) {
        rc = evbuffer_add_buffer(e->wr_buf, buf);
        if (rc != 0) {
            rc = -1;
        } else if (nodelay) {
            flush_bufferevent(e);
        }
    } else {
        rc = -3;
    }
    Pthread_mutex_unlock(&e->wr_lk);
out:if (buf) {
        evbuffer_free(buf);
    }
    if (rc) {
        hprintf("%s failed rc:%d\n", __func__, rc);
    }
    return rc;
}

int net_send_all_bufferevent(netinfo_type *netinfo_ptr, int n, void **buf,
                             int *len, int *type, int *flags)
{
    if (net_stop) {
        return 0;
    }
    struct net_msg *msg = net_msg_new(netinfo_ptr, n, buf, len, type, flags);
    if (msg == NULL) {
        return -1;
    }
    int rc = event_once(base, do_net_send_all, msg);
    if (rc) {
        net_msg_free(msg);
        logmsg(LOGMSG_ERROR, "%s failed rc:%d\n", __func__, rc);
    }
    return rc;
}

int net_flush_bufferevent(host_node_type *host_node_ptr)
{
    if (net_stop) {
        return 0;
    }
    struct event_info *e = host_node_ptr->event_info;
    Pthread_mutex_lock(&e->wr_lk);
    flush_bufferevent(e);
    Pthread_mutex_unlock(&e->wr_lk);
    return 0;
}

int net_send_bufferevent(netinfo_type *netinfo_ptr, const char *host,
                         int usertype, void *data, int datalen, int numtails,
                         void **tails, int *taillens, int flags)
{
    if (net_stop) {
        return 0;
    }
    char key[EVENT_HASH_KEY_SZ];
    make_event_hash_key(key, netinfo_ptr->service, host);
    Pthread_mutex_lock(&event_hash_lk);
    struct event_hash_entry *entry = hash_find(event_hash, key);
    Pthread_mutex_unlock(&event_hash_lk);
    if (!entry) {
        return NET_SEND_FAIL_INVALIDNODE;
    }
    host_node_type *host_node_ptr = HOST_NODE_PTR(entry->event_info);
    if (strcmp(host_node_ptr->host, gbl_myhostname) == 0) {
        return NET_SEND_FAIL_SENDTOME;
    }
    int n = numtails + 1;
    if (data && datalen) {
        ++n;
    }
    struct iovec iov[n], *i = iov;
    int total = 0;
    if (data && datalen) {
        ++i;
        i->iov_base = data;
        i->iov_len = datalen;
        total += datalen;
    }
    for (int t = 0; t < numtails; ++t) {
        ++i;
        i->iov_base = tails[t];
        i->iov_len = taillens[t];
        total += taillens[t];
    }
    Pthread_mutex_lock(&netinfo_ptr->seqlock);
    int seqnum = ++netinfo_ptr->seqnum;
    Pthread_mutex_unlock(&netinfo_ptr->seqlock);
    net_send_message_header hdr, tmp;
    tmp.usertype = usertype;
    tmp.seqnum = seqnum;
    tmp.waitforack = 0;
    tmp.datalen = total;
    net_send_message_header_put(&tmp, (uint8_t *)&hdr, (uint8_t*)(&hdr + 1));
    iov[0].iov_base = &hdr;
    iov[0].iov_len = sizeof(hdr);
    int rc = write_list_bufferevent(host_node_ptr, WIRE_HEADER_USER_MSG, iov, n, flags);
    switch (rc) {
    case  0: return 0;
    case -1: return NET_SEND_FAIL_MALLOC_FAIL;
    case -2: return NET_SEND_FAIL_QUEUE_FULL;
    case -3: return NET_SEND_FAIL_NOSOCK;
    default: return NET_SEND_FAIL_WRITEFAIL;
    }
}
