/*
   Copyright 2019 Bloomberg Finance L.P.

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
#include <signal.h> /////////////// tmp ///////////////
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <event2/util.h>

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
#include <portmuxapi.h>
#include <akq.h>

#define DISTRESS_COUNT 2

#define hprintf_format(a)                                                      \
    LOGMSG_USER, "[%.3s %-8s fd:%3d %18s] " a,                                 \
        host_node_ptr->netinfo_ptr->service, host_node_ptr->host,              \
        host_node_ptr->fd, __func__

#define check_enter_distress()                                                 \
    ({                                                                         \
        int ret;                                                               \
        struct event_info *e = host_node_ptr->event_info;                      \
        if (e == NULL || e->distress < DISTRESS_COUNT) {                       \
            ret = 0;                                                           \
        } else if (e->distressed) {                                            \
            ret = 2;                                                           \
        } else {                                                               \
            e->distressed = 1;                                                 \
            ret = 1;                                                           \
        }                                                                      \
        ret;                                                                   \
    })

#define distress_logmsg(...)                                                   \
    do {                                                                       \
        struct timeval hprintf_now;                                            \
        gettimeofday(&hprintf_now, NULL);                                      \
        int print_distress = check_enter_distress();                           \
        if (print_distress == 2) {                                             \
            break;                                                             \
        }                                                                      \
        logmsg(__VA_ARGS__);                                                   \
        if (print_distress) {                                                  \
            logmsg(hprintf_format("ENTERING DISTRESS MODE\n"));                \
        }                                                                      \
    } while (0)

#define hprintf(a, ...) distress_logmsg(hprintf_format(a), __VA_ARGS__)
#define hprintf0(a) distress_logmsg(hprintf_format(a))
#define HOST_NODE_PTR(x) (x)->host_node_ptr

int gbl_libevent = 0;

extern int gbl_exit;
extern int gbl_create_mode;
extern char *gbl_myhostname;
extern int gbl_pmux_route_enabled;

static int bev_flags = BEV_OPT_DEFER_CALLBACKS | BEV_OPT_THREADSAFE | BEV_OPT_UNLOCK_CALLBACKS;

static pthread_t base_thd;
static struct event_base *base;

static pthread_t timer_thd;
static struct event_base *timer;

static pthread_t rd_thd;
static struct event_base *rd_base;

static pthread_t wr_thd;
static struct event_base *wr_base;

static struct akq *akq; /* Queue used to process user-msgs in single_thread mode */
static int single_thread = 1;

static void *start_stop_callback_data;
static void (*start_callback)(void *);
static void (*stop_callback)(void *);

static pthread_once_t exit_once = PTHREAD_ONCE_INIT;
static pthread_cond_t exit_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t exit_mtx = PTHREAD_MUTEX_INITIALIZER;

struct net_info;
static void connect_host(int, short, void *);
static void event_info_free(struct event_info *);
static struct net_info *net_info_find(const char *);
static struct net_info *net_info_new(netinfo_type *);
static void teardown_heartbeats(struct event_info *);
static void do_teardown_heartbeats(int, short, void *);
static void pmux_close(struct bufferevent *, struct event_info *);
static void shutdown_close(int);
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
    int min = 2;
    int max = 10;
    int s = random() % (max - 1);
    struct timeval t = {min + s, 0};
    return t;
}

static int make_socket_blocking(int fd)
{
    int flags;
    if ((flags = fcntl(fd, F_GETFL, NULL)) < 0) {
        logmsgperror("fcntl:F_GETFL");
        exit(1);
    }
    if (!(flags & O_NONBLOCK)) {
        return 0;
    }
    flags &= ~O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) == -1) {
        logmsgperror("fcntl:F_SETFL");
        exit(1);
    }
    return 0;
}

static int make_socket_nolinger(int fd)
{
    struct linger lg = {.l_onoff = 0, .l_linger = 0};
    if (setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(struct linger))) {
        logmsgperror("setsockopt:SO_LINGER");
        return -1;
    }
    return 0;
}

static int make_socket_nodelay(int fd)
{
    int flag = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag))) {
        logmsgperror("setsockopt:TCP_NODELAY");
        return -1;
    }
    return 0;
}

static int make_socket_keepalive(int fd)
{
    int flag = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &flag, sizeof(flag))) {
        logmsgperror("setsockopt:SO_KEEPALIVE");
        return -1;
    }
    return 0;
}

static int make_socket_nonblocking(int fd)
{
    if (fd == -1) fd = socket(AF_INET, SOCK_STREAM, 0);
    evutil_make_socket_nonblocking(fd);
    make_socket_nolinger(fd);
    make_socket_nodelay(fd);
    make_socket_keepalive(fd);
    return fd;
}

/* Operations on host_list/event_list *must* be dispatched on main base. This
** keeps all such operations single-threaded and avoids race-conditions and
** locking complications present in existing net-lib.
**
** check_base_thd() detects when this is not the case and calls abort() to get
** a core. */

#define check_thd(thd)                                                         \
    if (thd != pthread_self()) {                                               \
        fprintf(stderr, "FATAL ERROR: EVENT NOT DISPATCHED ON " #thd " \n");   \
        abort();                                                               \
    }
#define check_base_thd() check_thd(base_thd)
#define check_timer_thd() check_thd(timer_thd)

struct host_info {
    LIST_HEAD(, event_info) event_list;
    LIST_ENTRY(host_info) entry;
    char *host; /* interned */
    struct akq *akq;
};

struct host_connected_info;
struct event_info {
    LIST_ENTRY(event_info) host_list_entry;
    LIST_ENTRY(event_info) net_list_entry;
    LIST_HEAD(, host_connected_info) host_connected_list;
    char *service;
    int wirehdr_len;
    wire_header_type *wirehdr[WIRE_HEADER_MAX];
    host_node_type *host_node_ptr;
    struct host_info *host_info;
    struct net_info *net_info;
    struct event *hb_check_ev;
    struct event *hb_send_ev;
    struct bufferevent *rd_bev;
    pthread_mutex_t wr_lk;
    struct evbuffer *wr_buf;
    struct bufferevent *wr_bev;
    time_t recv_at;
    time_t sent_at;
    int hdr;
    int state;
    int distress;
    int distressed;
    int skip_drain;
    size_t watermark;
    union {
        net_send_message_header msg;
        net_ack_message_payload_type ack;
    } u;
};

struct net_info {
    struct event *ev;
    struct event *pmux_ev;
    portmux_fd_t *pmux;
    netinfo_type *netinfo_ptr;
    LIST_ENTRY(net_info) entry;
    LIST_HEAD(, event_info) event_list;
};

struct net_dispatch_info {
    struct event_base *base;
    struct host_info *host_info;
    const char *who;
};

struct accept_info {
    int name_len;
    char *host;
    struct sockaddr_in ss;
    connect_message_type c;
    netinfo_type *netinfo_ptr;
};

struct net_msg_data {
    int sz;
    uint32_t ref;
    int flags;
    net_send_message_header msghdr;
    uint8_t buf[0];
};

struct net_msg {
    int num;
    netinfo_type *netinfo_ptr;
    struct net_msg_data *data[0];
};

struct user_msg_info {
    NETFP *func;
    char *name;
    host_node_type *host_node_ptr;
    net_send_message_header msg;
    int len;
    struct evbuffer *payload;
};

static LIST_HEAD(net_list, net_info) net_list = LIST_HEAD_INITIALIZER(net_list);
static LIST_HEAD(host_list, host_info) host_list = LIST_HEAD_INITIALIZER(host_list);

static void host_node_open(host_node_type *host_node_ptr, int fd)
{
    host_node_ptr->fd = fd;
    host_node_ptr->closed = 0;
    host_node_ptr->decom_flag = 0;
    host_node_ptr->distress = 0;
    host_node_ptr->really_closed = 0;
}

static void host_node_close(host_node_type *host_node_ptr)
{
    host_node_ptr->fd = -1;
    host_node_ptr->got_hello = 0;
    host_node_ptr->closed = 1;
    host_node_ptr->distress = 1;
    host_node_ptr->really_closed = 1;
}

static void akq_start_callback(void *dummy)
{
    start_callback(start_stop_callback_data);
}

static void akq_stop_callback(void *dummy)
{
    stop_callback(start_stop_callback_data);
}

static struct host_info *host_info_new(host_node_type *host_node_ptr)
{
    check_base_thd();
    struct host_info *h = calloc(1, sizeof(struct host_info));
    LIST_INSERT_HEAD(&host_list, h, entry);
    LIST_INIT(&h->event_list);
    h->host = host_node_ptr->host;
    h->akq = single_thread ? akq
                           : akq_init(user_msg_info, user_msg_func,
                                      akq_start_callback, akq_stop_callback);
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
    //event_base_free(h->rd_base);
    //event_base_free(h->wr_base);
    logmsg(LOGMSG_INFO, "%s - %s\n", __func__, h->host);
    free(h);
}

static struct host_info *host_info_find(const char *host)
{
    check_base_thd();
    if (!isinterned(host)) {
        abort();
    }
    struct host_info *h;
    LIST_FOREACH(h, &host_list, entry) {
        if (h->host == host) {
            break;
        }
    }
    return h;
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
    e->service = intern(netinfo_ptr->service);
    e->host_node_ptr = host_node_ptr;
    e->host_info = h;
    e->net_info = n;
    Pthread_mutex_init(&e->wr_lk, NULL);
    setup_wire_hdrs(e);
    return e;
}

static void event_info_free(struct event_info *e)
{
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
    if (e->wr_bev) {
        bufferevent_free(e->wr_bev);
    }
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    host_node_close(host_node_ptr);
    rem_from_netinfo(e->host_node_ptr->netinfo_ptr, e->host_node_ptr);
    for (int i = 1; i < WIRE_HEADER_MAX; ++i) {
        printf("********         FREEING WIREHDRs         ***********\n");
        free(e->wirehdr[i]);
    }
    printf("********         FREEING EVENTINFO      ***********\n");
    Pthread_mutex_unlock(&e->wr_lk);
    Pthread_mutex_destroy(&e->wr_lk);
    free(e);
}

static struct net_info *net_info_new(netinfo_type *netinfo_ptr)
{
    check_base_thd();
    struct net_info *n = calloc(1, sizeof(struct net_info));
    n->netinfo_ptr = netinfo_ptr;
    LIST_INSERT_HEAD(&net_list, n, entry);
    LIST_INIT(&n->event_list);
    return n;
}

static void net_info_free(struct net_info *n)
{
    check_base_thd();
    n->netinfo_ptr->myfd = -1;
    LIST_REMOVE(n, entry);
    int fd = event_get_fd(n->ev);
    event_del(n->ev);
    event_free(n->ev);
    shutdown_close(fd);
    if (n->pmux_ev) {
        event_del(n->pmux_ev);
        event_free(n->pmux_ev);
    }
    if (n->pmux) {
        portmux_close(n->pmux);
    }
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

static struct net_msg_data *net_msg_data_new(netinfo_type *netinfo_ptr,
                                             void *buf, int len, int type,
                                             int flags)
{
    struct net_msg_data *data = malloc(sizeof(struct net_msg_data) + len);
    memset(data, 0, sizeof(struct net_msg_data));
    data->sz = NET_SEND_MESSAGE_HEADER_LEN + len;
    /* No ack, no seqnum */
    data->ref = 1;
    data->flags = flags;
    data->msghdr.datalen = htonl(len);
    data->msghdr.usertype = htonl(type);
    memcpy(&data->buf, buf, len);
    return data;
}

static void net_msg_data_free(const void *unused, size_t datalen, void *extra)
{
    struct net_msg_data *data = extra;
    int ref = ATOMIC_ADD32(data->ref, -1);
    if (ref) return;
    free(data);
}

static void net_msg_data_add_ref(struct net_msg_data *data)
{
    ATOMIC_ADD32(data->ref, 1);
}

static struct net_msg *net_msg_new(netinfo_type *netinfo_ptr, int n)
{
    struct net_msg *msg =
        calloc(1, sizeof(struct net_msg) + sizeof(struct net_msg_data *) * n);
    msg->netinfo_ptr = netinfo_ptr;
    msg->num = n;
    return msg;
}

static void net_msg_free(struct net_msg *msg)
{
    for (int i = 0; i < msg->num; ++i) {
        struct net_msg_data *data = msg->data[i];
        net_msg_data_free(&data->msghdr, data->sz, data);
    }
    free(msg);
}

static void shutdown_close(int fd)
{
    if (fd <= 2) {
        raise(SIGINT);
    }
    shutdown(fd, SHUT_RDWR);
    close(fd);
}

static void reconnect_int(struct event_info *e, struct timeval *t)
{
    check_base_thd();
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    host_node_close(host_node_ptr);
    if (netinfo_ptr->hostdown_rtn) {
        netinfo_ptr->hostdown_rtn(netinfo_ptr, host_node_ptr->host);
    }
    hprintf("RECONNECT IN %ds\n", (int)t->tv_sec);
    event_base_once(base, -1, EV_TIMEOUT, connect_host, e, t);
}

struct reconnect_info {
    char *host; //interned string
    char *service; //interned string
};

static void do_reconnect(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct reconnect_info *info = data;
    struct host_info *h = host_info_find(info->host);
    if (h == NULL) return;
    struct timeval t = reconnect_time();
    struct event_info *e;
    LIST_FOREACH(e, &h->event_list, host_list_entry) {
        if (e->service == info->service) {
            reconnect_int(e, &t);
        }
    }
    free(data);
}

static void do_teardown_reconnect(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    teardown_heartbeats(e);
    struct reconnect_info *info = malloc(sizeof(struct reconnect_info));
    info->host = e->host_info->host;
    info->service = e->service;
    event_once(base, do_reconnect, info);
}

static void reconnect(struct event_info *e)
{
    if (gbl_exit) return;
    event_once(timer, do_teardown_reconnect, e);
}

static void get_host_event(host_node_type *host_node_ptr, struct host_info **hi,
                           struct event_info **ei)
{
    check_base_thd();
    *hi = NULL;
    *ei = NULL;
    struct host_info *h = host_info_find(host_node_ptr->host);
    if (h == NULL) return;
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    struct event_info *e;
    LIST_FOREACH(e, &h->event_list, host_list_entry) {
        if (strcmp(netinfo_ptr->service, e->service) == 0) {
            break;
        }
    }
    *hi = h;
    *ei = e;
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

static void do_send_decom_all(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    struct net_info *n = net_info_find(netinfo_ptr->service);
    struct event_info *to;
    LIST_FOREACH(to, &n->event_list, net_list_entry) {
        if (to->host_node_ptr == host_node_ptr) continue;
        write_decom(netinfo_ptr, to->host_node_ptr, host_node_ptr->host,
                    host_node_ptr->hostname_len, to->host_node_ptr->host);
    }
    hprintf0("DECOMMED\n");
}

static void do_decom_wr(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    Pthread_mutex_lock(&e->wr_lk);
    if (e->wr_buf) {
        evbuffer_free(e->wr_buf);
        e->wr_buf = NULL;
    }
    if (e->wr_bev) {
        int fd = bufferevent_getfd(e->wr_bev);
        bufferevent_free(e->wr_bev);
        e->wr_bev = NULL;
        hprintf("CLOSING fd:%d\n", fd);
        shutdown_close(fd);
    }
    Pthread_mutex_unlock(&e->wr_lk);

    /* TELL EVERYONE ABOUT IT */
    event_once(base, do_send_decom_all, e);
}

static void do_decom_rd(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    if (!single_thread) {
        if (e->rd_bev) {
            bufferevent_free(e->rd_bev);
            e->rd_bev = NULL;
        }
    }
    event_once(wr_base, do_decom_wr, e);
}

static void do_decom_heartbeat(int dummyfd, short what, void *data)
{
    check_timer_thd();
    struct event_info *e = data;
    teardown_heartbeats(e);
    event_once(rd_base, do_decom_rd, e);
}

static void do_decom(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct host_info *h = host_info_find(data);
    if (h == NULL) return;
    struct event_info *e;
    LIST_FOREACH(e, &h->event_list, host_list_entry) {
        host_node_type *host_node_ptr = HOST_NODE_PTR(e);
        if (host_node_ptr->decom_flag) {
            continue;
        } else {
            hprintf0("SETTING DECOM FLAG\n");
            host_node_ptr->decom_flag = 1;
            host_node_close(host_node_ptr);
            event_once(timer, do_decom_heartbeat, e);
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

static void user_msg_func(void *work)
{
    struct user_msg_info *info = work;
    void *payload = evbuffer_pullup(info->payload, info->len);
    user_msg_func_int(info->func, info->host_node_ptr, &info->msg, payload);
    evbuffer_free(info->payload);
    free(info);
}

static void user_msg(struct event_info *e, net_send_message_header *msg,
                     uint8_t *payload)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    NETFP *func = netinfo_ptr->userfuncs[msg->usertype].func;
    char *name = netinfo_ptr->userfuncs[msg->usertype].name;
    if (func == NULL) {
        /* Startup race without accept-on-child-net: Replication net is
         * ready and accepts offload connection but offload callbacks have not
         * registered yet. */
        reconnect(e);
        return;
    }
    if (is_offload_netinfo(netinfo_ptr)) {
        user_msg_func_int(func, host_node_ptr, msg, payload);
        return;
    }
    struct user_msg_info *info = akq_work_new(e->host_info->akq);
    info->func = func;
    info->name = name;
    info->host_node_ptr = host_node_ptr;
    info->msg = *msg;
    info->len = e->watermark;
    info->payload = evbuffer_new();
    struct evbuffer *input = bufferevent_get_input(e->rd_bev);
    evbuffer_remove_buffer(input, info->payload, info->len);
    e->skip_drain = 1;
    akq_enqueue(e->host_info->akq, info);
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
    stop_a_base(timer);
}

static void stop_rd_base()
{
    stop_a_base(rd_base);
}

static void stop_wr_base()
{
    stop_a_base(wr_base);
}

static void stop_user_msg_qs()
{
    if (single_thread) {
        akq_stop(akq);
        return;
    }
    struct host_info *h;
    LIST_FOREACH(h, &host_list, entry) {
        akq_stop(h->akq);
        printf("Stopped user_msg_q for %s\n", h->host);
    }
}

static int nomo = 0;
static void exit_once_func(void)
{
    nomo = 1;
    stop_main_base();
    if (!single_thread) {
        stop_timer_base();
        stop_rd_base();
        stop_wr_base();
    }

    stop_user_msg_qs();

    event_base_free(base);
    if (!single_thread) {
        event_base_free(timer);
        event_base_free(rd_base);
        event_base_free(wr_base);
    }

    Pthread_cond_destroy(&exit_cond);
    Pthread_mutex_destroy(&exit_mtx);
}


static int net_flush_bufferevent_int(struct event_info *e)
{
    return evbuffer_add_buffer(bufferevent_get_output(e->wr_bev), e->wr_buf);
}

static int flush_buffer(struct event_info *e)
{
    if (e->wr_buf) {
        if (evbuffer_get_length(e->wr_buf) != 0) {
            if (net_flush_bufferevent_int(e) == 0) {
                return 1; // had data in buffer and flushed it
            }
        }
    }
    return 0; // did not flush buffer
}

static void heartbeat_send(int dummyfd, short what, void *data)
{
    check_timer_thd();
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    time_t now = time(NULL);
    time_t last = e->sent_at;
    double diff = difftime(now, last);
    /* TODO FIXME XXX: re-register with pmux? */
    if (diff < netinfo_ptr->heartbeat_send_time) {
        return;
    }
    Pthread_mutex_lock(&e->wr_lk);
    int flushed = flush_buffer(e);
    Pthread_mutex_unlock(&e->wr_lk);
    if (flushed) {
        return;
    }
    int rc = write_heartbeat(netinfo_ptr, host_node_ptr);
    if (rc == 0) {
        return;
    }
    hprintf("write rc:%d after %d seconds, killing session\n", rc, (int)(now - last));
    teardown_heartbeats(e);
    reconnect(e);
}

static void heartbeat_check(int dummyfd, short what, void *data)
{
    check_timer_thd();
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    time_t now = time(NULL);
    time_t last = e->recv_at;
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    if ((now - last) > netinfo_ptr->heartbeat_check_time) {
        hprintf("no data in %d seconds, killing session\n", (int)(now - last));
        teardown_heartbeats(e);
        reconnect(e);
    }
}

static const char *hdr2str(int hdr)
{
    switch (hdr) {
    case WIRE_HEADER_HEARTBEAT:     return "WIRE_HEADER_HEARTBEAT";
    case WIRE_HEADER_HELLO:         return "WIRE_HEADER_HELLO";
    case WIRE_HEADER_DECOM:         return "WIRE_HEADER_DECOM";
    case WIRE_HEADER_USER_MSG:      return "WIRE_HEADER_USER_MSG";
    case WIRE_HEADER_ACK:           return "WIRE_HEADER_ACK";
    case WIRE_HEADER_HELLO_REPLY:   return "WIRE_HEADER_HELLO_REPLY";
    case WIRE_HEADER_DECOM_NAME:    return "WIRE_HEADER_DECOM_NAME";
    case WIRE_HEADER_ACK_PAYLOAD:   return "WIRE_HEADER_ACK_PAYLOAD";
    default:                        return "???";
    }
}

static void message_done(struct event_info *e)
{
    e->hdr = 0;
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
    char * long_hosts = (char *)(nodes + n);
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
    int distress = e->distress;
    e->distress = e->distressed = 0;
    if (distress >= DISTRESS_COUNT) {
        hprintf("LEAVING DISTRESS MODE (retries:%d)\n", distress);
    }
    if (!host_node_ptr->got_hello) {
        hprintf0("GOT HELLO\n");
    }
    host_node_ptr->got_hello = 1;
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
    net_send_message_header *msg = &e->u.msg;
    if (e->state == 0) {
        net_send_message_header_get(msg, payload, payload + NET_SEND_MESSAGE_HEADER_LEN);
        if (msg->datalen) {
            e->watermark = msg->datalen;
            return;
        }
    }
    user_msg(e, msg, payload);
    message_done(e);
}

static void process_ack(struct event_info *e, uint8_t *payload)
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
        Pthread_cond_broadcast(&host_node_ptr->ack_wakeup);
    }
    Pthread_mutex_unlock(&host_node_ptr->wait_mutex);
    message_done(e);
}

static void process_ack_payload(struct event_info *e, uint8_t *payload)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    net_ack_message_payload_type *ack = &e->u.ack;
    if (e->state == 0) {
        net_ack_message_payload_type_get(ack, payload, payload + NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN);
        e->watermark = ack->paylen;
        return;
    }
    Pthread_mutex_lock(&host_node_ptr->wait_mutex);
    seq_data *ptr = host_node_ptr->wait_list;
    while (ptr != NULL && (ptr->seqnum != ack->seqnum)) {
        ptr = ptr->next;
    }
    if (ptr) {
        ptr->ack = 1;
        ptr->outrc = ack->outrc;
        ptr->payload = payload;
        ptr->payloadlen = ack->paylen;
        Pthread_cond_broadcast(&host_node_ptr->ack_wakeup);
    }
    Pthread_mutex_unlock(&host_node_ptr->wait_mutex);
}

static void process_decom_node(struct event_info *e, uint8_t *payload)
{
    uint32_t n;
    memcpy(&n, payload, sizeof(uint32_t));
    n = htonl(n);
    message_done(e);
    decom(hostname(n));
}

static void process_decom_name(struct event_info *e, uint8_t *payload)
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
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    wire_header_type *hdr = (wire_header_type *)buf;
    int type = htonl(hdr->type);
    e->state = 0;
    e->hdr = type;
    switch (type) {
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
        hprintf("UNKNOWN HDR:%d\n", type);
        return -1;
    }
    return 0;
}

static int process_payload(struct event_info *e, uint8_t *payload)
{
    int hdr = e->hdr;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    switch (hdr) {
    case WIRE_HEADER_HELLO:
        process_hello_msg(e, payload);
        break;
    case WIRE_HEADER_DECOM:
        process_decom_node(e, payload);
        break;
    case WIRE_HEADER_USER_MSG:
        process_user_msg(e, payload);
        break;
    case WIRE_HEADER_ACK:
        process_ack(e, payload);
        break;
    case WIRE_HEADER_HELLO_REPLY:
        process_hello_msg_reply(e, payload);
        break;
    case WIRE_HEADER_DECOM_NAME:
        process_decom_name(e, payload);
        break;
    case WIRE_HEADER_ACK_PAYLOAD:
        process_ack_payload(e, payload);
        break;
    default:
        hprintf("UNKNOWN HDR:%d\n", hdr);
        return -1;
    }
    ++e->state;
    return 0;
}

static void readcb(struct bufferevent *bev, void *data)
{
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    struct evbuffer *input = bufferevent_get_input(bev);
    size_t need = e->watermark;
    uint8_t *buf;
    int rc = 0;
    while ((buf = evbuffer_pullup(input, need)) != NULL) {
        if (e->rd_bev != bev) {
            abort();
        }
        e->recv_at = time(NULL);
        if (e->hdr == 0) {
            rc = process_hdr(e, buf);
        } else {
            rc = process_payload(e, buf);
        }
        if (rc) {
            hprintf0("******************* FORCE RECONNECT\n");
            bufferevent_disable(bev, EV_READ);
            reconnect(e);
            return;
        }
        if (e->skip_drain) {
            e->skip_drain = 0;
        } else {
            evbuffer_drain(input, need);
        }
        need = e->watermark;
    }
    bufferevent_setwatermark(bev, EV_READ, need, 0);
}

static void do_readcb(int fd, short what, void *data)
{
    struct event_info *e = data;
    readcb(e->rd_bev, e);
}

static void writecb(struct bufferevent *bev, void *data)
{
    struct event_info *e = data;
    e->sent_at = time(NULL);
}

static void eventcb(struct bufferevent *bev, short events, void *data)
{
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    hprintf("fd:%3d RD:%d WR:%d EOF:%d ERR:%d T'OUT:%d CON'D:%d\n",
            bufferevent_getfd(bev), !!(events & BEV_EVENT_READING),
            !!(events & BEV_EVENT_WRITING), !!(events & BEV_EVENT_EOF),
            !!(events & BEV_EVENT_ERROR), !!(events & BEV_EVENT_TIMEOUT),
            !!(events & BEV_EVENT_CONNECTED));
    if (events & BEV_EVENT_ERROR) {
        int err = evutil_socket_geterror(bufferevent_getfd(bev));
        hprintf("[errno:%d]-[strerror:%s]\n", err, strerror(err));
    }
    bufferevent_disable(bev, EV_READ | EV_WRITE);
    if (e->wr_bev == bev || e->rd_bev == bev) {
        reconnect(e);
    }
}

static void setup_heartbeats(struct event_info *e)
{
    check_timer_thd();
    e->recv_at = e->sent_at = time(NULL);
    struct timeval one = {1, 0};
    if (e->hb_send_ev == NULL) {
        e->hb_send_ev = event_new(timer, -1, EV_PERSIST, heartbeat_send, e);
        event_add(e->hb_send_ev, &one);
    }
    if (e->hb_check_ev == NULL) {
        e->hb_check_ev = event_new(timer, -1, EV_PERSIST, heartbeat_check, e);
        event_add(e->hb_check_ev, &one);
    }
}

static void do_setup_heartbeats(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    setup_heartbeats(e);
}

static void teardown_heartbeats(struct event_info *e)
{
    check_timer_thd();
    if (e->hb_check_ev) {
        event_free(e->hb_check_ev);
        e->hb_check_ev = NULL;
    }
    if (e->hb_send_ev) {
        event_free(e->hb_send_ev);
        e->hb_send_ev = NULL;
    }
}

struct host_connected_info {
    LIST_ENTRY(host_connected_info) entry;
    int oldfd;
    int newfd;
    struct bufferevent *bev;
    struct event_info *e;
    int connect_msg;
};

static void create_rd_bev(struct event_info *e, int fd)
{
    if (e->rd_bev) {
        bufferevent_free(e->rd_bev);
    }
    e->rd_bev = bufferevent_socket_new(rd_base, fd, bev_flags);
    bufferevent_disable(e->rd_bev, EV_READ | EV_WRITE);
    bufferevent_setwatermark(e->rd_bev, EV_READ, e->wirehdr_len, 0);
    bufferevent_setcb(e->rd_bev, readcb, NULL, eventcb, e);
}

static void create_wr_bev(struct event_info *e, int fd)
{
    if (e->wr_buf) {
        evbuffer_free(e->wr_buf);
    }
    if (e->wr_bev) {
        bufferevent_free(e->wr_bev);
    }
    e->wr_buf = evbuffer_new();
    e->wr_bev = bufferevent_socket_new(wr_base, fd, bev_flags);
    bufferevent_disable(e->wr_bev, EV_READ | EV_WRITE);
    bufferevent_setcb(e->wr_bev, NULL, writecb, NULL, e);
}

static void create_rw_bev(struct event_info *e, struct host_connected_info *info)
{
    if (e->wr_buf) {
        evbuffer_free(e->wr_buf);
    }
    if (e->wr_bev) {
        bufferevent_free(e->wr_bev);
    }
    e->wr_buf = evbuffer_new();
    e->rd_bev = e->wr_bev = info->bev;
    info->bev = NULL;
    bufferevent_disable(e->wr_bev, EV_READ | EV_WRITE);
    bufferevent_setwatermark(e->rd_bev, EV_READ, e->wirehdr_len, 0);
    bufferevent_setcb(e->wr_bev, readcb, writecb, eventcb, e);
}

static void dispatch_host(struct host_info *h,
                          struct event_base *dispatch_base, void*(*func)(void*))
{
    struct net_dispatch_info *n = calloc(1, sizeof(struct net_dispatch_info));
    n->host_info = h;
    n->base = dispatch_base;
    pthread_t thd;
    Pthread_create(&thd, NULL, func, n);
    Pthread_detach(thd);
}

static struct host_connected_info *
host_connected_info_new(struct event_info *e, struct bufferevent *bev,
                        int connect_msg)
{
    struct host_connected_info *info =
        calloc(1, sizeof(struct host_connected_info));
    info->e = e;
    info->bev = bev;
    info->oldfd = e->rd_bev ? bufferevent_getfd(e->rd_bev) : -1;
    info->newfd = bufferevent_getfd(bev);
    info->connect_msg = connect_msg;
    LIST_INSERT_HEAD(&e->host_connected_list, info, entry);
    return info;
}

static void host_connected_info_free(struct host_connected_info *info)
{
    LIST_REMOVE(info, entry);
    free(info);
}

static void finish_host_setup(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct host_connected_info *info = data;
    struct event_info *e = info->e;
    int connect_msg = info->connect_msg;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    if (info->oldfd != -1) {
        shutdown_close(info->oldfd);
    }
    message_done(e);
    if (info->bev) {
        bufferevent_free(info->bev);
    }
    bufferevent_enable(e->rd_bev, EV_READ);
    bufferevent_enable(e->wr_bev, EV_WRITE);
    int newfd = bufferevent_getfd(e->rd_bev);
    Pthread_mutex_lock(&e->wr_lk);
    host_node_open(host_node_ptr, newfd);
    if (connect_msg) {
        netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
        write_connect_message(netinfo_ptr, host_node_ptr, NULL);
        Pthread_mutex_unlock(&e->wr_lk);
        write_hello(netinfo_ptr, host_node_ptr);
    } else {
        Pthread_mutex_unlock(&e->wr_lk);
        event_once(rd_base, do_readcb, e);
    }
    event_once(timer, do_setup_heartbeats, e);
    host_connected_info_free(info);
    if (!LIST_EMPTY(&e->host_connected_list)) {
        info = LIST_FIRST(&e->host_connected_list);
        LIST_REMOVE(info, entry);
        event_once(timer, do_teardown_heartbeats, info);
    }
}

static void setup_rd_bev(int dummyfd, short what, void *data)
{
    struct host_connected_info *info = data;
    struct event_info *e = info->e;
    create_rd_bev(e, info->newfd);
    evbuffer_prepend_buffer(bufferevent_get_input(e->rd_bev),
                            bufferevent_get_input(info->bev));
    event_once(base, finish_host_setup, info);
}

static void setup_wr_bev(int dummyfd, short what, void *data)
{
    struct host_connected_info *info = data;
    struct event_info *e = info->e;
    Pthread_mutex_lock(&e->wr_lk);
    create_wr_bev(e, info->newfd);
    Pthread_mutex_unlock(&e->wr_lk);
    event_once(rd_base, setup_rd_bev, info);
}

static void setup_rw_bev(int dummyfd, short what, void *data)
{
    struct host_connected_info *info = data;
    struct event_info *e = info->e;
    Pthread_mutex_lock(&e->wr_lk);
    create_rw_bev(e, info);
    Pthread_mutex_unlock(&e->wr_lk);
    event_once(base, finish_host_setup, info);
}

static void do_host_setup(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct host_connected_info *info = data;
    struct event_info *e = info->e;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    host_node_close(host_node_ptr);
    if (single_thread) {
        event_once(base, setup_rw_bev, info);
    } else {
        event_once(wr_base, setup_wr_bev, info);
    }
}

static void do_teardown_heartbeats(int dummyfd, short what, void *data)
{
    struct host_connected_info *info = data;
    struct event_info *e = info->e;
    teardown_heartbeats(e);
    event_once(base, do_host_setup, info);
}


static void host_connected(struct event_info *e, struct bufferevent *bev,
                           int connect_msg)
{
    check_base_thd();
    int dispatch = LIST_EMPTY(&e->host_connected_list) ? 1 : 0;
    struct host_connected_info *info = host_connected_info_new(e, bev, connect_msg);
    if (dispatch) {
        event_once(timer, do_teardown_heartbeats, info);
    }
}

static void connectcb(struct bufferevent *bev, short events, void *data)
{
    check_base_thd();
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    if (events & BEV_EVENT_CONNECTED) {
        struct host_connected_info *info = LIST_FIRST(&e->host_connected_list);
        if (info || host_node_ptr->fd != -1) {
            int fd = bufferevent_getfd(bev);
            hprintf("HAD CONNECTION closing fd:%3d\n", fd);
            bufferevent_free(bev);
            shutdown_close(fd);
        } else {
            hprintf("MADE NEW CONNECTION fd:%3d\n", bufferevent_getfd(bev));
            host_connected(e, bev, 1);
        }
        return;
    }
    hprintf("fd:%3d RD:%d WR:%d EOF:%d ERR:%d T'OUT:%d CON'D:%d\n",
            bufferevent_getfd(bev), !!(events & BEV_EVENT_READING),
            !!(events & BEV_EVENT_WRITING), !!(events & BEV_EVENT_EOF),
            !!(events & BEV_EVENT_ERROR), !!(events & BEV_EVENT_TIMEOUT),
            !!(events & BEV_EVENT_CONNECTED));
    close(bufferevent_getfd(bev));
    bufferevent_free(bev);
    struct timeval t = reconnect_time();
    reconnect_int(e, &t);
}

static void do_connect(struct event_info *e)
{
    check_base_thd();
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    struct host_connected_info *info = LIST_FIRST(&e->host_connected_list);
    if (info) {
        return;
    }
    if (host_node_ptr->fd != -1) {
        return;
    }
    if (host_node_ptr->decom_flag) {
        return;
    }
    struct sockaddr_in sin = {0};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(host_node_ptr->port);
    if (get_dedicated_conhost(host_node_ptr, &host_node_ptr->addr)) {
        struct timeval t = reconnect_time();
        reconnect_int(e, &t);
        return;
    }
    sin.sin_addr = host_node_ptr->addr;
    int fd = make_socket_nonblocking(-1);
    struct bufferevent *bev = bufferevent_socket_new(base, fd, bev_flags);
    bufferevent_setcb(bev, NULL, NULL, connectcb, e);
    bufferevent_disable(bev, EV_READ);
    bufferevent_enable(bev, EV_WRITE);
    bufferevent_socket_connect(bev, (struct sockaddr *)&sin, sizeof(sin));
}

static netinfo_type *pmux_netinfo(host_node_type *host_node_ptr)
{
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    if (netinfo_ptr->ischild && !netinfo_ptr->accept_on_child) {
        netinfo_ptr = netinfo_ptr->parent;
    }
    return netinfo_ptr;
}

static void process_pmux_rte(struct event_info *e, char *res, struct bufferevent *bev)
{
    int rc = -1;
    if (sscanf(res, "%d", &rc) == 1 && rc == 0) {
        connectcb(bev, BEV_EVENT_CONNECTED, e);
        return;
    }
    pmux_close(bev, e);
}

static void process_pmux_get(struct event_info *e, char *res)
{
    int port;
    char *app = NULL, *svc = NULL, *ins = NULL;
    if (sscanf(res, "%d %m[^/]/%m[^/]/%ms", &port, &app, &svc, &ins) == 4) {
        host_node_type *host_node_ptr = HOST_NODE_PTR(e);
        netinfo_type *netinfo_ptr = pmux_netinfo(host_node_ptr);
        if (strcmp(netinfo_ptr->app, app) == 0 &&
            strcmp(netinfo_ptr->service, svc) == 0 &&
            strcmp(netinfo_ptr->instance, ins) == 0 &&
            port >= 0 && port <= USHRT_MAX
        ){
            host_node_ptr->port = port;
            do_connect(e);
        }
    }
    os_free(app);
    os_free(svc);
    os_free(ins);
}

static void pmux_req(struct bufferevent *bev, host_node_type *host_node_ptr)
{
    netinfo_type *netinfo_ptr = pmux_netinfo(host_node_ptr);
    evbuffer_add_printf(bufferevent_get_output(bev), "%s %s/%s/%s\n",
                        gbl_pmux_route_enabled ? "rte" : "get /echo",
                        netinfo_ptr->app, netinfo_ptr->service,
                        netinfo_ptr->instance);
}

static void pmux_close(struct bufferevent *bev, struct event_info *e)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    int fd = bufferevent_getfd(bev);
    bufferevent_free(bev);
    shutdown_close(fd);
    if (host_node_ptr->port == 0) {
        reconnect(e);
    }
}

static void pmux_readcb(struct bufferevent *bev, void *data)
{
    struct event_info *e = data;
    size_t len;
    struct evbuffer *input = bufferevent_get_input(bev);
    char *res = evbuffer_readln(input, &len, EVBUFFER_EOL_ANY);
    if (!res) return;
    if (gbl_pmux_route_enabled) {
        process_pmux_rte(e, res, bev);
    } else {
        process_pmux_get(e, res);
        pmux_close(bev, e);
    }
    free(res);
}

static void pmux_eventcb(struct bufferevent *bev, short events, void *data)
{
    struct event_info *e = data;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    if (events & BEV_EVENT_CONNECTED) {
        pmux_req(bev, host_node_ptr);
        return;
    }
    hprintf("fd:%3d RD:%d WR:%d EOF:%d ERR:%d T'OUT:%d CON'D:%d\n",
            bufferevent_getfd(bev), !!(events & BEV_EVENT_READING),
            !!(events & BEV_EVENT_WRITING), !!(events & BEV_EVENT_EOF),
            !!(events & BEV_EVENT_ERROR), !!(events & BEV_EVENT_TIMEOUT),
            !!(events & BEV_EVENT_CONNECTED));
    pmux_close(bev, e);
}

static void do_pmux(struct event_info *e)
{
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    if (get_dedicated_conhost(host_node_ptr, &host_node_ptr->addr)) {
        reconnect(e);
        return;
    }
    struct sockaddr_in sin = {0};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(get_portmux_port());
    sin.sin_addr = host_node_ptr->addr;
    int fd = make_socket_nonblocking(-1);
    struct bufferevent *bev = bufferevent_socket_new(base, fd, bev_flags);
    bufferevent_setcb(bev, pmux_readcb, NULL, pmux_eventcb, e);
    bufferevent_enable(bev, EV_READ | EV_WRITE);
    bufferevent_socket_connect(bev, (struct sockaddr *)&sin, sizeof(sin));
}

static void connect_host(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct event_info *e = data;
    ++e->distress;
    do_pmux(e);
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

/*
static struct timeval get_rd_timeout(netinfo_type *netinfo_ptr)
{
    int ms = netinfo_ptr->netpoll > 0 ? netinfo_ptr->netpoll : 100;
    return ms_to_timeval(ms);
}
*/

static int get_connect_msg(struct bufferevent *bev, struct accept_info *a)
{
    uint8_t *buf;
    struct evbuffer *input = bufferevent_get_input(bev);
    if (a->name_len) {
        buf = evbuffer_pullup(input, a->name_len);
        if (buf == NULL) return -1;
        if (a->host == NULL) {
            a->host = intern((char *)buf);
        }
        evbuffer_drain(input, a->name_len);
        return 0;
    }
    buf = evbuffer_pullup(input, NET_CONNECT_MESSAGE_TYPE_LEN);
    if (buf == NULL) return -1;
    net_connect_message_get(&a->c, buf, buf + NET_CONNECT_MESSAGE_TYPE_LEN);
    evbuffer_drain(input, NET_CONNECT_MESSAGE_TYPE_LEN);
    if (a->c.my_hostname[0] == '.') {
        a->name_len += atoi(a->c.my_hostname + 1);
    } else {
        a->host = intern(a->c.my_hostname);
    }
    if (a->c.to_hostname[0] == '.') {
        a->name_len += atoi(a->c.to_hostname + 1);
    }
    if (a->name_len == 0) return 0;
    buf = evbuffer_pullup(input, a->name_len);
    if (buf == NULL) {
        bufferevent_setwatermark(bev, EV_READ, a->name_len, 0);
        return 1;
    }
    if (a->host == NULL) {
        a->host = intern((char *)buf);
    }
    evbuffer_drain(input, a->name_len);
    return 0;
}

static int connect_readcb_int(struct bufferevent *bev, struct accept_info *a)
{
    int rc = get_connect_msg(bev, a);
    if (rc != 0) return rc;
    char *host = a->host;
    int port = a->c.my_portnum;
    int netnum = port >> 16;
    port &= 0xffff;
    netinfo_type *netinfo_ptr = NULL;
    if (netnum == 0) {
        netinfo_ptr = a->netinfo_ptr;
    } else if (netnum > 0 && netnum < a->netinfo_ptr->num_child_nets){
        netinfo_ptr = a->netinfo_ptr->child_nets[netnum];
    }
    if (netinfo_ptr == NULL) {
        logmsg(LOGMSG_ERROR, "connection from node:%d host:%s netnum:%d (%d) failed\n",
               a->c.my_nodenum, host, netnum, a->netinfo_ptr->netnum);
        return -1;
    }
    if (netinfo_ptr->allow_rtn && !netinfo_ptr->allow_rtn(netinfo_ptr, host)) {
        logmsg(LOGMSG_ERROR, "connection from node:%d host:%s not allowed\n",
               a->c.my_nodenum, host);
        return -2;
    }
    host_node_type *host_node_ptr = get_host_node_by_name_ll(netinfo_ptr, host);
    if (host_node_ptr == NULL) {
        host_node_ptr = add_to_netinfo(netinfo_ptr, host, port);
    }
    if (host_node_ptr == NULL) {
        return -3;
    }
    host_node_ptr->port = port;
    host_node_ptr->addr = a->ss.sin_addr;
    if (netinfo_ptr->new_node_rtn) {
        netinfo_ptr->new_node_rtn(netinfo_ptr, host, port);
    }
    hprintf("ACCEPTED NEW CONNECTION fd:%3d\n", bufferevent_getfd(bev));
    struct host_info *h = NULL;
    struct event_info *e = NULL;
    get_host_event(host_node_ptr, &h, &e);
    if (h == NULL) {
        h = host_info_new(host_node_ptr);
    }
    if (e == NULL) {
        e = event_info_new(host_node_ptr, h);
    } else {
        update_wire_hdrs(e);
    }
    bufferevent_disable(bev, EV_READ | EV_WRITE);
    host_connected(e, bev, 0);
    return 0;
}

static void connect_readcb(struct bufferevent *bev, void *data)
{
    int rc = connect_readcb_int(bev, data);
    if (rc == 1) return; /* Need more data for long host names */
    free(data);
    if (rc < 0) bufferevent_free(bev);
}

static void connect_eventcb(struct bufferevent *bev, short events,
                                void *data)
{
    logmsg(LOGMSG_INFO,
           "%s: RD:%d WR:%d EOF:%d ERR:%d T'OUT:%d CON'D:%d\n", __func__,
           !!(events & BEV_EVENT_READING), !!(events & BEV_EVENT_WRITING),
           !!(events & BEV_EVENT_EOF), !!(events & BEV_EVENT_ERROR),
           !!(events & BEV_EVENT_TIMEOUT), !!(events & BEV_EVENT_CONNECTED));
    free(data);
    close(bufferevent_getfd(bev));
    bufferevent_free(bev);
}

static void read_connect(int fd, struct accept_info *a)
{
    struct bufferevent *bev = bufferevent_socket_new(base, fd, bev_flags);
    bufferevent_setwatermark(bev, EV_READ, NET_CONNECT_MESSAGE_TYPE_LEN, 0);
    bufferevent_enable(bev, EV_READ);
    bufferevent_disable(bev, EV_WRITE);
    bufferevent_setcb(bev, connect_readcb, NULL, connect_eventcb, a);
    return;
}

#if 0
static void handle_timeout(int fd, struct accept_info *a)
{
    char ip[64];
    char host[HOST_NAME_MAX];
    struct sockaddr_in saddr;
    struct sockaddr *addr = (struct sockaddr *)&saddr;
    socklen_t addrlen = sizeof(saddr);
    getpeername(fd, addr, &addrlen);
    inet_ntop(saddr.sin_family, &saddr.sin_addr, ip, sizeof(ip));
    getnameinfo(addr, addrlen, host, sizeof(host), NULL, 0, 0);
    logmsg(LOGMSG_ERROR,
           "%s: timeout reading from socket, peeraddr=%s host:%s\n", __func__,
           ip, host);
}
#endif

static void do_read(int fd, short what, void *data)
{
    check_base_thd();
    struct accept_info *a = data;
#   if 0
    if (what & EV_TIMEOUT) {
        handle_timeout(fd, a);
        shutdown_close(fd);
        free(a);
        return;
    }
#   endif
    uint8_t b;
    ssize_t n = recv(fd, &b, 1, 0);
    if (n == 0) {
        return;
    } else if (n == -1) {
        char buf[512];
        char ip[128];
        char host[HOST_NAME_MAX];
        inet_ntop(a->ss.sin_family, &a->ss.sin_addr, ip, sizeof(ip));
        getnameinfo((struct sockaddr *)&a->ss, sizeof(a->ss), host,
                    sizeof(host), NULL, 0, 0);
        fprintf(stderr, "%s: recv rc:%zd from fd:%3d [%s] [%s:%s]\n", __func__,
                n, fd, strerror_r(errno, buf, sizeof(buf)), ip, host);
        shutdown_close(fd);
        free(a);
        return;
    }
    if (b == 0) { /* replication, offload etc. */
        read_connect(fd, a);
    } else {
        make_socket_blocking(fd);
        SBUF2 *sb = sbuf2open(fd, 0);
        do_appsock(a->netinfo_ptr, &a->ss, sb, b);
        free(a);
    }
}

static void do_accept_int(int fd, struct net_info *n, struct sockaddr_in *addr)
{
    netinfo_type *netinfo_ptr = n->netinfo_ptr;
    netinfo_ptr->num_accepts++;
    /* TODO FIXME XXX */
    //set_socket_bufsz(fd);
    make_socket_nonblocking(fd);
    struct accept_info *a = calloc(1, sizeof(struct accept_info));
    a->netinfo_ptr = n->netinfo_ptr;
    a->ss = *addr;
    event_base_once(base, fd, EV_READ, do_read, a, NULL);
}

static void do_pmux_accept(int accept_fd, short what, void *data)
{
    /* TODO FIXME XXX: THIS IS sUPER KLUDGY AND WILL BLOCK - just for now use like this*/
    struct net_info *n = data;
    int fd = portmux_accept(n->pmux, -1);
    if (fd < 0) {
        return;
    }
    struct sockaddr_in saddr;
    struct sockaddr *addr = (struct sockaddr *)&saddr;
    socklen_t addrlen = sizeof(saddr);
    getpeername(fd, addr, &addrlen);
    do_accept_int(fd, n, &saddr);
}

static void do_accept(int accept_fd, short what, void *data)
{
    struct sockaddr_in saddr;
    struct sockaddr *addr = (struct sockaddr *)&saddr;
    socklen_t addrlen = sizeof(saddr);
    int fd = accept(accept_fd, addr, &addrlen);
    if (fd < 0) {
        return;
    }
    do_accept_int(fd, data, &saddr);
}

static void net_accept(netinfo_type *netinfo_ptr)
{
    check_base_thd();
    struct net_info *n = net_info_find(netinfo_ptr->service);
    if (n) return;
    n = net_info_new(netinfo_ptr);
    if (netinfo_ptr->ischild && !netinfo_ptr->accept_on_child) {
        return;
    }
    if (gbl_pmux_route_enabled) {
        char *app = netinfo_ptr->app;
        char *svc = netinfo_ptr->service;
        char *ins = netinfo_ptr->instance;
        char hello[128];
        snprintf(hello, sizeof(hello), "%s/%s/%s", app, svc, ins);
        int pmux_fd;
        portmux_hello("localhost", hello, &pmux_fd);
        n->pmux = portmux_listen_setup(app, svc, ins, pmux_fd);//netinfo_ptr->myfd);
        int pmux_listen_fd = portmux_fds_get_listenfd(n->pmux);
        n->pmux_ev = event_new(base, pmux_listen_fd, EV_READ | EV_PERSIST, do_pmux_accept, n);
        printf("myfd:%d listenfd:%d pmuxfd:%d pmux:%p\n", netinfo_ptr->myfd, pmux_listen_fd, pmux_fd, n->pmux);
        event_add(n->pmux_ev, NULL);
        logmsg(LOGMSG_INFO, "[%s time:%d fd:%3d thd:%p %s] accepting thru pmux\n",
               netinfo_ptr->service, (int)time(NULL), pmux_fd,
               (void *)pthread_self(), __func__);
    }
    int fd = netinfo_ptr->myfd;
    make_socket_nonblocking(fd);
    n->ev = event_new(base, fd, EV_READ | EV_PERSIST, do_accept, n);
    logmsg(LOGMSG_INFO, "[%s time:%d fd:%3d thd:%p %s] accepting on port:%d\n",
           netinfo_ptr->service, (int)time(NULL), fd, (void *)pthread_self(),
           __func__, netinfo_ptr->myport);
    event_add(n->ev, NULL);
}

static void do_add_host(int accept_fd, short what, void *data)
{
    host_node_type *host_node_ptr = data;
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    net_accept(netinfo_ptr);
    if (host_node_ptr->host == netinfo_ptr->myhostname) return;
    struct host_info *h = NULL;
    struct event_info *e = NULL;
    get_host_event(host_node_ptr, &h, &e);
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
    connect_host(-1, EV_TIMEOUT, e);
}

static void init_base_int(pthread_t *t, struct event_base **bb,
                          void *(*f)(void *), const char *who)
{
    struct net_dispatch_info *info;
    info = calloc(1, sizeof(struct net_dispatch_info));
    struct event_config *cfg = event_config_new();
    event_config_set_flag(cfg, EVENT_BASE_FLAG_EPOLL_USE_CHANGELIST); /* EVENT_BASE_FLAG_IGNORE_ENV */
    *bb = event_base_new_with_config(cfg);
    event_config_free(cfg);
    printf("Using Libevent %s with backend method %s\n", event_get_version(),
           event_base_get_method(*bb));
    info->who = who;
    info->base = *bb;
    Pthread_create(t, NULL, f, info);
    Pthread_detach(*t);
}

static void init_base(void)
{
    if (base)
        abort();
    init_base_int(&base_thd, &base, net_dispatch_main, "main");
    if (single_thread) {
        akq = akq_init(user_msg_info, user_msg_func, akq_start_callback,
                       akq_stop_callback);
        timer_thd = rd_thd = wr_thd = base_thd;
        timer = rd_base = wr_base = base;
    } else {
        init_base_int(&timer_thd, &timer, net_dispatch_timer, "timer");
        init_base_int(&rd_thd, &rd_base, net_dispatch_rd, "read");
        init_base_int(&wr_thd, &wr_base, net_dispatch_wr, "write");
    }
}

static void init_event_net(netinfo_type *netinfo_ptr)
{
    if (base) return;
    evthread_use_pthreads();
#   ifdef PER_THREAD_MALLOC
    event_set_mem_functions(malloc, realloc, free);
#   endif
    start_stop_callback_data = netinfo_ptr->callback_data;
    start_callback = netinfo_ptr->start_thread_callback;
    stop_callback = netinfo_ptr->stop_thread_callback;
    init_base();
}

static void net_send_one(struct event_info *e, struct net_msg *msg)
{
    int rc = 0;
    int nodelay = 0;
    struct evbuffer *buf = e->wr_buf;
    host_node_type *host_node_ptr = HOST_NODE_PTR(e);
    for (int i = 0; i < msg->num; ++i) {
        struct net_msg_data *data = msg->data[i];
        nodelay |= data->flags & NET_SEND_NODELAY;
        rc = evbuffer_add_reference(buf, e->wirehdr[WIRE_HEADER_USER_MSG], e->wirehdr_len, NULL, NULL);
        if (rc) break;
        rc = evbuffer_add_reference(buf, &data->msghdr, data->sz, net_msg_data_free, data);
        if (rc) break;
        net_msg_data_add_ref(data);
    }
    if (rc != 0) {
        hprintf("Failed to add ref #msgs:%d rc:%d\n", msg->num, rc);
        return;
    }
    if (nodelay) {
        if ((rc = net_flush_bufferevent_int(e)) != 0) {
            hprintf("Failed to add buf #msgs:%d rc:%d\n", msg->num, rc);
        }
    }
}

static void do_net_send_all(int dummy_fd, short what, void *data)
{
    check_base_thd();
    struct net_msg *msg = data;
    netinfo_type *netinfo_ptr = msg->netinfo_ptr;
    struct net_info *n = net_info_find(netinfo_ptr->service);
    struct event_info *e;
    LIST_FOREACH(e, &n->event_list, net_list_entry) {
        host_node_type *host_node_ptr = HOST_NODE_PTR(e);
        if (host_node_ptr->host == netinfo_ptr->myhostname ||
            host_node_ptr->fd < 0 || host_node_ptr->got_hello == 0) {
            continue;
        }
        Pthread_mutex_lock(&e->wr_lk);
        if (e->wr_bev) {
            net_send_one(e, msg);
        }
        Pthread_mutex_unlock(&e->wr_lk);
    }
    net_msg_free(msg);
}

static int write_list_int(host_node_type *host_node_ptr,
                          const struct iovec *iov, int n, int flags)
{
    struct event_info *e = host_node_ptr->event_info;
    struct evbuffer *buf = e->wr_buf;
    int rc = 0;
    for (int i = 0; i < n; ++i) {
        rc = evbuffer_add(buf, iov[i].iov_base, iov[i].iov_len);
        if (rc) {
            hprintf("failed to add msg:%d rc:%d\n", i, rc);
            return rc;
        }
    }
    if (flags & NET_SEND_NODELAY) {
        rc = net_flush_bufferevent_int(e);
    }
    return rc;
}

/***************************************************************************
 ***********************    PUBLIC INTERFACE    ****************************
 **************************************************************************/

void stop_event_net(void)
{
    Pthread_once(&exit_once, exit_once_func);
}

void add_host(host_node_type *host_node_ptr)
{
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    init_event_net(netinfo_ptr);
    event_once(base, do_add_host, host_node_ptr);
}

void add_event(int fd, event_callback_fn fn, void *data)
{
    if (gbl_create_mode) return;
    make_socket_nonblocking(fd);
    struct event *ev = event_new(timer, fd, EV_READ | EV_PERSIST, fn, data);
    event_add(ev, NULL);
}

void add_timer_event(event_callback_fn fn, void *data, int ms)
{
    if (gbl_create_mode) return;
    struct event *ev = event_new(timer, -1, EV_PERSIST, fn, data);
    struct timeval t = ms_to_timeval(ms);
    event_add(ev, &t);
}

void decom(char *host)
{
    if (strcmp(host, gbl_myhostname) == 0) return;
    event_once(base, do_decom, host);
}

/***********************************************************
 *
 *
 *                     TODO XXX FIXME
 *                  bdb add ?
 *
 *
 *                     TODO XXX FIXME
 *              NEED A WRITE MESSAGE HEAD
 *
 *
 *                     TODO XXX FIXME
 *              limit size of bufferevent queue 
 *
 *
 **********************************************************/

int write_stream_bufferevent(host_node_type *host_node_ptr, const void *data,
                             size_t len)
{
    if (gbl_create_mode || nomo) return 0;
    struct event_info *e = host_node_ptr->event_info;
    struct evbuffer *buf = e->wr_buf;
    int rc = -1;
    Pthread_mutex_lock(&e->wr_lk);
    if (e->wr_buf) {
        rc = evbuffer_add(buf, data, len);
    }
    Pthread_mutex_unlock(&e->wr_lk);
    if (rc == 0) {
        return len;
    }
    logmsg(LOGMSG_ERROR, "%s failed rc:%d\n", __func__, rc);
    return -1;
}

/* Exclusively for writing connect-msg with wr_lk already held */
int write_list_bufferevent_no_hdr(host_node_type *host_node_ptr,
                                  const struct iovec *iov, int n)
{
    if (gbl_create_mode || nomo) return 0;
    check_base_thd();
    int rc = write_list_int(host_node_ptr, iov, n, NET_SEND_NODELAY);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed rc:%d\n", __func__, rc);
    }
    return rc;
}

int write_list_bufferevent(host_node_type *host_node_ptr, int type,
                           const struct iovec *iov, int n, int flags)
{
    if (gbl_create_mode || nomo) return 0;
    struct event_info *e = host_node_ptr->event_info;
    int rc = -1;
    Pthread_mutex_lock(&e->wr_lk);
    struct evbuffer *buf = e->wr_buf;
    if (buf) {
        rc = evbuffer_add_reference(buf, e->wirehdr[type], e->wirehdr_len, NULL, NULL);
        if (rc == 0) {
            rc = write_list_int(host_node_ptr, iov, n, flags);
        }
    }
    Pthread_mutex_unlock(&e->wr_lk);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed rc:%d\n", __func__, rc);
    }
    return rc;
}

int net_send_all_bufferevent(netinfo_type *netinfo_ptr, int n, void **buf,
                             int *len, int *type, int *flags)
{
    if (gbl_create_mode || nomo) return 0;
    struct net_msg *msg = net_msg_new(netinfo_ptr, n);
    for (int i = 0; i < n; ++i) {
        msg->data[i] = net_msg_data_new(netinfo_ptr, buf[i], len[i], type[i], flags[i]);
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
    if (gbl_create_mode || nomo) return 0;
    struct event_info *e = host_node_ptr->event_info;
    int rc = -1;
    Pthread_mutex_lock(&e->wr_lk);
    if (e->wr_buf) {
        rc = net_flush_bufferevent_int(e);
    }
    Pthread_mutex_unlock(&e->wr_lk);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s failed rc:%d\n", __func__, rc);
    }
    return rc;
}
