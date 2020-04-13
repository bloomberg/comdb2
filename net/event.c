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
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include <event2/buffer.h>
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

#define MB(x) ((x) * 1024 * 1024)
#define TCP_BUFSZ MB(8)
#define DISTRESS_COUNT 5
#define hprintf_lvl LOGMSG_USER
#define hprintf_format(a) "[%.3s %-8s fd:%-3d %18s] " a, e->service, e->host, e->fd, __func__
#define distress_logmsg(...)                                                   \
    do {                                                                       \
        if (e->distressed) {                                                   \
            break;                                                             \
        } else if (e->distress_count >= DISTRESS_COUNT) {                      \
            logmsg(hprintf_lvl, hprintf_format("ENTERING DISTRESS MODE\n"));   \
            e->distressed = 1;                                                 \
            break;                                                             \
        }                                                                      \
        logmsg(hprintf_lvl, __VA_ARGS__);                                      \
    } while (0)

#define no_distress_logmsg(...) logmsg(hprintf_lvl, __VA_ARGS__)

#define hprintf(a, ...) distress_logmsg(hprintf_format(a), __VA_ARGS__)
#define hputs(a) distress_logmsg(hprintf_format(a))

#define hprintf_nd(a, ...) no_distress_logmsg(hprintf_format(a), __VA_ARGS__)
#define hputs_nd(a) no_distress_logmsg(hprintf_format(a))

int gbl_libevent = 1;

extern char *gbl_myhostname;
extern int gbl_create_mode;
extern int gbl_exit;
extern int gbl_fullrecovery;
extern int gbl_netbufsz;
extern int gbl_pmux_route_enabled;

static double resume_lvl = 0.33;
static struct timeval one_sec = {1, 0};
static struct timeval connect_timeout = {1, 0};

enum policy {
    POLICY_NONE,
    POLICY_SINGLE,
    POLICY_PER_NET,
    POLICY_PER_HOST,
    POLICY_PER_EVENT
};

static int dedicated_timer = 0;
static enum policy akq_policy = POLICY_PER_NET;
static enum policy reader_policy = POLICY_PER_HOST;
static enum policy writer_policy = POLICY_PER_HOST;

struct policy_info {
    struct akq *akq;
    pthread_t rdthd;
    pthread_t wrthd;
    struct event_base *rdbase;
    struct event_base *wrbase;
};

static struct policy_info single;
static pthread_t base_thd;
static struct event_base *base;
static pthread_t timer_thd;
static struct event_base *timer_base;

#define get_akq()                                                              \
    ({                                                                         \
        struct akq *q = NULL;                                                  \
        switch (akq_policy) {                                                  \
        case POLICY_NONE: break;                                               \
        case POLICY_SINGLE: q = single.akq; break;                             \
        case POLICY_PER_NET: q = e->net_info->per_net.akq; break;              \
        case POLICY_PER_HOST: q = e->host_info->per_host.akq; break;           \
        case POLICY_PER_EVENT: q = e->per_event.akq; break;                    \
        }                                                                      \
        q;                                                                     \
    })

#define get_rd_policy()                                                        \
    ({                                                                         \
        struct policy_info *f = NULL;                                          \
        switch (reader_policy) {                                               \
        case POLICY_NONE:                                                      \
        case POLICY_SINGLE: f = &single; break;                                \
        case POLICY_PER_NET: f = &e->net_info->per_net; break;                 \
        case POLICY_PER_HOST: f = &e->host_info->per_host; break;              \
        case POLICY_PER_EVENT: f = &e->per_event; break;                       \
        }                                                                      \
        f;                                                                     \
    })

#define get_wr_policy()                                                        \
    ({                                                                         \
        struct policy_info *f = NULL;                                          \
        switch (writer_policy) {                                               \
        case POLICY_NONE:                                                      \
        case POLICY_SINGLE: f = &single; break;                                \
        case POLICY_PER_NET: f = &e->net_info->per_net; break;                 \
        case POLICY_PER_HOST: f = &e->host_info->per_host; break;              \
        case POLICY_PER_EVENT: f = &e->per_event; break;                       \
        }                                                                      \
        f;                                                                     \
    })

#define rd_thd ({ get_rd_policy()->rdthd; })
#define rd_base ({ get_rd_policy()->rdbase; })

#define wr_thd ({ get_wr_policy()->wrthd; })
#define wr_base ({ get_wr_policy()->wrbase; })

#undef SKIP_CHECK_THD

#ifdef SKIP_CHECK_THD
#  define check_thd(...)
#else
#  define check_thd(thd)                                                       \
    if (thd != pthread_self()) {                                               \
        fprintf(stderr, "FATAL ERROR: %s EVENT NOT DISPATCHED on " #thd "\n",  \
                __func__);                                                     \
        abort();                                                               \
    }
#endif

#define check_base_thd() check_thd(base_thd)
#define check_timer_thd() check_thd(timer_thd)
#define check_rd_thd() check_thd(rd_thd)
#define check_wr_thd() check_thd(wr_thd)

static void *start_stop_callback_data;
static void (*start_callback)(void *);
static void (*stop_callback)(void *);

static pthread_once_t exit_once = PTHREAD_ONCE_INIT;
static pthread_cond_t exit_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t exit_mtx = PTHREAD_MUTEX_INITIALIZER;

struct connect_info;
struct event_info;
struct net_info;

static void init_base(pthread_t *, struct event_base **, void *(*)(void *), const char *);
static void event_info_free(struct event_info *);
static void flushcb(int, short, void *);
static void do_open(int, short, void *);
static void pmux_connect(int, short, void *);
static void pmux_reconnect(struct connect_info *c);
static struct akq *setup_akq(void);
static void unix_connect(int, short, void *);

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
    int min = 5;
    int max = 10;
    int range = max - min;
    int r = random() % range;
    struct timeval t = {r + min, 0};
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
#   ifdef __linux__
    int rc, sz;
    socklen_t len;

    sz = TCP_BUFSZ;
    len = sizeof(sz);
    rc = setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sz, len);
    if (rc < 0) {
        logmsg(LOGMSG_ERROR, "%s fd:%d snd err:%s\n", __func__, fd, strerror(errno));
    }
    sz = TCP_BUFSZ;
    len = sizeof(sz);
    rc = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sz, len);
    if (rc < 0) {
        logmsg(LOGMSG_ERROR, "%s fd:%d rcv err:%s\n", __func__, fd, strerror(errno));
    }
#   endif
}

static void make_socket_nonblocking(int fd)
{
    evutil_make_socket_nonblocking(fd);
    make_socket_nolinger(fd);
    make_socket_nodelay(fd);
    make_socket_keepalive(fd);
    make_socket_reusable(fd);
    set_socket_bufsz(fd);
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
    struct policy_info per_host;
};

struct event_info {
    host_node_type *host_node_ptr;
    LIST_HEAD(, host_connected_info) host_connected_list;
    LIST_ENTRY(event_info) host_list_entry;
    LIST_ENTRY(event_info) net_list_entry;
    int fd;
    int port;
    char *host;
    char *service;
    int wirehdr_len;
    wire_header_type *wirehdr[WIRE_HEADER_MAX]; /* network byte-order */
    struct net_info *net_info;
    struct host_info *host_info;
    struct policy_info per_event;
    struct event *hb_check_ev;
    struct event *hb_send_ev;
    struct event *connect_ev;
    time_t recv_at;
    time_t sent_at;
    int distress_count;
    int distressed;
    int decomissioned;
    int got_hello;

    /* Write ops guarded by wr_lk */
    pthread_mutex_t wr_lk;
    struct evbuffer *wr_buf;
    int wr_full;

    /* Flush ops guarded by flush_lk */
    pthread_mutex_t flush_lk;
    struct evbuffer *flush_buf;
    struct event *flush_pending;

    /* Read ops happen on read base */
    struct evbuffer *rd_evbuf;
    struct event rd_event;
    uint8_t *rdbuf;
    int rdbuf_sz;
    uint64_t need;
    uint64_t rd_size; /* data enqueued in akq */
    int rd_full;
    int state;
    wire_header_type hdr;
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
    struct event_info *event_info;
    net_send_message_header msg;
    size_t len;
    uint8_t *buf;
};

#if 0
struct user_event_info {
    void *data;
    struct event *ev;
    event_callback_fn func;
    LIST_ENTRY(user_event_info) entry;
};

static LIST_HEAD(, user_event_info) user_event_list = LIST_HEAD_INITIALIZER(user_event_list);
#endif
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

static void nop(int dummyfd, short what, void *data)
{
    /* EVLOOP_NO_EXIT_ON_EMPTY is not available in LibEvent v2.0. This keeps
     * base around even if there are no events. */
}

static void *net_dispatch(void *arg)
{
    struct net_dispatch_info *n = arg;
    struct event *ev = event_new(n->base, -1, EV_PERSIST, nop, n);
    struct timeval ten = {10, 0};
    event_add(ev, &ten);
    if (start_callback) {
        start_callback(start_stop_callback_data);
    }
    event_base_dispatch(n->base);
    if (stop_callback) {
        stop_callback(start_stop_callback_data);
    }
    event_del(ev);
    event_free(ev);
    free(n);
    Pthread_mutex_lock(&exit_mtx);
    Pthread_cond_signal(&exit_cond);
    Pthread_mutex_unlock(&exit_mtx);
    return NULL;
}

static struct host_info *host_info_new(char *host)
{
    check_base_thd();
    struct host_info *h = calloc(1, sizeof(struct host_info));
    LIST_INSERT_HEAD(&host_list, h, entry);
    LIST_INIT(&h->event_list);
    h->host = intern(host);
    if (akq_policy == POLICY_PER_HOST) {
        h->per_host.akq = setup_akq();
    }
    if (reader_policy == POLICY_PER_HOST) {
        init_base(&h->per_host.rdthd, &h->per_host.rdbase, net_dispatch, h->host);
    }
    if (writer_policy == POLICY_PER_HOST) {
        init_base(&h->per_host.wrthd, &h->per_host.wrbase, net_dispatch, h->host);
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
    struct host_info *h;
    LIST_FOREACH(h, &host_list, entry) {
        if (strcmp(h->host, host) == 0) {
            break;
        }
    }
    return h;
}

struct net_info {
    netinfo_type *netinfo_ptr;
    LIST_HEAD(, event_info) event_list;
    LIST_ENTRY(net_info) entry;
    int port;
    char *service;
    char *instance;
    char *app;
    uint64_t rd_max;
    struct event *unix_ev;
    struct evbuffer *unix_buf;
    struct evconnlistener *listener;
    struct policy_info per_net;
};

static struct net_info *net_info_new(netinfo_type *netinfo_ptr)
{
    check_base_thd();
    struct net_info *n = calloc(1, sizeof(struct net_info));
    n->netinfo_ptr = netinfo_ptr;
    n->service = strdup(netinfo_ptr->service);
    n->instance = strdup(netinfo_ptr->instance);
    n->app = strdup(netinfo_ptr->app);
    n->port = netinfo_ptr->myport;
    n->rd_max = MB(128);
    if (akq_policy == POLICY_PER_NET) {
        n->per_net.akq = setup_akq();
    }
    if (reader_policy == POLICY_PER_NET) {
        init_base(&n->per_net.rdthd, &n->per_net.rdbase, net_dispatch, n->service);
    }
    if (writer_policy == POLICY_PER_NET) {
        init_base(&n->per_net.wrthd, &n->per_net.wrbase, net_dispatch, n->service);
    }
    LIST_INSERT_HEAD(&net_list, n, entry);
    LIST_INIT(&n->event_list);
    return n;
}

static void net_info_free(struct net_info *n)
{
    check_base_thd();
    LIST_REMOVE(n, entry);
    event_free(n->unix_ev);
    evbuffer_free(n->unix_buf);
    evconnlistener_free(n->listener);
    free(n->service);
    free(n->instance);
    free(n->app);
    free(n);
}

static struct net_info *net_info_find(const char *name)
{
    struct net_info *n;
    LIST_FOREACH(n, &net_list, entry) {
        if (strcmp(n->service, name) == 0) {
            break;
        }
    }
    return n;
}

static struct event_info *event_info_find(struct net_info *n, struct host_info *h)
{
    struct event_info *e;
    LIST_FOREACH(e, &h->event_list, host_list_entry) {
        if (strcmp(e->service, n->service) == 0) {
            return e;
        }
    }
    return NULL;
}

static void update_wire_hdrs(struct event_info *e)
{
    for (int i = 1; i < WIRE_HEADER_MAX; ++i) {
        e->wirehdr[i]->toport = htonl(e->port);
    }
}

static void setup_wire_hdrs(struct event_info *e)
{
    int fromlen = strlen(gbl_myhostname);
    int tolen = strlen(e->host);
    size_t len = NET_WIRE_HEADER_TYPE_LEN;
    if (fromlen > HOSTNAME_LEN)
        len += fromlen;
    if (tolen > HOSTNAME_LEN)
        len += tolen;
    e->wirehdr_len = len;
    for (int i = 1; i < WIRE_HEADER_MAX; ++i) {
        e->wirehdr[i] = calloc(1, len);
        char *name = (char *)e->wirehdr[i] + NET_WIRE_HEADER_TYPE_LEN;
        if (fromlen > HOSTNAME_LEN) {
            sprintf(e->wirehdr[i]->fromhost, ".%d", fromlen);
            strcpy(name, gbl_myhostname);
            name += fromlen;
        } else {
            strcpy(e->wirehdr[i]->fromhost, gbl_myhostname);
        }
        e->wirehdr[i]->fromport = htonl(e->net_info->port);
        if (tolen > HOSTNAME_LEN) {
            sprintf(e->wirehdr[i]->tohost, ".%d", tolen);
            strcpy(name, e->host);
        } else {
            strcpy(e->wirehdr[i]->tohost, e->host);
        }
        e->wirehdr[i]->toport = htonl(e->port);
        e->wirehdr[i]->type = htonl(i);
    }
}

static struct event_info *event_info_new(struct net_info *n, struct host_info *h)
{
    check_base_thd();
    struct event_info *e = calloc(1, sizeof(struct event_info));
    LIST_INSERT_HEAD(&h->event_list, e, host_list_entry);
    LIST_INSERT_HEAD(&n->event_list, e, net_list_entry);
    LIST_INIT(&e->host_connected_list);
    e->fd = -1;
    e->host = h->host;
    e->service = n->service;
    e->host_info = h;
    e->net_info = n;
    Pthread_mutex_init(&e->wr_lk, NULL);
    Pthread_mutex_init(&e->flush_lk, NULL);
    setup_wire_hdrs(e);
    struct event_hash_entry *entry = malloc(sizeof(struct event_hash_entry));
    make_event_hash_key(entry->key, n->service, h->host);
    entry->event_info = e;
    Pthread_mutex_lock(&event_hash_lk);
    hash_add(event_hash, entry);
    Pthread_mutex_unlock(&event_hash_lk);
    if (akq_policy == POLICY_PER_EVENT) {
        e->per_event.akq = setup_akq();
    }
    if (reader_policy == POLICY_PER_EVENT) {
        init_base(&e->per_event.rdthd, &e->per_event.rdbase, net_dispatch, entry->key);
    }
    if (writer_policy == POLICY_PER_EVENT) {
        init_base(&e->per_event.wrthd, &e->per_event.wrbase, net_dispatch, entry->key);
    }
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

static struct net_msg_data *net_msg_data_new(void *buf, int len, int type)
{
    struct net_msg_data *data = malloc(sizeof(struct net_msg_data) + len);
    if (data == NULL) {
        return NULL;
    }
    memset(data, 0, sizeof(struct net_msg_data));
    data->sz = NET_SEND_MESSAGE_HEADER_LEN + len;
    data->ref = 1;
    net_send_message_header tmp = {
        .usertype = type,
        .datalen = len,
    };
    net_send_message_header *hdr = &data->msghdr;
    net_send_message_header_put(&tmp, (uint8_t *)hdr, (uint8_t*)(hdr + 1));
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
        msg->data[i] = net_msg_data_new(buf[i], len[i], type[i]);
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

static void set_hello_message(struct event_info *e)
{
    e->got_hello = 1;
    e->host_node_ptr->got_hello = 1;
}

static void set_decom(struct event_info *e)
{
    e->decomissioned = 1;
    net_decom_node(e->net_info->netinfo_ptr, e->host_node_ptr->host);
}

static void update_host_node_ptr(host_node_type *host_node_ptr, struct event_info *e)
{
    host_node_ptr->event_info = e;
    e->host_node_ptr = host_node_ptr;
}

static void update_event_port(struct event_info *e, int port)
{
    e->port = port;
    e->host_node_ptr->port = port;
}

static void update_event_fd(struct event_info *e, int fd)
{
    e->fd = fd;
    e->host_node_ptr->fd = fd;
}

static void update_net_info(struct net_info *n, int fd, int port)
{
    n->port = port;
    n->netinfo_ptr->myfd = fd;
    n->netinfo_ptr->myport = port;
}

static void resume_read(int, short, void *);
static void suspend_read(int, short, void *);

static void akq_work_free(struct event_info *e, struct user_msg_info *info)
{
    free(info->buf);
    int64_t len = info->len * -1;
    uint64_t outstanding = ATOMIC_ADD64(e->rd_size, len);
    uint64_t max_bytes = e->net_info->rd_max;
    if (!max_bytes) {
        return;
    }
    if (e->rd_full) {
        if (outstanding <= max_bytes * resume_lvl) {
            e->rd_full = 0;
            hprintf("RESUMING RD bytes:%lu (max:%lu)\n", outstanding, max_bytes);
            event_once(rd_base, resume_read, e);
        }
    } else {
        if (outstanding > max_bytes) {
            e->rd_full = 1;
            hprintf("SUSPENDING RD bytes:%lu (max:%lu)\n", outstanding, max_bytes);
            event_once(rd_base, suspend_read, e);
        }
    }
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
    check_wr_thd();
    free(i);
    Pthread_mutex_lock(&e->wr_lk);
    Pthread_mutex_lock(&e->flush_lk);
    if (e->wr_buf) {
        evbuffer_free(e->wr_buf);
        e->wr_buf = NULL;
    }
    e->got_hello = 0;

    if (e->flush_buf) {
        evbuffer_free(e->flush_buf);
        e->flush_buf = NULL;
    }
    if (e->flush_pending) {
        event_del(e->flush_pending);
        e->flush_pending = NULL;
    }
    if (e->fd != -1) {
        hprintf("CLOSING CONNECTION fd:%d\n", e->fd);
        shutdown_close(e->fd);
        e->fd = -1;
    }
    Pthread_mutex_unlock(&e->flush_lk);
    Pthread_mutex_unlock(&e->wr_lk);
    event_once(base, func, e);
}

static void do_disable_read(struct event_info *e)
{
    check_rd_thd();
    if (e->rd_evbuf) {
        evbuffer_free(e->rd_evbuf);
        e->rd_evbuf= NULL;
        event_del(&e->rd_event);
    }
}

static int truncate_func(void *work, void *arg)
{
    struct user_msg_info *i = work;
    struct event_info *e = arg;
    if (i->event_info == e) {
        akq_work_free(e, i);
        return 1;
    }
    return 0;
}

static void disable_read(int dummyfd, short what, void *data)
{
    struct disable_info *i = data;
    struct event_info *e = i->event_info;
    do_disable_read(e);
    struct akq *q = get_akq();
    if (q) {
        akq_truncate_if(q, truncate_func, e);
    }
    event_once(wr_base, disable_write, i);
}

static void do_disable_heartbeats(struct event_info *e)
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

static void disable_heartbeats(int dummyfd, short what, void *data)
{
    struct disable_info *i = data;
    struct event_info *e = i->event_info;
    do_disable_heartbeats(e);
    event_once(rd_base, disable_read, i);
}

static void do_host_close(int dummyfd, short what, void *data)
{
    struct disable_info *i = data;
    struct event_info *e = i->event_info;
    host_node_close(e->host_node_ptr);
    event_once(timer_base, disable_heartbeats, i);
}

static void do_close(struct event_info *e, event_callback_fn func)
{
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    if (netinfo_ptr->hostdown_rtn) {
        netinfo_ptr->hostdown_rtn(netinfo_ptr, e->host);
    }
    struct disable_info *i = malloc(sizeof(struct disable_info));
    i->event_info = e;
    i->func = func;
    event_once(base, do_host_close, i);
}

static int skip_connect(struct event_info *e)
{
    if (e->decomissioned) {
        return 1;
    }
    return 0;
}

static void do_reconnect(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct event_info *e = data;
    if (skip_connect(e)) {
        return;
    }
    if (e->connect_ev) {
        return;
    }
    struct timeval t = reconnect_time();
    hprintf("RECONNECT IN %ds\n", (int)t.tv_sec);
    e->connect_ev = event_new(base, -1, EV_TIMEOUT, pmux_connect, e);
    event_add(e->connect_ev, &t);
}

static void reconnect(struct event_info *e)
{
    if (gbl_exit) {
        return;
    }
    do_close(e, do_reconnect);
}

static void send_decom_all(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    struct net_info *n = e->net_info;
    struct event_info *to;
    LIST_FOREACH(to, &n->event_list, net_list_entry) {
        /*
        if (e == to) {
            continue;
        }
        */
        write_decom(n->netinfo_ptr, to->host_node_ptr, e->host, strlen(e->host), to->host);
    }
    hputs_nd("DECOMMISSIONED\n");
}

static void do_user_msg(struct event_info *e, net_send_message_header *msg, uint8_t *payload)
{
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    NETFP *func = netinfo_ptr->userfuncs[msg->usertype].func;
    char *host = e->host;
    void *usrptr = netinfo_ptr->usrptr;
    ack_state_type ack = {
        .seqnum = msg->seqnum,
        .needack = msg->waitforack,
        .fromhost = host,
        .netinfo = netinfo_ptr,
    };
    func(&ack, usrptr, host, msg->usertype, payload, msg->datalen, 1);
}

static void akq_start_callback(void *dummy)
{
    if (start_callback) {
        start_callback(start_stop_callback_data);
    }
}

static void akq_stop_callback(void *dummy)
{
    if (stop_callback) {
        stop_callback(start_stop_callback_data);
    }
}

static void akq_work_callback(void *work)
{
    struct user_msg_info *info = work;
    struct event_info *e = info->event_info;
    do_user_msg(e, &info->msg, info->buf);
    akq_work_free(e, info);
}

static struct akq *setup_akq(void)
{
    return akq_new(sizeof(struct user_msg_info), akq_work_callback,
                   akq_start_callback, akq_stop_callback);
}

static void user_msg(struct event_info *e, net_send_message_header *msg,
                     uint8_t *payload)
{
    check_rd_thd();
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    NETFP *func = netinfo_ptr->userfuncs[msg->usertype].func;
    if (func == NULL) {
        /* Startup race without accept-on-child-net: Replication net is
         * ready and accepts offload connection but offload callbacks have not
         * registered yet. */
        reconnect(e);
        return;
    }
    struct akq *q = get_akq();
    if (!q) {
        do_user_msg(e, msg, payload);
        return;
    }
    struct user_msg_info info;
    info.event_info = e;
    info.msg = *msg;
    info.buf = payload;
    info.len = e->need;
    e->rdbuf = NULL;
    e->rdbuf_sz = 0;
    ATOMIC_ADD64(e->rd_size, e->need);
    akq_enqueue_work(q, &info);
}

static void stop_base(struct event_base *b)
{
    Pthread_mutex_lock(&exit_mtx);
    event_base_loopbreak(b);
    Pthread_cond_wait(&exit_cond, &exit_mtx);
    Pthread_mutex_unlock(&exit_mtx);
}

static int net_stop = 1;
static void exit_once_func(void)
{
    struct net_info *n;
    struct host_info *h;
    struct event_info *e;
    LIST_FOREACH(n, &net_list, entry) {
        LIST_FOREACH(e, &n->event_list, net_list_entry) {
            write_decom(n->netinfo_ptr, e->host_node_ptr, gbl_myhostname,
                        strlen(gbl_myhostname), e->host);
        }
    }
    net_stop = 1;
    stop_base(base);
    if (dedicated_timer) {
        stop_base(timer_base);
    }
    switch (reader_policy) {
    case POLICY_NONE:
        break;
    case POLICY_SINGLE:
        stop_base(single.rdbase);
        break;
    case POLICY_PER_NET:
        LIST_FOREACH(n, &net_list, entry) {
            stop_base(n->per_net.rdbase);
        }
        break;
    case POLICY_PER_HOST:
        LIST_FOREACH(h, &host_list, entry) {
            stop_base(h->per_host.rdbase);
        }
        break;
    case POLICY_PER_EVENT:
        LIST_FOREACH(n, &net_list, entry) {
            LIST_FOREACH(e, &n->event_list, net_list_entry) {
                stop_base(e->per_event.rdbase);
            }
        }
        break;
    }
    switch (akq_policy) {
    case POLICY_NONE:
        break;
    case POLICY_SINGLE:
        akq_stop(single.akq);
        break;
    case POLICY_PER_NET:
        LIST_FOREACH(n, &net_list, entry) {
            akq_stop(n->per_net.akq);
        }
        break;
    case POLICY_PER_HOST:
        LIST_FOREACH(h, &host_list, entry) {
            akq_stop(h->per_host.akq);
        }
        break;
    case POLICY_PER_EVENT:
        LIST_FOREACH(n, &net_list, entry) {
            LIST_FOREACH(e, &n->event_list, net_list_entry) {
                akq_stop(e->per_event.akq);
            }
        }
        break;
    }
    switch (writer_policy) {
    case POLICY_NONE:
        break;
    case POLICY_SINGLE:
        stop_base(single.wrbase);
        break;
    case POLICY_PER_NET:
        LIST_FOREACH(n, &net_list, entry) {
            stop_base(n->per_net.wrbase);
        }
        break;
    case POLICY_PER_HOST:
        LIST_FOREACH(h, &host_list, entry) {
            stop_base(h->per_host.wrbase);
        }
        break;
    case POLICY_PER_EVENT:
        LIST_FOREACH(n, &net_list, entry) {
            LIST_FOREACH(e, &n->event_list, net_list_entry) {
                stop_base(e->per_event.wrbase);
            }
        }
        break;
    }
    Pthread_cond_destroy(&exit_cond);
    Pthread_mutex_destroy(&exit_mtx);
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
    double diff = difftime_now(e->recv_at);
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    if (diff > netinfo_ptr->heartbeat_check_time) {
        hprintf("no data in %d seconds\n", (int)diff);
        do_disable_heartbeats(e);
        reconnect(e);
    }
}

static void heartbeat_send(int dummyfd, short what, void *data)
{
    check_timer_thd();
    struct event_info *e = data;
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    double diff = difftime_now(e->sent_at);
    if (diff == 0) {
        return;
    }
    write_heartbeat(netinfo_ptr, e->host_node_ptr);
    diff = difftime_now(e->sent_at);
    if (diff < netinfo_ptr->heartbeat_send_time) {
        return;
    }
    hprintf("no data in %d seconds\n", (int)diff);
    do_disable_heartbeats(e);
    reconnect(e);
}

static void message_done(struct event_info *e)
{
    e->hdr.type = 0;
    e->need = e->wirehdr_len;
}

static void hello_hdr_common(struct event_info *e, uint8_t *payload)
{
    uint32_t n;
    memcpy(&n, payload, sizeof(uint32_t));
    e->need = htonl(n) - sizeof(uint32_t);
}

static void hello_msg(struct event_info *e, uint8_t *payload)
{
    uint32_t n;
    memcpy(&n, payload, sizeof(uint32_t));
    n = htonl(n);
    if (n > REPMAX) {
        reconnect(e);
        return;
    }
    payload += sizeof(int);
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
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
    set_hello_message(e);
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
    int send_reply = 0;
    if (!e->got_hello) {
        send_reply = 1;
        hputs("GOT HELLO\n");
    }
    hello_msg(e, payload);
    message_done(e);
    if (send_reply) {
        write_hello_reply(e->net_info->netinfo_ptr, e->host_node_ptr);
    }
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
            e->need = msg->datalen;
            return;
        }
    }
    user_msg(e, msg, payload);
    message_done(e);
}

static void process_ack_no_payload(struct event_info *e, uint8_t *payload)
{
    host_node_type *host_node_ptr = e->host_node_ptr;
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
    host_node_type *host_node_ptr = e->host_node_ptr;
    net_ack_message_payload_type *ack = &e->ack;
    if (e->state == 0) {
        uint8_t *start = payload;
        uint8_t *end = payload + NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN;
        net_ack_message_payload_type_get(ack, start, end);
        e->need = ack->paylen;
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
        e->need = htonl(n);
        return;
    }
    message_done(e);
    decom((char *)payload);
}

static int process_hdr(struct event_info *e, uint8_t *buf)
{
    wire_header_type tmp;
    net_wire_header_get(&tmp, buf, buf + e->need);
    e->hdr = tmp;
    e->state = 0;
    switch (tmp.type) {
    case WIRE_HEADER_HEARTBEAT:
        message_done(e);
        break;
    case WIRE_HEADER_HELLO:
        e->need = sizeof(uint32_t);
        break;
    case WIRE_HEADER_DECOM:
        e->need = sizeof(uint32_t);
        break;
    case WIRE_HEADER_USER_MSG:
        e->need = NET_SEND_MESSAGE_HEADER_LEN;
        break;
    case WIRE_HEADER_ACK:
        e->need = NET_ACK_MESSAGE_TYPE_LEN;
        break;
    case WIRE_HEADER_HELLO_REPLY:
        e->need = sizeof(uint32_t);
        break;
    case WIRE_HEADER_DECOM_NAME:
        e->need = sizeof(uint32_t);
        break;
    case WIRE_HEADER_ACK_PAYLOAD:
        e->need = NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN;
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

#define DISABLE_AND_RECONNECT()                                                \
    do {                                                                       \
        do_disable_read(e);                                                    \
        reconnect(e);                                                          \
        return;                                                                \
    } while (0)

static void readcb(int fd, short what, void *data)
{
    struct event_info *e = data;
    check_rd_thd();
    struct evbuffer *input = e->rd_evbuf;
#   ifdef __linux__
#       define RD_BUFSZ TCP_BUFSZ
#       define NVEC 16
#   else
#       define RD_BUFSZ MB(2)
#       define NVEC 4
#   endif
    struct iovec v[NVEC];
    const int nv = evbuffer_reserve_space(input, RD_BUFSZ, v, NVEC);
    if (nv == -1) {
        hputs("evbuffer_reserve_space failed\n");
        DISABLE_AND_RECONNECT();
    }
    ssize_t n = readv(e->fd, v, nv);
    if (n <= 0) {
        DISABLE_AND_RECONNECT();
    }
    for (int i = 0; i < nv; ++i) {
        if (v[i].iov_len < n) {
            n -= v[i].iov_len;
            continue;
        }
        v[i].iov_len = n;
        n = 0;
    }
    evbuffer_commit_space(input, v, nv);
    while (evbuffer_get_length(input) >= e->need) {
        if (e->need > e->rdbuf_sz) {
            if (e->rdbuf) {
                free(e->rdbuf);
            }
#           define MIN_RDBUF 1024
            e->rdbuf_sz = e->need > MIN_RDBUF ? e->need : MIN_RDBUF;
            e->rdbuf = malloc(e->rdbuf_sz);
            if (!e->rdbuf) {
                abort();
            }
        }
        if (evbuffer_remove(input, e->rdbuf, e->need) != e->need) {
            hputs("evbuffer_remove failed\n");
            DISABLE_AND_RECONNECT();
        }
        e->recv_at = time(NULL);
        int rc;
        if (e->hdr.type == 0) {
            rc = process_hdr(e, e->rdbuf);
        } else {
            rc = process_payload(e, e->rdbuf);
        }
        if (rc) {
            DISABLE_AND_RECONNECT();
        }
    }
}

static void resume_read(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    check_rd_thd();
    if (!e->rd_evbuf) {
        return;
    }
    event_assign(&e->rd_event, rd_base, e->fd, EV_READ | EV_PERSIST, readcb, e);
    event_add(&e->rd_event, NULL);
}

static void suspend_read(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    check_rd_thd();
    if (!e->rd_evbuf) {
        return;
    }
    event_del(&e->rd_event);
}

static void do_queued(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    struct host_connected_info *info = LIST_FIRST(&e->host_connected_list);
    event_once(base, do_open, info);
}

static void finish_host_setup(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct host_connected_info *info = data;
    struct event_info *e = info->event_info;
    int connect_msg = info->connect_msg;
    host_connected_info_free(info);
    if (!LIST_EMPTY(&e->host_connected_list)) {
        hputs("WORKING ON QUEUED CONNECTION\n");
        do_close(e, do_queued);
    } else if (connect_msg) {
        hputs("WRITING HELLO\n");
        netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
        write_connect_message(netinfo_ptr, e->host_node_ptr, NULL);
        write_hello(netinfo_ptr, e->host_node_ptr);
    }
}

static void enable_heartbeats(int dummyfd, short what, void *data)
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

static void enable_read(int dummyfd, short what, void *data)
{
    struct host_connected_info *info = data;
    struct event_info *e = info->event_info;
    check_rd_thd();
    message_done(e);
    e->rd_full = 0;
    e->need = e->wirehdr_len;
    e->rd_evbuf = evbuffer_new();
    event_assign(&e->rd_event, rd_base, info->fd, EV_READ | EV_PERSIST, readcb, e);
    event_add(&e->rd_event, NULL);
    event_once(timer_base, enable_heartbeats, info);
}

static void enable_write(int dummyfd, short what, void *data)
{
    struct host_connected_info *info = data;
    struct event_info *e = info->event_info;
    check_base_thd();
    Pthread_mutex_lock(&e->wr_lk);
    Pthread_mutex_lock(&e->flush_lk);
    if (e->flush_buf || e->flush_pending) {
        /* should be cleaned up prior i think */
        abort();
    }
    e->flush_buf = evbuffer_new();
    e->fd = info->fd;
    if (e->wr_buf) {
        /* should be cleaned up prior i think */
        abort();
    }
    e->wr_buf = evbuffer_new();
    e->wr_full = 0;
    Pthread_mutex_unlock(&e->flush_lk);
    Pthread_mutex_unlock(&e->wr_lk);
    event_once(rd_base, enable_read, info);
}

static void do_open(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct host_connected_info *info = data;
    struct event_info *e = info->event_info;
    host_node_open(e->host_node_ptr, info->fd);
    enable_write(dummyfd, what, info);
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

static netinfo_type *pmux_netinfo(struct event_info *e)
{
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    if (netinfo_ptr->ischild && !netinfo_ptr->accept_on_child) {
        netinfo_ptr = netinfo_ptr->parent;
    }
    return netinfo_ptr;
}

static void comdb2_connected(int fd, short what, void *data)
{
    check_base_thd();
    struct connect_info *c = data;
    struct event_info *e = c->event_info;
    if (what & EV_TIMEOUT) {
        pmux_reconnect(c);
        return;
    }
    if (e->fd != -1) {
        hputs("ALREADY HAVE CONNECTION\n");
    } else if (!LIST_EMPTY(&e->host_connected_list)) {
        hputs("HAVE PENDING CONNECTION\n");
    } else if (skip_connect(e)) {
        hputs("SKIP CONNECTION\n");
    } else {
        hprintf("MADE NEW CONNECTION fd:%d\n", fd);
        host_connected(e, fd, 1);
        c->fd = -1;
    }
    connect_info_free(c);
}

static void comdb2_connect(struct connect_info *c, int port)
{
    check_base_thd();
    struct event_info *e = c->event_info;
    struct sockaddr_in sin = {0};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(e->port);
    if (get_dedicated_conhost(e->host_node_ptr, &sin.sin_addr)) {
        pmux_reconnect(c);
        return;
    }
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
    event_base_once(base, fd, EV_WRITE, comdb2_connected, c, &connect_timeout);
}

static void pmux_rte_readcb(int fd, short what, void *data)
{
    check_base_thd();
    struct connect_info *c = data;
    if ((what & EV_TIMEOUT) || evbuffer_read(c->buf, fd, -1) <= 0) {
        pmux_reconnect(c);
        return;
    }
    size_t len;
    char *res = evbuffer_readln(c->buf, &len, EVBUFFER_EOL_ANY);
    if (res == NULL) {
        event_base_once(base, fd, EV_READ, pmux_rte_readcb, c, &connect_timeout);
        return;
    }
    int pmux_rc = -1;
    int rc = sscanf(res, "%d", &pmux_rc);
    free(res);
    if (rc != 1 || pmux_rc != 0) {
        pmux_reconnect(c);
        return;
    }
    event_base_once(base, fd, EV_WRITE, comdb2_connected, c, &connect_timeout);
}

static void pmux_rte_writecb(int fd, short what, void *data)
{
    check_base_thd();
    struct connect_info *c = data;
    if ((what & EV_TIMEOUT) || evbuffer_write(c->buf, fd) <= 0) {
        pmux_reconnect(c);
        return;
    }
    if (evbuffer_get_length(c->buf) == 0) {
        event_base_once(base, fd, EV_READ, pmux_rte_readcb, c, &connect_timeout);
    } else {
        event_base_once(base, fd, EV_WRITE, pmux_rte_writecb, c, &connect_timeout);
    }
}

static void pmux_rte(struct connect_info *c)
{
    check_base_thd();
    struct event_info *e = c->event_info;
    netinfo_type *netinfo_ptr = pmux_netinfo(e);
    evbuffer_add_printf(c->buf, "rte %s/%s/%s\n",
                        netinfo_ptr->app, netinfo_ptr->service,
                        netinfo_ptr->instance);
    event_base_once(base, c->fd, EV_WRITE, pmux_rte_writecb, c, &connect_timeout);
}

static void pmux_get_readcb(int fd, short what, void *data)
{
    check_base_thd();
    struct connect_info *c = data;
    struct event_info *e = c->event_info;
    netinfo_type *netinfo_ptr = pmux_netinfo(e);
    if ((what & EV_TIMEOUT) || evbuffer_read(c->buf, fd, -1) <= 0) {
        pmux_reconnect(c);
        return;
    }
    size_t len;
    char *res = evbuffer_readln(c->buf, &len, EVBUFFER_EOL_ANY);
    if (res == NULL) {
        event_base_once(base, fd, EV_READ, pmux_get_readcb, c, &connect_timeout);
        return;
    }
    int good = 0;
    int port = -1;
    sscanf(res, "%d", &port);
    if (port > 0 && port <= USHRT_MAX) {
        char expected[len + 1];
        snprintf(expected, sizeof(expected), "%d %s/%s/%s", port,
                 netinfo_ptr->app, netinfo_ptr->service, netinfo_ptr->instance);
        if (strcmp(res, expected) == 0) {
            good = 1;
        }
    }
    free(res);
    if (!good) {
        pmux_reconnect(c);
        return;
    }
    update_event_port(e, port);
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
    check_base_thd();
    struct connect_info *c = data;
    if ((what & EV_TIMEOUT) || evbuffer_write(c->buf, fd) <= 0) {
        pmux_reconnect(c);
        return;
    }
    if (evbuffer_get_length(c->buf) == 0) {
        event_base_once(base, fd, EV_READ, pmux_get_readcb, c, &connect_timeout);
    } else {
        event_base_once(base, fd, EV_WRITE, pmux_get_writecb, c, &connect_timeout);
    }
}

static void pmux_get(struct connect_info *c)
{
    check_base_thd();
    struct event_info *e = c->event_info;
    netinfo_type *netinfo_ptr = pmux_netinfo(e);
    evbuffer_add_printf(c->buf, "get /echo %s/%s/%s\n", netinfo_ptr->app,
                        netinfo_ptr->service, netinfo_ptr->instance);
    event_base_once(base, c->fd, EV_WRITE, pmux_get_writecb, c, &connect_timeout);
}

static void pmux_reconnect(struct connect_info *c)
{
    check_base_thd();
    struct event_info *e = c->event_info;
    connect_info_free(c);
    if (e->fd == -1) {
        do_reconnect(-1, 0, e);
    }
}

static void pmux_connect(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct event_info *e = data;
    if (e->connect_ev) {
        event_del(e->connect_ev);
        e->connect_ev = NULL;
    }
    if (skip_connect(e)) {
        return;
    }
    struct connect_info *c = connect_info_new(e);
    ++e->distress_count;
    struct sockaddr_in sin = {0};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(get_portmux_port());
    socklen_t len = sizeof(sin);
    if (get_dedicated_conhost(e->host_node_ptr, &sin.sin_addr)) {
        hputs("get_dedicated_conhost failed\n");
        pmux_reconnect(c);
        return;
    }
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
    if (gbl_pmux_route_enabled && e->port != 0) {
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
    check_base_thd();
    int port = a->from_port;
    char *host = a->from_host_interned;
    netinfo_type *netinfo_ptr = a->netinfo_ptr;
    struct net_info *n = net_info_find(netinfo_ptr->service);
    if (n == NULL) {
        n = net_info_new(netinfo_ptr);
    }
    struct host_info *h = host_info_find(host);
    if (h == NULL) {
        h = host_info_new(host);
    }
    struct event_info *e = event_info_find(n, h);
    if (e == NULL) {
        e = event_info_new(n, h);
    }
    host_node_type *host_node_ptr = get_host_node_by_name_ll(netinfo_ptr, host);
    if (host_node_ptr == NULL) {
        host_node_ptr = add_to_netinfo(netinfo_ptr, host, port);
    }
    if (host_node_ptr == NULL) {
        return -1;
    }
    update_host_node_ptr(host_node_ptr, e);
    update_event_port(e, port);
    update_wire_hdrs(e);
    if (netinfo_ptr->new_node_rtn) {
        netinfo_ptr->new_node_rtn(netinfo_ptr, host, port);
    }
    hprintf("ACCEPTED NEW CONNECTION fd:%d\n", a->fd);
    host_connected(e, a->fd, 0);
    a->fd = -1;
    accept_info_free(a);
    return 0;
}

static int validate_host(struct accept_info *a)
{
    if (strcmp(a->from_host, gbl_myhostname) == 0) {
        logmsg(LOGMSG_WARN, "%s fd:%d invalid from:%s\n", __func__, a->fd,
               a->from_host);
        return -1;
    }
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
    return event_base_once(base, a->fd, EV_READ, read_long_hostname, a, NULL);
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
        sbuf2setbufsize(sb, a->netinfo_ptr->bufsz);
        do_appsock(a->netinfo_ptr, &a->ss, sb, b);
        a->fd = -1;
        accept_info_free(a);
        return;
    }
    a->buf = evbuffer_new();
    a->need = NET_CONNECT_MESSAGE_TYPE_LEN;
    if (event_base_once(base, fd, EV_READ, read_connect, a, NULL)) {
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

static int process_reg_reply(char *res, struct net_info *n, int unix_fd)
{
    int port;
    if (sscanf(res, "%d", &port) != 1) {
        return -1;
    }
    /* Accept connection on unix fd */
    evbuffer_free(n->unix_buf);
    n->unix_buf = NULL;
    event_free(n->unix_ev);
    n->unix_ev = event_new(base, unix_fd, EV_READ | EV_PERSIST, do_recvfd, n);
    logmsg(LOGMSG_INFO, "%s: svc:%s accepting on unix socket fd:%d\n",
           __func__, n->service, unix_fd);
    event_add(n->unix_ev, NULL);
    if (n->port == port && n->listener) {
        return 0;
    }
    /* Accept connection on new port */
    if (n->port != port) {
        logmsg(LOGMSG_ERROR, "%s: PORT CHANGED %d->%d\n", __func__, n->port, port);
    }
    if (n->listener) {
        evconnlistener_free(n->listener);
        n->listener = NULL;
    }
    struct sockaddr_in sin = {0};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    socklen_t len = sizeof(sin);
    struct sockaddr *s  = (struct sockaddr *)&sin;
    unsigned flags;
    flags = LEV_OPT_REUSEABLE | LEV_OPT_CLOSE_ON_FREE;
    n->listener = evconnlistener_new_bind(base, do_accept, n, flags, SOMAXCONN, s, len);
    if (n->listener == NULL) {
        return -1;
    }
    int fd = evconnlistener_get_fd(n->listener);
    update_net_info(n, fd, port);
    logmsg(LOGMSG_INFO, "%s svc:%s accepting on port:%d fd:%d\n", __func__,
           n->service, port, fd);
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
    errno = 0;
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
    if (process_reg_reply(res, n, fd) != 0) {
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
    errno = 0;
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
    if (n->unix_ev) {
        logmsg(LOGMSG_FATAL, "%s: LEFTOVER unix_ev for:%s\n", __func__, n->service);
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
    evbuffer_add_printf(n->unix_buf, "reg %s/%s/%s\n", n->app, n->service, n->instance);
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
    if (fd == -1) {
        return;
    }
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
    char *host = host_node_ptr->host;
    if (strcmp(host, gbl_myhostname) == 0) {
        return;
    }
    struct net_info *n = net_info_find(netinfo_ptr->service);
    if (n == NULL) {
        n = net_info_new(netinfo_ptr);
    }
    struct host_info *h = host_info_find(host);
    if (h == NULL) {
        h = host_info_new(host);
    }
    struct event_info *e = event_info_find(n, h);
    if (e != NULL) {
        update_host_node_ptr(host_node_ptr, e);
        return;
    }
    e = event_info_new(n, h);
    update_host_node_ptr(host_node_ptr, e);
    pmux_connect(-1, 0, e);
    /* Tell all nodes about this host */
    LIST_FOREACH(e, &n->event_list, net_list_entry) {
        if (e->got_hello) {
            write_hello_reply(netinfo_ptr, e->host_node_ptr);
        }
    }
}

static void init_base(pthread_t *t, struct event_base **bb,
                          void *(*f)(void *), const char *who)
{
    struct net_dispatch_info *info;
    info = calloc(1, sizeof(struct net_dispatch_info));
    *bb = event_base_new();
    info->who = who;
    info->base = *bb;
    Pthread_create(t, NULL, f, info);
    Pthread_detach(*t);
}

static void check_wr_full(struct event_info *e)
    /* TODO check flush buf instead */
{
    if (e->wr_full || e->wr_buf == NULL) {
        return;
    }
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    uint64_t max_bytes = netinfo_ptr->max_bytes;
    uint64_t outstanding = evbuffer_get_length(e->wr_buf);
    if (max_bytes && outstanding > max_bytes) {
        e->wr_full = 1;
        hprintf("SUSPENDING WR bytes:%lu (max:%lu)\n", outstanding, max_bytes);
    }
}

static void flush_evbuffer_int(struct event_info *e)
{
    int rc;
    int want = 0;
    int total = 0;
    do {
        rc = evbuffer_write(e->flush_buf, e->fd);
        if (rc > 0) total += rc;
        want = evbuffer_get_length(e->flush_buf);
    } while (want && rc > 0);
    if (rc <= 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        hprintf("writev failed %d:%s\n", errno, strerror(errno));
        evbuffer_free(e->flush_buf);
        e->flush_buf = NULL;
        if (e->flush_pending) {
            event_del(e->flush_pending);
            e->flush_pending = NULL;
        }
        reconnect(e);
        return;
    }
    if (total > 0) {
        e->sent_at = time(NULL);
    }
    if (want) {
       if (!e->flush_pending) {
            e->flush_pending = event_new(wr_base, e->fd, EV_WRITE|EV_PERSIST, flushcb, e);
            event_add(e->flush_pending, NULL);
       }
    } else if (e->flush_pending) {
        event_del(e->flush_pending);
        e->flush_pending = NULL;
    }
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    uint64_t max_bytes = netinfo_ptr->max_bytes;
    if (e->wr_full && max_bytes) {
        if (want <= max_bytes * resume_lvl) {
            e->wr_full = 0;
            hprintf("RESUMING WR bytes:%d (max:%lu)\n", want, max_bytes);
        }
    }
}

static void flush_evbuffer(struct event_info *e)
{
    Pthread_mutex_lock(&e->flush_lk);
    if (e->fd != -1 && e->flush_buf) {
        evbuffer_add_buffer(e->flush_buf, e->wr_buf);
        if (!e->flush_pending) {
            e->flush_pending = event_new(wr_base, e->fd, EV_WRITE | EV_PERSIST, flushcb, e);
            event_add(e->flush_pending, NULL);
        }
    }
    Pthread_mutex_unlock(&e->flush_lk);
}

static void flushcb(int fd, short what, void *data)
{
    struct event_info *e = data;
    Pthread_mutex_lock(&e->flush_lk);
    if (!e->flush_buf) {
        abort();
    }
    if (fd != e->fd) {
        abort();
    }
    flush_evbuffer_int(e);
    Pthread_mutex_unlock(&e->flush_lk);
}

static inline int skip_send(struct event_info *e, int nodrop, int check_hello)
{
    return strcmp(e->host, gbl_myhostname) == 0 || e->fd == -1 ||
           e->decomissioned || (e->wr_full && !nodrop) ||
           (check_hello && !e->got_hello);
}

static void net_send_memcpy(struct event_info *e, int sz, int n, void **buf, int *len, int *type)
{
    if (!e->wr_buf) {
        return;
    }
    sz += n * (e->wirehdr_len + sizeof(net_send_message_header));
    struct iovec v[1];
    const int nv = evbuffer_reserve_space(e->wr_buf, sz, v, 1);
    if (nv != 1) {
        abort();
    }
    uint8_t *b = v[0].iov_base;
    v[0].iov_len = sz;
    net_send_message_header hdr = {0};
    for (int i = 0; i < n; ++i) {
        hdr.usertype = type[i];
        hdr.datalen = len[i];
        b = memcpy(b, e->wirehdr[WIRE_HEADER_USER_MSG], e->wirehdr_len) + e->wirehdr_len;
        b = net_send_message_header_put(&hdr, b, b + sizeof(net_send_message_header));
        b = memcpy(b, buf[i], len[i]) + len[i];
    }
    evbuffer_commit_space(e->wr_buf, v, 1);
}

static void net_send_addref(struct event_info *e, struct net_msg *msg)
{
    struct evbuffer *buf = e->wr_buf;
    if (!e->wr_buf) {
        return;
    }
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
    }
}

static void setup_base(void)
{
    init_base(&base_thd, &base, net_dispatch, "main");
    if (dedicated_timer) {
        init_base(&timer_thd, &timer_base, net_dispatch, "timer");
    } else {
        timer_thd = base_thd;
        timer_base = base;
    }

    if (writer_policy == POLICY_NONE) {
        single.wrthd = base_thd;
        single.wrbase = base;
    } else if (writer_policy == POLICY_SINGLE) {
        init_base(&single.wrthd, &single.wrbase, net_dispatch, "write");
    }

    if (reader_policy == POLICY_NONE) {
        single.rdthd = base_thd;
        single.rdbase = base;
    } else if (reader_policy == POLICY_SINGLE) {
        init_base(&single.rdthd, &single.rdbase, net_dispatch, "read");
    }

    if (akq_policy == POLICY_SINGLE) {
        single.akq = setup_akq();
    }

    logmsg(LOGMSG_USER, "Libevent %s with backend method %s\n",
           event_get_version(), event_base_get_method(base));
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
    setup_base();
    event_hash = hash_init_str(offsetof(struct event_hash_entry, key));
    net_stop = 0;
}

#if 0
static void user_event_func(int fd, short what, void *data)
{
    struct user_event_info *info = data;
    info->func(fd, what, info->data);
}

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

/********************/
/* PUBLIC INTERFACE */
/********************/

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
    struct host_info *h = host_info_find(host);
    if (h == NULL) {
        return;
    }
    struct event_info *e;
    LIST_FOREACH(e, &h->event_list, host_list_entry) {
        if (e->decomissioned) {
            continue;
        } else {
            set_decom(e);
            do_close(e, send_decom_all);
        }
    }
}

void stop_event_net(void)
{
    if (net_stop) {
        return;
    }
    Pthread_once(&exit_once, exit_once_func);
}

int write_connect_message_evbuffer(host_node_type *host_node_ptr,
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

int write_list_evbuffer(host_node_type *host_node_ptr, int type,
                           const struct iovec *iov, int n, int flags)
{
    if (net_stop) {
        return 0;
    }
    int rc;
    int nodrop = flags & WRITE_MSG_NOLIMIT;
    int nodelay = flags & WRITE_MSG_NODELAY;
    struct event_info *e = host_node_ptr->event_info;
    struct evbuffer *buf = evbuffer_new();
    if (buf == NULL) {
        rc = -1;
        goto out;
    }
    int sz = e->wirehdr_len;
    for (int i = 0; i < n; ++i) {
        sz += iov[i].iov_len;
    }
    struct iovec v[1];
    const int nv = evbuffer_reserve_space(buf, sz, v, 1);
    if (nv != 1) {
        rc = -1;
        goto out;
    }
    v[0].iov_len = sz;
    uint8_t *b = v[0].iov_base;
    b = memcpy(b, e->wirehdr[type], e->wirehdr_len) + e->wirehdr_len;
    for (int i = 0; i < n; ++i) {
        b = memcpy(b, iov[i].iov_base, iov[i].iov_len) + iov[i].iov_len;
    }
    evbuffer_commit_space(buf, v, nv);
    if (skip_send(e, nodrop, 0)) {
        rc = -2;
        goto out;
    }
    Pthread_mutex_lock(&e->wr_lk);
    if (e->wr_buf) {
        rc = evbuffer_add_buffer(e->wr_buf, buf);
        if (rc != 0) {
            rc = -1;
        } else {
            if (nodelay) {
                flush_evbuffer(e);
            }
            check_wr_full(e);
        }
    } else {
        rc = -3;
    }
    Pthread_mutex_unlock(&e->wr_lk);
out:if (buf) {
        evbuffer_free(buf);
    }
    return rc;
}

int net_send_all_evbuffer(netinfo_type *netinfo_ptr, int n, void **buf,
                          int *len, int *type, int *flags)
{
#   define ADDREF_CUTOFF 256
    if (net_stop) {
        return 0;
    }
    int sz = 0;
    int nodrop = 0;
    int nodelay = 0;
    struct net_msg *msg = NULL;
    for (int i = 0; i < n; ++i) {
        sz += len[i];
        nodrop |= flags[i] & NET_SEND_NODROP;
        nodelay |= flags[i] & NET_SEND_NODELAY;
    }
    if (sz >= ADDREF_CUTOFF) {
        msg = net_msg_new(netinfo_ptr, n, buf, len, type, flags);
        if (msg == NULL) {
            return NET_SEND_FAIL_MALLOC_FAIL;
        }
    }
    struct net_info *ni = net_info_find(netinfo_ptr->service);
    struct event_info *e;
    LIST_FOREACH(e, &ni->event_list, net_list_entry) {
        if (skip_send(e, nodrop, 1)) {
            continue;
        }
        Pthread_mutex_lock(&e->wr_lk);
        if (msg) {
            net_send_addref(e, msg);
        } else {
            net_send_memcpy(e, sz, n, buf, len, type);
        }
        if (nodelay) {
            flush_evbuffer(e);
        }
        Pthread_mutex_unlock(&e->wr_lk);
    }
    if (msg) {
        net_msg_free(msg);
    }
    return 0;
}


int net_flush_evbuffer(host_node_type *host_node_ptr)
{
    if (net_stop) {
        return 0;
    }
    struct event_info *e = host_node_ptr->event_info;
    Pthread_mutex_lock(&e->wr_lk);
    if (e->wr_buf) {
        flush_evbuffer(e);
    }
    Pthread_mutex_unlock(&e->wr_lk);
    return 0;
}

int net_send_evbuffer(netinfo_type *netinfo_ptr, const char *host,
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
    struct event_info *e = entry->event_info;
    if (strcmp(e->host, gbl_myhostname) == 0) {
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
    net_send_message_header hdr, tmp = {
        .usertype = usertype,
        .datalen = total
    };
    net_send_message_header_put(&tmp, (uint8_t *)&hdr, (uint8_t*)(&hdr + 1));
    iov[0].iov_base = &hdr;
    iov[0].iov_len = sizeof(hdr);
    int write_flags = flags & NET_SEND_NODROP ? WRITE_MSG_NOLIMIT : 0;
    write_flags |= flags & NET_SEND_NODELAY ? WRITE_MSG_NODELAY : 0;
    int rc = write_list_evbuffer(e->host_node_ptr, WIRE_HEADER_USER_MSG, iov, n, write_flags);
    switch (rc) {
    case  0: return 0;
    case -1: return NET_SEND_FAIL_MALLOC_FAIL;
    case -2: return NET_SEND_FAIL_QUEUE_FULL;
    case -3: return NET_SEND_FAIL_NOSOCK;
    default: return NET_SEND_FAIL_WRITEFAIL;
    }
}
