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

#ifdef __sun
#  define BSD_COMP /* for FIONREAD */
#endif

#include <alloca.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
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

#include <bb_oscompat.h>
#include <berkdb/dbinc/rep_types.h>
#include <comdb2_atomic.h>
#include <compat.h>
#include <intern_strings.h>
#include <sys_wrap.h>
#include <logmsg.h>
#ifdef PER_THREAD_MALLOC
  #include <mem_net.h>
  #include <mem_override.h>
#endif
#include <net.h>
#include <net_appsock.h>
#include <net_int.h>
#include <plhash.h>
#include <portmuxapi.h>
#include <ssl_bend.h>
#include <ssl_evbuffer.h>
#include <ssl_glue.h>
#include <timer_util.h>

#include <connectmsg.pb-c.h>
#include <hostname_support.h>

#ifndef FIONREAD
#  error FIONREAD not available
#endif

#define NEED_LSN_DEF
#include <rep_qstat.h>

#ifndef likely
#  if defined(__GNUC__)
#    define likely(x) __builtin_expect(!!(x), 1)
#    define unlikely(x) __builtin_expect(!!(x), 0)
#  else
#    define likely(x) (x)
#    define unlikely(x) (x)
#  endif
#endif

#define SBUF2UNGETC_BUF_MAX 8 /* See also, util/sbuf2.c */
#define MAX_DISTRESS_COUNT 3

#define hprintf_lvl LOGMSG_USER
#define hprintf_format(a) "[%.3s %-8s fd:%-4d %3s %24s] " a, e->service, e->host, e->fd, e->ssl_data ? "TLS" : "", __func__
#define distress_logmsg(...)                                                   \
    do {                                                                       \
        if (e->distressed) {                                                   \
            break;                                                             \
        } else if (e->distress_count >= MAX_DISTRESS_COUNT) {                  \
            logmsg(hprintf_lvl, hprintf_format("ENTERING DISTRESS MODE\n"));   \
            e->distressed = 1;                                                 \
            break;                                                             \
        }                                                                      \
        logmsg(hprintf_lvl, __VA_ARGS__);                                      \
    } while (0)

#define no_distress_logmsg(...) logmsg(hprintf_lvl, __VA_ARGS__)

#define hprintf(a, ...) distress_logmsg(hprintf_format(a), __VA_ARGS__)
#define hputs(a) distress_logmsg(hprintf_format(a))

/* nd: ignore distressed state and print */
#define hprintf_nd(a, ...) no_distress_logmsg(hprintf_format(a), __VA_ARGS__)
#define hputs_nd(a) no_distress_logmsg(hprintf_format(a))

int gbl_pb_connectmsg = 1;
int gbl_libevent_rte_only = 0;

extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern char *gbl_myhostname;
extern int gbl_accept_on_child_nets;
extern int gbl_create_mode;
extern int gbl_debug_pb_connectmsg_dbname_check;
extern int gbl_debug_pb_connectmsg_gibberish;
extern int gbl_exit;
extern int gbl_fullrecovery;
extern int gbl_pmux_route_enabled;
extern int gbl_ssl_allow_localhost;
extern int gbl_revsql_debug;
extern void pstack_self(void);

static struct timeval one_sec = {1, 0};
static struct timeval connect_timeout = {1, 0};

enum policy {
    POLICY_NONE,
    POLICY_SINGLE,
    POLICY_PER_NET,
    POLICY_PER_HOST,
    POLICY_PER_EVENT
};

static int dedicated_appsock = 1;
static int dedicated_timer = 1;
static int dedicated_fdb = 1;
static int dedicated_dist = 1;
static enum policy reader_policy = POLICY_PER_NET;
static enum policy writer_policy = POLICY_PER_HOST;

struct policy_info {
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
static struct timeval timer_tick;

static pthread_t fdb_thd;
static struct event_base *fdb_base;
static struct timeval fdb_tick;

static pthread_t dist_thd;
static struct event_base *dist_base;
static struct timeval dist_tick;

#define NUM_APPSOCK_RD 4
static pthread_t appsock_thd[NUM_APPSOCK_RD];
static struct event_base *appsock_base[NUM_APPSOCK_RD];
static struct timeval appsock_tick[NUM_APPSOCK_RD];

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

#define check_base_thd() check_thd(base_thd)
#define check_timer_thd() check_thd(timer_thd)
#define check_fdb_thd() check_thd(fdb_thd)
#define check_dist_thd() check_thd(dist_thd);
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

static void *rd_worker(void *);
static void do_add_host(int, short, void *);
static void do_open(int, short, void *);
static void pmux_connect(int, short, void *);
static void pmux_reconnect(struct connect_info *);
static void resume_read(int, short, void *);
static void unix_connect(int, short, void *);
static int wr_connect_msg_proto(struct event_info *);

static struct timeval reconnect_time(int retry)
{
    time_t sec;
    suseconds_t usec = (random() % 900000) + 100000;
    if (retry <= 8) {
        sec = 0;
    } else if (retry <= 16) {
        sec = 1;
    } else if (retry <= 32) {
        sec = 10 + (usec % 51); /* (10, 60) */
    } else {
        sec = 60 + (usec % 241); /* (60, 300) */
    }
    struct timeval t = {sec, usec};
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

void make_server_socket(int fd)
{
    evutil_make_socket_nonblocking(fd);
    make_socket_nolinger(fd);
    make_socket_nodelay(fd);
    make_socket_keepalive(fd);
    make_socket_reusable(fd);
}

static int get_nonblocking_socket(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        logmsgperror("get_nonblocking_socket socket");
    } else {
        make_server_socket(fd);
    }
    return fd;
}

static hash_t *appsock_hash;
struct appsock_info {
    const char *key;
    event_callback_fn cb;
};

static struct appsock_info *get_appsock_info(const char *key)
{
    return hash_find_readonly(appsock_hash, &key);
}

struct host_info {
    LIST_HEAD(, event_info) event_list;
    LIST_ENTRY(host_info) entry;
    struct interned_string *host_interned;
    char *host;
    struct policy_info per_host;
};

struct host_connected_info;

struct event_info {
    host_node_type *host_node_ptr;
    struct host_connected_info *host_connected;
    struct host_connected_info *host_connected_pending;
    LIST_ENTRY(event_info) host_list_entry;
    LIST_ENTRY(event_info) net_list_entry;
    int fd;
    int port;
    struct interned_string *host_interned;
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
    int got_hello_reply;
    struct ssl_data *ssl_data;

    /* read */
    ssize_t (*readv)(struct event_info *);
    int readv_gen;
    time_t rd_full;
    pthread_t rd_worker_thd;
    pthread_mutex_t rd_lk;
    pthread_cond_t rd_cond;
    uint8_t *rd_buf;
    size_t rd_worker_sz;
    struct evbuffer *readv_buf;
    struct event *rd_ev;
    size_t need;
    int state;
    wire_header_type hdr;
    net_send_message_header msg;
    net_ack_message_payload_type ack;

    /* write */
    ssize_t (*writev)(struct event_info *);
    pthread_mutex_t wr_lk;
    struct evbuffer *flush_buf;
    struct evbuffer *wr_buf;
    struct event *wr_ev;
    time_t wr_full;
};

#define EVENT_HASH_KEY_SZ 128
struct event_hash_entry {
    char key[EVENT_HASH_KEY_SZ];
    struct event_info *e;
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
    struct timeval *tick;
};

struct user_msg_info {
    int gen;
    struct event_info *e;
    net_send_message_header msg;
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
}

static void host_node_close(host_node_type *host_node_ptr)
{
    check_base_thd();
    host_node_ptr->fd = -1;
    host_node_ptr->got_hello = 0;
    host_node_ptr->closed = 1;
}

static void shutdown_close(int fd)
{
    if (fd <= 2) {
        logmsg(LOGMSG_FATAL, "%s closing fd:%d\n", __func__, fd);
        abort();
    }
    shutdown(fd, SHUT_RDWR);
    Close(fd);
}

static void event_tick(int dummyfd, short what, void *arg)
{
    struct net_dispatch_info *n = arg;
    if (n->tick) gettimeofday(n->tick, NULL);
}

static void *do_pstack(void *arg)
{
    pstack_self();
    return NULL;
}

#define timeval_to_ms(x) x.tv_sec * 1000 + x.tv_usec / 1000
int gbl_timer_warn_interval = 1500; //msec. To disable check, set to 0.
int gbl_timer_pstack_interval =  5 * 60; //sec. To disable pstack, but keep monitoring, set to 0.
extern struct timeval last_timer_pstack;
static struct timeval last_timer_check;
void check_timers(void)
{
    if (gbl_timer_warn_interval == 0) return;

    int ms, need_pstack = 0;
    struct timeval now, diff;
    gettimeofday(&now, NULL);

    timersub(&now, &last_timer_check, &diff);
    ms = timeval_to_ms(diff);
    if (ms >= gbl_timer_warn_interval) {
        logmsg(LOGMSG_WARN, "DELAYED TIMER CHECK:%dms\n", ms);
        need_pstack = 1;
    }

    timersub(&now, &timer_tick, &diff);
    ms = timeval_to_ms(diff);
    if (ms >= gbl_timer_warn_interval) {
        logmsg(LOGMSG_WARN, "LONG TIMER TICK:%dms\n", ms);
        need_pstack = 1;
    }

    timersub(&now, &fdb_tick, &diff);
    ms = timeval_to_ms(diff);
    if (ms >= gbl_timer_warn_interval) {
        logmsg(LOGMSG_WARN, "LONG FDB TICK:%dms\n", ms);
        need_pstack = 1;
    }

    timersub(&now, &dist_tick, &diff);
    ms = timeval_to_ms(diff);
    if (ms >= gbl_timer_warn_interval) {
        logmsg(LOGMSG_WARN, "LONG DIST TICK:%dms\n", ms);
        need_pstack = 1;
    }

    int thds = dedicated_appsock ? NUM_APPSOCK_RD : 0;
    for (int i = 0; i < thds; ++i) {
        timersub(&now, &appsock_tick[i], &diff);
        ms = timeval_to_ms(diff);
        if (ms < gbl_timer_warn_interval) continue;
        logmsg(LOGMSG_WARN, "LONG APPSOCK TICK:%dms %p\n", ms, (void*) appsock_thd[i]);
        need_pstack = 1;
    }

    last_timer_check = now;
    if (!need_pstack) return;
    timersub(&now, &last_timer_pstack, &diff);
    if (gbl_timer_pstack_interval == 0 || diff.tv_sec < gbl_timer_pstack_interval) return;
    logmsg(LOGMSG_WARN, "%s: Last pstack:%lds. Generating pstack\n", __func__, diff.tv_sec);
    pthread_t t;
    Pthread_create(&t, NULL, do_pstack, NULL);
    Pthread_detach(t);
}

static __thread struct event_base *current_base;
static void *net_dispatch(void *arg)
{
    struct net_dispatch_info *n = arg;
    comdb2_name_thread(n->who);

    current_base = n->base;
    struct event *ev = event_new(n->base, -1, EV_PERSIST, event_tick, n);
    struct timeval one = {1, 0};

    ENABLE_PER_THREAD_MALLOC(n->who);

    event_add(ev, &one);
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

static void init_base_priority(pthread_t *t, struct event_base **bb, const char *who, int priority,
                               struct timeval *tick)
{
    struct net_dispatch_info *info = calloc(1, sizeof(struct net_dispatch_info));
    *bb = event_base_new();
    if (priority) {
        event_base_priority_init(*bb, priority);
    }
    info->who = who;
    info->base = *bb;
    info->tick = tick;
    Pthread_create(t, NULL, net_dispatch, info);
    Pthread_detach(*t);
}

static void init_base(pthread_t *t, struct event_base **bb, const char *who)
{
    init_base_priority(t, bb, who, 0, NULL);
}

static struct host_info *host_info_new(char *host)
{
    check_base_thd();
    struct host_info *h = calloc(1, sizeof(struct host_info));
    h->host_interned = intern_ptr(host);
    h->host = h->host_interned->str;
    if (reader_policy == POLICY_PER_HOST) {
        init_base(&h->per_host.rdthd, &h->per_host.rdbase, h->host);
    }
    if (writer_policy == POLICY_PER_HOST) {
        init_base(&h->per_host.wrthd, &h->per_host.wrbase, h->host);
    }
    LIST_INIT(&h->event_list);
    LIST_INSERT_HEAD(&host_list, h, entry);
    return h;
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
    size_t rd_max;
    size_t wr_max;
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
    // Low during catchup; increase after 'I AM READY.'
    // See increase_net_buf()
    n->rd_max = n->wr_max = MB(8);
    if (reader_policy == POLICY_PER_NET) {
        init_base(&n->per_net.rdthd, &n->per_net.rdbase, n->service);
    }
    if (writer_policy == POLICY_PER_NET) {
        init_base(&n->per_net.wrthd, &n->per_net.wrbase, n->service);
    }
    LIST_INIT(&n->event_list);
    LIST_INSERT_HEAD(&net_list, n, entry);
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
            memcpy(name, gbl_myhostname, fromlen);
            name += fromlen;
        } else {
            memcpy(e->wirehdr[i]->fromhost, gbl_myhostname, fromlen);
        }
        e->wirehdr[i]->fromport = htonl(e->net_info->port);
        if (tolen > HOSTNAME_LEN) {
            sprintf(e->wirehdr[i]->tohost, ".%d", tolen);
            memcpy(name, e->host, tolen);
        } else {
            memcpy(e->wirehdr[i]->tohost, e->host, tolen);
        }
        e->wirehdr[i]->toport = htonl(e->port);
        e->wirehdr[i]->type = htonl(i);
    }
}

static struct event_info *event_info_new(struct net_info *n, struct host_info *h)
{
    check_base_thd();
    struct event_info *e = calloc(1, sizeof(struct event_info));
    e->fd = -1;
    e->host_interned = h->host_interned;
    e->host = h->host;
    e->service = n->service;
    e->host_info = h;
    e->net_info = n;
    e->readv_buf = evbuffer_new();
    setup_wire_hdrs(e);
    struct event_hash_entry *entry = malloc(sizeof(struct event_hash_entry));
    make_event_hash_key(entry->key, n->service, h->host);
    entry->e = e;
    Pthread_mutex_lock(&event_hash_lk);
    hash_add(event_hash, entry);
    Pthread_mutex_unlock(&event_hash_lk);
    if (reader_policy == POLICY_PER_EVENT) {
        init_base(&e->per_event.rdthd, &e->per_event.rdbase, entry->key);
    }
    if (writer_policy == POLICY_PER_EVENT) {
        init_base(&e->per_event.wrthd, &e->per_event.wrbase, entry->key);
    }
    /* set up rd_worker */
    Pthread_mutex_init(&e->rd_lk, NULL);
    Pthread_cond_init(&e->rd_cond, NULL);
    Pthread_mutex_lock(&e->rd_lk);
    pthread_attr_t attr;
    Pthread_attr_init(&attr);
    Pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    Pthread_attr_setstacksize(&attr, 1 * 1024 * 1024);
    Pthread_create(&e->rd_worker_thd, &attr, rd_worker, e);
    Pthread_attr_destroy(&attr);

    Pthread_cond_wait(&e->rd_cond, &e->rd_lk); /* wait for ready signal from rd_worker() */
    Pthread_mutex_unlock(&e->rd_lk);
    Pthread_mutex_init(&e->wr_lk, NULL);
    LIST_INSERT_HEAD(&h->event_list, e, host_list_entry);
    LIST_INSERT_HEAD(&n->event_list, e, net_list_entry);
    return e;
}

struct host_connected_info {
    int fd;
    struct event_info *e;
    int connect_msg;
    struct ssl_data *ssl_data;
};

static struct host_connected_info *
host_connected_info_new(struct event_info *e, int fd, int connect_msg, struct ssl_data *ssl_data)
{
    struct host_connected_info *info = calloc(1, sizeof(struct host_connected_info));
    info->e = e;
    info->fd = fd;
    info->connect_msg = connect_msg;
    info->ssl_data = ssl_data;
    return info;
}

static void host_connected_info_free(struct host_connected_info *info)
{
    if (info->ssl_data) {
        ssl_data_free(info->ssl_data);
    }
    free(info);
}

struct accept_info {
    int fd;
    int secure; /* whether connection is routed from a secure pmux port */
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
    struct event *ev;
    int uses_proto;
    char *dbname;
    struct ssl_data *ssl_data;
    char *origin;
    TAILQ_ENTRY(accept_info) entry;
};

static int pending_connections; /* accepted but didn't receive first-byte */
static int max_pending_connections = 1024;
static TAILQ_HEAD(, accept_info) accept_list = TAILQ_HEAD_INITIALIZER(accept_list);
static void do_read(int, short, void *);
static void accept_info_free(struct accept_info *);

static int close_oldest_pending_connection(void)
{
    struct accept_info *a = TAILQ_FIRST(&accept_list);
    if (!a) {
        return -1;
    }
    accept_info_free(a);
    logmsg(LOGMSG_USER, "%s closed oldest pending connection [outstanding:%d]\n", __func__, pending_connections);
    return 0;
}

static struct accept_info *accept_info_new(netinfo_type *netinfo_ptr, struct sockaddr_in *addr, int fd, int secure)
{
    check_base_thd();
    if (pending_connections > max_pending_connections) {
        close_oldest_pending_connection();
    }
    ++pending_connections;
    struct accept_info *a = calloc(1, sizeof(struct accept_info));
    a->netinfo_ptr = netinfo_ptr;
    a->ss = *addr;
    a->fd = fd;
    a->secure = secure;
    a->ev = event_new(base, fd, EV_READ, do_read, a);
    event_add(a->ev, NULL);
    TAILQ_INSERT_TAIL(&accept_list, a, entry);
    return a;
}

static void accept_info_free(struct accept_info *a)
{
    check_base_thd();
    TAILQ_REMOVE(&accept_list, a, entry);
    --pending_connections;
    if (a->ev) {
        event_free(a->ev);
    }
    if (a->fd != -1) {
        shutdown_close(a->fd);
    }
    if (a->buf) {
        evbuffer_free(a->buf);
    }
    if (a->ssl_data) {
        ssl_data_free(a->ssl_data);
    }
    free(a->from_host);
    free(a->to_host);
    free(a->dbname);
    free(a);
}

struct connect_info {
    int fd;
    struct evbuffer *buf;
    struct event_info *e;
};

static struct connect_info *connect_info_new(struct event_info *e)
{
    struct connect_info *c = calloc(1, sizeof(struct connect_info));
    c->fd = -1;
    c->e = e;
    c->buf = evbuffer_new();
    return c;
}

static void connect_info_free(struct connect_info *c)
{
    if (c->fd != -1) {
        struct event_info *e = c->e;
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
    e->host_node_ptr->decom_flag = 1;
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

struct disable_info {
    struct event_info *e;
    event_callback_fn func;
};

static void do_disable_write(struct event_info *e)
{
    check_wr_thd();
    if (e->wr_ev) {
        event_free(e->wr_ev);
        e->wr_ev = NULL;
    }
    if (e->flush_buf) {
        evbuffer_free(e->flush_buf);
        e->flush_buf = NULL;
    }
    if (e->wr_buf) {
        evbuffer_free(e->wr_buf);
        e->wr_buf = NULL;
    }
    e->got_hello = 0;
    e->got_hello_reply = 0;
}

static void disable_ssl(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct disable_info *d = data;
    struct event_info *e = d->e;
    if (e->ssl_data) {
        ssl_data_free(e->ssl_data);
        e->ssl_data = NULL;
    }
    d->func(-1, 0, e); /* -> do_reconnect, do_open */
    free(d);
}

static void disable_write(int dummyfd, short what, void *data)
{
    struct disable_info *d = data;
    struct event_info *e = d->e;
    Pthread_mutex_lock(&e->wr_lk);
    do_disable_write(e);
    if (e->fd != -1) {
        hprintf("CLOSING CONNECTION fd:%d\n", e->fd);
        shutdown_close(e->fd);
        e->fd = -1;
    }
    Pthread_mutex_unlock(&e->wr_lk);
    evtimer_once(base, disable_ssl, d);
}

static void do_disable_read(struct event_info *e)
{
    check_rd_thd();
    ++e->readv_gen;
    if (e->readv_buf) {
        evbuffer_free(e->readv_buf);
        e->readv_buf = NULL;
    }
    if (e->rd_ev) {
        event_free(e->rd_ev);
        e->rd_ev = NULL;
    }
}

static void disable_read(int dummyfd, short what, void *data)
{
    struct disable_info *d = data;
    struct event_info *e = d->e;
    Pthread_mutex_lock(&e->rd_lk);
    do_disable_read(e);
    Pthread_mutex_unlock(&e->rd_lk);
    evtimer_once(wr_base, disable_write, d);
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
    struct disable_info *d = data;
    struct event_info *e = d->e;
    do_disable_heartbeats(e);
    evtimer_once(rd_base, disable_read, d);
}

static void do_host_close(int dummyfd, short what, void *data)
{
    struct disable_info *d = data;
    struct event_info *e = d->e;
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    if (netinfo_ptr->hostdown_rtn) { /* net_hostdown_rtn or net_osql_nodedwn */
        netinfo_ptr->hostdown_rtn(netinfo_ptr, e->host_interned);
    }
    host_node_close(e->host_node_ptr);
    evtimer_once(timer_base, disable_heartbeats, d);
}

static void do_close(struct event_info *e, event_callback_fn func)
{
    if (e->fd != -1) hputs("CLOSE CONNECTION\n");
    struct disable_info *i = malloc(sizeof(struct disable_info));
    i->e = e;
    i->func = func;
    evtimer_once(base, do_host_close, i);
}

static int skip_connect(struct event_info *e)
{
    if (e->decomissioned) {
        hputs_nd("SKIPPING DECOM HOST\n");
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
        hputs("HAVE PENDING RECONNECT\n");
        return;
    }
    struct timeval t = reconnect_time(e->distress_count);
    hprintf("RECONNECT IN %lds.%ldus\n", t.tv_sec, (long int)t.tv_usec);
    e->connect_ev = event_new(base, -1, EV_TIMEOUT, pmux_connect, e);
    event_add(e->connect_ev, &t);
}

static void reconnect(struct event_info *e)
{
    if (gbl_exit) {
        return;
    }
    hputs("CLOSE AND RECONNECT\n");
    do_close(e, do_reconnect);
}

static void check_rd_full(struct event_info *e)
{
    if (e->rd_full) return;
    size_t max_bytes = e->net_info->rd_max;
    size_t outstanding = evbuffer_get_length(e->readv_buf) + ATOMIC_LOAD64(e->rd_worker_sz);
    if (outstanding < max_bytes) return;
    e->rd_full = time(NULL);
    event_del(e->rd_ev);
    hprintf("SUSPENDING RD outstanding:%zumb\n", max_bytes / MB(1));
}

static void check_wr_full(struct event_info *e)
{
    if (e->wr_full) return;
    size_t max_bytes = e->net_info->wr_max;
    size_t outstanding = evbuffer_get_length(e->flush_buf) + evbuffer_get_length(e->wr_buf);
    if (outstanding < max_bytes) return;
    e->wr_full = time(NULL);
    hprintf("SUSPENDING WR outstanding:%zumb\n", max_bytes / MB(1));
}

static ssize_t writev_ciphertext(struct event_info *e)
{
    return wr_ssl_evbuffer(e->ssl_data, e->wr_buf);
}

static ssize_t writev_plaintext(struct event_info *e)
{
    return evbuffer_write(e->wr_buf, e->fd);
}

static void writecb(int fd, short what, void *data)
{
    struct event_info *e = data;
    Pthread_mutex_lock(&e->wr_lk);
    if (fd != e->fd || !e->flush_buf || !e->wr_buf) abort(); /* sanity check */
    evbuffer_add_buffer(e->wr_buf, e->flush_buf);
    if (e->host_node_ptr) {
        e->host_node_ptr->enque_count = 0;
        e->host_node_ptr->enque_bytes = 0;
    }
    Pthread_mutex_unlock(&e->wr_lk);
    size_t len = evbuffer_get_length(e->wr_buf);
    while (len) {
        int rc = e->writev(e); // -> writev_plaintext
        if (rc <= 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                hprintf("writev rc:%d errno:%d:%s\n", rc, errno, strerror(errno));
                Pthread_mutex_lock(&e->wr_lk);
                do_disable_write(e);
                Pthread_mutex_unlock(&e->wr_lk);
                reconnect(e);
            }
            return;
        }
        e->sent_at = time(NULL);
        Pthread_mutex_lock(&e->wr_lk);
        evbuffer_add_buffer(e->wr_buf, e->flush_buf);
        len = evbuffer_get_length(e->wr_buf);
        if (len == 0) {
            event_del(e->wr_ev);
            if (e->wr_full) {
                hprintf("RESUMING WR after:%ds\n", (int)(time(NULL) - e->wr_full));
                e->wr_full = 0;
            }
        }
        Pthread_mutex_unlock(&e->wr_lk);
    }
}

static void flush_evbuffer(struct event_info *e, int nodelay)
{
    size_t flush_threashold = e->net_info->wr_max / 2;
    if (nodelay || evbuffer_get_length(e->flush_buf) > flush_threashold) {
        event_add(e->wr_ev, NULL);
    }
    check_wr_full(e);
}

static void send_decom_all(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    struct net_info *n = e->net_info;
    struct event_info *to;
    LIST_FOREACH(to, &n->event_list, net_list_entry) {
        write_decom(n->netinfo_ptr, to->host_node_ptr, e->host, strlen(e->host), to->host);
    }
    hputs_nd("DECOMMISSIONED\n");
}

static void message_done(struct event_info *e)
{
    e->hdr.type = 0;
    e->need = e->wirehdr_len;
}

static int process_user_msg(struct event_info *e)
{
    net_send_message_header *msg = &e->msg;
    if (e->state == 0) {
        ++e->state;
        net_send_message_header_get(msg, e->rd_buf, e->rd_buf + sizeof(*msg));
        if (msg->usertype <= USER_TYPE_MIN || msg->usertype >= USER_TYPE_MAX) {
            hprintf("BAD USER MSG TYPE:%d (htonl:%d)\n", msg->usertype, htonl(msg->usertype));
            return -1;
        }
        if (msg->datalen) {
            e->need = msg->datalen;
            return 0;
        }
    }
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    NETFP *func = netinfo_ptr->userfuncs[msg->usertype].func;
    if (func == NULL) {
        /* Startup race without accept-on-child-net: Replication net is
         * ready and accepts offload connection but offload callbacks have not
         * registered yet. */
        hprintf("NO USERFUNC FOR USERTYPE:%d\n", msg->usertype);
        return -1;
    }
    char *host = e->host;
    struct interned_string *host_interned = e->host_interned;

    void *usrptr = netinfo_ptr->usrptr;
    ack_state_type ack = {
        .seqnum = msg->seqnum,
        .needack = msg->waitforack,
        .fromhost = host,
        .netinfo = netinfo_ptr,
    };
    func(&ack, usrptr, host, host_interned, msg->usertype, e->rd_buf, msg->datalen, 1);
    message_done(e);
    return 0;
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
    logmsg(LOGMSG_USER, "%s: STOP NET\n", __func__);
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
    if (dedicated_fdb) {
        stop_base(fdb_base);
    }
    if (dedicated_dist) {
        stop_base(dist_base);
    }
    if (dedicated_appsock) {
        for (int i = 0; i < NUM_APPSOCK_RD; ++i) {
            stop_base(appsock_base[i]);
        }
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

    LIST_FOREACH(n, &net_list, entry) {
        LIST_FOREACH(e, &n->event_list, net_list_entry) {
            Pthread_mutex_lock(&e->rd_lk);
            Pthread_cond_signal(&e->rd_cond);
            Pthread_mutex_unlock(&e->rd_lk);
        }
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
    logmsg(LOGMSG_USER, "%s: STOPPED NET\n", __func__);
}

static void heartbeat_check(int dummyfd, short what, void *data)
{
    check_timer_thd();
    struct event_info *e = data;
    if (e->rd_full) return;
    int diff = time(NULL) - e->recv_at;
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    if (diff >= netinfo_ptr->heartbeat_check_time) {
        hprintf("no data in %d seconds\n", diff);
        do_disable_heartbeats(e);
        reconnect(e);
    }
}

static void heartbeat_send(int dummyfd, short what, void *data)
{
    check_timer_thd();
    struct event_info *e = data;
    if (e->wr_full) return;
    int diff = time(NULL) - e->sent_at;
    if (!diff) return;
    if (diff < 10) {
        netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
        write_heartbeat(netinfo_ptr, e->host_node_ptr);
    }
    if (diff % 10 == 0) { /* reduce spew */
        hprintf("no data in %d seconds\n", diff);
    }
}

static void hello_hdr_common(struct event_info *e)
{
    uint32_t n;
    memcpy(&n, e->rd_buf, sizeof(n));
    e->need = htonl(n) - sizeof(n);
}

struct add_host_info {
    struct event_info *e;
    char *ihost;
    int port;
};

static void add_host_from_hello_msg(void *data)
{
    check_base_thd();
    struct add_host_info *i = data;
    struct event_info *e = i->e;
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    host_node_type *host_node_ptr = get_host_node_by_name_ll(netinfo_ptr, i->ihost);
    if (host_node_ptr) return;
    host_node_ptr = add_to_netinfo_ll(netinfo_ptr, i->ihost, i->port);
    if (!host_node_ptr) return;
    if (i->port) {
        host_node_ptr->port = i->port;
    }
    evtimer_once(base, do_add_host, host_node_ptr);
}

static int hello_msg_common(struct event_info *e)
{
    int rc = -1;
    uint32_t nn;
    uint8_t *buf = e->rd_buf;
    memcpy(&nn, buf, sizeof(nn));
    buf += sizeof(nn);
    const uint32_t n = htonl(nn);
    if (n > REPMAX) {
        hprintf("RECV'd BAD COUNT OF HOSTS:%u (max:%d)\n", n, REPMAX);
        return -1;
    }
    char **hosts = malloc(sizeof(char *) * n);
    for (uint32_t i = 0; i < n; ++i) {
        hosts[i] = malloc(HOSTNAME_LEN + 1);
        memcpy(hosts[i], buf, HOSTNAME_LEN);
        buf += HOSTNAME_LEN;
        hosts[i][HOSTNAME_LEN] = 0;
    }
    uint32_t *ports = malloc(sizeof(uint32_t) * n);
    for (uint32_t i = 0; i < n; ++i) {
        memcpy(&ports[i], buf, sizeof(uint32_t));
        buf += sizeof(ports[i]);
        ports[i] = htonl(ports[i]);
    }
    buf +=  (n * sizeof(uint32_t)); /* We have no use for node numbers */
    for (uint32_t i = 0; i < n; ++i) {
        int need_free = 0;
        char *host = hosts[i];
        if (*host == '.') {
            ++host;
            uint32_t s = atoi(host);
            if (s > HOST_NAME_MAX) {
                hprintf("RECV'd BAD HOSTNAME len:%u (max:%d)\n", s, HOST_NAME_MAX);
                goto out;
            }
            need_free = 1;
            host = malloc(s + 1);
            memcpy(host, buf, s);
            buf += s;
            host[s] = 0;
        }
        if (strcmp(host, gbl_myhostname) == 0) {
            if (need_free) free(host);
            continue;
        }
        struct host_info *hi = host_info_find(host);
        if (hi && event_info_find(e->net_info, hi)) {
            if (need_free) free(host);
            continue;
        }
        char *ihost = intern(host);
        if (need_free) free(host);
        netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
        if (netinfo_ptr->allow_rtn && !netinfo_ptr->allow_rtn(netinfo_ptr, ihost)) { /* net_allow_node */
            logmsg(LOGMSG_ERROR, "connection to host:%s not allowed\n", ihost);
            continue;
        }
        struct add_host_info info = {.e = e, .ihost = ihost, .port = ports[i]};
        run_on_base(base, add_host_from_hello_msg, &info);
    }
    set_hello_message(e);
    rc = 0;
out:free(ports);
    for (uint32_t i = 0; i < n; ++i) {
        free(hosts[i]);
    }
    free(hosts);
    return rc;
}

static void clear_distress(struct event_info *e)
{
    int distress_count = e->distress_count;
    e->distress_count = e->distressed = 0;
    if (distress_count >= MAX_DISTRESS_COUNT) {
        hprintf("LEAVING DISTRESS MODE (retries:%d)\n", distress_count);
    }
}

static int process_hello_msg(struct event_info *e)
{
    if (e->state == 0) {
        ++e->state;
        hello_hdr_common(e);
        return 0;
    }
    clear_distress(e);
    int send_reply = 0;
    if (!e->got_hello) {
        send_reply = 1;
        hputs("GOT HELLO\n");
    }
    if (hello_msg_common(e) != 0) {
        return -1;
    }
    if (send_reply) {
        hputs("WRITE HELLO REPLY\n");
        write_hello_reply(e->net_info->netinfo_ptr, e->host_node_ptr);
    }
    message_done(e);
    return 0;
}

static int process_hello_reply(struct event_info *e)
{
    if (e->state == 0) {
        ++e->state;
        hello_hdr_common(e);
        return 0;
    }
    clear_distress(e);
    if (!e->got_hello_reply) {
        e->got_hello_reply = 1;
        hputs("GOT HELLO REPLY\n");
    }
    if (hello_msg_common(e) != 0) {
        return -1;
    }
    message_done(e);
    return 0;
}

static int process_ack_no_payload(struct event_info *e)
{
    net_ack_message_type ack = {0};
    net_ack_message_type_get(&ack, e->rd_buf, e->rd_buf + sizeof(ack));
    host_node_type *host_node_ptr = e->host_node_ptr;
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
    return 0;
}

static int process_ack_with_payload(struct event_info *e)
{
    host_node_type *host_node_ptr = e->host_node_ptr;
    net_ack_message_payload_type *ack = &e->ack;
    if (e->state == 0) {
        ++e->state;
        net_ack_message_payload_type_get(ack, e->rd_buf, e->rd_buf + sizeof(*ack));
        e->need = ack->paylen;
        return 0;
    }
    void *p = malloc(ack->paylen);
    memcpy(p, e->rd_buf, ack->paylen);
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
    return 0;
}

static int process_decom_nodenum(struct event_info *e)
{
    uint32_t n;
    memcpy(&n, e->rd_buf, sizeof(n));
    n = htonl(n);
    decom(hostname(n));
    message_done(e);
    return 0;
}

static int process_decom_hostname(struct event_info *e)
{
    if (e->state == 0) {
        ++e->state;
        uint32_t n;
        memcpy(&n, e->rd_buf, sizeof(n));
        e->need = htonl(n);
        return 0;
    }
    char host[e->need + 1];
    memcpy(host, e->rd_buf, e->need);
    host[e->need] = 0;
    decom(host);
    message_done(e);
    return 0;
}

static int process_hdr(struct event_info *e)
{
    net_wire_header_get(&e->hdr, e->rd_buf, e->rd_buf + sizeof(wire_header_type));
    e->state = 0;
    switch (e->hdr.type) {
    case WIRE_HEADER_HEARTBEAT: message_done(e); return 0;
    case WIRE_HEADER_HELLO: e->need = sizeof(uint32_t); return 0;
    case WIRE_HEADER_DECOM: e->need = sizeof(uint32_t); return 0;
    case WIRE_HEADER_USER_MSG: e->need = NET_SEND_MESSAGE_HEADER_LEN; return 0;
    case WIRE_HEADER_ACK: e->need = NET_ACK_MESSAGE_TYPE_LEN; return 0;
    case WIRE_HEADER_HELLO_REPLY: e->need = sizeof(uint32_t); return 0;
    case WIRE_HEADER_DECOM_NAME: e->need = sizeof(uint32_t); return 0;
    case WIRE_HEADER_ACK_PAYLOAD: e->need = NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN; return 0;
    default: hprintf("UNKNOWN HDR:%d\n", e->hdr.type); return -1;
    }
}

static int process_payload(struct event_info *e)
{
    switch (e->hdr.type) {
    case WIRE_HEADER_HELLO: return process_hello_msg(e);
    case WIRE_HEADER_DECOM: return process_decom_nodenum(e);
    case WIRE_HEADER_USER_MSG: return process_user_msg(e);
    case WIRE_HEADER_ACK: return process_ack_no_payload(e);
    case WIRE_HEADER_HELLO_REPLY: return process_hello_reply(e);
    case WIRE_HEADER_DECOM_NAME: return process_decom_hostname(e);
    case WIRE_HEADER_ACK_PAYLOAD: return process_ack_with_payload(e);
    default: hprintf("UNKNOWN HDR:%d\n", e->hdr.type); return -1;
    }
}

static int process_net_msgs(struct event_info *e, struct evbuffer *buf, void **mbuf, size_t *msz)
{
    int rc;
    struct iovec v;
    do {
        const int need = e->need;
        if (evbuffer_get_contiguous_space(buf) >= need) {
            evbuffer_peek(buf, need, NULL, &v, 1);
            e->rd_buf = v.iov_base;
        } else {
            if (*msz < need) {
                *msz = need;
                free(*mbuf);
                *mbuf = malloc(need);
            }
            e->rd_buf = *mbuf;
            evbuffer_copyout(buf, e->rd_buf, need);
        }
        rc = e->hdr.type == 0 ? process_hdr(e) : process_payload(e);
        evbuffer_drain(buf, need);
        ATOMIC_ADD64(e->rd_worker_sz, -need);
        if (rc) break;
    } while (evbuffer_get_length(buf) >= e->need);
    return rc;
}

static void *rd_worker(void *data)
{
    struct event_info *e = data;
    char thdname[16];
    snprintf(thdname, sizeof(thdname), "rd:%s", e->host);
    comdb2_name_thread(thdname);

    netinfo_type *n = e->net_info->netinfo_ptr;
    if (n->start_thread_callback) {
        n->start_thread_callback(n->callback_data);
    }

    int gen = -1;
    size_t msz = 0;
    void *mbuf = NULL;
    struct evbuffer *buf = evbuffer_new();

    Pthread_mutex_lock(&e->rd_lk);
    Pthread_cond_signal(&e->rd_cond); // this allows event_info_new() to continue
    Pthread_cond_wait(&e->rd_cond, &e->rd_lk); // now we wait until readcb() has data

    while (1) {
        if (net_stop) break;
        if (!e->readv_buf) { // wait for connection
            Pthread_cond_wait(&e->rd_cond, &e->rd_lk);
            continue;
        }
        if (e->readv_gen != gen) {
            evbuffer_free(buf);
            buf = evbuffer_new();
            gen = e->readv_gen;
            e->rd_worker_sz = 0;
            message_done(e);
        }
        evbuffer_add_buffer(buf, e->readv_buf);
        e->rd_worker_sz = evbuffer_get_length(buf);
        if (e->rd_worker_sz < e->need) {
            if (e->rd_full) {
                hprintf("RESUMING RD after:%ds\n", (int)(time(NULL) - e->rd_full));
                e->rd_full = 0;
                evtimer_once(rd_base, resume_read, e);
            }
            Pthread_cond_wait(&e->rd_cond, &e->rd_lk);
            continue;
        }
        Pthread_mutex_unlock(&e->rd_lk);
        int rc = process_net_msgs(e, buf, &mbuf, &msz);
        if (msz > MB(4))  msz = 0;
        Pthread_mutex_lock(&e->rd_lk);
        if (rc) {
            if (gen == e->readv_gen) {
                reconnect(e);
            }
            while (!net_stop && e->readv_gen == gen) {
                hputs("waiting for new connection\n");
                Pthread_cond_wait(&e->rd_cond, &e->rd_lk);
            }
        }
    }
    free(mbuf);
    evbuffer_free(buf);
    Pthread_mutex_unlock(&e->rd_lk);

    if (n->stop_thread_callback) {
        n->stop_thread_callback(n->callback_data);
    }
    return NULL;
}

#define advance_evbuffer_ptr(ptr, size) evbuffer_ptr_set(e->flush_buf, (ptr), (size), EVBUFFER_PTR_ADD)

static int get_stat_from_user_msg(struct event_info *e, struct evbuffer *buf, struct evbuffer_ptr *p, net_queue_stat_t *stat)
{
    /* seqnum */
    if (advance_evbuffer_ptr(p, sizeof(uint32_t)) != 0) return -1;
    uint32_t n = sizeof(uint32_t);

    struct {
        uint32_t sz;
        uint32_t crc;
    } rep, ctrl;

    /* rep record */
    if (evbuffer_copyout_from(buf, p, &rep, sizeof(rep)) != sizeof(rep)) {
        return -1;
    }
    rep.sz = ntohl(rep.sz);
    if (advance_evbuffer_ptr(p, sizeof(rep) + rep.sz) != 0) return -1;
    n += (sizeof(rep) + rep.sz);

    /* control record */
    if (evbuffer_copyout_from(buf, p, &ctrl, sizeof(ctrl)) != sizeof(ctrl)) {
        return -1;
    }
    ctrl.sz = ntohl(ctrl.sz);
    if (advance_evbuffer_ptr(p, sizeof(ctrl)) != 0) return -1;
    n += sizeof(ctrl);

    /* rep_control */
    struct {
        uint32_t rep_version;
        uint32_t log_version;
        struct __db_lsn lsn;
        uint32_t rectype;
    } rep_ctrl;
    if (evbuffer_copyout_from(buf, p, &rep_ctrl, sizeof(rep_ctrl)) != sizeof(rep_ctrl)) {
        return -1;
    }
    if (advance_evbuffer_ptr(p, ctrl.sz) != 0) return -1;
    n += ctrl.sz;
    rep_ctrl.lsn.file = ntohl(rep_ctrl.lsn.file);
    rep_ctrl.lsn.offset = ntohl(rep_ctrl.lsn.offset);
    rep_ctrl.rectype = ntohl(rep_ctrl.rectype);

    ++stat->total_count;
    if (rep_ctrl.rectype <= 0 || rep_ctrl.rectype >= REP_MAX_TYPE) {
        return -1;
    }
    ++stat->type_counts[rep_ctrl.rectype];
    if (stat->min_lsn.file == 0) {
        stat->min_lsn = stat->max_lsn = rep_ctrl.lsn;
    }
    if (rep_ctrl.lsn.file <= stat->min_lsn.file) {
        if (rep_ctrl.lsn.file < stat->min_lsn.file || rep_ctrl.lsn.offset < stat->min_lsn.offset) {
            stat->min_lsn = rep_ctrl.lsn;
        }
    }
    if (rep_ctrl.lsn.file >= stat->max_lsn.file) {
        if (rep_ctrl.lsn.file > stat->max_lsn.file || rep_ctrl.lsn.offset > stat->max_lsn.offset) {
            stat->max_lsn = rep_ctrl.lsn;
        }
    }
    return n;
}

static void get_stat_evbuffer(struct event_info *e, struct evbuffer *buf, net_queue_stat_t *stat)
{
    if (!buf) return;
    if (!e->got_hello && !e->got_hello_reply) return;
    uint32_t n;
    wire_header_type hdr;
    net_send_message_header msg;
    net_ack_message_payload_type ack;
    struct evbuffer_ptr p;
    evbuffer_ptr_set(buf, &p, 0, EVBUFFER_PTR_SET);
    while (evbuffer_copyout_from(buf, &p, &hdr, sizeof(hdr)) == sizeof(hdr)) {
        if (advance_evbuffer_ptr(&p, e->wirehdr_len) != 0) return;
        int rc = -1;
        int type = ntohl(hdr.type);
        int known = 0;
        switch (type) {
        case WIRE_HEADER_HEARTBEAT:
            rc = 0;
            break;
        case WIRE_HEADER_HELLO:
        case WIRE_HEADER_HELLO_REPLY:
            if (evbuffer_copyout_from(buf, &p, &n, sizeof(n)) != sizeof(n)) {
                break;
            }
            n = ntohl(n);
            rc = advance_evbuffer_ptr(&p, n);
            break;
        case WIRE_HEADER_DECOM:
            rc = advance_evbuffer_ptr(&p, sizeof(n));
            break;
        case WIRE_HEADER_USER_MSG:
            if (evbuffer_copyout_from(buf, &p, &msg, sizeof(msg)) != sizeof(msg)) {
                break;
            }
            advance_evbuffer_ptr(&p, NET_SEND_MESSAGE_HEADER_LEN);
            msg.datalen = ntohl(msg.datalen);
            msg.usertype = ntohl(msg.usertype);
            if (msg.usertype != USER_TYPE_BERKDB_REP) {
                rc = advance_evbuffer_ptr(&p, msg.datalen);
            } else if (get_stat_from_user_msg(e, buf, &p, stat) == msg.datalen) {
                rc = 0;
                known = 1;
            }
            break;
        case WIRE_HEADER_ACK:
            rc = advance_evbuffer_ptr(&p, NET_ACK_MESSAGE_TYPE_LEN);
            break;
        case WIRE_HEADER_DECOM_NAME:
            if (evbuffer_copyout_from(buf, &p, &n, sizeof(n)) != sizeof(n)) {
                break;
            }
            n = ntohl(n);
            rc = advance_evbuffer_ptr(&p, sizeof(n) + n);
            break;
        case WIRE_HEADER_ACK_PAYLOAD:
            if (evbuffer_copyout_from(buf, &p, &ack, sizeof(ack)) != sizeof(ack)) {
                break;
            }
            n = ntohl(ack.paylen);
            rc = advance_evbuffer_ptr(&p, NET_ACK_MESSAGE_PAYLOAD_TYPE_LEN + n);
            break;
        default:
            break;
        }
        if (rc) {
            hprintf_nd("got error on type:%d (%d)\n", type, ntohl(type));
            break;
        } else if (!known) {
            ++stat->unknown_count;
        }
    }
}

static ssize_t readv_plaintext(struct event_info *e)
{
#   define NVEC 8
    struct iovec v[NVEC];
    int avail;
    (void)ioctl(e->fd, FIONREAD, &avail);
    if (avail <= 0) avail = KB(8);
    const int nv = evbuffer_reserve_space(e->readv_buf, avail, v, NVEC);
    if (nv <= 0) {
        hprintf("evbuffer_reserve_space failed nv:%d need:%d\n", nv, avail);
        errno = ENOMEM;
        return -1;
    }
    const ssize_t sz = readv(e->fd, v, nv);
    if (sz <= 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return sz;
    }
    ssize_t n = sz;
    for (int i = 0; i < nv; ++i) {
        if (v[i].iov_len < n) {
            n -= v[i].iov_len;
            continue;
        }
        v[i].iov_len = n;
        n = 0;
    }
    evbuffer_commit_space(e->readv_buf, v, nv);
    return sz;
}

static ssize_t readv_ciphertext(struct event_info *e)
{
    int eof;
    int rc = rd_ssl_evbuffer(e->readv_buf, e->ssl_data, &eof);
    return eof ? -1 : rc;
}

static void readcb(int fd, short what, void *data)
{
    struct event_info *e = data;
    check_rd_thd();
    if (fd != e->fd) abort();
    Pthread_mutex_lock(&e->rd_lk);
    /*
    ** evbuffer_set_max_read() is not released yet..
    ** Writing my own readv wrapper; Observed max read of 4K with:
    ** ssize_t n = evbuffer_read(e->readv_buf, e->fd, -1);
    */
    ssize_t n = e->readv(e); // -> readv_plaintext()
    if (n > 0) {
        check_rd_full(e);
        e->recv_at = time(NULL);
        Pthread_cond_signal(&e->rd_cond); // -> rd_worker()
    } else {
        do_disable_read(e);
        reconnect(e);
    }
    Pthread_mutex_unlock(&e->rd_lk);
}

static void resume_read(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    check_rd_thd();
    if (!e->rd_ev) return;
    event_add(e->rd_ev, NULL);
}

static void write_hello_evbuffer(struct event_info *e)
{
    hputs("WRITING HELLO\n");
    write_hello(e->net_info->netinfo_ptr, e->host_node_ptr);
    event_add(e->rd_ev, NULL);
    event_add(e->hb_send_ev, &one_sec);
}

static void net_connect_ssl_error(void *data)
{
    struct event_info *e = data;
    reconnect(e);
}

static void net_connect_ssl_success(void *data)
{
    struct event_info *e = data;
    hputs("SSL CONNECTED\n");
    e->readv = readv_ciphertext;
    e->writev = writev_ciphertext;
    write_hello_evbuffer(e);
}

static void finish_host_setup(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct event_info *e = data;
    struct host_connected_info *i = e->host_connected;
    e->host_connected = NULL;
    int connect_msg = i->connect_msg;
    host_connected_info_free(i);
    if (e->host_connected_pending) {
        e->host_connected = e->host_connected_pending;
        e->host_connected_pending = NULL;
        hprintf("WORKING ON PENDING CONNECTION fd:%d\n", e->host_connected->fd);
        do_close(e, do_open);
    } else if (connect_msg) {
        netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
        if (gbl_pb_connectmsg) {
            wr_connect_msg_proto(e);
        } else {
            hputs("WRITING CONNECT MSG\n");
            write_connect_message(netinfo_ptr, e->host_node_ptr, NULL);
        }
        if (e->ssl_data) abort();
        if (SSL_IS_REQUIRED(gbl_rep_ssl_mode)) {
            net_flush_evbuffer(e->host_node_ptr);
            e->ssl_data = ssl_data_new(e->fd, e->host);
            hputs("SSL CONNECT\n");
            connect_ssl_evbuffer(e->ssl_data, base, net_connect_ssl_error, net_connect_ssl_success, e);
        } else {
            write_hello_evbuffer(e);
        }
    } else {
        hputs("CONNECTED\n");
    }
}

static void enable_heartbeats(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    check_timer_thd();
    if (e->hb_send_ev || e->hb_check_ev) {
        abort();
    }
    e->recv_at = e->sent_at = time(NULL);
    e->hb_check_ev = event_new(timer_base, -1, EV_PERSIST, heartbeat_check, e);
    e->hb_send_ev = event_new(timer_base, -1, EV_PERSIST, heartbeat_send, e);
    event_add(e->hb_check_ev, &one_sec);

    struct host_connected_info *i = e->host_connected;
    if (!i->connect_msg) event_add(e->hb_send_ev, &one_sec); /* enable after sending connect-msg */
    evtimer_once(base, finish_host_setup, e);
}

static void enable_read(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    check_rd_thd();
    e->rd_full = 0;
    e->need = e->wirehdr_len;
    if (e->readv_buf || e->rd_ev) {
        abort();
    }
    e->readv_buf = evbuffer_new();
    e->rd_ev = event_new(rd_base, e->fd, EV_READ | EV_PERSIST, readcb, e);
    struct host_connected_info *i = e->host_connected;
    if (!i->connect_msg) event_add(e->rd_ev, NULL); /* enable after sending connect-msg */
    evtimer_once(timer_base, enable_heartbeats, e);
}

static void enable_write(int dummyfd, short what, void *data)
{
    struct event_info *e = data;
    struct host_connected_info *i = e->host_connected;
    check_wr_thd();
    Pthread_mutex_lock(&e->wr_lk);
    update_event_fd(e, i->fd);
    if (e->flush_buf || e->wr_ev || e->ssl_data) {
        abort();
    }
    e->wr_ev = event_new(wr_base, e->fd, EV_WRITE | EV_PERSIST, writecb, e);
    e->flush_buf = evbuffer_new();
    e->wr_buf = evbuffer_new();
    e->wr_full = 0;
    e->decomissioned = 0;
    if (i->ssl_data) {
        e->readv = readv_ciphertext;
        e->writev = writev_ciphertext;
        e->ssl_data = i->ssl_data;
        i->ssl_data = NULL;
        hputs("SSL ACCEPTED\n");
    } else {
        e->readv = readv_plaintext;
        e->writev = writev_plaintext;
    }
    Pthread_mutex_unlock(&e->wr_lk);
    evtimer_once(rd_base, enable_read, e);
}

static void do_open(int dummyfd, short what, void *data)
{
    check_base_thd();
    struct event_info *e = data;
    struct host_connected_info *i = e->host_connected;
    hprintf("ENABLE CONNECTION fd:%d\n", i->fd);
    host_node_open(e->host_node_ptr, i->fd);
    evtimer_once(wr_base, enable_write, e);
}

static void host_connected(struct event_info *e, int fd, int connect_msg, struct ssl_data *ssl_data)
{
    check_base_thd();
    struct host_connected_info *i = host_connected_info_new(e, fd, connect_msg, ssl_data);
    struct host_connected_info *pending = e->host_connected_pending;
    if (pending) {
        if (e->host_connected == NULL) {
            abort();
        }
        hprintf("ONGOING fd:%d  PENDING fd:%d  REPLACE WITH fd:%d\n", e->host_connected->fd, pending->fd, fd);
        shutdown_close(pending->fd);
        host_connected_info_free(pending);
        e->host_connected_pending = i;
    } else if (e->host_connected) {
        hprintf("ONGOING fd:%d  ENQUEUE fd:%d\n", e->host_connected->fd, fd);
        e->host_connected_pending = i;
    } else {
        hprintf("PROCESS CONNECTION fd:%d\n", fd);
        e->host_connected = i;
        do_close(e, do_open);
    }
}

static void dist_heartbeat(int dummyfd, short what, void *data)
{
    check_dist_thd();
    dist_hbeats_type *dt = data;
    if (dt->ev_hbeats) {
        dist_heartbeats(dt);
    }
}

static void do_enable_dist_heartbeats(int dummyfd, short what, void *data)
{
    dist_hbeats_type *dt = data;

    check_dist_thd();
    if (dt->ev_hbeats)
        abort();

    dt->ev_hbeats = event_new(dist_base, -1, EV_PERSIST, dist_heartbeat, dt);
    if (!dt->ev_hbeats) {
        logmsg(LOGMSG_ERROR, "Failed to create new event for dist_heartbeat\n");
        return;
    }
    dt->tv.tv_sec = 2;
    dt->tv.tv_usec = 0;

    event_add(dt->ev_hbeats, &dt->tv);
}

int enable_dist_heartbeats(dist_hbeats_type *dt)
{
    return event_base_once(dist_base, -1, EV_TIMEOUT, do_enable_dist_heartbeats, dt, NULL);
}

static void do_disable_dist_heartbeats_and_free(int dummyfd, short what, void *data)
{
    dist_hbeats_type *dt = data;
    check_dist_thd();
    if (dt->ev_hbeats) {
        event_del(dt->ev_hbeats);
        event_free(dt->ev_hbeats);
        dt->ev_hbeats = NULL;
    }
    dist_heartbeat_free_tran(dt);
}

static void do_disable_dist_heartbeats(int dummyfd, short what, void *data)
{
    dist_hbeats_type *dt = data;
    check_dist_thd();
    if (dt->ev_hbeats) {
        event_del(dt->ev_hbeats);
        event_free(dt->ev_hbeats);
        dt->ev_hbeats = NULL;
    }
}

int disable_dist_heartbeats_and_free(dist_hbeats_type *dt)
{
    return event_base_once(dist_base, -1, EV_TIMEOUT, do_disable_dist_heartbeats_and_free, dt, NULL);
}

int disable_dist_heartbeats(dist_hbeats_type *dt)
{
    return event_base_once(dist_base, -1, EV_TIMEOUT, do_disable_dist_heartbeats, dt, NULL);
}

extern int fdb_heartbeats(fdb_hbeats_type *hb);
static void fdb_heartbeat(int dummyfd, short what, void *data)
{
    check_fdb_thd();

    fdb_hbeats_type *hb = data;
    logmsg(LOGMSG_INFO, "Sending fdb heartbeat for tran %p\n", hb);
    fdb_heartbeats(hb);
}

static void do_enable_fdb_heartbeats(int dummyfd, short what, void *data)
{
    fdb_hbeats_type *hb = data;

    check_fdb_thd();
    if (hb->ev_hbeats)
        abort();

    hb->ev_hbeats = event_new(fdb_base, -1, EV_PERSIST, fdb_heartbeat, hb);
    if (!hb->ev_hbeats) {
        logmsg(LOGMSG_ERROR, "Failed to create new event for fdb_heartbeat\n");
        return;
    }
    hb->tv.tv_sec = 5; /*IOTIMEOUTMS/2*/
    hb->tv.tv_usec = 0;

    event_add(hb->ev_hbeats, &hb->tv);
}

extern void fdb_heartbeat_free_tran(fdb_hbeats_type *hb);
static void do_disable_fdb_heartbeats_and_free(int dummyfd, short what, void *data)
{
    fdb_hbeats_type *hb= data;

    check_fdb_thd();
    if (hb->ev_hbeats) {
        event_del(hb->ev_hbeats);
        event_free(hb->ev_hbeats);
        hb->ev_hbeats = NULL;
    }
    fdb_heartbeat_free_tran(hb);
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
    struct event_info *e = c->e;
    if (what & EV_TIMEOUT) {
        hprintf("TIMEOUT fd:%d\n", fd);
        pmux_reconnect(c);
        return;
    }
    if (e->fd != -1) {
        hputs("HAVE ACTIVE CONNECTION\n");
    } else if (e->host_connected) {
        struct host_connected_info *info = e->host_connected;
        hprintf("HAVE PENDING CONNECTION fd:%d\n", info->fd);
    } else if (!skip_connect(e)) {
        hprintf("MADE NEW CONNECTION fd:%d\n", fd);
        host_connected(e, fd, 1, NULL);
        c->fd = -1;
    }
    connect_info_free(c);
}

static void comdb2_connect(struct connect_info *c, int port)
{
    check_base_thd();
    struct event_info *e = c->e;
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
    int rc;
    struct connect_info *c = data;
    struct event_info *e = c->e;
    if (what & EV_TIMEOUT) {
        hprintf("TIMEOUT fd:%d\n", fd);
        pmux_reconnect(c);
        return;
    } else if ((rc = evbuffer_read(c->buf, fd, -1)) <= 0) {
        hprintf("FAILED read fd:%d rc:%d errno:%d -- %s\n", fd, rc, errno, strerror(errno));
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
    rc = sscanf(res, "%d", &pmux_rc);
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
    int rc;
    struct connect_info *c = data;
    struct event_info *e = c->e;
    if (what & EV_TIMEOUT) {
        hprintf("TIMEOUT fd:%d\n", fd);
        pmux_reconnect(c);
        return;
    } else if ((rc = evbuffer_write(c->buf, fd)) <= 0) {
        hprintf("FAILED write fd:%d rc:%d errno:%d -- %s\n", fd, rc, errno, strerror(errno));
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
    struct event_info *e = c->e;
    netinfo_type *netinfo_ptr = pmux_netinfo(e);
    evbuffer_add_printf(c->buf, "rte %s/%s/%s\n",
                        netinfo_ptr->app, netinfo_ptr->service,
                        netinfo_ptr->instance);
    event_base_once(base, c->fd, EV_WRITE, pmux_rte_writecb, c, &connect_timeout);
}

static void pmux_get_readcb(int fd, short what, void *data)
{
    check_base_thd();
    int rc;
    struct connect_info *c = data;
    struct event_info *e = c->e;
    netinfo_type *netinfo_ptr = pmux_netinfo(e);
    if (what & EV_TIMEOUT) {
        hprintf("TIMEOUT fd:%d\n", fd);
        pmux_reconnect(c);
        return;
    } else if ((rc = evbuffer_read(c->buf, fd, -1)) <= 0) {
        hprintf("FAILED read fd:%d rc:%d errno:%d -- %s\n", fd, rc, errno, strerror(errno));
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
    /*
    if (gbl_pmux_route_enabled) {
        pmux_rte(c);
        return;
    }
    */
    c->fd = -1;
    shutdown_close(fd);
    comdb2_connect(c, port);
}

static void pmux_get_writecb(int fd, short what, void *data)
{
    check_base_thd();
    int rc;
    struct connect_info *c = data;
    struct event_info *e = c->e;
    if (what & EV_TIMEOUT) {
        hprintf("TIMEOUT fd:%d\n", fd);
        pmux_reconnect(c);
        return;
    } else if ((rc = evbuffer_write(c->buf, fd)) <= 0) {
        hprintf("FAILED write fd:%d rc:%d errno:%d -- %s\n", fd, rc, errno, strerror(errno));
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
    struct event_info *e = c->e;
    netinfo_type *netinfo_ptr = pmux_netinfo(e);
    evbuffer_add_printf(c->buf, "get /echo %s/%s/%s\n", netinfo_ptr->app,
                        netinfo_ptr->service, netinfo_ptr->instance);
    event_base_once(base, c->fd, EV_WRITE, pmux_get_writecb, c, &connect_timeout);
}

static void pmux_reconnect(struct connect_info *c)
{
    check_base_thd();
    struct event_info *e = c->e;
    hprintf("FAILED CONNECTING fd:%d\n", c->fd);
    connect_info_free(c);
    if (e->fd == -1 && !e->host_connected) {
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
    if (e->fd != -1) {
        hputs("HAVE ACTIVE CONNECTION\n");
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
    /*
    if (gbl_pmux_route_enabled && e->port != 0) {
        pmux_rte(c);
    } else {
        pmux_get(c);
    }
    */
    pmux_get(c);
}

static struct timeval ms_to_timeval(int ms)
{
    struct timeval t = {0};
    if (ms >= 1000) {
        t.tv_sec = ms / 1000;
        ms %= 1000;
    }
    t.tv_usec = ms * 1000;
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
    if (!host_node_ptr) {
        host_node_ptr = add_to_netinfo_ll(netinfo_ptr, host, port);
        if (!host_node_ptr) return -1;
    }
    host_node_ptr->addr = a->ss.sin_addr;
    update_host_node_ptr(host_node_ptr, e);
    update_event_port(e, port);
    update_wire_hdrs(e);
    if (netinfo_ptr->new_node_rtn) { /* net_newnode_rtn */
        netinfo_ptr->new_node_rtn(netinfo_ptr, h->host_interned, port);
    }
    if (a->ssl_data) {
        hprintf("ACCEPTED NEW SSL CONNECTION fd:%d\n", a->fd);
    } else {
        hprintf("ACCEPTED NEW CONNECTION fd:%d\n", a->fd);
    }
    host_connected(e, a->fd, 0, a->ssl_data);
    a->ssl_data = NULL;
    a->fd = -1;
    accept_info_free(a);
    return 0;
}

static void net_accept_ssl_success(void *data)
{
    struct accept_info *a = data;
    if (verify_ssl_evbuffer(a->ssl_data, gbl_rep_ssl_mode) == 0) {
        if (accept_host(a) == 0) return;
        logmsg(LOGMSG_ERROR, "%s: accept_host failed host:%s fd:%d\n", __func__, a->origin, a->fd);
    } else {
        logmsg(LOGMSG_ERROR, "%s: verify_ssl_evbuffer failed host:%s fd:%d\n", __func__, a->origin, a->fd);
    }
    accept_info_free(a);
}

static void net_accept_ssl_error(void *data)
{
    struct accept_info *a = data;
    logmsg(LOGMSG_ERROR, "%s: accept_ssl_evbuffer failed host:%s fd:%d\n", __func__, a->origin, a->fd);
    accept_info_free(a);
}

static int validate_host(struct accept_info *a)
{
    if (strcmp(a->from_host, gbl_myhostname) == 0) {
        logmsg(LOGMSG_WARN, "%s fd:%d invalid from:%s\n", __func__, a->fd, a->from_host);
        return -1;
    }

    char *dbname = gbl_dbname;
    if (gbl_debug_pb_connectmsg_dbname_check) {
        dbname = "icthxdb";
    }

    if (a->dbname && strcmp(a->dbname, dbname) != 0) {
        logmsg(LOGMSG_WARN, "%s fd:%d invalid dbname:%s (exp:%s)\n", __func__, a->fd, a->dbname, dbname);
        return -1;
    }
    int check_port = 1;
    int port = a->c.from_portnum;
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
               a->c.from_nodenum, a->from_host, netnum, a->netinfo_ptr->netnum);
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
    if (netinfo_ptr->allow_rtn && !netinfo_ptr->allow_rtn(netinfo_ptr, host)) { /* net_allow_node */
        logmsg(LOGMSG_ERROR, "connection from node:%d host:%s not allowed\n", a->c.from_nodenum, host);
        return -1;
    }
    if (a->c.flags & CONNECT_MSG_SSL) {
        if (!SSL_IS_ABLE(gbl_rep_ssl_mode)) {
            logmsg(LOGMSG_ERROR, "Peer requested SSL, but I don't have an SSL key pair.\n");
            return -1;
        }
        a->origin = get_hostname_by_fileno(a->fd);
        a->ssl_data = ssl_data_new(a->fd, a->origin);
        accept_ssl_evbuffer(a->ssl_data, base, net_accept_ssl_error, net_accept_ssl_success, a);
        return 0;
    } else if (SSL_IS_REQUIRED(gbl_rep_ssl_mode)) {
        logmsg(LOGMSG_ERROR, "Replicant SSL connections are required.\n");
        return -1;
    }
    return accept_host(a);
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
    if (read_hostname(&a->from_host, &a->from_len, a->c.from_hostname) != 0) {
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

static int process_connect_message_proto(struct accept_info *a)
{
    struct evbuffer *input = a->buf;
    uint8_t *buf = evbuffer_pullup(input, a->need);
    if (buf == NULL) {
        return -1;
    }
    NetConnectmsg *c = net_connectmsg__unpack(NULL, a->need, buf);
    evbuffer_drain(input, a->need);
    int bad = 0;
    char *missing = "Connect message missing field";
    if (c->to_hostname)
        a->to_host = strdup(c->to_hostname);
    else {
        logmsg(LOGMSG_ERROR, "%s to_hostname\n", missing);
        bad = 1;
    }
    if (c->has_to_portnum)
        a->c.to_portnum = c->to_portnum;
    else {
        logmsg(LOGMSG_ERROR, "%s to_portnum\n", missing);
        bad = 1;
    }
    if (c->from_hostname)
        a->from_host = strdup(c->from_hostname);
    else {
        logmsg(LOGMSG_ERROR, "%s from_hostname\n", missing);
        bad = 1;
    }
    if (c->has_from_portnum)
        a->c.from_portnum = c->from_portnum;
    else {
        logmsg(LOGMSG_ERROR, "%s from_portnum\n", missing);
        bad = 1;
    }
    if (c->dbname)
        a->dbname = strdup(c->dbname);
    else {
        logmsg(LOGMSG_ERROR, "%s dbname\n", missing);
        bad = 1;
    }
    if (c->has_ssl && c->ssl) {
        a->c.flags |= CONNECT_MSG_SSL;
    }
    net_connectmsg__free_unpacked(c, NULL);
    return bad ? -1 : validate_host(a);
}

static void rd_connect_msg(int fd, short what, void *data)
{
    struct accept_info *a = data;
    int need = a->need - evbuffer_get_length(a->buf);
    int n = evbuffer_read(a->buf, fd, need);
    if (n <= 0 && (what & EV_READ)) {
        accept_info_free(a);
        return;
    }
    int rc;
    if (n == need) {
        if (a->uses_proto) {
            rc = process_connect_message_proto(a);
        } else {
            rc = process_connect_message(a);
        }
    } else {
        rc = event_base_once(base, fd, EV_READ, rd_connect_msg, a, NULL);
    }
    if (rc) {
        accept_info_free(a);
    }
}

static int wr_connect_msg_proto(struct event_info *e)
{
    netinfo_type *netinfo_ptr = e->net_info->netinfo_ptr;
    host_node_type *host_node_ptr = e->host_node_ptr;
    hputs("WRITING CONNECT MSG\n");
    char type = 0;
    char star = '*';
    NetConnectmsg connect_message = NET_CONNECTMSG__INIT;

    // fill in message
    connect_message.to_hostname = host_node_ptr->host;
    connect_message.has_to_portnum = 1;
    connect_message.to_portnum = host_node_ptr->port;
    connect_message.from_hostname = netinfo_ptr->myhostname;
    connect_message.has_from_portnum = 1;
    if (gbl_accept_on_child_nets || !netinfo_ptr->ischild) {
        connect_message.from_portnum = netinfo_ptr->myport;
    } else {
        connect_message.from_portnum = netinfo_ptr->parent->myport | (netinfo_ptr->netnum << 16);
    }
    connect_message.dbname = gbl_dbname;
    if (gbl_debug_pb_connectmsg_dbname_check) {
        connect_message.dbname = "icthxdb";
    }
    if (SSL_IS_REQUIRED(gbl_rep_ssl_mode)) {
        connect_message.has_ssl = 1;
        connect_message.ssl = 1;
    }

    // send message
    int len = net_connectmsg__get_packed_size(&connect_message);
    int net_len = htonl(len);

    uint8_t *buf = malloc(len);
    net_connectmsg__pack(&connect_message, buf);

    int i = 0;
    int n = 4;
    struct iovec iov[n];
    int rc;

    iov[i].iov_base = &type;
    iov[i].iov_len = sizeof(type);
    ++i;

    iov[i].iov_base = &star;
    iov[i].iov_len = sizeof(star);
    ++i;

    iov[i].iov_base = &net_len;
    iov[i].iov_len = sizeof(net_len);
    ++i;

    iov[i].iov_base = buf;
    iov[i].iov_len = len;
    ++i;

    rc = write_connect_message_evbuffer(host_node_ptr, iov, n);
    free(buf);
    return rc;
}

static void handle_appsock(netinfo_type *netinfo_ptr, struct sockaddr_in *ss, int first_byte, struct evbuffer *buf, int fd)
{
    int n = evbuffer_get_length(buf);
    char req[n + 1];
    evbuffer_copyout(buf, req, -1);
    evbuffer_free(buf);
    make_socket_blocking(fd);
    SBUF2 *sb = sbuf2open(fd, 0);
    for (int i = n - 1; i > 0; --i) {
        sbuf2ungetc(req[i], sb);
    }
    do_appsock(netinfo_ptr, ss, sb, first_byte);
}

/* retrive next 5 bytes to search for '*' and len */
static void rd_connect_msg_len(int fd, short what, void *data)
{
    char first;
    int len, n;
    struct accept_info *a = data;
    int need = sizeof(first) + sizeof(len) - evbuffer_get_length(a->buf);

    if (need > 0) {
        n = evbuffer_read(a->buf, fd, need);
        if (n <= 0 && (what & EV_READ)) {
            accept_info_free(a);
            return;
        }
    } else {
        n = need; // we are done reading
    }

    if (n == need) {
        evbuffer_copyout(a->buf, &first, 1);
        if (first == '*' && !gbl_debug_pb_connectmsg_gibberish) { // protobuf connect message
            evbuffer_drain(a->buf, 1);
            evbuffer_remove(a->buf, &len, sizeof(len));
            len = ntohl(len);
            a->need = len;
            a->uses_proto = 1;
        } else {
            a->need = NET_CONNECT_MESSAGE_TYPE_LEN;
            a->uses_proto = 0;
        }
        rd_connect_msg(fd, 0, a);
    } else if (event_base_once(base, fd, EV_READ, rd_connect_msg_len, a, NULL)) {
        accept_info_free(a);
    }
}

static int do_appsock_evbuffer(struct evbuffer *buf, struct sockaddr_in *ss, int fd, int is_readonly, int secure)
{
    struct appsock_info *info = NULL;
    struct evbuffer_ptr b = evbuffer_search(buf, "\n", 1, NULL);
    if (b.pos == -1) {
        b = evbuffer_search(buf, " ", 1, NULL);
    }

    if (b.pos != -1) {
        char key[b.pos + 2];
        evbuffer_copyout(buf, key, b.pos + 1);
        key[b.pos + 1] = 0;
        if (secure && strstr(key, "newsql") == NULL) {
            logmsg(LOGMSG_ERROR, "appsock '%s' disallowed on secure port\n", key);
            return 1;
        }

        info = get_appsock_info(key);
    }

    if (info == NULL) return 1;

    evbuffer_drain(buf, b.pos + 1);
    struct appsock_handler_arg *arg = malloc(sizeof(*arg));
    arg->fd = fd;
    arg->addr = *ss;
    arg->rd_buf = buf;
    arg->is_readonly = is_readonly;
    arg->secure = secure;

    static int appsock_counter = 0;
    arg->base = appsock_base[appsock_counter++];
    if (appsock_counter == NUM_APPSOCK_RD) appsock_counter = 0;
    evtimer_once(arg->base, info->cb, arg); /* handle_newsql_request_evbuffer */
    return 0;
}

static void do_read(int fd, short what, void *data)
{
    check_base_thd();
    struct accept_info *a = data;
    struct evbuffer *buf = evbuffer_new();
    ssize_t n = evbuffer_read(buf, fd, SBUF2UNGETC_BUF_MAX);
    if (n <= 0) {
        evbuffer_free(buf);
        accept_info_free(a);
        return;
    }
    uint8_t first_byte;
    evbuffer_copyout(buf, &first_byte, 1);
    if (first_byte == 0) {
        evbuffer_drain(buf, 1);
        a->buf = buf;
        rd_connect_msg_len(fd, 0, a);
        return;
    }
    netinfo_type *netinfo_ptr = a->netinfo_ptr;
    struct sockaddr_in ss = a->ss;
    int secure = a->secure;
    a->fd = -1;
    accept_info_free(a);
    a = NULL;
    if (should_reject_request()) {
        evbuffer_free(buf);
        shutdown_close(fd);
        return;
    }
    if ((do_appsock_evbuffer(buf, &ss, fd, 0, secure)) == 0) return;
    handle_appsock(netinfo_ptr, &ss, first_byte, buf, fd);
}

static void accept_cb(struct evconnlistener *listener, evutil_socket_t fd,
                      struct sockaddr *addr, int len, void *data)
{
    check_base_thd();
    struct net_info *n = data;
    netinfo_type *netinfo_ptr = n->netinfo_ptr;
    netinfo_ptr->num_accepts++;
    accept_info_new(netinfo_ptr, (struct sockaddr_in *)addr, fd, 0);
}

static void accept_secure(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *addr, int len,
                          void *data)
{
    check_base_thd();
    struct net_info *n = data;
    netinfo_type *netinfo_ptr = n->netinfo_ptr;
    netinfo_ptr->num_accepts++;
    accept_info_new(netinfo_ptr, (struct sockaddr_in *)addr, fd, 1);
}

static void accept_error_cb(struct evconnlistener *listener, void *data)
{
    check_base_thd();
    int err = EVUTIL_SOCKET_ERROR();
    if (err == EMFILE && close_oldest_pending_connection() == 0) {
        return;
    }
    logmsg(LOGMSG_FATAL,
           "%s err:%d [%s] [outstanding fds:%d] [appsock fds:%d]\n",
           __func__, err, evutil_socket_error_to_string(err),
           pending_connections, ATOMIC_LOAD32(active_appsock_conns));
    abort();
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

static int recvfd(int pmux_fd, int *secure)
{
    int newfd = -1;
    char buf[sizeof("pmux") - 1];
    struct iovec iov = {.iov_base = buf, .iov_len = sizeof(buf)};
    struct msghdr msg = {0};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
#   ifdef __sun
    msg.msg_accrights = (caddr_t)&newfd;
    msg.msg_accrightslen = sizeof(newfd);
    ssize_t rc = recvmsg(pmux_fd, &msg, 0);
    if (rc != sizeof(buf)) {
        logmsg(LOGMSG_ERROR, "%s:recvmsg pmux_fd:%d rc:%zd expected:%zu (%s)\n",
               __func__, pmux_fd, rc, sizeof(buf), strerror(errno));
        return -1;
    }
    if (msg.msg_accrightslen != sizeof(newfd)) {
        return -1;
    }
#   else
    struct cmsghdr *cmsgptr = alloca(CMSG_SPACE(sizeof(int)));
    msg.msg_control = cmsgptr;
    msg.msg_controllen = CMSG_SPACE(sizeof(int));
    ssize_t rc = recvmsg(pmux_fd, &msg, 0);
    if (rc == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }
        logmsg(LOGMSG_ERROR, "%s:recvmsg pmux_fd:%d rc:%zd errno:%d [%s]\n",
               __func__, pmux_fd, rc, errno, strerror(errno));
        return -1;
    }
    if (rc != sizeof(buf)) {
        logmsg(LOGMSG_ERROR, "%s:recvmsg pmux_fd:%d rc:%zd expected:%zu\n",
               __func__, pmux_fd, rc, sizeof(buf));
        return -1;
    }
    struct cmsghdr *tmp, *m;
    m = tmp = CMSG_FIRSTHDR(&msg);
    if (!m) {
        logmsg(LOGMSG_ERROR, "%s:CMSG_FIRSTHDR NULL msghdr\n", __func__);
        return -2;
    }
    if (CMSG_NXTHDR(&msg, tmp) != NULL) {
        logmsg(LOGMSG_ERROR, "%s:CMSG_NXTHDR unexpected msghdr\n", __func__);
        return -1;
    }
    if (!is_fd(m)) {
        logmsg(LOGMSG_ERROR, "%s: bad msg attributes\n", __func__);
        return -1;
    }
    newfd = *(int *)CMSG_DATA(m);
#   endif
    if (memcmp(buf, "pmux", sizeof(buf)) != 0 && memcmp(buf, "spmu", sizeof(buf)) != 0) {
        shutdown_close(newfd);
        logmsg(LOGMSG_ERROR, "%s:recvmsg pmux_fd:%d unexpected msg:%.*s\n", __func__,
               pmux_fd, (int)sizeof(buf), buf);
        return -1;
    }
    *secure = (memcmp(buf, "spmu", sizeof(buf)) == 0);
    return newfd;
}

static void do_recvfd(int pmux_fd, short what, void *data)
{
    check_base_thd();
    struct net_info *n = data;
    int secure;
    int newfd = recvfd(pmux_fd, &secure);
    switch (newfd) {
    case 0: return;
    case -1: reopen_unix(pmux_fd, n); return;
    case -2: close_oldest_pending_connection(); return;
    }
    make_server_socket(newfd);
    ssize_t rc = write(newfd, "0\n", 2);
    if (rc != 2) {
        logmsg(LOGMSG_ERROR, "%s:write pmux_fd:%d rc:%zd (%s)\n", __func__, pmux_fd, rc, strerror(errno));
        shutdown_close(newfd);
        return;
    }
    struct sockaddr_in saddr;
    struct sockaddr *addr = (struct sockaddr *)&saddr;
    socklen_t addrlen = sizeof(saddr);
    getpeername(newfd, addr, &addrlen);
    if (!secure)
        accept_cb(NULL, newfd, addr, addrlen, n);
    else
        accept_secure(NULL, newfd, addr, addrlen, n);
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
    logmsg(LOGMSG_USER, "%s: svc:%s accepting on unix socket fd:%d\n",
           __func__, n->service, unix_fd);
    event_add(n->unix_ev, NULL);

    /* Accept connection on new port */
    if (gbl_libevent_rte_only) {
        logmsg(LOGMSG_WARN, "%s: svc:%s rte-mode only\n", __func__, n->service);
        n->port = port;
        return 0;
    }
    if (n->port == port && n->listener) {
        return 0;
    }
    if (n->port != port) {
        logmsg(LOGMSG_WARN, "%s: PORT CHANGED %d->%d\n", __func__, n->port, port);
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
    n->listener = evconnlistener_new_bind(base, accept_cb, n, flags, SOMAXCONN, s, len);
    if (n->listener == NULL) {
        return -1;
    }
    evconnlistener_set_error_cb(n->listener, accept_error_cb);
    int fd = evconnlistener_get_fd(n->listener);
    update_net_info(n, fd, port);
    logmsg(LOGMSG_USER, "%s svc:%s accepting on port:%d fd:%d\n", __func__,
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
    logmsg(LOGMSG_USER, "%s fd:%d\n", __func__, fd);

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
    if (gbl_libevent_rte_only) {
        return;
    }
    int fd = netinfo_ptr->myfd;
    if (fd == -1) {
        return;
    }
    make_server_socket(fd);
    n->listener = evconnlistener_new(base, accept_cb, n, LEV_OPT_CLOSE_ON_FREE,
                                     gbl_net_maxconn ? gbl_net_maxconn : SOMAXCONN, fd);
    evconnlistener_set_error_cb(n->listener, accept_error_cb);
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
            hprintf("HELLO NEW NODE:%s\n", host);
            write_hello_reply(netinfo_ptr, e->host_node_ptr);
        }
    }
}

static inline int skip_send(struct event_info *e, int nodrop, int check_hello)
{
    if (e->fd == -1 || e->decomissioned || (check_hello && !e->got_hello)) return -3;
    if (e->wr_full && !nodrop) return -2;
    return 0;
}

struct shared_msg {
    int sz;
    uint32_t ref;
    net_send_message_header hdr;
    uint8_t buf[0];
};

static struct shared_msg *shared_msg_new(void *buf, int len, int type)
{
    struct shared_msg *msg = malloc(sizeof(struct shared_msg) + len);
    if (msg == NULL) {
        return NULL;
    }
    msg->sz = NET_SEND_MESSAGE_HEADER_LEN + len;
    msg->ref = 1;
    net_send_message_header tmp = {
        .usertype = type,
        .datalen = len,
    };
    net_send_message_header *hdr = &msg->hdr;
    net_send_message_header_put(&tmp, (uint8_t *)hdr, (uint8_t *)(hdr + 1));
    memcpy(&msg->buf, buf, len);
    return msg;
}

static void shared_msg_free(const void *unused0, size_t unused1, void *ptr)
{
    struct shared_msg *msg = ptr;
    int ref = ATOMIC_ADD32(msg->ref, -1);
    if (ref) {
        return;
    }
    free(msg);
}

static void shared_msg_addref(struct shared_msg *msg)
{
    ATOMIC_ADD32(msg->ref, 1);
}

static int addref_evbuffer(struct evbuffer *buf, struct event_info *e, int n, struct shared_msg **msg)
{
    int rc = 0;
    for (int i = 0; i < n; ++i) {
        rc = evbuffer_add(buf, e->wirehdr[WIRE_HEADER_USER_MSG], e->wirehdr_len);
        if (rc) break;
        rc = evbuffer_add_reference(buf, &msg[i]->hdr, msg[i]->sz, shared_msg_free, msg[i]);
        if (rc) break;
        shared_msg_addref(msg[i]);
    }
    return rc;
}

static int memcpy_evbuffer(struct evbuffer *flush_buf, struct event_info *e, int sz, int n, void **buf, int *len, int *type)
{
    sz += (n * e->wirehdr_len);
    struct iovec v[1];
    const int nv = evbuffer_reserve_space(flush_buf, sz, v, 1);
    if (nv != 1) {
        return -1;
    }
    uint8_t *b = v[0].iov_base;
    v[0].iov_len = sz;
    net_send_message_header hdr = {0};
    for (int i = 0; i < n; ++i) {
        memcpy(b, e->wirehdr[WIRE_HEADER_USER_MSG], e->wirehdr_len);
        b += e->wirehdr_len;

        hdr.usertype = type[i];
        hdr.datalen = len[i];
        b = net_send_message_header_put(&hdr, b, b + sizeof(net_send_message_header));

        memcpy(b, buf[i], len[i]);
        b += len[i];
    }
    evbuffer_commit_space(flush_buf, v, 1);
    return 0;
}

/* All you base are belong to me.. */
static void setup_bases(void)
{
    init_base(&base_thd, &base, "main");
    if (dedicated_timer) {
        gettimeofday(&timer_tick, NULL);
        init_base_priority(&timer_thd, &timer_base, "timer", 0, &timer_tick);
    } else {
        timer_thd = base_thd;
        timer_base = base;
    }
    if (dedicated_fdb) {
        gettimeofday(&fdb_tick, NULL);
        init_base_priority(&fdb_thd, &fdb_base, "fdb", 0, &fdb_tick);
    } else {
        fdb_thd = base_thd;
        fdb_base = base;
    }
    if (dedicated_dist) {
        gettimeofday(&dist_tick, NULL);
        init_base_priority(&dist_thd, &dist_base, "dist", 0, &dist_tick);
    } else {
        dist_thd = base_thd;
        dist_base = base;
    }
    if (dedicated_appsock) {
        for (int i = 0; i < NUM_APPSOCK_RD; ++i) {
            gettimeofday(&appsock_tick[i], NULL);
            char thdname[16];
            snprintf(thdname, sizeof(thdname), "appsock:%d", i);
            init_base_priority(&appsock_thd[i], &appsock_base[i], thdname, 2, &appsock_tick[i]);
        }
    } else {
        for (int i = 0; i < NUM_APPSOCK_RD; ++i) {
            appsock_base[i] = base;
            appsock_thd[i] = base_thd;
        }
    }
    if (writer_policy == POLICY_NONE) {
        single.wrthd = base_thd;
        single.wrbase = base;
    } else if (writer_policy == POLICY_SINGLE) {
        init_base(&single.wrthd, &single.wrbase, "write");
    }

    if (reader_policy == POLICY_NONE) {
        single.rdthd = base_thd;
        single.rdbase = base;
    } else if (reader_policy == POLICY_SINGLE) {
        init_base(&single.rdthd, &single.rdbase, "read");
    }

    logmsg(LOGMSG_USER, "Libevent %s with backend method %s\n", event_get_version(), event_base_get_method(base));
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
    setup_bases();
    event_hash = hash_init_str(offsetof(struct event_hash_entry, key));
    net_stop = 0;
}

/* This allows waiting for async call to finish */
struct run_base_func_info {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    run_on_base_fn func;
    void *arg;
};

static void run_base_func(int dummyfd, short what, void *data)
{
    struct run_base_func_info *info = data;
    info->func(info->arg);
    Pthread_mutex_lock(&info->lock);
    Pthread_cond_signal(&info->cond);
    Pthread_mutex_unlock(&info->lock);
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
    info->func = func;
    info->data = data;
    info->ev = event_new(rd_base, fd, EV_READ | EV_PERSIST, user_event_func, info);
    LIST_INSERT_HEAD(&user_event_list, info, entry);
    event_add(info->ev, NULL);
}

void add_tcp_event(int fd, event_callback_fn func, void *data)
{
    make_server_socket(fd);
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
    info->func = func;
    info->data = data;
    info->ev = event_new(timer_base, -1, EV_TIMEOUT | EV_PERSIST, user_event_func, info);
    struct timeval t = ms_to_timeval(ms);
    LIST_INSERT_HEAD(&user_event_list, info, entry);
    event_add(info->ev, &t);
}
#endif

struct get_hosts_info {
    int max_hosts;
    int num_hosts;
    host_node_type **hosts;
};

static void get_hosts_evbuffer_impl(void *arg)
{
    check_base_thd();
    struct get_hosts_info *info = arg;
    struct net_info *n = net_info_find("replication");
    host_node_type *me = get_host_node_by_name_ll(n->netinfo_ptr, gbl_myhostname);
    if (!me) {
        abort();
    }
    info->hosts[0] = me;
    int i = 1;
    struct event_info *e;
    LIST_FOREACH(e, &n->event_list, net_list_entry) {
        if (e->decomissioned || !e->host_node_ptr) continue;
        info->hosts[i] = e->host_node_ptr;
        ++i;
        if (i == info->max_hosts) {
            break;
        }
    }
    info->num_hosts = i;
}

static void do_increase_net_buf(void *data)
{
    struct net_info *n;
    LIST_FOREACH(n, &net_list, entry) {
        n->rd_max = n->wr_max = MB(64);
    }
}

/********************/
/* PUBLIC INTERFACE */
/********************/


void add_host(host_node_type *host_node_ptr)
{
    netinfo_type *netinfo_ptr = host_node_ptr->netinfo_ptr;
    int fake = netinfo_ptr->fake;
    if (gbl_create_mode || gbl_exit || fake) {
        return;
    }
    init_event_net(netinfo_ptr);
    evtimer_once(base, do_add_host, host_node_ptr);
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
        if (e->decomissioned) continue;
        net_decom_node(e->net_info->netinfo_ptr, e->host);
        set_decom(e);
        do_close(e, send_decom_all);
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
    struct evbuffer *buf = e->flush_buf;
    if (buf) {
        for (int i = 0; i < n; ++i) {
            evbuffer_add(buf, iov[i].iov_base, iov[i].iov_len);
        }
    }
    Pthread_mutex_unlock(&e->wr_lk);
    return 0;
}

int write_list_evbuffer(host_node_type *host_node_ptr, int type, const struct iovec *iov, int n, int flags)
{
    if (net_stop) {
        return 0;
    }
    int rc;
    int nodrop = flags & WRITE_MSG_NOLIMIT;
    int nodelay = flags & WRITE_MSG_NODELAY;
    struct event_info *e = host_node_ptr->event_info;
    int total = e->wirehdr_len;
    for (int i = 0; i < n; ++i) total += iov[i].iov_len;
    Pthread_mutex_lock(&e->wr_lk);
    if (!e->flush_buf) {
       rc = -3;
    } else if ((rc = skip_send(e, nodrop, 0)) == 0) {
        if ((rc = evbuffer_expand(e->flush_buf, total)) == 0) {
            evbuffer_add(e->flush_buf, e->wirehdr[type], e->wirehdr_len);
            for (int i = 0; i < n; ++i) evbuffer_add(e->flush_buf, iov[i].iov_base, iov[i].iov_len);
            flush_evbuffer(e, nodelay);
        } else {
            rc = -1;
        }
    }
    if (rc==0) {
        e->net_info->netinfo_ptr->stats.bytes_written += total;
        host_node_ptr->stats.bytes_written += total;
        update_host_net_queue_stats(host_node_ptr, 1, total);
    }
    Pthread_mutex_unlock(&e->wr_lk);
    return rc;
}

int net_send_all_evbuffer(netinfo_type *netinfo_ptr, int n, void **buf, int *len, int *type, int *flags)
{
    if (net_stop) {
        return 0;
    }
    int nodrop = 0;
    int nodelay = 0;
    int logput = 0;
    int sz = (n * NET_SEND_MESSAGE_HEADER_LEN);
    for (int i = 0; i < n; ++i) {
        sz += len[i];
        nodrop |= flags[i] & NET_SEND_NODROP;
        nodelay |= flags[i] & NET_SEND_NODELAY;
        logput |= flags[i] & NET_SEND_LOGPUT;
    }
    struct shared_msg **msg = NULL;
    if (sz > KB(1)) {
        msg = alloca(sizeof(struct shared_msg *) * n);
        int i;
        for (i = 0; i < n; ++i) {
            if ((msg[i] = shared_msg_new(buf[i], len[i], type[i])) == NULL) break;
        }
        if (i < n) {
            for (int j = 0; j < i; ++j) {
                shared_msg_free(0, 0, msg[j]);
            }
            return NET_SEND_FAIL_MALLOC_FAIL;
        }
    }
    struct net_info *ni = net_info_find(netinfo_ptr->service);
    struct event_info *e;
    LIST_FOREACH(e, &ni->event_list, net_list_entry) {
        if (logput && netinfo_ptr->throttle_rtn && (netinfo_ptr->throttle_rtn)(netinfo_ptr, e->host_interned)) {
            continue;
        }
        Pthread_mutex_lock(&e->wr_lk);
        if (e->flush_buf && !skip_send(e, nodrop, 1)) {
            if (msg) {
                addref_evbuffer(e->flush_buf, e, n, msg);
            } else {
                memcpy_evbuffer(e->flush_buf, e, sz, n, buf, len, type);
            }
            flush_evbuffer(e, nodelay);
        }
        Pthread_mutex_unlock(&e->wr_lk);
        if (e->host_node_ptr) {
            e->host_node_ptr->stats.bytes_written += sz;
            update_host_net_queue_stats(e->host_node_ptr, 1, sz);
        }
        netinfo_ptr->stats.bytes_written += sz;
    }
    if (msg) {
        for (int i = 0; i < n; ++i) {
            shared_msg_free(0, 0, msg[i]);
        }
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
    if (e->flush_buf) {
        flush_evbuffer(e, 1);
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
    struct event_hash_entry *obj = hash_find(event_hash, key);
    Pthread_mutex_unlock(&event_hash_lk);
    if (!obj) {
        return NET_SEND_FAIL_INVALIDNODE;
    }
    struct event_info *e = obj->e;
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
    net_send_message_header_put(&tmp, (uint8_t *)&hdr, (uint8_t *)(&hdr + 1));
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

int get_hosts_evbuffer(int max_hosts, host_node_type **hosts)
{
    struct get_hosts_info info = {.max_hosts = max_hosts, .hosts = hosts};
    run_on_base(base, get_hosts_evbuffer_impl, &info);
    return info.num_hosts;
}

int add_appsock_handler(const char *key, event_callback_fn cb)
{
    size_t keylen = strlen(key);
    if (keylen > SBUF2UNGETC_BUF_MAX) {
        /* Sanity check: If appsock type is unknown, we need to fallback to sbuf2
         * and put data back into its ungetc buffer. */
        logmsg(LOGMSG_ERROR, "bad appsock parameter:%s\n", key);
        return -1;
    }
    if (appsock_hash == NULL) {
        appsock_hash = hash_init_strptr(offsetof(struct appsock_info, key));
    }
    struct appsock_info *info = get_appsock_info(key);
    if (info) {
        /* Sanity check: plugins should not re-register */
        logmsg(LOGMSG_ERROR, "attempt to re-register:%s\n", key);
        return -1;
    }
    info = malloc(sizeof(struct appsock_info));
    info->key = key;
    info->cb = cb;
    hash_add(appsock_hash, info);
    return 0;
}

void run_on_base(struct event_base *base, run_on_base_fn func, void *arg)
{
    if (base == current_base) {
        func(arg);
        return;
    }
    struct run_base_func_info info;
    info.func = func;
    info.arg = arg;
    Pthread_mutex_init(&info.lock, NULL);
    Pthread_cond_init(&info.cond, NULL);
    Pthread_mutex_lock(&info.lock);
    if (event_base_once(base, -1, EV_TIMEOUT, run_base_func, &info, NULL) != 0) abort();
    Pthread_cond_wait(&info.cond, &info.lock);
    Pthread_mutex_unlock(&info.lock);
    Pthread_mutex_destroy(&info.lock);
    Pthread_cond_destroy(&info.cond);
}

void net_queue_stat_iterate_evbuffer(netinfo_type *netinfo_ptr, QSTATITERFP func, struct net_get_records *arg)
{
    struct net_info *ni = net_info_find(netinfo_ptr->service);
    struct event_info *e;
    LIST_FOREACH(e, &ni->event_list, net_list_entry) {
        int type_counts[REP_MAX_TYPE] = {0};
        net_queue_stat_t stat = {0};
        Pthread_mutex_init(&stat.lock, NULL);
        stat.nettype = e->service;
        stat.hostname = e->host;
        stat.max_type = REP_MAX_TYPE - 1;
        stat.type_counts = type_counts;

        Pthread_mutex_lock(&e->wr_lk);
        get_stat_evbuffer(e, e->flush_buf, &stat);
        Pthread_mutex_unlock(&e->wr_lk);

        func(arg, &stat); /* net_to_systable */
        Pthread_mutex_destroy(&stat.lock);
    }
}

void increase_net_buf(void)
{
    run_on_base(base, do_increase_net_buf, NULL);
}

int enable_fdb_heartbeats(fdb_hbeats_type  *hb)
{
    return event_base_once(fdb_base, -1, EV_TIMEOUT, do_enable_fdb_heartbeats, hb, NULL);
}

int disable_fdb_heartbeats_and_free(fdb_hbeats_type *hb)
{
    return event_base_once(fdb_base, -1, EV_TIMEOUT, do_disable_fdb_heartbeats_and_free, hb, NULL);
}

struct event_base *get_dispatch_event_base(void)
{
    return appsock_base[0];
}

struct event_base *get_main_event_base(void)
{
    return base;
}

void do_revconn_evbuffer(int fd, short what, void *data)
{
    check_base_thd();
    if (what & EV_TIMEOUT) {
        logmsg(LOGMSG_USER, "revconn: %s: Timeout reading from fd:%d\n", __func__, fd);
        shutdown_close(fd);
        return;
    }
    int rc;
    struct evbuffer *buf = evbuffer_new();
    if ((rc = evbuffer_read(buf, fd, -1)) <= 0) {
        logmsg(LOGMSG_USER, "revconn: %s: Failed to read from fd:%d rc:%d\n", __func__, fd, rc);
        evbuffer_free(buf);
        shutdown_close(fd);
        return;
    }
    if (gbl_revsql_debug) {
        logmsg(LOGMSG_USER, "revconn: %s: Received 'newsql' request over 'reversesql' connection fd:%d\n", __func__, fd);
    }
    struct sockaddr_in addr;
    socklen_t laddr = sizeof(addr);
    getsockname(fd, (struct sockaddr *)&addr, &laddr);
    do_appsock_evbuffer(buf, &addr, fd, 1, 0);
}
