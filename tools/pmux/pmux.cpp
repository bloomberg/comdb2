/*
   Copyright 2017 Bloomberg Finance L.P.

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

// This program provides a simple port assigner service.  Databases request a
// port to listen on, which is persistent for multiple database runs. Clients
// ask which port to connect to for a specific database. Can use a central
// Comdb2 as a backing store, or SQLite for smaller setups.

#include <cassert>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <syslog.h>
#include <unistd.h>

#include <event2/buffer.h>
#include <event2/event.h>
#include <event2/event_struct.h>
#include <event2/listener.h>
#include <event2/util.h>

#include <bb_daemon.h>

#include "comdb2_store.h"
#include "no_store.h"
#include "pmux_store.h"
#include "sqlite_store.h"

#ifdef VVERBOSE
#   define VERBOSE
#   define reply_log(...) syslog(LOG_DEBUG, __VA_ARGS__)
#else
#   define reply_log(...)
#endif

#ifdef VERBOSE
#   define verbose_log(...) syslog(LOG_DEBUG, __VA_ARGS__)
#else
#   define verbose_log(...)
#endif

static event_base *base;
static std::map<std::string, int> port_map;
static std::map<std::string, int> fd_map;
static std::set<int> free_ports;
static bool foreground_mode = false;
static std::unique_ptr<pmux_store> pmux_store;
static std::vector<in_addr> local_addresses;
static std::vector<std::pair<int, int>> port_ranges;
static std::string unix_bind_path("/tmp/portmux.socket");

bool operator==(const in_addr&a, const in_addr &b)
{
    return a.s_addr == b.s_addr;
}

static int get_fd(const char *svc)
{
    int fd_ret = -1;
    std::string key(svc);
    const auto &fd = fd_map.find(key);
    if (fd != fd_map.end()) {
        fd_ret = fd->second;
    }
    return fd_ret;
}

static int alloc_fd(const char *svc, int fd)
{
    verbose_log("%s svc:%s fd:%d\n", __func__, svc, fd);
    fd_map.insert(std::make_pair(svc, fd));
    return 0;
}

static int dealloc_fd(const char *svc)
{
    std::string key(svc);
    const auto &i = fd_map.find(key);
    if (i == fd_map.end()) {
        return 0;
    }
    verbose_log("%s svc:%s fd:%d\n", __func__, svc, i->second);
    fd_map.erase(i);
    return 0;
}

static int get_svc_port(const char *svc)
{
    std::string key(svc);
    const auto &it = port_map.find(key);
    if (it == port_map.end())
        return -1;
    return it->second;
}

static bool is_port_in_range(int port)
{
    for (auto &range : port_ranges) {
        if (range.first <= port && port <= range.second)
            return true;
    }
    return false;
}

static void dealloc_svc_running_on_port(int port)
{
    for (std::map<std::string, int>::iterator it = port_map.begin();
         it != port_map.end(); ++it) {
        if (it->second == port) {
            pmux_store->del_port(it->first.c_str());
            port_map.erase(it);
            if (is_port_in_range(port))
                free_ports.insert(port);
            break;
        }
    }
}

static int dealloc_port(const char *svc)
{
    std::string key(svc);
    const auto &it = port_map.find(key);
    if (it == port_map.end()) {
        return -1;
    }

    int port = it->second;
    pmux_store->del_port(it->first.c_str());
    port_map.erase(it);
    if (is_port_in_range(port))
        free_ports.insert(port);

    return 0;
}

/* Service svc is informing us that it is using a certain port.
 * If port is in use by another service we need to update the
 * mapping. If port is outside of range, we still need to honor
 * and store it in the mapping.
 */
static int use_port(const char *svc, int port)
{
    // remember the old port for this service, if any
    int usedport = get_svc_port(svc);

    // service can tell us repeatedly that it's using the port, thats fine
    if (usedport == port) // we dont need to do anything
        return 0;

    // service was using a different port before so remove that mapping
    if (usedport != -1) {
        dealloc_port(svc);
    }

    // now find what's running in our port
    const auto &it = free_ports.find(port);

    // find who's using port and dealloc it since db is up and running
    // on the specified port which it is trying to register with pmux
    dealloc_svc_running_on_port(port);

    if (is_port_in_range(port)) {
        const auto &it = free_ports.find(port);
        assert(it != free_ports.end()); // port should be free
        free_ports.erase(it);
    }
    std::pair<std::string, int> kv(svc, port);
    port_map.insert(kv);
    pmux_store->sav_port(svc, port);
    return 0;
}

static int alloc_port(const char *svc)
{
    const auto &i = free_ports.begin();
    if (i == free_ports.end()) {
        syslog(LOG_ERR, "%s -- exhausted free ports", __func__);
        return -1;
    }
    int port = *i;
    free_ports.erase(i);

    std::pair<std::string, int> kv(svc, port);
    port_map.insert(kv);

    pmux_store->sav_port(svc, port);
    return port;
}

static void writecb(int fd, short what, void *arg)
{
    evbuffer *out = (evbuffer *)arg;
    int rc = evbuffer_write(out, fd);
    if (rc <= 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            evbuffer_free(out);
            return;
        }
    }
    if (evbuffer_get_length(out) == 0) {
        evbuffer_free(out);
    } else {
        event_base_once(base, fd, EV_WRITE, writecb, out, NULL);
    }
}

#define reply(...)                                                             \
    do {                                                                       \
        reply_log("reply:" __VA_ARGS__);                                       \
        evbuffer *out = evbuffer_new();                                        \
        evbuffer_add_printf(out, __VA_ARGS__);                                 \
        int rc = evbuffer_write(out, fd);                                      \
        if (rc <= 0) {                                                         \
            if (errno != EAGAIN && errno != EWOULDBLOCK) {                     \
                evbuffer_free(out);                                            \
                break;                                                         \
            }                                                                  \
        }                                                                      \
        if (evbuffer_get_length(out) == 0) {                                   \
            evbuffer_free(out);                                                \
        } else {                                                               \
            event_base_once(base, fd, EV_WRITE, writecb, out, NULL);           \
        }                                                                      \
    } while (0)

#define used()                                                                 \
    for (auto &kv : port_map) {                                                \
        reply("port %-7d name %s\n", kv.second, kv.first.c_str());             \
    }

#define active()                                                               \
    for (auto &kv : fd_map) {                                                  \
        const auto &svc = kv.first;                                            \
        const auto &port = port_map.find(svc);                                 \
        if (port != port_map.end()) {                                          \
            reply("port %-7d name %s\n", port->second, svc.c_str());           \
        }                                                                      \
    }

#define disallowed_write()                                                     \
    reply("-1 write requests not permitted from this host\n");

struct connection {
    evbuffer *buf;
    event ev;
    int fd;
    int is_unix;
    int routed;
    in_addr addr;
    std::string svc;
    connection(int f, int u, in_addr a)
        : fd(f), is_unix(u), addr(a), routed(0), buf(evbuffer_new()) {}
    ~connection()
    {
        event_del(&ev);
        evbuffer_free(buf);
        dealloc_fd(svc.c_str());
        verbose_log("%s close fd:%d\n", __func__, fd);
        close(fd);
    }
    int is_remote()
    {
        if (is_unix) return 0;
        for (auto& l : local_addresses) {
            if (l == addr) return 0;
        }
        return 1;
    }
};

static void make_socket_blocking(int fd)
{
    int flags;
    if ((flags = fcntl(fd, F_GETFL, NULL)) < 0) {
        perror("fcntl:F_GETFL");
        exit(1);
    }
    if (!(flags & O_NONBLOCK)) {
        return;
    }
    flags &= ~O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) == -1) {
        perror("fcntl:F_SETFL");
        exit(1);
    }
}

static void route(int fd, short what, void *arg)
{
    connection *c = (connection *)arg;
    int newfd = c->fd;
    make_socket_blocking(newfd);
    verbose_log("%s send fd:%d to fd:%d\n", __func__, newfd, fd);
    char buf[] = {'p', 'm', 'u', 'x'};
    iovec iov = {.iov_base = buf, .iov_len = sizeof(buf)};
    msghdr msg = {0};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
#ifdef __sun
    msg.msg_accrights = (caddr_t)&newfd;
    msg.msg_accrightslen = sizeof(newfd);
#else
    void *fd_buf = alloca(CMSG_SPACE(sizeof(int)));
    msg.msg_control = fd_buf;
    msg.msg_controllen = CMSG_SPACE(sizeof(int));
    cmsghdr *cmsgptr = CMSG_FIRSTHDR(&msg);
    cmsgptr->cmsg_len = CMSG_LEN(sizeof(int));
    cmsgptr->cmsg_level = SOL_SOCKET;
    cmsgptr->cmsg_type = SCM_RIGHTS;
    *((int *)CMSG_DATA(cmsgptr)) = newfd;
#endif
    ssize_t rc = sendmsg(fd, &msg, 0);
    if (rc != sizeof(buf)) {
        /* TODO: reply -1 to client? */
        syslog(LOG_ERR, "%s:sendmsg fd:%d rc:%zd expected:%zu (%s)\n", __func__,
               fd, rc, sizeof(buf), strerror(errno));
    }
    delete (c);
}

static void run_cmd(char *in, connection *c)
{
    int fd = c->fd;
    int is_unix = c->is_unix;
    char *cmd = nullptr, *svc = nullptr, *sav;
    verbose_log("cmd:%s unix:%d remote:%d fd:%d\n", in, is_unix,
                c->is_remote(), fd);
    cmd = strtok_r(in, " ", &sav);
    if (cmd == nullptr)
        return;
    if (strcmp(cmd, "reg") == 0) {
        if (c->is_remote()) {
            disallowed_write();
            return;
        }
        svc = strtok_r(nullptr, " ", &sav);
        if (svc == nullptr) {
            reply("-1 missing service name\n");
            return;
        }
        int port = get_svc_port(svc);
        if (port == -1) {
            port = alloc_port(svc);
        }
        if (port > 0 && is_unix) {
            c->svc = svc;
            alloc_fd(svc, fd);
        }
        reply("%d\n", port);
    } else if (strcmp(cmd, "hello") == 0) {
        reply("ok\n");
    } else if (strcmp(cmd, "get") == 0) {
        char *echo = strtok_r(nullptr, " ", &sav);
        if (echo == nullptr) {
            reply("-1 missing service name\n");
        } else {
            if (strcmp(echo, "/echo") == 0) {
                svc = strtok_r(nullptr, " ", &sav);
            } else {
                svc = echo;
                echo = nullptr;
            }
            if (svc == nullptr) {
                reply("-1\n");
            } else {
                int port = get_svc_port(svc);
                if (echo)
                    reply("%d %s\n", port, svc);
                else
                    reply("%d\n", port);
            }
        }
    } else if (strcmp(cmd, "rte") == 0) {
        char *svc = strtok_r(nullptr, " ", &sav);
        if (svc == nullptr) {
            reply("-1\n");
            return;
        }
        int dest = get_fd(svc);
        if (dest <= 0) {
            reply("-1\n");
            return;
        }
        c->routed = 1;
        event_del(&c->ev);
        event_assign(&c->ev, base, dest, EV_WRITE, route, c);
        event_add(&c->ev, NULL);
        return;
    } else if (strcmp(cmd, "del") == 0) {
        if (c->is_remote()) {
            disallowed_write();
            return;
        }
        svc = strtok_r(nullptr, " ", &sav);
        if (svc == nullptr) {
            reply("-1 missing service name\n");
        } else {
            int rc = dealloc_port(svc);
            reply("%d\n", rc);
        }
    } else if (strcmp(cmd, "use") == 0) {
        if (c->is_remote()) {
            disallowed_write();
            return;
        }
        svc = strtok_r(nullptr, " ", &sav);
        if (svc == nullptr) {
            reply("-1 missing service name\n");
        } else {
            char *p = strtok_r(nullptr, " ", &sav);
            int use = p ? atoi(p) : 0;
            if (use == 0) {
                reply("-1 missing/invalid port\n");
            } else {
                int rc = use_port(svc, use);
                reply("%d\n", rc);
            }
        }
    } else if (strcmp(cmd, "stat") == 0) {
        reply("free ports: %lu\n", free_ports.size());
        for (const auto &i : port_map) {
            reply("%s -> %d\n", i.first.c_str(), i.second);
        }
    } else if (strcmp(cmd, "used") == 0 || strcmp(cmd, "list") == 0) {
        used();
    } else if (strcmp(cmd, "active") == 0) {
        active();
    } else if (strcmp(cmd, "exit") == 0) {
        if (c->is_remote()) {
            disallowed_write();
            return;
        }
        event_base_loopbreak(base);
    } else if (strcmp(cmd, "range") == 0) {
        for (auto &range : port_ranges) {
            reply("%d:%d\n", range.first, range.second);
        }
    } else if (strcmp(cmd, "help") == 0) {
        reply("active                  : list active connections\n"
              "del service             : forget port assignment for service\n"
              "exit                    : shutdown pmux (may be restarted by system)\n"
              "get [/echo] service     : discover port for service\n"
              "help                    : this help message\n"
              "range                   : print port range which this pmux can assign\n"
              "reg service             : obtain/discover port for new service\n"
              "rte                     : get route to instance service/port\n"
              "stat                    : dump some stats\n"
              "use service port        : set specific port registration for service\n"
              "used (or list)          : dump active port assignments\n");
    } else {
        reply("-1 unknown command, type 'help' for a brief usage description\n");
    }
}

static void readcb(int fd, short what, void *arg)
{
    connection *c = (connection *)arg;
    int rc;
    size_t len;
    while ((rc = evbuffer_read(c->buf, fd, 128)) > 0) {
        char *res = evbuffer_readln(c->buf, &len, EVBUFFER_EOL_ANY);
        if (res == NULL)
            continue;
        run_cmd(res, c);
        free(res);
        if (c->routed)
            return;
    }
    if (rc == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return;
    }
    delete (c);
    return;
}

static void tcp_cb(evconnlistener *listener, evutil_socket_t fd, sockaddr *addr,
                   int len, void *is_unix)
{
    sockaddr_in *in = (sockaddr_in *)addr;
    connection *c = new connection(fd, 0, in->sin_addr);
    event_assign(&c->ev, base, fd, EV_READ | EV_PERSIST, readcb, c);
    event_add(&c->ev, NULL);
}

static void unix_cb(evconnlistener *listener, evutil_socket_t fd,
                    sockaddr *addr, int len, void *is_unix)
{
    in_addr in = {0};
    connection *c = new connection(fd, 1, in);
    event_assign(&c->ev, base, fd, EV_READ | EV_PERSIST, readcb, c);
    event_add(&c->ev, NULL);
}

static void accept_errorcb(evconnlistener *listener, void *data)
{
    int err = EVUTIL_SOCKET_ERROR();
    syslog(LOG_CRIT, "%s fd:%d err:%d-%s\n", __func__,
           evconnlistener_get_fd(listener), err,
           evutil_socket_error_to_string(err));
    event_base_loopexit(base, NULL);
}

static int make_port_range(char *s, std::pair<int, int> &range)
{
    std::string orig(s);
    char *sav;
    char *first = strtok_r(s, ":", &sav);
    char *second = strtok_r(nullptr, ":", &sav);
    if (first == nullptr || second == nullptr) {
        syslog(LOG_CRIT, "bad port range -> %s\n", orig.c_str());
        return 1;
    }
    range = std::make_pair(atoi(first), atoi(second));
    return 0;
}

static int usage(FILE *out, int rc)
{
    fprintf(
        out,
        "Usage: pmux [-h] [-c pmuxdb cluster] [-d pmuxdb name] [-b bind path]\n"
        "[-p listen port] [-r free ports range x:y][-l|-n][-f]\n"
        "\n"
        "Options:\n"
        " -h            This help message\n"
        " -c            Cluster information for pmuxdb\n"
        " -d            Use Comdb2 to save port allocations\n"
        " -b            Unix bind path\n"
        " -p            Port pmux will listen on\n"
        " -r            Range of ports to allocate for databases\n"
        " -l            Use file to persist port allocation\n"
        " -n            Do not persist port allocation (default)\n"
        " -f            Run in foreground rather than put to background\n");
    return rc;
}

static bool init_router_mode()
{
    if (unlink(unix_bind_path.c_str()) == -1 && errno != ENOENT) {
        syslog(LOG_CRIT, "error unlinking path:%s rc:%d [%s]\n", unix_bind_path.c_str(),
               errno, strerror(errno));
        return false;
    }
    return true;
}

static int init_local_names()
{
    std::vector<std::string> names = {"localhost"};
    char name[HOST_NAME_MAX];
    if (gethostname(name, sizeof(name)) == 0 && strcmp(name, "localhost") != 0)
        names.push_back(name);
    for (const auto &n : names) {
        verbose_log("local name: %s\n", n.c_str());
        addrinfo *r, *res, hints = {0};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        if (getaddrinfo(n.c_str(), NULL, &hints, &res) == 0) {
           for (r = res; r != NULL; r = r->ai_next) {
               in_addr in = ((sockaddr_in*)r->ai_addr)->sin_addr;
               if (local_addresses.size()) {
                   /* cheap dedup prev addr */
                   if (in == local_addresses[local_addresses.size() - 1]) {
                       continue;
                   }
               }
               local_addresses.emplace_back(in);
           }
           freeaddrinfo(res);
        }
    }
    for (const auto& l : local_addresses) {
        verbose_log("local addr: %s\n", inet_ntoa(l));
    }
   return local_addresses.size() > 0;
}

static bool init(const std::vector<std::pair<int, int>> &pranges)
{

    if (!init_local_names()) {
        syslog(LOG_CRIT, "failed to get host names\n");
        return false;
    }
    for (auto &range : pranges) {
        verbose_log("%s free port range %d - %d\n", __func__, range.first,
                    range.second);
        for (int s = range.first; s <= range.second; ++s) {
            free_ports.insert(s);
        }
    }
    port_map = pmux_store->get_ports();
    for (auto &p : port_map) {
        free_ports.erase(p.second);
    }
    return true;
}

int main(int argc, char **argv)
{
    openlog("pmux", LOG_NDELAY | LOG_PERROR, LOG_USER);

    if (sizeof(event) != event_get_struct_event_size()) {
        syslog(LOG_CRIT, "incorrect sizeof(event):%zu expected:%zu\n",
               sizeof(event), event_get_struct_event_size());
        return EXIT_FAILURE;
    }

    long open_max = sysconf(_SC_OPEN_MAX);
    if (open_max == -1) {
        syslog(LOG_WARNING, "sysconf(_SC_OPEN_MAX): %d %s ", errno,
               strerror(errno));
        open_max = 20;
        syslog(LOG_WARNING, "setting open_max to:%ld\n", open_max);
    }

    std::string host;
    char *hostptr = getenv("HOSTNAME");
    if (hostptr == nullptr) {
        long hostname_max = sysconf(_SC_HOST_NAME_MAX);
        if (hostname_max == -1) {
            syslog(LOG_WARNING, "sysconf(_SC_HOST_NAME_MAX): %d %s ", errno,
                   strerror(errno));
            hostname_max = 255;
            syslog(LOG_WARNING, "setting hostname_max to:%ld\n", hostname_max);
        }
        char myhost[hostname_max + 1];
        int rc = gethostname(myhost, hostname_max);
        if (rc == 0) {
            myhost[hostname_max] = '\0';
            host = hostptr = myhost;
        } else {
            syslog(LOG_CRIT,
                   "Can't figure out hostname: please export HOSTNAME.\n");
            return EXIT_FAILURE;
        }
    } else {
        host = hostptr;
    }

    std::string cluster("prod");
    std::string dbname("pmuxdb");
    std::vector<int> listen_ports = {5105};
    bool default_ports = true;
    bool default_range = true;
    enum store_mode { MODE_NONE, MODE_LOCAL, MODE_COMDB2 };
    store_mode store_mode = MODE_NONE;
    std::pair<int, int> custom_range;
    port_ranges = {{19000, 19999}}; // default range

    int c;
    while ((c = getopt(argc, argv, "hc:d:b:p:r:lnf")) != -1) {
        switch (c) {
        case 'h':
            return usage(stdout, EXIT_SUCCESS);
            break;
        case 'c':
            store_mode = MODE_COMDB2;
            cluster = optarg;
            break;
        case 'd':
            dbname = optarg;
            break;
        case 'b':
            unix_bind_path = optarg;
            break;
        case 'p':
            if (default_ports) {
                listen_ports.resize(0);
                default_ports = false;
            }
            listen_ports.push_back(atoi(optarg));
            break;
        case 'r':
            if (default_range) {
                port_ranges.resize(0);
                default_range = false;
            }
            if (make_port_range(optarg, custom_range) != 0) {
                return usage(stderr, EXIT_FAILURE);
            }
            port_ranges.push_back(custom_range);
            break;
        case '?':
            return usage(stderr, EXIT_FAILURE);
            break;
        case 'l':
            store_mode = MODE_LOCAL;
            break;
        case 'n':
            store_mode = MODE_NONE;
            break;
        case 'f':
            foreground_mode = true;
            break;
        }
    }

    try {
        if (store_mode == MODE_LOCAL)
            pmux_store.reset(new sqlite_store());
        else if (store_mode == MODE_NONE)
            pmux_store.reset(new no_store());
        else
            pmux_store.reset(new comdb2_store(host.c_str(), dbname.c_str(),
                                              cluster.c_str()));
    } catch (std::exception &e) {
        syslog(LOG_CRIT, "%s\n", e.what());
        return EXIT_FAILURE;
    }

    if (!init(port_ranges)) {
        return EXIT_FAILURE;
    }

    if (unix_bind_path.length() + 1 >= sizeof(((sockaddr_un *)0)->sun_path)) {
        syslog(LOG_CRIT, "bad unix domain socket path:%s\n",
               unix_bind_path.c_str());
        return EXIT_FAILURE;
    }

    if (!foreground_mode) {
        bb_daemon();
    }

    sighold(SIGPIPE);
    base = event_base_new();
    verbose_log("Using Libevent %s with backend method %s\n",
                event_get_version(), event_base_get_method(base));
    std::vector<evconnlistener *> listeners;
    evconnlistener *listener;
    for (auto port : listen_ports) {
        sockaddr_in addr = {0};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        socklen_t len = sizeof(addr);
        listener = evconnlistener_new_bind(
            base, tcp_cb, NULL, LEV_OPT_REUSEABLE | LEV_OPT_CLOSE_ON_FREE,
            SOMAXCONN, (sockaddr *)&addr, len);
        if (listener) {
            evconnlistener_set_error_cb(listener, accept_errorcb);
            listeners.push_back(listener);
            verbose_log("accept on port:%d fd:%d\n", port,
                   evconnlistener_get_fd(listener));
            pmux_store->sav_port("pmux", port);
        } else {
            syslog(LOG_CRIT, "failed to listen on port:%d\n", port);
            return EXIT_FAILURE;
        }
    }
    init_router_mode();
    sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, unix_bind_path.c_str());
    socklen_t len = sizeof(addr);
    listener =
        evconnlistener_new_bind(base, unix_cb, NULL, LEV_OPT_CLOSE_ON_FREE,
                                SOMAXCONN, (sockaddr *)&addr, len);
    if (listener) {
        evconnlistener_set_error_cb(listener, accept_errorcb);
        listeners.push_back(listener);
        verbose_log("accept on path:%s fd:%d\n", unix_bind_path.c_str(),
               evconnlistener_get_fd(listener));
    } else {
        syslog(LOG_CRIT, "failed to listen on unix path:%s\n", unix_bind_path.c_str());
        return EXIT_FAILURE;
    }
    syslog(LOG_INFO, "READY\n");
    event_base_dispatch(base);
    for (auto l : listeners) {
        evconnlistener_free(l);
    }
    event_base_free(base);
    return EXIT_SUCCESS;
}
