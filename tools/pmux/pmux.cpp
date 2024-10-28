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

// This program provides a simple port assigner service.  Databases request a
// port to listen on, which is persistent for multiple database runs. Clients
// ask which port to connect to for a specific database. Can use a central
// Comdb2 as a backing store, or SQLite for smaller setups.

#include <cassert>
#include <cstdarg>
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
#include <sys/queue.h>
#include <sys/resource.h>
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
#include <event2/event_compat.h>

#include <bb_daemon.h>

#include "pmux_store.h"
#include "no_store.h"
#ifndef PMUX_SKIP_STORE
  #include "sqlite_store.h"
  #include "comdb2_store.h"
#endif

#ifdef PMUX_VERBOSE
#   define debug_log(...) syslog(LOG_DEBUG, __VA_ARGS__)
#else
#   define debug_log(...)
#endif

#ifdef __APPLE__
#define HOST_NAME_MAX 255
#endif

struct connection;

static event_base *base;
static std::map<std::string, int> port_map;
static std::map<std::string, connection *> connection_map;
static std::set<int> free_ports;
static std::unique_ptr<pmux_store> pmux_store;
static std::vector<in_addr_t> local_addresses;
static std::vector<std::pair<int, int>> port_ranges;
static std::string unix_bind_path = "/tmp/portmux.socket";

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
    if (it == port_map.end())
        return -1;
    int port = it->second;
    pmux_store->del_port(it->first.c_str());
    port_map.erase(it);
    if (is_port_in_range(port)) {
        free_ports.insert(port);
    }
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
        return port;

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
    port_map.insert(std::make_pair(std::string(svc), port));
    pmux_store->sav_port(svc, port);
    return port;
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
    port_map.insert(std::make_pair(std::string(svc), port));
    pmux_store->sav_port(svc, port);
    return port;
}

static void readcb(int, short, void *);
static void routefd(int serverfd, short what, void *arg);
static void writecb(int, short, void *);

/* accept_list is the list of accepted connections which don't "reg"ister with
 * pmux (typically short lived "get"/"rte" only.) */
static TAILQ_HEAD(, connection) accept_list = TAILQ_HEAD_INITIALIZER(accept_list);
static int active_connections;
static int max_active_connections;
static void close_oldest_active_connection(void);

static void set_max_active_connections(void)
{
    int max = -1;
    struct rlimit lim = {0};
    int rc = getrlimit(RLIMIT_NOFILE, &lim);
    if (rc == 0) {
        if (lim.rlim_cur < RLIM_INFINITY) {
            max = lim.rlim_cur;
        } else if (lim.rlim_max < RLIM_INFINITY) {
            max = lim.rlim_max;
        }
    } else {
        syslog(LOG_ERR, "%s getrlimit rc:%d errno:%d [%s]\n", __func__, rc, errno, strerror(errno));
    }
    if (max < 1 || max > 65536) {
        max = 16384;
    }
    int less;
    if (max <= 10240) {
        less = max / 4;
    } else {
        less = max / 10;
    }
    max_active_connections = max - less;
    syslog(LOG_DEBUG, "%s:%d\n", __func__, max_active_connections);
}

struct connection {
  protected:
    in_addr_t addr;
    event ev;
    evbuffer *rdbuf;
    evbuffer *wrbuf;
    void enable_read()
    {
        event_assign(&ev, base, fd, EV_READ | EV_PERSIST, readcb, this);
        event_add(&ev, NULL);
    }
    void enable_write()
    {
        event_assign(&ev, base, fd, EV_WRITE, writecb, this);
        event_add(&ev, NULL);
    }
    void init()
    {
        if (active_connections > max_active_connections) {
            close_oldest_active_connection();
        }
        ++active_connections;
        TAILQ_INSERT_TAIL(&accept_list, this, entry);
        enable_read();
    }
  public:
    char protocol[4];
    int fd;
    int is_unix;
    std::string svc;
    TAILQ_ENTRY(connection) entry;
    connection(int f)
        : addr{0}, rdbuf{evbuffer_new()}, wrbuf{evbuffer_new()}, fd{f}, is_unix{1}
    {
        init();
    }
    connection(int f, uint32_t a)
        : addr{a}, rdbuf{evbuffer_new()}, wrbuf{evbuffer_new()}, fd{f}, is_unix{0}, protocol{'p', 'm', 'u', 'x'}
    {
        init();
    }
    ~connection()
    {
        debug_log("%s fd:%d\n", __func__, fd);
        --active_connections;
        const auto& c = connection_map.find(svc);
        if (c != connection_map.end() && c->second == this) {
            debug_log("%s removing from active connections:%s fd:%d\n", __func__, svc.c_str(), fd);
            connection_map.erase(c);
        } else {
            TAILQ_REMOVE(&accept_list, this, entry);
        }
        event_del(&ev);
        if (rdbuf) evbuffer_free(rdbuf);
        if (wrbuf) evbuffer_free(wrbuf);
        rdbuf = wrbuf = nullptr;
        close(fd);
    }
    int is_remote()
    {
        if (is_unix) return 0;
        for (auto &l : local_addresses) {
            if (l == addr) return 0;
        }
        return 1;
    }
    int readln(char **out)
    {
        size_t len;
        *out = evbuffer_readln(rdbuf, &len, EVBUFFER_EOL_ANY);
        if (*out) {
            return 0;
        }
        len = evbuffer_get_length(rdbuf);
        if (len < 1024) {
            return 0;
        }
        debug_log("no newline in %zu bytes\n", len);
        delete this;
        return -1;
    }
    void route(int dest)
    {
        event_del(&ev);
        event_assign(&ev, base, dest, EV_WRITE | EV_TIMEOUT, routefd, this);
        struct timeval timeout;
        timeout.tv_sec = 0;
        timeout.tv_usec = 10 * 1000; //10ms
        event_add(&ev, &timeout);
    }
    int active()
    {
        ssize_t rc = read(fd, NULL, 0);
        if (rc != 0) return 0;
        return write(fd, NULL, 0) == 0;
    }
    int readbuf()
    {
        errno = 0;
        return evbuffer_read(rdbuf, fd, 256);
    }
    void writebuf()
    {
        int rc = evbuffer_write(wrbuf, fd);
        if (rc <= 0) {
            delete this;
            return;
        }
        event_del(&ev);
        if (evbuffer_get_length(wrbuf) == 0) {
            debug_log("%s fd:%d completed writing reply\n", __func__, fd);
            enable_read();
        } else {
            debug_log("%s fd:%d waiting to write reply\n", __func__, fd);
            enable_write();
        }
    }
    void reply(const char *fmt, ...)
    {
        va_list args;
        va_start(args, fmt);
#       ifdef PMUX_VERBOSE
        evbuffer *logbuf = evbuffer_new();
        evbuffer_add_printf(logbuf, "%s fd:%d %s", __func__, fd, fmt);
        char *logfmt = (char *)evbuffer_pullup(logbuf, -1);
        va_list log_args;
        va_copy(log_args, args);
        vsyslog(LOG_DEBUG, logfmt, log_args);
        va_end(log_args);
        evbuffer_free(logbuf);
#       endif
        evbuffer_add_vprintf(wrbuf, fmt, args);
        va_end(args);
        evbuffer_write(wrbuf, fd);
        if (evbuffer_get_length(wrbuf) != 0) {
            debug_log("%s fd:%d waiting to write reply\n", __func__, fd);
            event_del(&ev);
            enable_write();
        }
    }
    void reply_not_permitted()
    {
        reply("-1 write requests not permitted from this host\n");
    }
};

struct secure_connection : connection {
  public:
    secure_connection(int f, uint32_t a) : connection(f, a)
    {
        char spmu[] = {'s', 'p', 'm', 'u'};
        memcpy(protocol, spmu, sizeof(spmu));
    }
};

static void close_oldest_active_connection(void)
{
    connection *c = TAILQ_FIRST(&accept_list);
    if (!c) {
        syslog(LOG_CRIT, "%s no active connection to close [count:%d]\n", __func__, active_connections);
        event_base_loopbreak(base);
        return;
    }
    syslog(LOG_WARNING, "%s closing oldest active connection fd:%d [count:%d]\n", __func__, c->fd, active_connections);
    delete c;
}


static int get_fd(const char *key)
{
    const auto &c = connection_map.find(key);
    if (c != connection_map.end()) {
        return c->second->fd;
    }
    return -1;
}

static void writecb(int fd, short what, void *arg)
{
    connection *c = (connection *)(arg);
    c->writebuf();
}

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

static void routefd(int serverfd, short what, void *arg)
{
    connection *c = (connection *)(arg);
    int clientfd = c->fd;
    debug_log("%s send fd:%d to fd:%d\n", __func__, clientfd, serverfd);
    iovec iov = {.iov_base = c->protocol, .iov_len = sizeof(c->protocol)};
    msghdr msg = {0};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
#ifdef __sun
    msg.msg_accrights = (caddr_t)&clientfd;
    msg.msg_accrightslen = sizeof(clientfd);
#else
    void *fd_buf = alloca(CMSG_SPACE(sizeof(int)));
    msg.msg_control = fd_buf;
    msg.msg_controllen = CMSG_SPACE(sizeof(int));
    cmsghdr *cmsgptr = CMSG_FIRSTHDR(&msg);
    cmsgptr->cmsg_len = CMSG_LEN(sizeof(int));
    cmsgptr->cmsg_level = SOL_SOCKET;
    cmsgptr->cmsg_type = SCM_RIGHTS;
    *((int *)CMSG_DATA(cmsgptr)) = clientfd;
#endif
    ssize_t rc = sendmsg(serverfd, &msg, 0);
    if (rc == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return;
    }
    if (rc != sizeof(c->protocol)) {
        syslog(LOG_ERR, "%s:sendmsg fd:%d rc:%zd expected:%zu (%s)\n", __func__, serverfd, rc, sizeof(c->protocol),
               strerror(errno));
        const auto &s = connection_map.find(c->svc);
        if (s != connection_map.end()) {
            delete s->second;
        }
    }
    delete c;
}

static int check_active(const char *svc)
{
    return connection_map.find(svc) != connection_map.end();
}

static int run_cmd(char *cmd, connection *c)
{
    debug_log("%s fd:%d cmd:%s", __func__, c->fd, cmd);
    char *sav;
    cmd = strtok_r(cmd, " ", &sav);
    if (cmd == nullptr) {
        c->reply("-1 missing command\n");
        return 0;
    }
    if (strcmp(cmd, "reg") == 0) {
        if (c->is_remote()) {
            c->reply_not_permitted();
            return 0;
        }
        char *svc = strtok_r(nullptr, " ", &sav);
        if (svc == nullptr) {
            c->reply("-1 missing service name\n");
            return 0;
        }
        if (check_active(svc)) {
            c->reply("-1 service already active\n");
            return 0;
        }
        int port = get_svc_port(svc);
        if (port == -1) {
            port = alloc_port(svc);
        }
        debug_log("%s assigned %d", svc, port);
        if (port <= 0) {
            c->reply("%d\n", port);
            return 0;
        }
        if (c->is_unix) {
            c->svc = svc;
            connection_map.insert(std::make_pair(svc, c));
            TAILQ_REMOVE(&accept_list, c, entry);
        }
        c->reply("%d\n", port);
    } else if (strcmp(cmd, "get") == 0) {
        char *svc;
        char *echo = strtok_r(nullptr, " ", &sav);
        if (echo == nullptr) {
            c->reply("-1 missing service name\n");
        } else {
            if (strcmp(echo, "/echo") == 0) {
                svc = strtok_r(nullptr, " ", &sav);
            } else {
                svc = echo;
                echo = nullptr;
            }
            if (svc == nullptr) {
                c->reply("-1\n");
            } else {
                int port = get_svc_port(svc);
                if (echo)
                    c->reply("%d %s\n", port, svc);
                else
                    c->reply("%d\n", port);
            }
        }
    } else if (strcmp(cmd, "rte") == 0) {
        char *svc = strtok_r(nullptr, " ", &sav);
        if (svc == nullptr) {
            c->reply("-1\n");
            return 0;
        }
        int dest = get_fd(svc);
        if (dest <= 0) {
            c->reply("-1\n");
            return 0;
        }
        c->svc = svc;
        c->route(dest);
        return -1;
    } else if (strcmp(cmd, "del") == 0) {
        if (c->is_remote()) {
            c->reply_not_permitted();
            return 0;
        }
        char *svc = strtok_r(nullptr, " ", &sav);
        if (svc == nullptr) {
            c->reply("-1 missing service name\n");
        } else {
            debug_log("%s dealloc", svc);
            int rc = dealloc_port(svc);
            c->reply("%d\n", rc);
        }
    } else if (strcmp(cmd, "use") == 0) {
        if (c->is_remote()) {
            c->reply_not_permitted();
            return 0;
        }
        char *svc = strtok_r(nullptr, " ", &sav);
        if (svc == nullptr) {
            c->reply("-1 missing service name\n");
        } else {
            char *p = strtok_r(nullptr, " ", &sav);
            int use = p ? atoi(p) : 0;
            if (use == 0) {
                c->reply("-1 missing/invalid port\n");
            } else {
                int rc = use_port(svc, use);
                c->reply("%d\n", rc);
                debug_log("%s use %d", svc, use);
            }
        }
    } else if (strcmp(cmd, "stat") == 0) {
        c->reply("free ports: %lu\n", free_ports.size());
        for (const auto &i : port_map) {
            c->reply("%s -> %d\n", i.first.c_str(), i.second);
        }
    } else if (strcmp(cmd, "used") == 0 || strcmp(cmd, "list") == 0) {
        for (auto &kv : port_map) {
            c->reply("port %-7d name %s\n", kv.second, kv.first.c_str());
        }
    } else if (strcmp(cmd, "active") == 0) {
        for (auto &kv : connection_map) {
            const auto &svc = kv.first;
            const auto &port = port_map.find(svc);
            if (port != port_map.end()) {
                c->reply("port %-7d name %s\n", port->second, svc.c_str());
            }
        }
    } else if (strcmp(cmd, "exit") == 0) {
        if (c->is_remote()) {
            c->reply_not_permitted();
            return 0;
        }
        delete c;
        event_base_loopbreak(base);
        return -1;
    } else if (strcmp(cmd, "range") == 0) {
        for (auto &range : port_ranges) {
            c->reply("%d:%d\n", range.first, range.second);
        }
    } else if (strcmp(cmd, "help") == 0) {
        c->reply("active              : list active connections\n"
                 "del service         : forget port assignment for service\n"
                 "exit                : shutdown pmux (may be restarted by system)\n"
                 "get [/echo] service : discover port for service\n"
                 "help                : this help message\n"
                 "range               : print port range which this pmux can assign\n"
                 "reg service         : obtain/discover port for new service\n"
                 "rte                 : get route to instance service/port\n"
                 "stat                : dump some stats\n"
                 "use service port    : set specific port registration for service\n"
                 "used (or list)      : dump active port assignments\n");
    } else {
        c->reply("-1 unknown command, type 'help' for a brief usage description\n");
    }
    return 0;
}

static void readcb(int fd, short what, void *arg)
{
    debug_log("%s fd:%d start\n", __func__, fd);
    connection *c = (connection *)(arg);
    int rc = c->readbuf();
    if (rc <= 0) {
        debug_log("read fd:%d rc:%d errno:%d-%s\n", fd, rc, errno, strerror(errno));
        delete c;
        return;
    }
    char *res;
    while (c->readln(&res) == 0 && res != NULL) {
        int rc = run_cmd(res, c);
        free(res);
        if (rc) break;
    }
    debug_log("%s fd:%d done", __func__, fd);
}

static void tcp_cb(evconnlistener *listener, evutil_socket_t fd, sockaddr *addr,
                   int len, void *unused)
{
    debug_log("%s new connection fd:%d\n", __func__, fd);
    sockaddr_in &in = *(sockaddr_in *)addr;
    connection *c = new connection(fd, in.sin_addr.s_addr);
}

static void tcp_secure_cb(evconnlistener *listener, evutil_socket_t fd, sockaddr *addr, int len, void *unused)
{
    debug_log("%s new secure connection fd:%d\n", __func__, fd);
    sockaddr_in &in = *(sockaddr_in *)addr;
    connection *c = new secure_connection(fd, in.sin_addr.s_addr);
}

static void unix_cb(evconnlistener *listener, evutil_socket_t fd,
                    sockaddr *addr, int len, void *unused)
{
    debug_log("%s new connection fd:%d\n", __func__, fd);
    connection *c = new connection(fd);
}

static void accept_error_cb(evconnlistener *listener, void *data)
{
    int err = EVUTIL_SOCKET_ERROR();
    int fd = evconnlistener_get_fd(listener);
    const char *errstr = evutil_socket_error_to_string(err);
    syslog(LOG_CRIT, "%s fd:%d errno:%d [%s]\n", __func__, fd, err, errstr);
    if (err != EMFILE && err != ENFILE) {
        event_base_loopbreak(base);
        return;
    }
    close_oldest_active_connection();
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

static bool unlink_bind_path()
{
    if (unlink(unix_bind_path.c_str()) == -1 && errno != ENOENT) {
        syslog(LOG_CRIT, "error unlinking path:%s rc:%d [%s]\n", unix_bind_path.c_str(),
               errno, strerror(errno));
        return false;
    }
    return true;
}

void sigint_handler(int signum, short evs, void *arg)
{
    if(base)
        event_base_loopbreak(base);
}

static int init_local_names()
{
    std::vector<std::string> names = {"localhost"};
    char name[HOST_NAME_MAX];
    if (gethostname(name, sizeof(name)) == 0 && strcmp(name, "localhost") != 0)
        names.push_back(name);
    for (const auto &n : names) {
        debug_log("local name: %s\n", n.c_str());
        addrinfo *r, *res, hints = {0};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        if (getaddrinfo(n.c_str(), NULL, &hints, &res) == 0) {
           for (r = res; r != NULL; r = r->ai_next) {
               in_addr &addr = ((sockaddr_in *)r->ai_addr)->sin_addr;
               in_addr_t in = addr.s_addr;
               if (local_addresses.size()) {
                   /* cheap dedup prev addr */
                   if (in == local_addresses[local_addresses.size() - 1]) {
                       continue;
                   }
               }
               local_addresses.emplace_back(in);
               debug_log("local addr: %s\n", inet_ntoa(addr));
           }
           freeaddrinfo(res);
        }
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
        debug_log("%s free port range %d - %d\n", __func__, range.first,
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

    if (sizeof(event) < event_get_struct_event_size()) {
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

    char myhost[512] = {0};
    char *hostptr = getenv("HOSTNAME");
    if (hostptr == nullptr) {
        int rc = gethostname(myhost, sizeof(myhost) - 1);
        if (rc) {
            syslog(LOG_CRIT, "Can't figure out hostname: please export HOSTNAME.\n");
            return EXIT_FAILURE;
        }
        hostptr = myhost;
    }
    std::string host(hostptr);

    bool foreground_mode = false;
    std::string cluster("prod");
    std::string dbname("pmuxdb");
    std::vector<int> listen_ports;
    std::vector<int> listen_secure_ports;
    enum store_mode { MODE_NONE, MODE_LOCAL, MODE_COMDB2 };
    store_mode store_mode = MODE_NONE;
    std::pair<int, int> custom_range;

    int c;
    while ((c = getopt(argc, argv, "hc:d:b:p:s:r:lnf")) != -1) {
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
            listen_ports.push_back(atoi(optarg));
            break;
        case 's':
            listen_secure_ports.push_back(atoi(optarg));
            break;
        case 'r':
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

    /* Extern optind is populated by getopt() -- if it is less than argc
     * then it points to an argument that was not processed by getopt.
     * If that is the case it means invalid arguments were passed in
     */
    if (optind < argc) {
        fprintf(stderr, "%s: invalid argument '%s'\n", argv[0], argv[optind]);
        return usage(stderr, EXIT_FAILURE);
    }

    if (listen_ports.size() == 0) {
        listen_ports.push_back(5105); // default port
    }

    if (port_ranges.size() == 0) {
        port_ranges = {{19000, 19999}}; // default range
    }

    pmux_store.reset(new no_store());
#   ifndef PMUX_SKIP_STORE
    try {
        if (store_mode == MODE_LOCAL)
            pmux_store.reset(new sqlite_store());
        else if (store_mode == MODE_COMDB2)
            pmux_store.reset(new comdb2_store(host.c_str(), dbname.c_str(),
                                              cluster.c_str()));
    } catch (std::exception &e) {
        syslog(LOG_CRIT, "%s\n", e.what());
        return EXIT_FAILURE;
    }
#   endif

    if (!init(port_ranges)) {
        return EXIT_FAILURE;
    }

    if (unix_bind_path.length() + 1 >= sizeof(((sockaddr_un *)0)->sun_path)) {
        syslog(LOG_CRIT, "bad unix domain socket path:%s\n",
               unix_bind_path.c_str());
        return EXIT_FAILURE;
    }

    signal(SIGPIPE, SIG_IGN);

    struct event_config *cfg = event_config_new();
    event_config_set_flag(cfg, EVENT_BASE_FLAG_NOLOCK);
    base = event_base_new_with_config(cfg);
    event_config_free(cfg);
    debug_log("Using Libevent %s with backend method %s\n",
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
            evconnlistener_set_error_cb(listener, accept_error_cb);
            listeners.push_back(listener);
            debug_log("accept on port:%d fd:%d\n", port,
                      evconnlistener_get_fd(listener));
            pmux_store->sav_port("pmux", port);
        } else {
            syslog(LOG_CRIT, "failed to listen on port:%d\n", port);
            return EXIT_FAILURE;
        }
    }

    for (auto port : listen_secure_ports) {
        sockaddr_in addr = {0};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        socklen_t len = sizeof(addr);
        listener = evconnlistener_new_bind(base, tcp_secure_cb, NULL, LEV_OPT_REUSEABLE | LEV_OPT_CLOSE_ON_FREE,
                                           SOMAXCONN, (sockaddr *)&addr, len);
        if (listener) {
            evconnlistener_set_error_cb(listener, accept_error_cb);
            listeners.push_back(listener);
            debug_log("accept on secure port:%d fd:%d\n", port, evconnlistener_get_fd(listener));
            pmux_store->sav_port("pmux", port);
        } else {
            syslog(LOG_CRIT, "failed to listen on secure port:%d\n", port);
            return EXIT_FAILURE;
        }
    }

    unlink_bind_path();
    sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, unix_bind_path.c_str());
    socklen_t len = sizeof(addr);
    listener = evconnlistener_new_bind(
            base, unix_cb, NULL, LEV_OPT_CLOSE_ON_FREE, SOMAXCONN,
            (sockaddr *)&addr, len);
    if (listener) {
        evconnlistener_set_error_cb(listener, accept_error_cb);
        listeners.push_back(listener);
        debug_log("accept on path:%s fd:%d\n", unix_bind_path.c_str(),
                  evconnlistener_get_fd(listener));
    } else {
        syslog(LOG_CRIT, "failed to listen on unix path:%s\n", unix_bind_path.c_str());
        return EXIT_FAILURE;
    }

    if (!foreground_mode) {
        bb_daemon();
        event_reinit(base);
    }

    set_max_active_connections();

    struct event *trm = evsignal_new(base, SIGINT, sigint_handler, NULL);
    evsignal_add(trm, NULL);

    syslog(LOG_INFO, "READY\n");
    event_base_dispatch(base);
    for (const auto& l : listeners) {
        evconnlistener_free(l);
    }
    for (const auto& c : connection_map) {
        connection *conn = c.second;
        delete conn;
    }
    event_base_free(base);
    unlink_bind_path();
    syslog(LOG_INFO, "GOODBYE\n");
    return EXIT_SUCCESS;
}
