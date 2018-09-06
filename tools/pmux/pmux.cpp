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
// port to listen on, which is persistent for multiple database runs.
// Clients ask which port to connect to for a specific database.
// Can use a central Comdb2 as a backing store, or SQLite for smaller setups.

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <stdlib.h>
#include <cstring>
#include <cstdarg>
#include <strings.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <set>
#include <list>
#include <map>
#include <algorithm>
#include <memory>
#include <thread>
#include <mutex>

#include <netdb.h>
#include <unistd.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>
#include <poll.h>
#include <signal.h>
#include <syslog.h>
#include <netdb.h>
#ifdef VERBOSE
#include <fsnapf.h>
#endif
#include <passfd.h>

#include "pmux_store.h"
#include "comdb2_store.h"
#include "sqlite_store.h"
#include "no_store.h"
#include <bb_daemon.h>

static std::map<std::string, int> port_map;
static std::map<std::string, int> fd_map;
static std::mutex fdmap_mutex;
static std::mutex active_services_mutex;
static std::set<int> free_ports;
static long open_max;
static bool foreground_mode = false;
static std::unique_ptr<pmux_store> pmux_store;
static std::vector<struct in_addr> local_addresses(5);

struct connection {
    int fd;
    std::list<std::string> out;
    char inbuf[128];
    int inoff;
    bool writable;
    bool is_hello;
    std::string service;
    struct in_addr addr;
    connection(void)
        : fd{-1}, inbuf{0}, inoff{0}, writable{false}, addr{0}, out(),
          is_hello(false)
    {
    }
};

static std::set<std::string> active_services;

std::vector<connection> connections;

static char unix_bind_path[108] = "/tmp/portmux.socket";

static int get_fd(const char *svc)
{
    int fd_ret = -1;
    std::string key(svc);
    std::lock_guard<std::mutex> l(fdmap_mutex);
    const auto &fd = fd_map.find(key);
    if (fd != fd_map.end()) {
        fd_ret = fd->second;
    }
    return fd_ret;
}

static int dealloc_fd(const char *svc)
{
    std::string key(svc);
    std::lock_guard<std::mutex> l(fdmap_mutex);
    const auto &i = fd_map.find(key);
    if (i == fd_map.end()) {
        return 0;
    }
    if (i->second > 0)
        close(i->second);
    fd_map.erase(i);
    return 0;
}

static int alloc_fd(const char *svc, int fd)
{
    std::lock_guard<std::mutex> l(fdmap_mutex);
    std::pair<std::string, int> kv(svc, fd);
    fd_map.insert(kv);
    return 0;
}

static int get_port(const char *svc)
{
    std::string key(svc);
    const auto &port = port_map.find(key);
    if (port == port_map.end())
        return -1;
    return port->second;
}

static int connect_instance(int servicefd, char *name)
{
#ifdef VERBOSE
    fprintf(stderr, "Adding fd %d service %s\n", servicefd, name);
#endif
    int port = get_port(name);
    std::string s;
    std::stringstream out;
    out << port;
    s = out.str();
    write(servicefd, s.c_str(), strlen(s.c_str()));
    int oldfd = get_fd(name);
    if (oldfd > 0) {
        dealloc_fd(name);
    }
    int rc = alloc_fd(name, servicefd);
#ifdef VERBOSE
    std::cout << "connect " << name << " " << servicefd << std::endl;
#endif
    return rc;
}

int client_func(int fd)
{
    char service[128];
    int listenfd = fd;
#ifdef VERBOSE
    fprintf(stderr, "Starting client thread \n");
#endif
    ssize_t n = read(listenfd, service, sizeof(service));
#ifdef VERBOSE
    fprintf(stderr, "%s\n", service);
#endif
    char *sav;
    char *cmd = strtok_r(service + 4, " \n", &sav);
    if (strncasecmp(service, "reg", 3) == 0) {
        {
            std::lock_guard<std::mutex> l(active_services_mutex);
            if (active_services.find(cmd) == active_services.end()) {
                syslog(LOG_WARNING, "reg request from %s, but not an active service?\n",
                        cmd);
                close(fd);
                return -1;
            }
        }
        connect_instance(listenfd, cmd);
    } else {
        close(fd);
    }
    return 0;
}

static void unwatchfd(struct pollfd &fd)
{
    connections[fd.fd].inoff = 0;
    if (connections[fd.fd].is_hello) {
        {
            std::lock_guard<std::mutex> l(active_services_mutex);
            active_services.erase(connections[fd.fd].service);
        }
        std::string svc(connections[fd.fd].service);

#ifdef VERBOSE
        std::cout << "bye from " << svc << std::endl;
#endif

        {
            std::lock_guard<std::mutex> l(fdmap_mutex);
            auto ufd = fd_map.find(svc);
            if (ufd != fd_map.end()) {
                fd_map.erase(svc);
                int rc = close(ufd->second);
                if (rc) {
                    syslog(LOG_WARNING, "%s close fd %d rc %d\n", svc.c_str(),
                            ufd->second, rc);
                }
            }
        }
    }

    // Throw away any buffers we may have
    while (connections[fd.fd].out.size())
        connections[fd.fd].out.pop_front();
// Note that we dont shrink connections.
#ifdef VERBOSE
    syslog(LOG_INFO, "close conn %d\n", fd.fd);
#endif
    int rc = close(fd.fd);
    fd = {.fd = -1, .events = 0, .revents = 0};
}

static void accept_thd(int listenfd)
{
#ifdef VERBOSE
    printf("Unix domain socket accept thread starting for fd %d\n", listenfd);
#endif

    int lcl_exiting;
    while (1) {
        struct sockaddr_un client_addr;
        socklen_t clilen;
        int fd;

        clilen = sizeof(client_addr);

        fd = accept(listenfd, (struct sockaddr *)&client_addr, &clilen);
        if (fd == -1) {
            if (errno == EINTR)
                continue;
            syslog(LOG_ERR, "%s:accept %d %s\n", __func__, errno,
                    strerror(errno));
            exit(1);
        }

        std::thread t1(client_func, fd);
        t1.detach();
    }
}

static void init_router_mode()
{
    int listenfd;
    struct sockaddr_un serv_addr;

#ifdef VERBOSE
    printf("Will listen on local domain socket %s\n", unix_bind_path);
#endif

    listenfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listenfd == -1) {
        syslog(LOG_ERR, "Error socket: %d %s\n", errno, strerror(errno));
        exit(1);
    }

    if (unlink(unix_bind_path) == -1 && errno != ENOENT) {
        syslog(LOG_ERR, "Error unlinking '%s': %d %s\n", unix_bind_path, errno,
                strerror(errno));
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sun_family = AF_UNIX;
    strncpy(serv_addr.sun_path, unix_bind_path, sizeof(serv_addr.sun_path));

    if (bind(listenfd, (const struct sockaddr *)&serv_addr,
             sizeof(serv_addr)) == -1) {
        syslog(LOG_ERR, "Error bind: %d %s\n", errno, strerror(errno));
        exit(1);
    }

    if (listen(listenfd, 128) == -1) {
        syslog(LOG_ERR, "Error listen: %d %s\n", errno, strerror(errno));
        exit(1);
    }

    static struct stat save_st;

    if (-1 == stat(unix_bind_path, &save_st)) {
        syslog(LOG_ERR, "Unable to stat '%s': %d %s\n", unix_bind_path, errno,
                strerror(errno));
        exit(1);
    }
    chmod(unix_bind_path, 0777);

    std::thread t1(accept_thd, listenfd);
    t1.detach();
}

#if 0
#define VERBOSE
#define DEBUG_PARTIAL_IO
#endif

#ifdef DEBUG_PARTIAL_IO
int maybe_write(int fd, const void *buf, size_t count)
{
    if (count == 0)
        return 0;
    count = 1 + rand() % count;
    return write(fd, buf, count);
}

int maybe_read(int fd, void *buf, size_t count)
{
    if (count == 0)
        return 0;
    count = 1 + rand() % ((count > 5) ? 5 : count);
    printf("read %d\n", count);
    return read(fd, buf, count);
}
#define write maybe_write
#define read maybe_read
#endif

static void conn_printf(connection &c, const char *fmt, ...);
static int dealloc_port(const char *svc);

static int tcp_listen(uint16_t port)
{
    int rc;
    int fd;
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        syslog(LOG_ERR, "socket: %d %s", errno, strerror(errno));
        return -1;
    }

    int reuse = 1;
    rc = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(int));
    if (rc == -1) {
        syslog(LOG_ERR, "setsockopt: %d %s", errno, strerror(errno));
        return rc;
    }

    struct sockaddr_in bindaddr = {0};
    bindaddr.sin_family = AF_INET;
    bindaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    bindaddr.sin_port = htons(port);
    rc = bind(fd, (struct sockaddr *)&bindaddr, sizeof(bindaddr));
    if (rc == -1) {
        syslog(LOG_ERR, "bind: %d %s", errno, strerror(errno));
        return rc;
    }
    rc = listen(fd, SOMAXCONN);
    if (rc == -1) {
        syslog(LOG_ERR, "listen: %d %s", errno, strerror(errno));
        return rc;
    }
    if (port == 0) { // get allocated port
        struct sockaddr_in portaddr = {0};
        socklen_t len = sizeof(portaddr);
        rc = getsockname(fd, (struct sockaddr *)&portaddr, &len);
        if (rc == -1) {
            syslog(LOG_ERR, "getsockname: %d %s", errno, strerror(errno));
            return rc;
        }
        port = htons(portaddr.sin_port);
    }
#ifdef VERBOSE
    syslog(LOG_INFO, "pmux port:%d\n", port);
#endif
    pmux_store->sav_port("pmux", port);
    return fd;
}

static int use_port(const char *svc, int port)
{
    const auto &i = free_ports.find(port);
    // remember the old service->port mapping, if any
    int usedport = get_port(svc);

    if (i == free_ports.end()) {
        /* service can tell us over and over that it's using the port, that's
         * fine */
        if (usedport == port)
            return 0;

        syslog(LOG_ERR, "%s -- not using port, not on free list:%d\n", svc,
                port);
        return -1;
    } else {
        // was the service using a different port before?  remove that mapping
        if (usedport != -1)
            dealloc_port(svc);
    }
    free_ports.erase(i);

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

static int dealloc_port(const char *svc)
{
    std::string key(svc);
    const auto &i = port_map.find(key);
    if (i == port_map.end()) {
        return -1;
    }
    int port = i->second;
    port_map.erase(i);
    pmux_store->del_port(svc);
    free_ports.insert(port);
    return 0;
}
#if 0
static char hexmap[] = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
static void hprintf(FILE *f, char *b, size_t s)
{
    fprintf(f, "0x");
    for (int i = 0; i < s; ++i) {
        if (i % 4 == 0)
            fprintf(f, " ");
        fprintf(f, "%c%c", hexmap[(b[i] & 0xf0) >> 4],
            hexmap[b[i] & 0x0f]);
    }
    fprintf(f, "\n");
}
#endif

static bool invalid_fd(const struct pollfd &fd) { return fd.fd == -1; }

static bool is_local(struct in_addr addr)
{
    for (auto local_addr : local_addresses) {
        if (local_addr.s_addr == addr.s_addr)
            return true;
    }

    return false;
}

static int watchfd(int fd, std::vector<struct pollfd> &fds, struct in_addr addr)
{
    auto freefd = std::find_if(fds.begin(), fds.end(), invalid_fd);
    if (freefd != fds.end()) {
        *freefd = {.fd = fd, .events = POLLIN, .revents = 0};
    } else {
        if (fds.size() >= open_max) {
            return 1;
        }
        fds.push_back({.fd = fd, .events = POLLIN, .revents = 0});
    }
    if (fd >= connections.size()) {
        int oldsize = connections.size();
        connections.resize(fd + 1);
    }

#ifdef VERBOSE
    syslog(LOG_INFO, "got conn %d\n", fd);
#endif
    connections[fd].fd = fd;
    connections[fd].writable = is_local(addr);
    connections[fd].addr = addr;
    connections[fd].is_hello = false;
    return 0;
}

static void used(connection &c)
{
    for (auto &port : port_map) {
        conn_printf(c, "port %-7d name %s\n", port.second, port.first.c_str());
    }
}

static void conn_printf(connection &c, const char *fmt, ...)
{
    va_list args;
    char *out;
    va_start(args, fmt);
    out = sqlite3_vmprintf(fmt, args);
    va_end(args);
#ifdef VERBOSE
    syslog(LOG_INFO, "sending: %s\n", out);
#endif
    c.out.push_back(out);
    sqlite3_free(out);
}

static int route_to_instance(char *svc, int fd)
{
    int routefd = get_fd(svc);
    if (routefd > 0) {
        const char *msg = "pmux";
        return send_fd(routefd, msg, size_t(strlen(msg)), fd);
    }

    return -1;
}

void disallowed_write(connection &c, char *cmd)
{
    char *ip = inet_ntoa(c.addr);
    syslog(LOG_INFO, "attempt to write (%s) from remote connection %s\n", cmd,
           ip);
    conn_printf(c, "-1 write requests not permitted from this host\n");
}

static int run_cmd(struct pollfd &fd, std::vector<struct pollfd> &fds, char *in,
                   connection &c)
{
    int bad = 0;
    char *cmd = NULL, *svc = NULL, *sav;

#ifdef VERBOSE
    syslog(LOG_INFO, "%d: cmd: %s\n", fd.fd, in);
//  fsnapf(stdout, in, strlen(in));
#endif

    cmd = strtok_r(in, " ", &sav);
    if (cmd == NULL)
        goto done;

again:
    if (strcmp(cmd, "reg") == 0) {
        if (c.writable) {
            svc = strtok_r(NULL, " ", &sav);
            if (svc == NULL) {
                conn_printf(c, "-1 missing service name\n");
            } else {
                int port = get_port(svc);
                if (port == -1) {
                    port = alloc_port(svc);
                }
                conn_printf(c, "%d\n", port);
            }
        } else {
            disallowed_write(c, cmd);
        }
    } else if (strcmp(cmd, "get") == 0) {
        char *echo = strtok_r(NULL, " ", &sav);
        if (echo == NULL) {
            conn_printf(c, "-1 missing service name\n");
        } else {
            if (strcmp(echo, "/echo") == 0) {
                svc = strtok_r(NULL, " ", &sav);
            } else {
                svc = echo;
                echo = NULL;
            }
            if (svc == NULL) {
                conn_printf(c, "-1\n");
            } else {
                int port = get_port(svc);
                if (echo)
                    conn_printf(c, "%d %s\n", port, svc);
                else
                    conn_printf(c, "%d\n", port);
            }
        }
    } else if (strcmp(cmd, "rte") == 0) {
        char *svc = strtok_r(NULL, " ", &sav);
        if (svc == NULL) {
            conn_printf(c, "-1\n");
        } else {
            int rc = route_to_instance(svc, fd.fd);
            if (rc == 0) {
                unwatchfd(fd);
            } else {
                dealloc_fd(svc);
                conn_printf(c, "-1\n");
            }
        }
    } else if (strcmp(cmd, "del") == 0) {
        if (c.writable) {
            svc = strtok_r(NULL, " ", &sav);
            if (svc == NULL) {
                conn_printf(c, "-1 missing service name\n");
            } else {
                int rc = dealloc_port(svc);
                conn_printf(c, "%d\n", rc);
            }
        } else {
            disallowed_write(c, cmd);
        }
    } else if (strcmp(cmd, "use") == 0) {
        if (c.writable) {
            svc = strtok_r(NULL, " ", &sav);
            if (svc == NULL) {
                conn_printf(c, "-1 missing service name\n");
            } else {
                char *p = strtok_r(NULL, " ", &sav);
                int use = p ? atoi(p) : 0;
                if (use == 0) {
                    conn_printf(c, "-1 missing/invalid port\n");
                } else {
                    int rc = use_port(svc, use);
                    conn_printf(c, "%d\n", rc);
                }
            }
        } else {
            disallowed_write(c, cmd);
        }
    } else if (strcmp(cmd, "stat") == 0) {
        conn_printf(c, "free ports: %lu\n", free_ports.size());
        for (const auto &i : port_map) {
            conn_printf(c, "%s -> %d\n", i.first.c_str(), i.second);
        }
    } else if (strcmp(cmd, "used") == 0 || strcmp(cmd, "list") == 0) {
        used(c);
    } else if (strcmp(cmd, "hello") == 0) {
        svc = strtok_r(NULL, " ", &sav);
        if (c.writable && svc != nullptr) {
            if (svc != nullptr) {
                c.is_hello = true;
                {
                    std::lock_guard<std::mutex> l(active_services_mutex);
                    active_services.insert(std::string(svc));
                }
                c.service = std::string(svc);
                conn_printf(c, "ok\n");
#ifdef VERBOSE
                std::cout << "hello from " << svc << std::endl;
#endif
            }
        } else {
            disallowed_write(c, cmd);
        }
    } else if (strcmp(cmd, "active") == 0) {
        std::lock_guard<std::mutex> l(active_services_mutex);
        conn_printf(c, "%d\n", active_services.size());
        for (auto it : active_services) {
            conn_printf(c, "%s\n", it.c_str());
        }
    } else if (strcmp(cmd, "exit") == 0) {
        if (c.writable) {
            return 1;
        } else {
            disallowed_write(c, cmd);
        }
    } else if (strcmp(cmd, "help") == 0) {
        conn_printf(c, "get [/echo] service     : discover port for service\n"); 
        conn_printf(c, "reg service             : obtain/discover port for new service\n");
        conn_printf(c, "del service             : forget port assignment for service\n");
        conn_printf(c, "use service port        : set specific port registration for service\n");
        conn_printf(c, "stat                    : dump some stats\n");
        conn_printf(c,
                    "used (or list)          : dump active port assignments\n");
        conn_printf(c, "rte                     : get route to instance service/port\n");
        conn_printf(c, "hello service           : keep active connection\n");
        conn_printf(c, "active                  : list active connections\n");
        conn_printf(c, "exit                    : shutdown pmux (may be restarted by system)\n");
    } else {
        conn_printf(c, "-1 unknown command, type 'help' for a brief usage description\n");
    }
done:
    // schedule to write later when it won't block (ok to do even if we didn't
    // write anything)
    if (!c.out.empty())
        fd.events |= POLLOUT;
    fd.revents = 0;

    return 0;
}

/* Poll tells us there's data to be read - read it, find commands, and run them.
 */
static int do_cmd(struct pollfd &fd, std::vector<struct pollfd> &fds)
{
    connection &c = connections[fd.fd];
    ssize_t n = read(fd.fd, c.inbuf + c.inoff, sizeof(c.inbuf) - c.inoff);
    if (n <= 0) {
        unwatchfd(fd);
        return 0;
    }
#ifdef VERBOSE
    syslog(LOG_INFO, "read %d:\n", n);
//  fsnapf(stdout, c.inbuf + c.inoff, n);
#endif

    int rc = 0;
    c.inoff += n;
    int off = 0;
    std::string s(c.inbuf, c.inoff);
    while (off < s.length() && rc == 0 && fd.fd >= 0) {
        int pos;
        pos = s.find('\n', off);
        if (pos != std::string::npos) {
            /* found something - run it */
            int len = pos - off;
            c.inbuf[pos] = 0;
            if (pos > 1 && c.inbuf[pos - 1] == '\r')
                c.inbuf[pos - 1] = 0;
            rc = run_cmd(fd, fds, c.inbuf + off, c);
            off = pos + 1;
        }
        if (pos == std::string::npos || off >= s.length()) {
            if (off == 0) {
                /* full buffer and no newline */
                unwatchfd(fd);
                return 0;
            }
            std::string left(s.substr(off));
            memcpy(c.inbuf, left.data(), left.length());
            c.inoff = left.length();
            break;
        }
    }
    return rc;
}

static int do_accept(struct pollfd &fd, std::vector<struct pollfd> &fds)
{
    struct sockaddr_in req = {0};
    socklen_t len = sizeof(req);
    int rfd = accept(fd.fd, (struct sockaddr *)&req, &len);
    if (rfd == -1) {
        syslog(LOG_WARNING, "accept: %d %s", errno, strerror(errno));
        return 0;
    }
#ifdef VERBOSE
    char *ip;
    ip = inet_ntoa(req.sin_addr);
    syslog(LOG_DEBUG, "accept from %s writable %d", ip,
           (int)is_local(req.sin_addr));
#endif
    return watchfd(rfd, fds, req.sin_addr);
}

#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE)
#include <netdb.h>
#endif

static bool init_local_names()
{
    struct hostent *me;
    me = gethostbyname("localhost");

    if (me == NULL) {
        syslog(LOG_ERR, "gethostbyname(\"localhost\") %d\n", h_errno);
        return false;
    }

    int i;
    char *p;
    for (i = 0, p = me->h_addr_list[0]; p; i++, p = me->h_addr_list[i]) {
        struct in_addr addr = *(struct in_addr *)p;
        char *ip;
        ip = inet_ntoa(addr);
#ifdef VERBOSE
        syslog(LOG_DEBUG, "accepting writes from %s\n", ip);
#endif
        local_addresses.push_back(addr);
    }

    return true;
}

static bool init(std::vector<std::pair<int, int>> port_ranges)
{

    if (!init_local_names())
        return false;

    for (auto &range : port_ranges) {
#ifdef VERBOSE
        syslog(LOG_INFO, "%s free port range %d - %d\n", __func__, range.first,
               range.second);
#endif
        for (int s = range.first; s < range.second; ++s) {
            free_ports.insert(s);
        }
    }
    port_map = pmux_store->get_ports();
    for (auto &p : port_map) {
        free_ports.erase(p.second);
    }
    return true;
}

static int poll_loop(const std::vector<int> &ports,
                     std::vector<struct pollfd> &fds)
{
    int rc = 0;
    for (size_t i = 0, n = fds.size(); i < n; ++i) {
        auto &fd = fds[i];
        if (fd.revents & POLLIN) {
            // ready for input
            if (std::find(ports.begin(), ports.end(), fd.fd) != ports.end()) {
                rc |= do_accept(fd, fds);
            } else {
                rc |= do_cmd(fd, fds);
            }
        } else if (fd.revents & POLLOUT) {
            // ready for output
            int bytes_written;
            std::string out;
            auto &wl = connections[fd.fd].out;

            out = wl.front();
            wl.pop_front();
            bytes_written = write(fd.fd, out.data(), out.size());
#ifdef VERBOSE
            syslog(LOG_INFO, "wrote %d/%d bytes\n", bytes_written, out.size());
#endif
            if (bytes_written == -1)
                unwatchfd(fd);
            else if (bytes_written < out.size()) {
                // wrote partial - put the unwritten piece back
                out = std::string(
                    out.substr(bytes_written, out.size() - bytes_written));
                wl.push_front(out);
            }
            if (wl.size() == 0)
                fd.events &= ~POLLOUT;
            // else we've consumed one output string, and we're done with it
        } else if (fd.revents & POLLERR || fd.revents & POLLHUP ||
                   fd.revents & POLLNVAL) {
            if (std::find(ports.begin(), ports.end(), fd.fd) != ports.end()) {
                // accept() fd error
                // should restart server
                abort();
            }
            unwatchfd(fd);
        } else {
            fd.revents = 0;
        }
    }
    return rc;
}

static int make_range(char *s, std::pair<int, int> &range)
{
    std::string orig(s);
    char *sav;
    char *first = strtok_r(s, ":", &sav);
    char *second = strtok_r(NULL, ":", &sav);
    if (first == NULL || second == NULL) {
        syslog(LOG_ERR, "bad port range -> %s\n", orig.c_str());
        return 1;
    }
    range = std::make_pair(atoi(first), atoi(second));
    return 0;
}

static int usage(int rc)
{
    printf(
        "Usage: pmux [-h] [-c pmuxdb cluster] [-d pmuxdb name] [-b bind path]\n"
        "[-p listen port] [-r free ports range x:y][-l|-n][-f]\n"
        "\n"
        "Options:\n"
        " -h            This help message\n"
        " -c            Cluster information for pmuxdb\n"
        " -d            Db information for pmuxdb\n"
        " -b            Unix bind path\n"
        " -p            Port pmux will listen on\n"
        " -r            Range of ports to allocate for databases\n"
        " -l            Use file to persist port allocation\n"
        " -n            Use only store in memory, will not persist port "
        "allocation\n"
        " -f            Run in foreground rather than put to background\n");
    return rc;
}

int main(int argc, char **argv)
{
    sigignore(SIGPIPE);
#   ifndef LOG_PERROR
#       define LOG_PERROR 0
#   endif
    openlog("pmux", LOG_NDELAY | LOG_PERROR, LOG_USER);

    open_max = sysconf(_SC_OPEN_MAX);
    if (open_max == -1) {
        syslog(LOG_WARNING, "sysconf(_SC_OPEN_MAX): %d %s ", errno,
               strerror(errno));
        open_max = 20;
        syslog(LOG_WARNING, "setting open_max to:%ld\n", open_max);
    }
    char *host = getenv("HOSTNAME");
    if (host == NULL) {
        long hostname_max = sysconf(_SC_HOST_NAME_MAX);
        if (hostname_max == -1) {
            syslog(LOG_WARNING, "sysconf(_SC_HOST_NAME_MAX): %d %s ", errno,
                   strerror(errno));
            hostname_max = 255;
            syslog(LOG_WARNING, "setting hostname_max to:%ld\n", hostname_max);
        }
        char myhost[hostname_max + 1];
        int rc = gethostname(myhost, hostname_max);
        myhost[hostname_max] = '\0';
        if (rc == 0)
            host = strdup(myhost);
    }
    if (host == NULL) {
        syslog(LOG_CRIT,
               "Can't figure out hostname: please export HOSTNAME.\n");
        return EXIT_FAILURE;
    }
    const char *cluster = "prod";
    const char *dbname = "pmuxdb";
    std::vector<int> listen_ports = {5105};
    std::vector<std::pair<int, int>> port_ranges{{19000, 19999}};
    bool default_ports = true;
    bool default_range = true;
    enum store_mode { MODE_NONE, MODE_LOCAL, MODE_COMDB2DB };
    store_mode store_mode = MODE_NONE;
    std::pair<int, int> custom_range;
    int c;
    struct sockaddr_un serv_addr;
    while ((c = getopt(argc, argv, "hc:d:b:p:r:lnf")) != -1) {
        switch (c) {
        case 'h':
            return usage(EXIT_SUCCESS);
            break;
        case 'c':
            cluster = strdup(optarg);
            break;
        case 'd':
            dbname = strdup(optarg);
            break;
        case 'b':
            if (strlen(optarg) >= sizeof(serv_addr.sun_path)) {
                fprintf(stderr, "Filename too long: %s\n", optarg);
                exit(2);
            }
            strncpy(unix_bind_path, optarg, sizeof(unix_bind_path));
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
            if (make_range(optarg, custom_range) != 0) {
                return usage(EXIT_FAILURE);
            }
            port_ranges.push_back(custom_range);
            break;
        case '?':
            return usage(EXIT_FAILURE);
            break;
        case 'l':
            store_mode = MODE_LOCAL;
            break;
        case 'n':
            store_mode = MODE_NONE;
            break;
        case 'f':
            foreground_mode=true;
            break;
        }
    }

    try {
        if (store_mode == MODE_LOCAL)
            pmux_store.reset(new sqlite_store());
        else if (store_mode == MODE_NONE)
            pmux_store.reset(new no_store());
        else
            pmux_store.reset(new comdb2_store(host, dbname, cluster));
    } catch (std::exception &e) {
        syslog(LOG_ERR, "%s\n", e.what());
        return EXIT_FAILURE;
    }

    if (!init(port_ranges)) {
        /* init will falure readon syslog if needed */
        return EXIT_FAILURE;
    }

    std::vector<int> afds;
    std::vector<struct pollfd> pfds; // fds to poll
    pfds.reserve(open_max);
    for (auto port : listen_ports) {
        int fd = tcp_listen(port);
        if (fd == -1) {
            syslog(LOG_CRIT, "tcplisten rc:%d for port:%d\n", fd, port);
            return EXIT_FAILURE;
        }
        afds.push_back(fd);
        struct pollfd pfd = {.fd = fd, .events = POLLIN, .revents = 0};
        pfds.push_back(pfd);
    }

    if (store_mode == MODE_LOCAL)
        pmux_store.reset(new sqlite_store());
    else if (store_mode == MODE_NONE)
        pmux_store.reset(new no_store());
    else
        pmux_store.reset(new comdb2_store(host, dbname, cluster));

    init(port_ranges);

    for (auto port : listen_ports)
        pmux_store->sav_port("pmux", port);

    if (!foreground_mode) {
        bb_daemon();
    }

    init_router_mode();
    syslog(LOG_INFO, "READY\n");

    int rc;
    while (poll(pfds.data(), pfds.size(), -1) >= 0) {
        if (poll_loop(afds, pfds) != 0)
            break;
    }

    for (auto fd : afds) {
        shutdown(fd, SHUT_RDWR);
        close(fd);
    }

    return EXIT_SUCCESS;
}
