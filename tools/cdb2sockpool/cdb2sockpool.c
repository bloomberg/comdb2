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

#ifdef __hpux
#define _XOPEN_SOURCE_EXTENDED
#endif

#include <unistd.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <fcntl.h>
#include <poll.h>

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <signal.h>
#include <utime.h>
#include <syslog.h>

#include <passfd.h>
#include <sockpool.h>
#include <sockpool_p.h>

#include <lockmacros.h>
#include <fsnapf.h>
#include <list.h>
#include <plhash.h>
#include <bb_daemon.h>

#include "cdb2sockpool.h"
#include <locks_wrap.h>

#define MAX_TYPESTR_LEN 2048

enum {
    SOCKET_POOL_TIMER_NO = 1,
    CHECK_PIPE_TIMER_NO = 2,
    FD_CHECK_TIMER_NO = 3
};

struct stats {
    unsigned fds_donated;   /* number of fds donated to the pool */
    unsigned fds_requested; /* number of fds requested from the pool */
    unsigned fds_returned;  /* number of fds actually returned to
                               requestor */
};

struct db_number_info {
    int dbnum;

    /* how many descriptors we have pooled for this dbnum right now */
    unsigned pool_count;

    /* link vector in the active list */
    LINKC_T(struct db_number_info) linkv;
};

static hash_t *dbs_info_hash = NULL;

struct client {
    /* client file descriptor */
    int fd;

    /* client credentials */
    int pid;
    int slot;
    char progname[80];
    char envname[9];

    struct stats stats;

    /* linked list of all clients */
    LINKC_T(struct client) linkv;
};

static char unix_bind_path[128] = SOCKPOOL_SOCKET_NAME;
static char msgtrap_fifo[128] = "/tmp/msgtrap.sockpool";
static int foreground_mode = 0;

static struct stat save_st;

static LISTC_T(struct db_number_info) active_list;

/* Keep an overall count of pooled sockets */
static int pooled_socket_count;

/* List of active clients. */
static pthread_mutex_t client_lock = PTHREAD_MUTEX_INITIALIZER;
static LISTC_T(struct client) client_list;
static int max_clients;

/* Global stats.  These are updated locklessly. */
static struct stats gbl_stats;

/* File descriptor limit, determined at run-time
 * in increase_file_descriptor_limit() */
static long max_fd_limit;

static pthread_mutex_t gbl_exiting_lock = PTHREAD_MUTEX_INITIALIZER;
static int gbl_exiting = 0;

extern pthread_mutex_t sockpool_lk;

/* socket port hints, to reduce communication with portmux */

struct port_hint {
    int portnum;
    char typestr[1];
};

static pthread_mutex_t gbl_port_hints_lock = PTHREAD_MUTEX_INITIALIZER;
static hash_t *port_hints = NULL;
static int num_port_hints = 0;

static int pthread_create_attrs(pthread_t *tid, int detachstate,
                                size_t stacksize,
                                void *(*start_routine)(void *), void *arg)
{
    int rc;
    pthread_attr_t attr;
    pthread_t local_tid;
    if (!tid)
        tid = &local_tid;
    Pthread_attr_init(&attr);

    if (stacksize > 0) {
        Pthread_attr_setstacksize(&attr, stacksize);
    }
    rc = pthread_attr_setdetachstate(&attr, detachstate);
    if (rc != 0) {
        syslog(LOG_ERR, "%s:pthread_attr_setdetachstate: %d %s\n", __func__, rc,
               strerror(rc));
        Pthread_attr_destroy(&attr);
        return -1;
    }
    rc = pthread_create(tid, &attr, start_routine, arg);
    if (rc != 0) {
        syslog(LOG_ERR, "%s:pthread_create: %d %s\n", __func__, rc,
               strerror(rc));
        Pthread_attr_destroy(&attr);
        return -1;
    }
    Pthread_attr_destroy(&attr);
    return 0;
}

static int get_num_clients()
{
    int num_clients;
    LOCK(&client_lock) { num_clients = listc_size(&client_list); }
    UNLOCK(&client_lock);
    return num_clients;
}

static int get_pooled_socket_count()
{
    int pooled_sockets;
    Pthread_mutex_lock(&sockpool_lk);
    {
        pooled_sockets = pooled_socket_count;
    }
    Pthread_mutex_unlock(&sockpool_lk);
    return pooled_sockets;
}

static void fd_destructor(enum socket_pool_event event, const char *typestr,
                          int fd, int dbnum, int flags, int ttl, void *voidarg)
{
    if (event != SOCKET_POOL_EVENT_DONATE) {
        if (VERBOSE) {
            syslog(LOG_DEBUG, "%s: closing fd %d for event %d", __func__, fd,
                   event);
        }
        if (close(fd) == -1) {
            syslog(LOG_NOTICE, "%s: close fd %d for '%s': %d %s\n", __func__,
                   fd, typestr, errno, strerror(errno));
        }
    }

    pooled_socket_count--;
    if (dbnum > 0) {
        struct db_number_info *dbs_info;
        dbs_info = hash_find(dbs_info_hash, &dbnum);
        if (dbs_info) {
            dbs_info->pool_count--;
            if (dbs_info->pool_count == 0) {
                listc_rfl(&active_list, dbs_info);
            }
        }
    }
}

int recvall(int fd, void *bufp, int len)
{
    uint8_t *buf = (uint8_t *)bufp;
    int rc;
    int bytesleft = len;
    int off = 0;

    while (bytesleft) {
        rc = recv(fd, buf + off, bytesleft, MSG_WAITALL);
        if (rc == -1) {
            if (errno == EINTR || errno == EAGAIN)
                continue;
            return -1;
        } else if (rc == 0) /* EOF before entire message read */
            return -1;
        else {
            bytesleft -= rc;
            off += rc;
            if (bytesleft < 0) {
                /* shouldn't happen */
                return -1;
            }
        }
    }
    return 0;
}

enum fsql_request {
#ifdef _LINUX_SOURCE
    FSQL_RESET = 1811939328
#else
    FSQL_RESET = 108
#endif
};

struct fsqlreq {
    int request;   /* enum fsql_request */
    int flags;     /* unused. should be useful later */
    int parm;      /* extra word of into differs per request */
    int followlen; /* how much data follows header*/
};

static void send_reset(int fd)
{
    int ret;
    struct fsqlreq req;

    req.request = FSQL_RESET; /* special reset opcode */
    req.flags = 0;
    req.parm = 0;
    req.followlen = 0;

    ret = write(fd, &req, sizeof(req));
    if (ret == -1)
        syslog(LOG_ERR, "send_reset(): Failed to write to fd. %s\n",
               strerror(errno));

    if (ret != sizeof(req))
        syslog(LOG_ERR, "send_reset(): Wrote %d bytes instead of %zu to fd.\n",
               ret, sizeof(req));
}

static int cache_port(char *typestr, int fd, char *prefix)
{
    struct port_hint *hint;
    struct sockaddr_in in;
    socklen_t len = sizeof(struct sockaddr_in);
    int rc = 0;
    int portnum;

    rc = getpeername(fd, (struct sockaddr *)&in, &len);
    if (rc) {
        syslog(LOG_NOTICE, "%s:%d getpeername rc=%d errno=%d\n", __FILE__,
               __LINE__, rc, errno);
        return -1;
    }

    portnum = ntohs(in.sin_port);

    LOCK(&gbl_port_hints_lock)
    {
        if (!gbl_exiting) {
            if (VERBOSE)
                syslog(LOG_DEBUG, "%s: %s: looking for \"%s\"\n", prefix,
                       __func__, typestr);

            hint = hash_find(port_hints, typestr);
            if (hint) {
                if (hint->portnum != portnum) {
                    if (VERBOSE) {
                        syslog(LOG_DEBUG,
                               "%s %s: \"%s\" updating portnum=%d (old=%d)\n",
                               prefix, __func__, typestr, portnum,
                               hint->portnum);
                    }

                    hint->portnum = portnum;
                } else {
                    if (VERBOSE) {
                        syslog(LOG_DEBUG,
                               "%s: %s: \"%s\" Found same portnum=%d\n", prefix,
                               __func__, typestr, portnum);
                    }
                }
            } else {
                hint = malloc(offsetof(struct port_hint, typestr) +
                              strlen(typestr) + 1);
                if (hint) {
                    strcpy(hint->typestr, typestr);
                    hint->portnum = portnum;

                    hash_add(port_hints, hint);
                    num_port_hints++;

                    if (VERBOSE) {
                        syslog(LOG_DEBUG, "%s: %s: \"%s\" caching port %d\n",
                               prefix, __func__, hint->typestr, hint->portnum);
                    }
                } else {
                    syslog(LOG_ERR, "Failed to get blk\n");
                }
            }
        }
    }
    UNLOCK(&gbl_port_hints_lock);

    return 0;
}

int bb_get_pid_argv0(pid_t pid, char *argv0, int sz);

static int cdb2_get_progname_by_pid(pid_t pid, char *pname, int pnamelen)
{
    int rc = -1;

    /**
     * The SUN/IBM implementations here differ slightly from the
     * new generic ones.  Leaving these unchanged to start for fear of
     * breaking something.  Ideally - any local enhancements here would be
     * migrated into bb_get_pid_argv0() and this routine would call that one
     * unconditionally.
     **/
    if (pname != NULL) {
        strncpy(pname, "???", pnamelen); /* call bb_get_pid_argv0 here? */
        pname[pnamelen - 1] = 0;
        rc = 0;
    }
    return rc;
}

void *client_thd(void *voidarg)
{
    int rc, fd = (intptr_t)voidarg;
    struct sockpool_hello hello;
    ssize_t nbytes;
    char prefix[128];
    char *typestrbuf = NULL;
    int maxtypestrlen = 0;

    /* First read the client's hello message to find out what protocol version
     * he wants to use. */
    nbytes = recv(fd, &hello, sizeof(hello), MSG_WAITALL);
    if (nbytes == -1) {
        syslog(LOG_NOTICE, "%s: recv hello: %d %s\n", __func__, errno,
               strerror(errno));
        close(fd);
        return NULL;
    } else if (nbytes != sizeof(hello)) {
        syslog(LOG_NOTICE, "%s: premature eof on reading hello\n", __func__);
        close(fd);
        return NULL;
    }
    if (memcmp(hello.magic, "SQLP", 4) != 0) {
        syslog(LOG_NOTICE, "%s: wrong magic in hello message\n", __func__);
        close(fd);
        return NULL;
    }
    if (hello.protocol_version < 0 || hello.protocol_version > 1) {
        syslog(LOG_NOTICE,
               "%s: client requested unsupported protocol version %d\n",
               __func__, hello.protocol_version);
        close(fd);
        return NULL;
    }

    struct client clnt = {.fd = fd, .pid = hello.pid, .slot = hello.slot};
    bzero(&clnt.stats, sizeof(clnt.stats));

    rc = cdb2_get_progname_by_pid(clnt.pid, clnt.progname,
                                  sizeof(clnt.progname));
    if (rc != 0) {
        strncpy(clnt.progname, "???", sizeof(clnt.progname));
    }

    strncpy(clnt.envname, "???", sizeof(clnt.envname));

    snprintf(prefix, sizeof(prefix), "<fd %d pid %d (%s) slot %d (%s)>", fd,
             clnt.pid, clnt.progname, clnt.slot, clnt.envname);
    LOCK(&client_lock)
    {
        listc_atl(&client_list, &clnt);
        if (listc_size(&client_list) > max_clients) {
            max_clients = listc_size(&client_list);
        }
    }
    UNLOCK(&client_lock);

    if (VERBOSE) {
        syslog(LOG_DEBUG, "%s: connected, protocol version %d\n", prefix,
               hello.protocol_version);
    }

    while (1) {
        struct sockpool_msg_vers0 msg0;
        struct sockpool_msg_vers1 msg1;
        int rc, newfd;
        int request = 0;
        char *typestr = 0;
        int dbnum;
        int timeout = 0;
        void *msg;
        int msglen;

        switch (hello.protocol_version) {
        case 0:
            msg = &msg0;
            msglen = sizeof(msg0);
            break;
        case 1:
            msg = &msg1;
            msglen = sizeof(msg1);
            break;

        default:
            /* We shouldn't make it this far - this is checked on
               initial hello. */
            goto disconnect;
            break;
        }

        errno = 0;
        rc = recv_fd(fd, msg, msglen, &newfd);

        if (rc != PASSFD_SUCCESS) {
            if (rc != PASSFD_EOF) {
                syslog(LOG_NOTICE, "%s: recv_fd rc %d errno %d %s\n", prefix,
                       rc, errno, strerror(errno));
            }
            break;
        }

        dbnum = 0;
        if (hello.protocol_version == 0) {
            if (msg0.typestr[sizeof(msg0.typestr) - 1] != '\0') {
                syslog(LOG_NOTICE,
                       "%s: received non null terminated type string\n",
                       prefix);
                close(newfd);
                continue;
            }

            if (msg0.dbnum < 0) {
                syslog(
                    LOG_NOTICE,
                    "%s: received fd for out of range dbnum %d typestr '%s'\n",
                    prefix, msg0.dbnum, msg0.typestr);
                close(newfd);
                continue;
            }

            request = msg0.request;
            typestr = msg0.typestr;
            dbnum = msg0.dbnum;
            timeout = msg0.timeout;
        } else if (hello.protocol_version == 1) {
            if (msg1.typestrlen < 0 || msg1.typestrlen > MAX_TYPESTR_LEN) {
                syslog(LOG_NOTICE, "%s: invalid typestr len %d\n", prefix,
                       msg1.typestrlen);
                close(newfd);
                goto disconnect;
            }
            if (msg1.typestrlen > maxtypestrlen) {
                typestrbuf = realloc(typestrbuf, msg1.typestrlen);
                if (typestrbuf == NULL) {
                    syslog(LOG_NOTICE,
                           "%s: failed to allocate memory for typestr len %d\n",
                           prefix, msg1.typestrlen);
                    close(newfd);
                    goto disconnect;
                }
            }

            rc = recvall(fd, typestrbuf, msg1.typestrlen);
            if (rc) {
                syslog(LOG_NOTICE, "%s: error reading typestring\n", prefix);
                close(newfd);
                goto disconnect;
            }
            if (typestrbuf[msg1.typestrlen - 1] != 0) {
                syslog(LOG_NOTICE,
                       "%s: received non null terminated type string\n",
                       prefix);
                close(newfd);
                goto disconnect;
            }

            request = msg1.request;
            typestr = typestrbuf;
            dbnum = 0;
            timeout = msg1.timeout;
        }

        if (request == SOCKPOOL_DONATE) {

            if (VERBOSE) {
                syslog(LOG_DEBUG, "%s: DONATING\n", prefix);
            }

            /* Donating a socket for the given database number. */
            if (newfd == -1) {
                syslog(LOG_NOTICE, "%s: no fd received with donation\n",
                       prefix);
                continue;
            }

            /* If there's an associated database number then increment our
             * count of sockets pooled for this dbnum.  If later on we can't
             * pool it our destructor (fd_destructor) will be called and will
             * decrement the count.  Also of course light the shared memory
             * bit to indicate that we have fds available for this dbnum. */
            Pthread_mutex_lock(&sockpool_lk);
            {
                pooled_socket_count++;
                if (dbnum > 0) {
                    struct db_number_info *dbs_info;
                    dbs_info = hash_find(dbs_info_hash, &dbnum);
                    if (dbs_info == NULL) {
                        dbs_info = calloc(1, sizeof(struct db_number_info));
                        dbs_info->dbnum = dbnum;
                        hash_add(dbs_info_hash, dbs_info);
                    }
                    if (dbs_info->pool_count == 0) {
                        listc_atl(&active_list, dbs_info);
                    }
                    dbs_info->pool_count++;
                }
            }
            Pthread_mutex_unlock(&sockpool_lk);

            clnt.stats.fds_donated++;
            gbl_stats.fds_donated++;

            cache_port(typestr, newfd, prefix);

            /* if it's a comdb2 that supports heartbeats, send a reset */
            if (strncmp("comdb2/", typestr, 7) == 0)
                send_reset(newfd);

            socket_pool_donate_ext(typestr, newfd, timeout, dbnum, 0,
                                   fd_destructor, NULL);

            if (VERBOSE) {
                syslog(LOG_DEBUG,
                       "%s: donated fd %d for %s (dbnum %d) timeout %d\n",
                       prefix, newfd, typestr, dbnum, timeout);
            }

        } else if (request == SOCKPOOL_REQUEST) {

            if (VERBOSE) {
                syslog(LOG_DEBUG, "%s: REQUESTING\n", prefix);
            }

            /* Request for a socket for the given database number. */
            if (newfd != -1) {
                syslog(LOG_NOTICE, "%s: unexpectedly received a socket\n",
                       prefix);
                close(newfd);
                break;
            }

            newfd = socket_pool_get(typestr);

            request = SOCKPOOL_DONATE;

            /* if no socket, try to find the port hint */
            if (newfd == -1 && 1) {
                LOCK(&gbl_port_hints_lock)
                {
                    if (!gbl_exiting) {
                        struct port_hint *hint = hash_find(port_hints, typestr);
                        if (hint) {
                            short portn = htons(hint->portnum);
                            memcpy(&msg0.padding[1], &portn, sizeof(portn));

                            if (VERBOSE) {
                                syslog(LOG_DEBUG, "%s: %s: \"%s\" no socket, "
                                                  "returning port %d\n",
                                       prefix, __func__, typestrbuf,
                                       hint->portnum);
                            }
                        } else {
                            if (VERBOSE) {
                                syslog(LOG_DEBUG, "%s: %s: \"%s\" no socket,  "
                                                  "no cached port\n",
                                       prefix, __func__, typestrbuf);
                            }
                        }
                    }
                }
                UNLOCK(&gbl_port_hints_lock);
            } else {
                if (VERBOSE) {
                    syslog(LOG_DEBUG,
                           "%s: %s: \"%s\" found fd=%d (not using hints)\n",
                           prefix, __func__, typestrbuf, newfd);
                }
            }

            errno = 0;
            /* The only parts of the response the client care about is the file
               descriptor and the port, so we don't need to waste time sending
               back the full typestring - send msg0 unconditionally. */
            rc = send_fd(fd, &msg0, sizeof(msg0), newfd);
            if (rc != PASSFD_SUCCESS) {
                syslog(LOG_NOTICE, "%s: send_fd rc %d errno %d %s\n", prefix,
                       rc, errno, strerror(errno));
            }

            clnt.stats.fds_requested++;
            gbl_stats.fds_requested++;
            if (newfd != -1 && rc == PASSFD_SUCCESS) {
                clnt.stats.fds_returned++;
                gbl_stats.fds_returned++;
            }

            /* Whether we sent it or not, close the fd in this process. */
            if (newfd != -1) {
                if (close(newfd) == -1) {
                    syslog(LOG_NOTICE, "%s: close fd %d: %d %s\n", prefix, fd,
                           errno, strerror(errno));
                }
            }

            if (VERBOSE) {
                syslog(LOG_DEBUG,
                       "%s: requested socket for %s - returned fd %d\n", prefix,
                       typestrbuf, newfd);
            }

        } else if (request == SOCKPOOL_FORGET_PORT) {
            LOCK(&gbl_port_hints_lock)
            {
                struct port_hint *h;
                h = hash_find(port_hints, typestr);
                if (VERBOSE) {
                    syslog(LOG_DEBUG,
                           "%s: asking to forget: dbnum %d str %s hint %p\n",
                           prefix, dbnum, typestr, h);
                }
                if (h) {
                    int port;
                    port = h->portnum;
                    hash_del(port_hints, h);
                    num_port_hints--;

                    if (VERBOSE) {
                        syslog(LOG_DEBUG, "%s: \"%.*s\" forgetting port %d\n",
                               prefix, (int)sizeof(typestr), typestr, port);
                    }
                }
            }
            UNLOCK(&gbl_port_hints_lock);
        } else {
            syslog(LOG_NOTICE, "%s: bad request %d\n", prefix, (int)request);
            if (newfd != -1)
                close(newfd);
            break;
        }
    }

disconnect:
    if (VERBOSE) {
        syslog(LOG_DEBUG, "%s: disconnect\n", prefix);
    }

    if (typestrbuf)
        free(typestrbuf);

    LOCK(&client_lock) { listc_rfl(&client_list, &clnt); }
    UNLOCK(&client_lock);
    close(fd);
    return NULL;
}

static void *accept_thd(void *voidarg)
{
    int listenfd = (intptr_t)voidarg;

    syslog(LOG_INFO, "Unix domain socket accept thread starting for fd %d\n",
           listenfd);

    int lcl_exiting;
    do {
        struct sockaddr_un client_addr;
        socklen_t clilen;
        int fd;

        clilen = sizeof(client_addr);

        fd = accept(listenfd, (struct sockaddr *)&client_addr, &clilen);
        if (fd == -1) {
            if (errno == EINTR)
                continue;
            syslog(LOG_NOTICE, "%s:accept %d %s\n", __func__, errno,
                   strerror(errno));
            exit(1);
        }

        if (pthread_create_attrs(NULL, PTHREAD_CREATE_DETACHED,
                                 CLIENT_STACK_SIZE, client_thd,
                                 (void *)(intptr_t)fd) != 0) {
            close(fd);
        }

        LOCK(&gbl_exiting_lock) { lcl_exiting = gbl_exiting; }
        UNLOCK(&gbl_exiting_lock);
    } while (!lcl_exiting);

    return NULL;
}

static void purge_hints(void)
{
    LOCK(&gbl_port_hints_lock)
    {
        if (!gbl_exiting) {
            hash_clear(port_hints);
        }
    }
    UNLOCK(&gbl_port_hints_lock);
}

static void cleanexit(void)
{
    syslog(LOG_INFO, "Exiting...\n");
    LOCK(&gbl_exiting_lock) { gbl_exiting = 1; }
    UNLOCK(&gbl_exiting_lock);

    LOCK(&gbl_port_hints_lock)
    {
        hash_free(port_hints);
        num_port_hints = 0;
    }
    UNLOCK(&gbl_port_hints_lock);

    socket_pool_close_all();
    exit(0);
}

static const char *help_menu[] = {
    "exit               - Exit the program",
    "stat               - Status overview",
    "stat clnt          - Detailed client stats",
    "stat pool          - Details socket_pool.c stats",
    "stat port          - Details cached ports",
    "set <name> <value> - Change tunable",
    "closeall           - Close all pooled sockets",
    "purgeports         - Removes all the port hints",
    NULL};

#include "strbuf.h"
static void do_stat(void)
{
    struct db_number_info *p;
    int count = 0;
    syslog(LOG_INFO, "Socket pool server running.\n");
    syslog(LOG_INFO, "Unix domain socket: %s\n", unix_bind_path);
    char *fifo = getenv("MSGTRAP_SOCKPOOL");
    if (fifo == NULL) {
        fifo = msgtrap_fifo;
    }
    syslog(LOG_INFO, "Msg trap socket: %s\n", msgtrap_fifo);
    syslog(LOG_INFO, "---\n");
    print_all_settings();
    syslog(LOG_INFO, "---\n");
    Pthread_mutex_lock(&sockpool_lk);
    syslog(LOG_INFO, "Currently holding sockets for %d discrete dbnums:\n",
           listc_size(&active_list));
    strbuf *stb = strbuf_new();
    LISTC_FOR_EACH(&active_list, p, linkv)
    {
        if (count == 0)
            strbuf_appendf(stb, "    ");
        else
            strbuf_appendf(stb, ", ");
        strbuf_appendf(stb, "%d", p->dbnum);
        count++;
        if (count == 10) {
            syslog(LOG_INFO, "%s", strbuf_buf(stb));
            strbuf_clear(stb);
            count = 0;
        }
    }
    if (count > 0)
        syslog(LOG_INFO, "%s", strbuf_buf(stb));
    strbuf_free(stb);
    Pthread_mutex_unlock(&sockpool_lk);
    syslog(LOG_INFO, "---\n");
    LOCK(&client_lock)
    {
        syslog(LOG_INFO, "current number of clients : %u\n",
               listc_size(&client_list));
        syslog(LOG_INFO, "peak number of clients    : %u\n", max_clients);
    }
    UNLOCK(&client_lock);
    syslog(LOG_INFO, "fds donated from clients  : %u\n", gbl_stats.fds_donated);
    syslog(LOG_INFO, "fds requested by clients  : %u\n",
           gbl_stats.fds_requested);
    syslog(LOG_INFO, "fds returned to clients   : %u\n",
           gbl_stats.fds_returned);
    syslog(LOG_INFO, "---\n");
    socket_pool_dump_stats_ex(stdout, 0, 1, 0);
    syslog(LOG_INFO, "---\n");

    const int num_clients = get_num_clients();
    const int num_pooled_sockets = get_pooled_socket_count();
    const int total_sockets = num_clients + num_pooled_sockets;

    if (max_fd_limit > 0) {
        /* we determined available fds at startup */
        const double perc_used = ((double)total_sockets) / max_fd_limit;
        syslog(LOG_INFO, "%.2f%% of file descriptors in use:\n"
                         " - fd limit: %ld\n",
               (perc_used * 100), max_fd_limit);
    }
    syslog(LOG_INFO, " - %d pooled sockets\n"
                     " - %d clients\n",
           num_pooled_sockets, num_clients);
}

static void do_stat_clnt(void)
{
    LOCK(&client_lock)
    {
        struct client *clnt;
        syslog(LOG_INFO,
               "%d current clients (max %d) stats donated/requested/returned:",
               listc_size(&client_list), max_clients);
        LISTC_FOR_EACH(&client_list, clnt, linkv)
        {
            syslog(LOG_INFO,
                   "  fd %4d  pid %10d slot %5d (%8s)  stats %4u/%4u/%4u  %s",
                   clnt->fd, clnt->pid, clnt->slot, clnt->envname,
                   clnt->stats.fds_donated, clnt->stats.fds_requested,
                   clnt->stats.fds_returned, clnt->progname);
        }
    }
    UNLOCK(&client_lock);
}

int port_hint_dump(void *obj, void *voidarg)
{
    struct port_hint *hint = (struct port_hint *)obj;
    syslog(LOG_INFO, "%-32s %d\n", hint->typestr, hint->portnum);
    return 0;
}

static void do_stat_port(void)
{
    LOCK(&gbl_port_hints_lock)
    {
        if (!gbl_exiting) {
            syslog(LOG_INFO, "=== Cached %d ports ===\n", num_port_hints);
            hash_for(port_hints, port_hint_dump, NULL);
            syslog(LOG_INFO, "=== Done ===\n");
        }
    }
    UNLOCK(&gbl_port_hints_lock);
}

static void do_stat_pool(void) { socket_pool_dump_stats_syslog(0, 1); }

int dumphint(void *hintp, void *unused)
{
    struct port_hint *hint;
    hint = (struct port_hint *)hintp;
    syslog(LOG_INFO, " >> %s %d\n", hint->typestr, hint->portnum);
    return 0;
}

static void message_trap(int ntoks, char *toks[])
{
    if (ntoks == 0) {
        syslog(LOG_INFO, "no message?\n");

    } else if (strcasecmp(toks[0], "help") == 0) {
        int ii;
        for (ii = 0; help_menu[ii]; ii++) {
            syslog(LOG_INFO, "%s\n", help_menu[ii]);
        }

    } else if (strcasecmp(toks[0], "exit") == 0) {
        cleanexit();

    } else if (strcasecmp(toks[0], "stat") == 0) {
        if (ntoks > 1) {
            if (strcasecmp(toks[1], "clnt") == 0) {
                do_stat_clnt();
            } else if (strcasecmp(toks[1], "pool") == 0) {
                do_stat_pool();
            } else if (strcasecmp(toks[1], "port") == 0) {
                do_stat_port();
            } else {
                syslog(LOG_INFO, "Unknown stat tail '%s'\n", toks[1]);
            }
        } else {
            do_stat();
        }

    } else if (strcasecmp(toks[0], "closeall") == 0) {
        syslog(LOG_INFO, "Closing all pooled sockets\n");
        socket_pool_close_all();

    } else if (strcasecmp(toks[0], "purgeports") == 0) {
        syslog(LOG_INFO, "Purging all cached port hints\n");
        purge_hints();

    } else if (strcasecmp(toks[0], "set") == 0) {
        unsigned new;
        char *endptr;

        if (ntoks != 3) {
            syslog(LOG_INFO, "expected setting number/name and new value\n");
            return;
        }

        errno = 0;
        endptr = 0;
        new = strtoul(toks[2], &endptr, 10);
        if (errno != 0 || !endptr || *endptr != 0) {
            syslog(LOG_INFO,
                   "bad new setting - expected an unsigned integer\n");
            return;
        }

        set_setting(toks[1], new);
    } else if (strcasecmp(toks[0], "sethint") == 0) {
        int newport;

        if (ntoks != 3) {
            syslog(LOG_INFO, "expected type string and port for sethint\n");
            return;
        }
        if (strlen(toks[1]) > 47) {
            syslog(LOG_INFO, "invalid type string\n");
            return;
        }
        newport = atoi(toks[2]);
        if (newport < 0 || newport > 65535) {
            syslog(LOG_INFO, "invalid port\n");
            return;
        }

        LOCK(&gbl_port_hints_lock)
        {
            char msg[48];
            struct port_hint *hint;

            strcpy(msg, toks[1]);
            hint = hash_find(port_hints, msg);
            if (hint) {
                syslog(LOG_INFO, "changing port for %s, was %d now %d\n", msg,
                       hint->portnum, newport);
                hint->portnum = newport;
            } else {
                hint = malloc(offsetof(struct port_hint, typestr) +
                              strlen(msg) + 1);
                if (hint) {
                    strcpy(hint->typestr, msg);
                    hint->portnum = newport;
                    syslog(LOG_INFO, "added port hint for %s, port %d\n", msg,
                           newport);
                    hash_add(port_hints, hint);
                    num_port_hints++;
                }
            }
        }
        UNLOCK(&gbl_port_hints_lock);

    } else if (strcasecmp(toks[0], "dumphints") == 0) {
        LOCK(&gbl_port_hints_lock) { hash_for(port_hints, dumphint, NULL); }
        UNLOCK(&gbl_port_hints_lock);
    } else {
        syslog(LOG_INFO, "unknown message trap '%s'\n", toks[0]);
    }
}

void setting_changed(unsigned *setting)
{
    if (setting == &POOL_MAX_FDS) {
        socket_pool_set_max_fds(POOL_MAX_FDS);
    } else if (setting == &POOL_MAX_FDS_PER_DB) {
        socket_pool_set_max_fds_per_typestr(POOL_MAX_FDS_PER_DB);
#if 0      
   } else if(setting == &CHECK_PIPE_FREQ) {
      comdb2_cantim(CHECK_PIPE_TIMER_NO);
      if(CHECK_PIPE_FREQ > 0) {
         timprm(1000 * CHECK_PIPE_FREQ, CHECK_PIPE_TIMER_NO);
      }
#endif
    }
}

static void increase_file_descriptor_limit()
{
    struct rlimit rlp;
    if (getrlimit(RLIMIT_NOFILE, &rlp) == -1) {
        syslog(LOG_ERR, "Failed to get rlimit for open files. %s\n",
               strerror(errno));
        exit(EXIT_FAILURE);
    }

    max_fd_limit = rlp.rlim_cur;

    if (rlp.rlim_cur < rlp.rlim_max) {
        syslog(LOG_INFO,
               "Increasing soft file descriptor limit from current "
               "value of %ld to the hard max (%ld).\n",
               (long int)rlp.rlim_cur, (long int)rlp.rlim_max);

        rlp.rlim_cur = rlp.rlim_max;

        if (setrlimit(RLIMIT_NOFILE, &rlp) == -1) {
            syslog(LOG_ERR, "Failed to set rlimit for open files. %s\n",
                   strerror(errno));
        } else {
            max_fd_limit = rlp.rlim_cur;
        }
    }
}

static int cdb2_waitft()
{
    char *fifo = NULL;
    fifo = getenv("MSGTRAP_SOCKPOOL");
    if (fifo == NULL) {
        fifo = msgtrap_fifo;
    }
    syslog(LOG_INFO, "Will listen for msg trap on socket %s\n", fifo);

    unlink(fifo);
    int rc = mkfifo(fifo, 0666);
    if (rc) {
        perror("mkfifo");
        return 2;
    }
    int fd = 0;
    while (1) {
        int i = 0;
        int rc;
        char rec[1000] = {0};
        fd = open(fifo, O_RDONLY | O_NONBLOCK);

        fd_set set;
        FD_ZERO(&set);
        FD_SET(fd, &set);

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        /* Every second check for sockets that need timing out */
        rc = select(fd + 1, &set, NULL, NULL, &timeout);
        if (rc > 0) {
            char *toks[20];
            char *last = NULL;
            char *tok = NULL;
            if (read(fd, rec, 1000) > 0) {
                tok = strtok_r(rec, " \n", &last);
                while (tok != NULL) {
                    toks[i] = tok;
                    i++;
                    tok = strtok_r(NULL, " \n", &last);
                }
            }
            if (i)
                message_trap(i, toks);
            socket_pool_timeout();
        } else {
            socket_pool_timeout();
        }
        close(fd);
    }
}

int main(int argc, char *argv[])
{
    extern char *optarg;
    extern int optind, optopt;

    int c;
    int listenfd;
    struct sockaddr_un serv_addr = {.sun_family = AF_UNIX};

    sigignore(SIGPIPE);
#ifndef LOG_PERROR
#define LOG_PERROR 0
#endif
    openlog("cdb2sockpool", LOG_NDELAY | LOG_PERROR, LOG_USER);

    setvbuf(stdout, 0, _IOLBF, 0);

    optind = 1;

    while ((c = getopt(argc, argv, "p:f")) != EOF) {
        switch (c) {
        case 'p':
            /* Of the two limits sizeof(sun_addr.sun_path) is smaller */
            if (strlen(optarg) >= sizeof(serv_addr.sun_path)) {
                syslog(LOG_ERR, "Filename too long: %s\n", optarg);
                exit(2);
            }
            strncpy(unix_bind_path, optarg, sizeof(unix_bind_path));
            break;

        case 'f':
            foreground_mode = 1;
            break;

        case '?':
            syslog(LOG_ERR, "Unrecognised option: -%c\n", optopt);
            exit(2);
        }
    }

    /*
     * We hit a problem where there were enough sockpool clients
     * to exhaust the soft limit for file descriptors.
     *
     * To deal with this, we increase the soft limit to meet the hard
     * limit here.
     */
    increase_file_descriptor_limit();

    socket_pool_enable();

    /* Don't donate file descriptors to the global socket pool - I *am*
     * the global socket pool... */
    sockpool_disable();

    socket_pool_set_max_fds(POOL_MAX_FDS);
    socket_pool_set_max_fds_per_typestr(POOL_MAX_FDS_PER_DB);

    dbs_info_hash = hash_init(sizeof(int32_t));

    listc_init(&active_list, offsetof(struct db_number_info, linkv));
    listc_init(&client_list, offsetof(struct client, linkv));
    port_hints = hash_init_str(offsetof(struct port_hint, typestr));

    syslog(LOG_INFO, "Will listen on local domain socket %s\n", unix_bind_path);

    listenfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listenfd == -1) {
        syslog(LOG_ERR, "Error socket: %d %s\n", errno, strerror(errno));
        exit(1);
    }

    if (unlink(unix_bind_path) == -1 && errno != ENOENT) {
        syslog(LOG_ERR, "Error unlinking '%s': %d %s\n", unix_bind_path, errno,
               strerror(errno));
    }

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

    if (-1 == stat(unix_bind_path, &save_st)) {
        syslog(LOG_ERR, "Unable to stat '%s': %d %s\n", unix_bind_path, errno,
               strerror(errno));
        exit(1);
    }

    if (!foreground_mode) {
        bb_daemon();
    }

    if (pthread_create_attrs(NULL, PTHREAD_CREATE_DETACHED, 64 * 1024,
                             accept_thd, (void *)(intptr_t)listenfd) != 0) {
        syslog(LOG_ERR, "Could not create unix domain socket accept thread\n");
        exit(1);
    }

    return cdb2_waitft();
}
