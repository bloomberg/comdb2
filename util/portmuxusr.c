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

/* register/get port through portmux */

#include <portmuxapi.h>

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include <passfd.h>
#include <sbuf2.h>
#include <tcputil.h>
#include <sizeof_helpers.h>
#include <epochlib.h>
#include <cdb2_constants.h>
#include <logmsg.h>
#include <mem.h>
#include <str0.h>

#ifndef PORTMUXUSR_TESTSUITE
#include "mem_util.h"
#include "mem_override.h"
#endif

int gbl_pmux_route_enabled = 1;

#define PORTMUX_ROUTE_MODE_ENABLED() gbl_pmux_route_enabled
#define PORTMUX_SET_NO_LINGER() 1
#define PORTMUX_USE_ECHO_GET() 1
#define PORTMUX_USE_REFACTORED_GET() 1
#define PORTMUX_VALIDATE_NAMES() 1
#define PORTMUX_DENAGLE() 1
/* Controls if calls to portmux_poll() are directed to portmux_poll_v() */
#define PORTMUX_USE_POLL_V() 1
/* Controls if we attempt end-to-end validation when we connect */
#define PORTMUX_VALIDATE() 1

/* example:
         app=comdb2
     service=replication
    instance=secdb
*/

extern int db_is_exiting(void);
int portmux_port = 5105;

enum {
    TIMEOUTMS = 10000,
    MAX_WAIT_TIMEOUTMS = 60000,
    MIN_VALIDATION_TIMEOUTMS = 1000,
    RECONNECT_POLL_TIMEOUTMS = 2000,
    NAMELEN = 64
};
static int portmux_default_timeout = TIMEOUTMS;
static int max_wait_timeoutms = MAX_WAIT_TIMEOUTMS;

#define PORTMUX_UNIX_SOCKET_DEFAULT_PATH "/tmp/portmux.socket"
static char portmux_unix_socket[PATH_MAX] = PORTMUX_UNIX_SOCKET_DEFAULT_PATH;

static int (*reconnect_callback)(void *) = NULL;
static void *reconnect_callback_arg;

/*make these private for now*/
static int portmux_register_route(const char *app, const char *service,
                                  const char *instance, int *port,
                                  uint32_t options);
static int portmux_get_unix_socket(const char *unix_bind_path);
static int portmux_route_to(struct in_addr in, const char *app,
                            const char *service, const char *instance,
                            int timeoutms);
static int portmux_poll(portmux_fd_t *fds, int timeoutms,
                        void (*accept_hndl)(int fd, void *user_data),
                          void *user_data, struct sockaddr_in *);
static int portmux_poll_v(portmux_fd_t **fds, nfds_t nfds, int timeoutms,
                          int (*accept_hndl)(int which, int fd,
                                             void *user_data),
                          void *user_data, struct sockaddr_in *);
static int portmux_connecti_int(struct in_addr addr, const char *app,
                                const char *service, const char *instance,
                                int myport, int timeoutms);
static bool portmux_client_side_validation(int fd, const char *app,
                                           const char *service,
                                           const char *instance, int timeoutms);

/* Check the name for whitespace characters - if we find any, alarm that
 * the API is being misused.  Before adding this alarm such requests would
 * have been silently truncated by portmux.  e.g. the app may have
 * requested 'foo/bar/sheep dip' but portmux would have treated this as
 * a request for 'foo/bar/sheep'.
 * NOTE: Allow trailing space, as this has historically been ok and won't
 * cause a problem.  It's a bit quirky but some programs have crept in
 * that do this - e.g. request 'foo/bar/sheep ' rather than 'foo/bar/sheep' */
static void portmux_validate_name(const char *name, const char *caller)
{
    bool found_whitespace = false;
    bool found_non_trailing_whitespace = false;
    for (const char *c = name; *c; c++) {
        if (isspace(*c)) {
            found_whitespace = true;
        } else if (found_whitespace) {
            found_non_trailing_whitespace = true;
            break;
        }
    }
    if (found_non_trailing_whitespace) {
        logmsg(LOGMSG_ERROR, "API_MISUSE_ALMN: %s called with name '%s' which "
                        "contains whitespace\n",
                caller, name);
    }
}

static void portmux_denagle(int fd)
{
    if (PORTMUX_DENAGLE()) {
        int nodelay = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
    }
}

/* returns how many milliseconds of timeoutms remain, timed
 * from startms, zero if time is up, or -1 if timeoutms is -1
 */
static int remaining_timeoutms(int startms, int timeoutms)
{
    if (timeoutms == -1) {
        return -1;
    }

    int elapsedms = comdb2_time_epochms() - startms;
    return (elapsedms < timeoutms) ? timeoutms - elapsedms : 0;
}

int portmux_cmd(const char *cmd, const char *app, const char *service,
                const char *instance, const char *post)
{
    char name[strlen(app) + strlen(service) + strlen(instance) + 3 +
              MAX_DBNAME_LENGTH];
    char res[32];
    SBUF2 *ss;
    int rc, fd, port;
    rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name)) {
        logmsg(LOGMSG_ERROR, "%s: snprintf truncated output, rc %d\n", __func__, rc);
        return -1;
    }
    if (PORTMUX_VALIDATE_NAMES())
        portmux_validate_name(name, __func__);
    fd = tcpconnecth("localhost", get_portmux_port(), 0);
    if (fd < 0) {
        logmsg(LOGMSG_ERROR, "%s: tcpconnecth fd -1\n", __func__);
        return -1;
    }

    ss = sbuf2open(fd, SBUF2_WRITE_LINE);
    if (ss == 0) {
        close(fd);
        logmsg(LOGMSG_ERROR, "%s: sbuf2open error\n", __func__);
        return -1;
    }
    sbuf2printf(ss, "%s %s%s\n", cmd, name, post);
    res[0] = 0;
    sbuf2gets(res, sizeof(res), ss);
    sbuf2close(ss);

    if (res[0] == 0) {
        logmsg(LOGMSG_ERROR, "%s: sbuf2gets read 0\n", __func__);
        return -1;
    }
    if (strcmp(res, "-1 service already active") == 0) {
        logmsg(LOGMSG_ERROR, "%s: Service with this name (%s) is already active (running)\n", __func__, name);
        return -1;
    }
    port = atoi(res);
    if (port <= 0) {
        logmsg(LOGMSG_ERROR, "%s: atoi error (res='%s' name=%s)\n", __func__, res, name);
        port = -1;
    }
    return port;
}

/* returns port number, or -1 for error*/
int portmux_use(const char *app, const char *service, const char *instance,
                int port)
{
    char portstr[20];
    snprintf(portstr, sizeof(portstr), " %d", port);
    return portmux_cmd("use", app, service, instance, portstr);
}

/* returns port number, or -1 for error*/
int portmux_register(const char *app, const char *service, const char *instance)
{
    return portmux_cmd("reg", app, service, instance, "");
}

int portmux_deregister(const char *app, const char *service,
                       const char *instance)
{
    char name[NAMELEN * 2];
    char res[32];
    SBUF2 *ss;
    int rc, fd;
    rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name))
        return -1;
    if (PORTMUX_VALIDATE_NAMES())
        portmux_validate_name(name, __func__);
    fd = tcpconnecth("localhost", get_portmux_port(), 0);
    if (fd < 0)
        return -1;
    ss = sbuf2open(fd, SBUF2_WRITE_LINE);
    if (ss == 0) {
        close(fd);
        return -1;
    }
    sbuf2printf(ss, "del %s\n", name);
    res[0] = 0;
    sbuf2gets(res, sizeof(res), ss);
    sbuf2close(ss);
    if (res[0] == 0) {
        return -1;
    }
    rc = atoi(res);
    return rc;
}

static int strIsEmpty(const char *psz)
{
    return (psz) ? '\0' == *psz : 1; /*	A null pointer is also empty.	*/
}

static const char szWhiteChars[] = " \t\n\r"; /*	Default white chars. */
static char *strtrail(char *s, const char *trims)
{
    char *pTail = s;

    /*	Detect trailing white spaces by default.
     */
    if (strIsEmpty(trims))
        trims = szWhiteChars;

    while (*s) {
        if (!strchr(trims, *s++))
            pTail = s;
    }

    return pTail;
}

static char *strrtrims(char *s, const char *trims)
{
    (*strtrail(s, trims)) = '\0';

    return s;
}

static int portmux_get_int(const struct in_addr *in, const char *remote_host,
                           const char *app, const char *service,
                           const char *instance, int timeout_ms)
{
    char name[NAMELEN * 2];
    int rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name))
        return -1;
    if (PORTMUX_VALIDATE_NAMES())
        portmux_validate_name(name, __func__);

    int fd;
    if (in) {
        fd = tcpconnect_to(*in, get_portmux_port(), 0, timeout_ms);
    } else if (remote_host) {
        fd = tcpconnecth_to(remote_host, get_portmux_port(), 0, timeout_ms);
    } else {
        logmsg(LOGMSG_ERROR, "%s: INTERNAL ERROR - neither in nor remote_host set\n",
                __func__);
        return -1;
    }
    if (fd < 0)
        return -1;

    /*
     * some applications thrash portmux, and then DOS
     * themselves and others by generating millions of TIME_WAIT state ports.
     * We prevent this by setting SO_LINGER with a timeout of 0 so that when
     * we call close() the socket is killed stone dead.
     * This feature should be enabled in conjunction with PORTMUX_USE_ECHO_GET()
     * as that gives us some protection at the application level against the
     * wandering duplicate attack, which is normally protected against by
     * the TIME_WAIT feature.
     */
    if (PORTMUX_SET_NO_LINGER()) {
        struct linger lg;
        lg.l_onoff = 1;
        lg.l_linger = 0;
        rc = setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(struct linger));
        if (rc == -1) {
            logmsg(LOGMSG_ERROR, "%s: setsockopt(SO_LINGER) failed: %s\n", __func__,
                    strerror(errno));
        }
    }

    SBUF2 *ss = sbuf2open(fd, SBUF2_WRITE_LINE);
    if (ss == 0) {
        close(fd);
        return -1;
    }

    sbuf2settimeout(ss, portmux_default_timeout, portmux_default_timeout);

    bool echo_get = PORTMUX_USE_ECHO_GET();
    if (echo_get) {
        strrtrims(name, NULL);
        sbuf2printf(ss, "get /echo %s\n", name);
    } else {
        sbuf2printf(ss, "get %s\n", name);
    }

    /* Read back result and close the connection, it's not needed any more. */
    char res[NAMELEN * 2];
    sbuf2gets(res, sizeof(res), ss);
    sbuf2close(ss);

    /* First token on line should be port number */
    static const char *delims = " \t\r\n";
    char *lasts;
    char *tok = strtok_r(res, delims, &lasts);
    if (!tok)
        return -1;
    int port = atoi(tok);
    if (port <= 0)
        return -1;

    if (echo_get) {
        /* If we're using the echo get request, next token should be the
         * service name echoed back to us. */
        tok = strtok_r(NULL, delims, &lasts);
        if (!tok) {
            logmsg(LOGMSG_ERROR, "%s: service name missing from response\n",
                    __func__);
            return -1;
        }
        if (strcmp(tok, name) != 0) {
            logmsg(LOGMSG_ERROR, 
                    "%s: mismatched response from portmux for request '%s'"
                    " - got '%s'\n",
                    __func__, name, tok);
            return -1;
        }
    }

    return port;
}

/* returns port number, or -1 for error*/
int portmux_get(const char *remote_host, const char *app, const char *service,
                const char *instance)
{
    if (PORTMUX_USE_REFACTORED_GET()) {
        return portmux_get_int(NULL, remote_host, app, service, instance,
                               portmux_default_timeout);
    }
    char name[NAMELEN * 2];
    char res[32];
    SBUF2 *ss;
    int rc, fd, port;
    rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name))
        return -1;
    fd = tcpconnecth_to((char *)remote_host, get_portmux_port(), 0,
                        portmux_default_timeout);
    if (fd < 0)
        return -1;
    ss = sbuf2open(fd, SBUF2_WRITE_LINE);
    if (ss == 0) {
        close(fd);
        return -1;
    }
    sbuf2printf(ss, "get %s\n", name);
    res[0] = 0;
    sbuf2gets(res, sizeof(res), ss);
    sbuf2close(ss);
    if (res[0] == 0) {
        return -1;
    }
    port = atoi(res);
    if (port <= 0)
        port = -1;
    return port;
}

int portmux_geti(struct in_addr in, const char *app, const char *service,
                 const char *instance)
{
    if (PORTMUX_USE_REFACTORED_GET()) {
        return portmux_get_int(&in, NULL, app, service, instance,
                               portmux_default_timeout);
    }
    char name[NAMELEN * 2];
    char res[32];
    SBUF2 *ss;
    int rc, fd, port;
    rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name))
        return -1;
    fd = tcpconnect_to(in, get_portmux_port(), 0, portmux_default_timeout);
    if (fd < 0)
        return -1;
    ss = sbuf2open(fd, SBUF2_WRITE_LINE);
    if (ss == 0) {
        close(fd);
        return -1;
    }

    sbuf2settimeout(ss, portmux_default_timeout, portmux_default_timeout);

    sbuf2printf(ss, "get %s\n", name);
    res[0] = 0;
    sbuf2gets(res, sizeof(res), ss);
    sbuf2close(ss);
    if (res[0] == 0) {
        return -1;
    }
    port = atoi(res);
    if (port <= 0)
        port = -1;
    return port;
}

int portmux_get_to(const char *remote_host, const char *app,
                   const char *service, const char *instance, int timeout_ms)
{
    return portmux_get_int(NULL, remote_host, app, service, instance,
                           timeout_ms);
}

int portmux_geti_to(struct in_addr in, const char *app, const char *service,
                    const char *instance, int timeout_ms)
{
    return portmux_get_int(&in, NULL, app, service, instance, timeout_ms);
}

static int portmux_register_route(const char *app, const char *service,
                                  const char *instance, int *port,
                                  uint32_t options)
{
    char name[NAMELEN * 2];
    char res[32];
    SBUF2 *ss;
    int rc, listenfd;
    char sock_name[128];

    if (port) {
        *port = 0;
    }

    rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name)) {
        return -1;
    }
    if (PORTMUX_VALIDATE_NAMES())
        portmux_validate_name(name, __func__);

    /*get a unix socket to tell stuff about us*/
    rc = snprintf(sock_name, sizeof(sock_name), "/tmp/pmux_%s_%s_%s_%d.socket",
                  app, service, instance, (int)getpid());
    if (rc < 1 || rc >= sizeof(sock_name)) {
        return -1;
    }

    /*connect to portmux*/
    listenfd = portmux_get_unix_socket(sock_name);
    if (listenfd < 0) {
        return -1;
    }

    ss = sbuf2open(listenfd, SBUF2_WRITE_LINE);
    if (ss == 0) {
        close(listenfd);
        return -1;
    }

    sbuf2settimeout(ss, portmux_default_timeout, portmux_default_timeout);

    /*register with portmux using unix socket*/
    if ((options & PORTMUX_PORT_SUPPRESS) != 0) {
        sbuf2printf(ss, "reg %s n\n", name);
    } else {
        sbuf2printf(ss, "reg %s\n", name);
    }
    res[0] = 0;
    sbuf2gets(res, sizeof(res), ss);
    if (res[0] == 0) {
        sbuf2close(ss);
        return -1;
    }
    rc = atoi(res);
    sbuf2free(ss);

    if (rc < 0) {
        /*error*/
        close(listenfd);
        listenfd = -1;
    } else if (port) {
        *port = rc;
    }

    return listenfd;
}

static int portmux_get_unix_socket(const char *unix_bind_path)
{
    struct sockaddr_un addr;
    socklen_t len;
    int listenfd;

    errno = 0;
    listenfd = socket(AF_UNIX, SOCK_STREAM, 0 /*default proto*/);
    if (listenfd < 0) {
        logmsg(LOGMSG_ERROR, "%s:%d error socket errno=[%d] %s\n", __func__,
                __LINE__, errno, strerror(errno));
        return -1;
    }

    /*bind to give a name to our unix socket*/
    if (unlink(unix_bind_path) == -1 && (errno != ENOENT)) {
        logmsg(LOGMSG_ERROR, "%s:%d warning unlinking '%s' errno=[%d] %s\n",
                __func__, __LINE__, unix_bind_path, errno, strerror(errno));
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    len = strlen(unix_bind_path);
    if (len >= sizeof(addr.sun_path)) {
        logmsg(LOGMSG_ERROR, "%s:%d Filename too long: %s\n", __func__, __LINE__,
                unix_bind_path);
        return -1;
    }
    strncpy(addr.sun_path, unix_bind_path, sizeof(addr.sun_path) - 1);
    len += offsetof(struct sockaddr_un, sun_path);

    if (fcntl(listenfd, F_SETFD, 1 /*TRUE: close-on-exec*/) < 0) {
        logmsg(LOGMSG_ERROR, "%s:%d erro fcntl(F_SETFD) errno[%d]=%s\n", __func__,
                __LINE__, errno, strerror(errno));
    }

    if (bind(listenfd, (const struct sockaddr *)&addr, len) == -1) {
        logmsg(LOGMSG_ERROR, "%s:%d error bind errno=[%d] %s\n", __func__, __LINE__,
                errno, strerror(errno));
    }
    unlink(unix_bind_path); /*unneeded now*/

    /*connect to portmux unix socket*/
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    int uslen = strlen(portmux_unix_socket);

    if (uslen >= sizeof(addr.sun_path)) {
        logmsg(LOGMSG_ERROR, "%s:%d Portmux unix socket path too long.",
               __func__, __LINE__);
        return -1;
    } else {
        strcpy(addr.sun_path, portmux_unix_socket);
    }

    len = offsetof(struct sockaddr_un, sun_path) + uslen;

    if (connect(listenfd, (struct sockaddr *)&addr, len) < 0) {
        if ((errno != ENOENT) && (errno != ECONNREFUSED)) {
            logmsg(LOGMSG_ERROR, "%s:%d error connecting to portmux "
                                 "on %s errno[%d]=%s\n",
                   __func__, __LINE__, portmux_unix_socket, errno,
                   strerror(errno));
        }
        close(listenfd);
        return -1;
    }

    return listenfd;
}

static int portmux_route_to(struct in_addr in, const char *app,
                            const char *service, const char *instance,
                            int timeoutms)
{
    char cmd[NAMELEN * 2]; /* space for command + 64 char app/svc/inst string */
    char res[2];
    int rc, fd, len;

    /* apparently the fd can get "lost" on
     * AIX when service is in core'ing state. pass_fd is
     * successful but fd will not get cleaned up by OS when
     * service is. Therefore put a timeout here. Not very keen
     * on this as it forces the service to be "responsive"
     * within a certain time. It should however avoid waiting
     * forever which is worse.
     */
    if (timeoutms <= 0)
        timeoutms = portmux_default_timeout;

    int startms = comdb2_time_epochms();

    len = snprintf(cmd, sizeof(cmd), "rte %s/%s/%s%s\n", app, service, instance,
                   PORTMUX_VALIDATE() ? " v" : "");
    if (len < 1 || len >= sizeof(cmd))
        return -1;
    fd = tcpconnect_to(in, get_portmux_port(), 0,
                       remaining_timeoutms(startms, timeoutms));
    if (fd < 0)
        return -1;

    /* Write request for routing to service */
    rc = tcpwritemsg(fd, cmd, len, remaining_timeoutms(startms, timeoutms));
    if (rc != len) {
        close(fd);
        return -1;
    }

    /* The response on success is two bytes long ("0\n" or "1\n"). On error it
     * is
     * more ("-1\n").  On success we must not read back more than two bytes
     * from the socket since the rest is not intended for is but for the
     * application.  Therefore read 2 bytes and if they match what we
     * expect we're done, otherwise we failed. */
    rc = tcpreadmsg(fd, res, 2, remaining_timeoutms(startms, timeoutms));
    if (rc != 2 || (res[0] != '0' && res[0] != '1') || res[1] != '\n') {
        close(fd);
        return -1;
    }

    /* valid routing we should be connected to 'name' now */
    /* If the response was '1' then we are connected to a server that
     * supports connection validation and will be waiting to do it, i.e.
     * we must do the validation since we made the 'rte' request above,
     * regardless of any change in PORTMUX_VALIDATE() since then.
     */
    if (res[0] == '1') {

        if (!portmux_client_side_validation(
                fd, app, service, instance,
                remaining_timeoutms(startms, timeoutms))) {
            close(fd);
            return -1;
        }
    }
    return fd;
}

/* we can wait for activity on 2 sockets:*/
struct portmux_fd {
    int listenfd;    /**< Unix socket, wait for portmux to send clients fds */
    int tcplistenfd; /**< Standard accept for direct tcp client connections */
    int port;        /**< Attributed port for tcp binding                   */
    int recov_conn;  /**< If TRUE and listenfd gets bad, retry connecting   */
    int nretries;    /**< if recovering keep track of # attempts            */
    char app[80];
    char service[80];
    char instance[80];
    time_t last_recover_time;
    uint32_t options;
};

/*return 1 if no connectivity: aka no TCP bound port and no UNIX socket*/
static int portmux_recover_route(portmux_fd_t *fds)
{
    int port;

    fds->listenfd = portmux_register_route(fds->app, fds->service,
                                           fds->instance, &port, fds->options);
    if (fds->listenfd >= 0) {
        /*reconnected, check port# is still the same*/
        if (port != fds->port) {
            if (fds->port != 0) {
                logmsg(LOGMSG_ERROR, "%s:%d portmux port# inconsistency: "
                                "myport# %d newport# %d for <%s/%s/%s>\n",
                        __func__, __LINE__, fds->port, port, fds->app,
                        fds->service, fds->instance);
            }

            if (fds->tcplistenfd >= 0) {
                close(fds->tcplistenfd);
                fds->tcplistenfd = -1;
            }

            fds->port = port;
        }
    }

    /*if we have a port# check TCP connection is established*/
    if ((fds->port > 0) && (fds->tcplistenfd < 0)) {
        fds->tcplistenfd = tcplisten(port);
        if (fds->tcplistenfd >= 0) {
            /*set close-on-exec*/
            if (fcntl(fds->tcplistenfd, F_SETFD, 1) < 0) {
                logmsg(LOGMSG_ERROR, "%s:%d error "
                                "fcntl(F_SETFD) errno[%d]=%s\n",
                        __func__, __LINE__, errno, strerror(errno));
            }
        }
    }

    /*check if connected via either unix socket or TCP socket*/
    if ((fds->listenfd >= 0) || (fds->tcplistenfd >= 0)) {
        return 0; /*connected*/
    }

    /*no connectivity*/
    return 1;
}

static int portmux_handle_recover(portmux_fd_t *fds, int timeoutms)
{
    int rc;
    time_t now;

    if (!fds->recov_conn || (fds->listenfd >= 0)) {
        return 0; /*nothing to do, continue*/
    }

    /* Add an artificial delay if we already did recovery once this second.
     * This is to protect against the silly situation where two servers have
     * claimed the same name in portmux and are fighting with each other for
     * the socket.  This doesn't prevent the fight, but it does slow it
     * down a lot. */
    fds->nretries += 1;
    time(&now);
    if (now == fds->last_recover_time) {
        sleep(1);
    }
    fds->last_recover_time = now;

    while ((rc = portmux_recover_route(fds)) != 0) {
        if (fds->nretries == 1) {
            logmsg(LOGMSG_ERROR, "%s:%d no connectivity with portmux "
                            "for <%s/%s/%s> ... recovering ...\n",
                    __func__, __LINE__, fds->app, fds->service, fds->instance);
        }
        /*this is bad: no connection at all, wait a bit
         *and try connecting again */
        if (timeoutms > 0) {
            if ((timeoutms / 1000) < fds->nretries) {
                /*return: timeout expired before
                 *we regain connectivity*/
                return 1;
            }
        }

        sleep(1);
        if ((fds->nretries % 10) == 0) {
            logmsg(LOGMSG_ERROR, "%s:%d WARNING connectivity with "
                            "portmux still not established for <%s/%s/%s>\n",
                    __func__, __LINE__, fds->app, fds->service, fds->instance);
        }
        /* retry */
        ++fds->nretries;
    }

    if (fds->listenfd >= 0) {
        /*recovered unix socket connection*/
        logmsg(LOGMSG_ERROR,
               "%s:%d reconnected to %s on fd# %d for <%s/%s/%s>\n", __func__,
               __LINE__, portmux_unix_socket, fds->listenfd, fds->app,
               fds->service, fds->instance);
        fds->nretries = 0;
    }

    return 0;
}

/* we read/write the request and response values as a single byte to avoid
 * endian and size issues.
 */
enum request { V_WHO = 1, V_NAK = 2, V_ACK = 3 };
enum response { V_NONE = 0, V_ID = 1 };

static bool portmux_client_side_validation(int fd, const char *app,
                                           const char *service,
                                           const char *instance, int timeoutms)
{
    /* this is the client end of the validation processing - we drive
     * the protocol by requesting information then telling the
     * server whether to proceed or not.
     *
     * The expected sequence is
     * send V_WHO
     * rcv V_ID, features, app/service/instance
     *
     * The uint32_t features value is currently zero because there are no
     * additional features supported. If the server end provides some extra
     * functionality then we would detect that in this value and do some
     * additional validation.
     *
     * if app, service, instance is as expected
     * send V_ACK, return true
     * otherwise
     * send V_NAK, return false
     */
    int state = 0;
    int next_state = 0;

    int startms = comdb2_time_epochms();
    if (timeoutms < MIN_VALIDATION_TIMEOUTMS) {
        timeoutms = MIN_VALIDATION_TIMEOUTMS;
    }

    struct sockaddr_in client_addr;
    socklen_t len = sizeof(client_addr);
    char dotted_quad[16]; // nnn.nnn.nnn.nnn0
    if (getpeername(fd, (struct sockaddr *)&client_addr, &len) == 0) {
        unsigned char *cptr = (unsigned char *)&(client_addr.sin_addr.s_addr);
        snprintf(dotted_quad, sizeof(dotted_quad), "%u.%u.%u.%u", cptr[0],
                 cptr[1], cptr[2], cptr[3]);
    } else {
        snprintf(dotted_quad, sizeof(dotted_quad), "???.???.???.???");
    }

    while (true) {
        uint32_t features;
        char request, expected_response = 0;

        switch (state) {
        case 0:
            request = V_WHO;
            expected_response = V_ID;
            next_state = 1;
            break;

        case 1:
            /* read back the server's features, app, service, instance.
             */
            request = V_NAK;
            expected_response = V_NONE;
            next_state = 999;

            uint32_t size;
            if (tcpread(fd, &size, sizeof(size),
                        remaining_timeoutms(startms, timeoutms)) ==
                sizeof(size)) {
                size = ntohl(size);
                /* sanity check - there is a limit on the number of bytes we
                 * can expect from the server end of the validation.
                 * At the time of writing we are limited to 80 characters for
                 * each part of the triplet so expect no more than 240
                 * characters "1-79/1-79/1-79\0.".
                 * The minimum triplet length is 6, e.g. "a/b/c\0"
                 * The value we read should be nul terminated.
                 *
                 * [The reality is that the values can never be this long due
                 * to limits applied in other parts of the code.]
                 *
                 * If any of those checks fail then we assume that we aren't
                 * connected to a server doing the portmux validation sequence
                 * despite earlier appearances.
                 */
                char buf[512];
                if (size < sizeof(uint32_t) + 6 || size > sizeof(buf)) {
                    logmsg(LOGMSG_ERROR, 
                            "%s: Bad size (%u) received from server %s.\n",
                            __func__, size, dotted_quad);
                } else if (tcpread(fd, buf, size,
                                   remaining_timeoutms(startms, timeoutms)) !=
                           size) {
                    logmsg(LOGMSG_ERROR, 
                            "%s: failed to tcpread %u byte buffer from %s.\n",
                            __func__, size, dotted_quad);
                } else if (buf[size - 1] != '\0') {
                    int len = (size > 32) ? 32 : size;
                    logmsg(LOGMSG_ERROR, "%s: %d byte buffer from %s ends with %d, "
                                    "expected a nul.  First %d bytes are '",
                            __func__, size, dotted_quad, buf[size - 1], len);
                    for (int ii = 0; ii < len; ++ii) {
                        if (isprint(buf[ii])) {
                            fputc(buf[ii], stderr);
                        } else {
                            logmsg(LOGMSG_ERROR, "\\%03o",
                                    ((unsigned char)buf[ii]) & 0xff);
                        }
                    }
                    logmsg(LOGMSG_ERROR, "'\n");
                } else {
                    memcpy(&features, buf, sizeof(features));
                    features = ntohl(features);

                    const char *app_s = buf + sizeof(uint32_t);
                    const char *service_s = NULL;
                    const char *instance_s = NULL;

                    char *pos = strchr(app_s, '/');
                    if (pos != NULL) {
                        *pos++ = '\0';
                        service_s = pos;
                        if ((pos = strchr(service_s, '/')) != NULL) {
                            *pos++ = '\0';
                            instance_s = pos;
                        }
                    }
                    if (app_s == NULL || service_s == NULL ||
                        instance_s == NULL) {
                        logmsg(LOGMSG_ERROR, "%s: Badly formed app/service/instance "
                                        "received from server %s.\n",
                                __func__, dotted_quad);
                    } else if (strcmp(app, app_s) != 0 ||
                               strcmp(service, service_s) != 0 ||
                               strcmp(instance, instance_s) != 0) {
                        logmsg(LOGMSG_ERROR, "%s: Wanted '%s/%s/%s' but connected "
                                        "to '%s/%s/%s' on %s\n",
                                __func__, app, service, instance, app_s,
                                service_s, instance_s, dotted_quad);
                    } else {
                        request = V_ACK;
                        next_state = 100;
                    }
                }
            }
            break;

        case 100:
            return true;

        case 999:
            return false;
        }

        if (tcpwritemsg(fd, &request, 1,
                        remaining_timeoutms(startms, timeoutms)) != 1) {
            return false;
        }
        if (expected_response != V_NONE) {
            char actual_response;

            if (tcpreadmsg(fd, &actual_response, 1,
                           remaining_timeoutms(startms, timeoutms)) != 1) {
                return false;
            } else if (actual_response != expected_response) {
                logmsg(LOGMSG_ERROR, 
                    "%s: Response from %s was %d but I expected %d\n",
                    __func__, dotted_quad, actual_response, expected_response);
                return false;
            }
        }
        state = next_state;
    }
    /* NOT REACHED */
}

static bool portmux_server_side_validation(portmux_fd_t *fds, int fd,
                                           int timeoutms)
{
    /* A client has connected to us, enter validation mode. We will return
     * false if we have any communication errors or if the client sends us
     * V_NAK; returning true if the client sends us V_ACK.
     */
    int startms = comdb2_time_epochms();

    if (timeoutms < MIN_VALIDATION_TIMEOUTMS) {
        timeoutms = MIN_VALIDATION_TIMEOUTMS;
    }

    while (true) {
        char request, response;

        if (tcpreadmsg(fd, &request, 1,
                       remaining_timeoutms(startms, timeoutms)) != 1) {
            return false;
        }

        switch (request) {
        case V_WHO:
            response = V_ID;
            if (tcpwritemsg(fd, &response, 1,
                            remaining_timeoutms(startms, timeoutms)) != 1) {
                return false;
            }
            uint32_t size = 0;
            uint32_t features = htonl(0);
            /* format the triplet into a string.
             * snprintf returns the number of characters printed, excluding
             * the null byte used to end output to strings - we want that too.
             */
            char buf[512];
            uint32_t buflen = snprintf(buf, sizeof(buf), "%s/%s/%s", fds->app,
                                       fds->service, fds->instance) +
                              1;
            if (buflen > sizeof(buf)) {
                return false;
            }

            struct iovec iov[3];

            iov[0].iov_base = (void *)&size;
            iov[0].iov_len = sizeof(uint32_t);

            iov[1].iov_base = (void *)&features;
            iov[1].iov_len = sizeof(uint32_t);

            iov[2].iov_base = (void *)buf;
            iov[2].iov_len = buflen;

            size = iov[1].iov_len + iov[2].iov_len;
            size = htonl(size);

            int nwritten =
                tcpwritemsgv(fd, sizeof(iov) / sizeof(iov[0]), iov,
                             remaining_timeoutms(startms, timeoutms));
            if (nwritten < 0) {
                return false;
            }
            break;

        case V_NAK:
            return false;

        case V_ACK:
            return true;
        }
    }
    /* NOT REACHED */
}

struct pollv_accept_data {
    void (*accept_hndl)(int fd, void *user_data);
    void *user_data;
};

/* This function is used to convert an accept callback from portmux_poll_v()
 * into a call to an accept callback function for portmux_poll().
 */
static int portmux_poll_v_accept(int which, int fd, void *data)
{
    struct pollv_accept_data *pollv_data = (struct pollv_accept_data *)data;

    pollv_data->accept_hndl(fd, pollv_data->user_data);
    return 0;
}

/* Wait for connections from clients on either unix domain socket or TCP socket.
 * Connections on unix socket are done via file descriptor passing: client
 * connects to portmux.tsk, portmux.tsk passes connection to server.
 * Connections on TCP socket are done via accept.
 * On established client connection the given callback function is called with
 * connection handle. If no callback is passed as parameter then the
 * connection handle is returned to the caller */
static int portmux_poll(portmux_fd_t *fds, int timeoutms,
                        void (*accept_hndl)(int fd, void *user_data),
                        void *user_data, struct sockaddr_in *cliaddr)
{
    int clientfd;
    struct pollfd pollfds[2];
    nfds_t nfds;
    int poll_unixsocket, poll_tcpsocket;
    char msg[64];
    int rc;

    /* If enabled (expected to be) then use the pollv function; otherwise
     * we use the original code. That doesn't support end-to-end validation.
     */
    if (PORTMUX_USE_POLL_V()) {
        if (accept_hndl == NULL) {
            return portmux_poll_v(&fds, 1, timeoutms, NULL, NULL, cliaddr);
        } else {
            struct pollv_accept_data pollv_data;

            pollv_data.accept_hndl = accept_hndl;
            pollv_data.user_data = user_data;
            return portmux_poll_v(&fds, 1, timeoutms, portmux_poll_v_accept,
                                  &pollv_data, cliaddr);
        }
    }

    /*loop, on error cleanup and return*/
    while (1) {
        /* Check connectivity */
        rc = portmux_handle_recover(fds, timeoutms);
        if (rc) {
            /*not reachable via portmux no unix socket, no TCP port*/
            errno = EAGAIN;
            return -1;
        }

        /* Setup poll call */
        poll_unixsocket = -1;
        poll_tcpsocket = -1;
        clientfd = -1;
        nfds = 0;
        if (fds->listenfd >= 0) {
            pollfds[nfds].fd = fds->listenfd;
            pollfds[nfds].events = POLLIN;
            poll_unixsocket = nfds;
            nfds += 1;
        }
        if (fds->tcplistenfd >= 0) {
            pollfds[nfds].fd = fds->tcplistenfd;
            pollfds[nfds].events = POLLIN;
            poll_tcpsocket = nfds;
            nfds += 1;
        }

        rc = poll(pollfds, nfds, timeoutms);
        if (rc < 0) {
            /*if interrupted just retry*/
            if (errno == EINTR) {
                continue;
            }

            return -1;
        }

        /* Handle timeout */
        if (rc == 0) {
            errno = ETIMEDOUT;
            return 0;
        }

        /* Check unix socket */
        if (poll_unixsocket >= 0) {
            if (pollfds[poll_unixsocket].revents & POLLERR) {
                close(fds->listenfd);
                fds->listenfd = -1;
            } else if (pollfds[poll_unixsocket].revents & POLLIN) {
                /* get handle via passfd */
                rc = recv_fd(fds->listenfd, &msg, strlen("pmux"), &clientfd);
                if (rc >= 0) {
                    /*acknowledge connection*/
                    rc = tcpwritemsg(clientfd, "0\n", 2, timeoutms);
                    if (rc != 2) {
                        /* Failed to write successful connect acknowledgement
                         * message within required timeout, so give up and
                         * move on. */
                        close(clientfd);
                    } else if (accept_hndl) {
                        accept_hndl(clientfd, user_data);
                    } else {
                        return clientfd;
                    }
                } else {
                    /*anomaly: it could be
                     *portmux unix has closed
                     *so cleanup handle and let
                     *reconnect logic kick in*/
                    close(fds->listenfd);
                    fds->listenfd = -1;
                }
            }
        }

        /* Check TCP socket */
        if (poll_tcpsocket >= 0) {
            if (pollfds[poll_tcpsocket].revents & POLLERR) {
                close(fds->tcplistenfd);
                fds->tcplistenfd = -1;
            } else if (pollfds[poll_tcpsocket].revents & POLLIN) {
                clientfd = tcpaccept(fds->tcplistenfd, cliaddr);
                if (clientfd >= 0) {
                    portmux_denagle(clientfd);
                    if (accept_hndl) {
                        accept_hndl(clientfd, user_data);
                    } else {
                        return clientfd;
                    }
                }
            }
        }
    }

    /* NOT REACHED */
}

/* Check if we are connected to the portmux server process, attempt
 * to open a connection if we aren't.
 *
 * If this fails too many times, and we are listening on a TCP
 * port then we stop trying to open the connection and rely on
 * connections on that port number.
 *
 * This function is used for 'vector' polling - we just try to recover once
 * and return. The caller is responsible for determining when or if to try
 * again.
 *
 * Return value:
 *
 * 0 - a connection is established
 *
 * 1 - there is no connection, but you should try again later.
 *
 * 2 - there is no connection, no point trying again (but no harm if you do.)
 *
 */
static int portmux_handle_recover_v(portmux_fd_t *fds)
{
    int rc;

    if (fds->listenfd >= 0) {
        return 0; /*nothing to do, continue*/
    } else if (!fds->recov_conn) {
        /* We have given up trying to connect
         */
        return 2;
    }

    time_t now;
    time(&now);
    if (now == fds->last_recover_time) {
        /* We throttle our recovery attempts to at
         * most once a second per fds.
         */
        return 1;
    }

    /* See if we can connect to pmux. */
    if (reconnect_callback) {
        rc = reconnect_callback(reconnect_callback_arg);
        if (rc) return rc;
    }

    fds->nretries += 1;
    fds->last_recover_time = now;

    if ((rc = portmux_recover_route(fds)) != 0) {
        if (fds->nretries == 1) {
            logmsg(LOGMSG_ERROR, "%s:%d no connectivity with portmux "
                            "for <%s/%s/%s> ... recovering ...\n",
                    __func__, __LINE__, fds->app, fds->service, fds->instance);
        }
        if ((fds->nretries % 10) == 0) {
            logmsg(LOGMSG_ERROR, "%s:%d WARNING connectivity with "
                            "portmux still not established for <%s/%s/%s>\n",
                    __func__, __LINE__, fds->app, fds->service, fds->instance);
        }
        ++fds->nretries;
    }

    if (fds->listenfd >= 0) {
        /*recovered unix socket connection*/
        logmsg(LOGMSG_WARN,
               "%s:%d reconnected to %s on fd# %d for <%s/%s/%s>\n", __func__,
               __LINE__, portmux_unix_socket, fds->listenfd, fds->app,
               fds->service, fds->instance);
        fds->nretries = 0;
    }

    return (fds->listenfd >= 0) ? 0 : 1;
}

/* Wait for connections from clients on either unix domain socket or TCP socket.
 * Connections on unix socket are done via file descriptor passing: client
 * connects to portmux.tsk, portmux.tsk passes connection to server.
 * Connections on TCP socket are done via accept.
 * On established client connection the given callback function is called with
 * which entry in fds[] connected along with the connection handle.
 */
static int portmux_poll_v(portmux_fd_t **fds, nfds_t nfds, int timeoutms,
                          int (*accept_hndl)(int which, int fd,
                                             void *user_data),
                          void *user_data, struct sockaddr_in *cliaddr)
{
    int clientfd;
    struct pollfd *pollfds;
    nfds_t npollfds = 0;
    char msg[64];
    struct polldata {
        bool is_tcp;
        nfds_t slot;
    } * polldata;

    /* I used to determine the exact number of file descriptors we will be
     * polling by inspecting fds[]; however, it seems much safer
     * to just allocate the space we need as if both descriptors are being
     * used - it protects us from things changing underfoot in someway.
     *
     * We have a data structure, pooldata, to track whether a pollfd entry
     * is a tcp or unix socket and to store which portmux_fd_t the
     * fd belongs to.
     */

    pollfds = malloc(sizeof(struct pollfd) * (nfds * 2));
    if (pollfds == NULL) {
        return -1;
    }
    polldata = malloc(sizeof(struct polldata) * (nfds * 2));
    if (polldata == NULL) {
        free(pollfds);
        return -1;
    }

    int result = 0;
    bool build_pollfds = true;

    int startms = comdb2_time_epochms();

    while (!db_is_exiting()) {
        if (build_pollfds) {
            /* Iterate over all the portmux descriptors and build
             * the array of descriptors to pass to poll().
             *
             * We will build this list on our first iteration and again when
             * we have any connection problems.
             */
            npollfds = 0;
            bool build_pollfds_again = false;
            for (int ii = 0; ii < nfds; ++ii) {
                if (fds[ii] != NULL) {
                    /* Check connectivity to portmux
                     */
                    switch (portmux_handle_recover_v(fds[ii])) {
                    case 0:
                        /* Unix socket is connected
                         */
                        pollfds[npollfds].fd = fds[ii]->listenfd;
                        pollfds[npollfds].events = POLLIN;
                        polldata[npollfds].is_tcp = false;
                        polldata[npollfds].slot = ii;
                        ++npollfds;
                        break;

                    case 1:
                        /* We don't have a unix socket connection to portmux,
                         * keep trying to establish it.
                         */
                        build_pollfds_again = true;
                        break;

                    case 2:
                        /* No connection, and won't ever get one from now on.
                         */
                        break;
                    }

                    if (fds[ii]->tcplistenfd >= 0) {
                        /* We are listening on a TCP socket
                         */
                        pollfds[npollfds].fd = fds[ii]->tcplistenfd;
                        pollfds[npollfds].events = POLLIN;
                        polldata[npollfds].is_tcp = true;
                        polldata[npollfds].slot = ii;
                        ++npollfds;
                    }
                }
            }
            build_pollfds = build_pollfds_again;
        }

        int nready;
        do {
            /* Here we want to poll for connections, but it may be that we
             * have some pending connection recovery that is required.
             * If build_pollfds is true here we set up so we regularly return
             * from the poll() call so we can try and reconnect.
             * The reconnection code limits attempts to at most once per
             * time() second. We give it a shot at least every
             * RECONNECT_POLL_TIMEOUTMS.
             */
            int pollms;

            if (timeoutms < 0) {
                /* user wants 'blocking' call,
                 */
                if (build_pollfds) {
                    pollms = RECONNECT_POLL_TIMEOUTMS;
                } else {
                    pollms = -1;
                }
            } else {
                pollms = remaining_timeoutms(startms, timeoutms);
                if (build_pollfds && pollms > RECONNECT_POLL_TIMEOUTMS) {
                    pollms = RECONNECT_POLL_TIMEOUTMS;
                }
            }
            if (pollms == -1) {
                do {
                    nready = poll(pollfds, npollfds, 1000); // poll for 1000ms
                } while (!db_is_exiting() && nready == 0);
                if (nready == 0)
                    break;
            } else
                nready = poll(pollfds, npollfds, pollms);
        } while (nready < 0 && errno == EINTR);

        if (nready < 0) {
            result = -1;
            goto clean_up_and_return;
        }

        if (nready == 0) {
            /* If we really have timed out then return; otherwise just
             * continue, which will do nothing because nready
             * is zero and loop back to rebuild the pollfds.
             */
            if (remaining_timeoutms(startms, timeoutms) == 0) {
                errno = ETIMEDOUT;
                result = 0;
                goto clean_up_and_return;
            }
        }

        /* Iterate over all the pollfds and handle any that
         * have errors, or are ready.
         */
        for (int ii = 0; ii < npollfds && nready > 0; ++ii) {
            int slot = polldata[ii].slot;
            if (pollfds[ii].revents & POLLERR) {
                /* On error, we close the associated file descriptor and
                 * it will be 'recovered' on the next iteration.
                 */
                close(pollfds[ii].fd);
                if (polldata[ii].is_tcp) {
                    fds[slot]->tcplistenfd = -1;
                } else {
                    fds[slot]->listenfd = -1;
                }
                build_pollfds = true;
                --nready;
            } else if (pollfds[ii].revents & POLLIN) {
                /* We are expecting to handle an incoming tcp connection or
                 * receive a connected fd over a unix socket.
                 * A successful connection assigns the connection fd to
                 * clientfd; otherwise -1.
                 */
                if (polldata[ii].is_tcp) {
                    clientfd = tcpaccept(fds[slot]->tcplistenfd, cliaddr);
                } else {
                    /* get handle via passfd */
                    if (recv_fd(fds[slot]->listenfd, &msg, strlen("pmux"),
                                &clientfd) < 0) {
                        /* It could be the unix socket has closed so
                         * cleanup handle and let reconnect logic kick in on
                         * before the next poll.
                         */
                        close(fds[slot]->listenfd);
                        fds[slot]->listenfd = -1;
                        clientfd = -1;
                        build_pollfds = true;
                    } else {
                        /* acknowledge connection, performing end-to-end
                         * validation if applicable. Any failure and we
                         * just drop the connection.
                         */
                        bool do_validate =
                            (PORTMUX_VALIDATE() &&
                             (strncmp(msg, "pmuv", strlen("pmuv")) == 0));

                        char *response = (do_validate) ? "1\n" : "0\n";
                        if (tcpwritemsg(
                                clientfd, response, 2,
                                remaining_timeoutms(startms, timeoutms)) != 2 ||
                            (do_validate &&
                             !portmux_server_side_validation(
                                 fds[slot], clientfd,
                                 remaining_timeoutms(startms, timeoutms)))) {
                            close(clientfd);
                            clientfd = -1;
                        }
                    }
                    socklen_t addrlen = sizeof(struct sockaddr_in);
                    int rc = getpeername(clientfd, (struct sockaddr*) cliaddr, &addrlen);
                    if (rc ) {
                        close(clientfd);
                        clientfd = -1;
                    }
                }

                if (clientfd >= 0) {
                    /* We have a connection to pass to the user. If a callback
                     * function has been provided then pass the descriptor to
                     * that, otherwise we return the descriptor to the caller.
                     * A non-zero return from accept_hndl() means
                     * we stop processing here and return that value
                     * to the caller.
                     */
                    portmux_denagle(clientfd);
                    if (accept_hndl != NULL) {
                        if ((result = accept_hndl(slot, clientfd, user_data)) !=
                            0) {
                            goto clean_up_and_return;
                        }
                    } else {
                        result = clientfd;
                        goto clean_up_and_return;
                    }
                }
                --nready;
            }
        }
    }

clean_up_and_return:
    free(polldata);
    free(pollfds);
    return result;
}

portmux_fd_t *portmux_listen_options_setup(const char *app, const char *service,
                                           const char *instance,
                                           int tcplistenfd, uint32_t options)
{
    portmux_fd_t *fds;
    int listenfd = -1;
    int port = 0;

    if (!app || !service || !instance) {
        logmsg(LOGMSG_ERROR, "%s:%d invalid arguments\n", __func__, __LINE__);
        return NULL;
    }

    if (strlen(app) >= sizeof(fds->app) ||
        strlen(service) >= sizeof(fds->service) ||
        strlen(instance) >= sizeof(fds->instance)) {
        logmsg(LOGMSG_ERROR, "%s:%d argument too long\n", __func__, __LINE__);
        return NULL;
    }

    if (PORTMUX_ROUTE_MODE_ENABLED()) {
        /* try to establish connection with local portmux
         * using unix domain sockets */
        logmsg(LOGMSG_INFO, "PORTMUX route mode Enabled \n");
        listenfd =
            portmux_register_route(app, service, instance, &port, options);
        if (listenfd >= 0 && tcplistenfd < 0) {
            if (port > 0) {
                tcplistenfd = tcplisten(port);
            } else {
                tcplistenfd = -1;
            }
        } else if (listenfd  < 0) {
            logmsg(LOGMSG_ERROR, "%s:%d portmux listen errno=[%d] %s\n", __func__,
                    __LINE__, errno, strerror(errno));
            return NULL;
        }
    }

    if (listenfd < 0 && tcplistenfd < 0) {
        /*get a tcp port from portmux*/
        port = portmux_register(app, service, instance);
        if (port < 0) {
            logmsg(LOGMSG_ERROR, "%s:%d portmux_register(%s, %s, %s) failure\n",
                    __func__, __LINE__, app, service, instance);
            return NULL;
        }

        /*listen for clients*/
        tcplistenfd = tcplisten(port);
        if (tcplistenfd < 0) {
            logmsg(LOGMSG_ERROR, "%s:%d tpclisten errno=[%d] %s\n", __func__,
                    __LINE__, errno, strerror(errno));
            return NULL;
        }
    }

    fds = (portmux_fd_t *)malloc(sizeof(portmux_fd_t));
    if (!fds) {
        if (listenfd >= 0)
            close(listenfd);
        if (tcplistenfd >= 0)
            close(tcplistenfd);
        return NULL;
    }
    fds->listenfd = listenfd;
    fds->tcplistenfd = tcplistenfd;
    fds->port = port;
    strncpy(fds->app, app, sizeof(fds->app) - 1);
    strncpy(fds->service, service, sizeof(fds->service) - 1);
    strncpy(fds->instance, instance, sizeof(fds->instance) - 1);
    fds->options = options;

    /*check if we have connection on portmux unix socket*/
    if (PORTMUX_ROUTE_MODE_ENABLED()) {
        /*in case connection is dropped try to recover it*/
        fds->recov_conn = 1 /*TRUE*/;
    } else {
        fds->recov_conn = 0 /*FALSE*/;
    }
    fds->nretries = 0;

    if (fds->tcplistenfd >= 0) {
        /*set close-on-exec*/
        if (fcntl(fds->tcplistenfd, F_SETFD, 1) < 0) {
            logmsg(LOGMSG_ERROR, "%s:%d erro fcntl(F_SETFD) errno[%d]=%s\n",
                    __func__, __LINE__, errno, strerror(errno));
        }
    }

    return fds;
}

portmux_fd_t *portmux_listen_setup(const char *app, const char *service,
                                   const char *instance, int tcplistenfd)
{
    return portmux_listen_options_setup(app, service, instance, tcplistenfd, 0);
}

int portmux_accept(portmux_fd_t *fds, int timeoutms, struct sockaddr_in *cliaddr)
{
    if (!fds) {
        return -1;
    }

    if (timeoutms <= 0) {
        timeoutms = -1; /*no timeout*/
    }

    return portmux_poll(fds, timeoutms, NULL, NULL, cliaddr);
}

int portmux_acceptv(portmux_fd_t **fds, int nfds, int timeoutms,
                    int (*accept_hndl)(int which, int fd, void *user_data),
                    void *user_data, struct sockaddr_in *cliaddr)
{
    if (fds == NULL) {
        return -1;
    }

    if (timeoutms <= 0) {
        timeoutms = -1; /*no timeout*/
    }

    return portmux_poll_v(fds, nfds, timeoutms, accept_hndl, user_data, cliaddr);
}

int portmux_listen_options(const char *app, const char *service,
                           const char *instance,
                           void (*accept_hndl)(int fd, void *user_data),
                           void *user_data, uint32_t options)
{
    int save_errno;
    portmux_fd_t *fds;

    if (!app || !service || !instance || !accept_hndl) {
        logmsg(LOGMSG_ERROR, "%s:%d invalid arguments\n", __func__, __LINE__);
        return -1;
    }

    fds = portmux_listen_options_setup(app, service, instance, -1, options);
    if (!fds) {
        return -1;
    }

    /*accept connections*/
    portmux_poll(fds, -1 /*no timeout*/, accept_hndl, user_data, NULL);

    /*deregister properly*/
    save_errno = errno;
    portmux_deregister(app, service, instance);
    portmux_close(fds);
    errno = save_errno;

    return -1;
}

int portmux_listen(const char *app, const char *service, const char *instance,
                   void (*accept_hndl)(int fd, void *user_data),
                   void *user_data)
{
    return portmux_listen_options(app, service, instance, accept_hndl,
                                  user_data, 0);
}

void portmux_close(portmux_fd_t *fds)
{
    if (!fds) {
        return;
    }

    if (fds->listenfd >= 0) {
        close(fds->listenfd);
    }

    if (fds->tcplistenfd >= 0) {
        close(fds->tcplistenfd);
    }

    free(fds);
    return;
}

int portmux_fds_get_tcpport(portmux_fd_t *fds)
{
    if (!fds) {
        return 0;
    }

    return fds->port;
}

int portmux_connect(const char *remote_host, const char *app,
                    const char *service, const char *instance)
{
    return portmux_connect_to(remote_host, app, service, instance,
                              0 /*no timeout*/);
}

int portmux_connect_to(const char *remote_host, const char *app,
                       const char *service, const char *instance, int timeoutms)
{
    int rc;
    struct in_addr addr;
    int port;

    if ((rc = tcpresolve((char *)remote_host, &addr, &port)) != 0) {
        return rc;
    }

    return portmux_connecti_to(addr, app, service, instance, timeoutms);
}

int portmux_connecti(struct in_addr addr, const char *app, const char *service,
                     const char *instance)
{
    return portmux_connecti_to(addr, app, service, instance, 0 /*no timeout*/);
}

int portmux_connecti_to(struct in_addr addr, const char *app,
                        const char *service, const char *instance,
                        int timeoutms)
{
    return portmux_connecti_int(addr, app, service, instance, 0 /*no bind*/,
                                timeoutms);
}

static int portmux_connecti_int(struct in_addr addr, const char *app,
                                const char *service, const char *instance,
                                int myport, int timeoutms)
{
    int port;
    int fd;

    if (!app || !service || !instance || (timeoutms < 0)) {
        logmsg(LOGMSG_ERROR, "%s:%d invalid arguments\n", __func__, __LINE__);
        return -1;
    }

    if (PORTMUX_ROUTE_MODE_ENABLED()) {
        fd = portmux_route_to(addr, app, service, instance, timeoutms);
        if (fd >= 0) {
            portmux_denagle(fd);
            return fd;
        }

        /*an error//refusal happened, fallback to old logic*/
        /*FALLTHRU*/
    }

    port = portmux_geti(addr, app, service, instance);
    if (port < 0) {
        return -1;
    }

    fd = tcpconnect_to(addr, port, myport, timeoutms);
    if (fd >= 0) {
        portmux_denagle(fd);
    }
    return fd;
}

void portmux_set_default_timeout(unsigned timeoutms)
{
    portmux_default_timeout = timeoutms;
}

void portmux_set_max_wait_timeout(unsigned timeoutms)
{
    max_wait_timeoutms = timeoutms;
}

const char *portmux_fds_get_app(portmux_fd_t *fds)
{
    return fds->app;
}

const char *portmux_fds_get_service(portmux_fd_t *fds)
{
    return fds->service;
}

const char *portmux_fds_get_instance(portmux_fd_t *fds)
{
    return fds->instance;
}

int get_portmux_port(void)
{
    return portmux_port;
}

void set_portmux_port(int port)
{
    portmux_port = port;
}

char *get_portmux_bind_path(void)
{
    return portmux_unix_socket;
}

int set_portmux_bind_path(const char *path)
{
    path = (path) ? path : PORTMUX_UNIX_SOCKET_DEFAULT_PATH;
    if (strlen(path) < sizeof(((struct sockaddr_un *)0)->sun_path)) {
        strncpy0(portmux_unix_socket, path, sizeof(portmux_unix_socket));
        return 0;
    }
    logmsg(LOGMSG_ERROR, "%s bad unix socket path", __func__);
    return -1;
}

int portmux_hello(char *host, char *name, int *fdout)
{
    struct in_addr addr;
    int port;
    int fd;
    int rc;

    if (fdout) *fdout = -1;

    rc = tcpresolve(host, &addr, &port);
    if (rc) return rc;

    fd = tcpconnect_to(addr, get_portmux_port(), 0, 5000);
    if (fd == -1) return 1;
    portmux_denagle(fd);

    SBUF2 *sb = sbuf2open(fd, SBUF2_WRITE_LINE | SBUF2_NO_CLOSE_FD);
    sbuf2printf(sb, "hello %s\n", name);
    rc = sbuf2flush(sb);
    if (rc < 0) return 2;
    char line[10];
    /* The reponse is always ok. We read it to make sure
     * pmux had a chance to register us. */
    sbuf2gets(line, sizeof(line), sb);
    sbuf2close(sb);
    if (fdout) *fdout = fd;
    return 0;
}

void portmux_register_reconnect_callback(int (*callback)(void *), void *arg)
{
    reconnect_callback = callback;
    reconnect_callback_arg = arg;
}

#ifdef PORTMUXUSR_TESTSUITE
/* here's makefile
# MAKEFILE FOR portmuxusr_test.tsk generated by pcomp

TASK=portmuxusr_test.tsk
IS_CMAIN=true
IS_PTHREAD=true
IS_GCC_WARNINGS_CLEAN=yes

USER_CFLAGS+=-I. -DPORTMUXUSR_TESTSUITE -DDEBUG
OBJS= portmuxusr.o
LIBS=-ldbutil -lbbipc -lsysutil

MKINCL?=/bbsrc/mkincludes/
include ${MKINCL}machdep.newlink
include ${MKINCL}linktask.newlink

 */
#include <pthread.h>
static pthread_attr_t attr;
static char host[128] = "localhost";

static void client_thr(int fd)
{
    SBUF2 *sb;
    char line[128];

    struct sockaddr_in client_addr;
    socklen_t len = sizeof(client_addr);
    if (getpeername(fd, (struct sockaddr *)&client_addr, &len) == 0) {
        unsigned char *cptr = (unsigned char *)&(client_addr.sin_addr.s_addr);
        printf("server: accepted connection on fd# %d from %u.%u.%u.%u\n", fd,
               cptr[0], cptr[1], cptr[2], cptr[3]);
    } else {
        printf("server: accepted connection on fd# %d (No client address "
               "available)\n",
               fd);
    }

    sb = sbuf2open(fd, 0);
    if (sb == NULL) {
        fprintf(stderr, "sbuf2open: errno=[%d] %s\n", errno, strerror(errno));
        close(fd);
        return;
    }

    while (sbuf2gets(line, sizeof(line), sb) > 0) {
        printf("server(fd#%d): %s", fd, line);

        /* I know, first client to say "exit" will shutdown server */
        if (!strncmp(line, "exit", 4)) {
            exit(0);
        }
    }

    sbuf2close(sb);
}

void server_accept_hndl(int fd, void *userdata)
{
    pthread_t tid;
    int rc;

    // ok, spawn a client thread and go wait for somebody else
    rc = pthread_create(&tid, &attr, (void *(*)(void *))client_thr, (void *)fd);
    if (rc) {
        fprintf(stderr, "accept_hndl:pthread_create(): errno=[%d] %s\n", rc,
                strerror(rc));
        close(fd); // close fd
    }
}

static void server(char *server_name, int pure_one_port_mode)
{
    Pthread_attr_init(&attr);
    Pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    char *app = server_name;
    char *service = server_name;
    char *instance = server_name;
    char *idstr = NULL;

    if (strchr(server_name, '/') != NULL) {
        idstr = strdup(server_name);
        app = idstr;
        char *pos = strchr(app, '/');
        *pos++ = '\0';
        service = pos;
        if ((pos = strchr(service, '/')) != NULL) {
            *pos++ = '\0';
            instance = pos;
        }
    }

    uint32_t options = pure_one_port_mode ? PORTMUX_PORT_SUPPRESS : 0;
    printf("I am %s, ready to accept connections to %s/%s/%s\n", __func__, app,
           service, instance);

    portmux_listen_options(app, service, instance, server_accept_hndl, NULL,
                           options);
    fprintf(stderr, "portmux_listen: errno=[%d] %s\n", errno, strerror(errno));

    exit(1);
}

static void server_no_callback(char *server_name, int pure_one_port_mode)
{
    Pthread_attr_init(&attr);
    Pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    char *app = server_name;
    char *service = server_name;
    char *instance = server_name;
    char *idstr = NULL;

    if (strchr(server_name, '/') != NULL) {
        idstr = strdup(server_name);
        app = idstr;
        char *pos = strchr(app, '/');
        *pos++ = '\0';
        service = pos;
        if ((pos = strchr(service, '/')) != NULL) {
            *pos++ = '\0';
            instance = pos;
        }
    }

    uint32_t options = pure_one_port_mode ? PORTMUX_PORT_SUPPRESS : 0;

    portmux_fd_t *pmux_hndl;

    pmux_hndl = portmux_listen_options_setup(app, service, instance, options);
    if (pmux_hndl == NULL) {
        fprintf(stderr, "portmux_listen_setup: errno=[%d] %s\n", errno,
                strerror(errno));
    } else {
        printf("I am %s, ready to accept connections to %s/%s/%s\n", __func__,
               app, service, instance);

        while (true) {
            int clientfd = portmux_accept(pmux_hndl, 0, NULL);
            if (clientfd < 0) {
                fprintf(stderr, "error in portmux_accept\n");
                break;
            }
            server_accept_hndl(clientfd, NULL);
        }
        portmux_close(pmux_hndl);
    }
    exit(1);
}

static void client(char *server_name)
{
    int fd;
    char line[128];
    int len;
    int rc;

    char *app = server_name;
    char *service = server_name;
    char *instance = server_name;
    char *idstr = NULL;

    if (strchr(server_name, '/') != NULL) {
        idstr = strdup(server_name);
        app = idstr;
        char *pos = strchr(app, '/');
        *pos++ = '\0';
        service = pos;
        if ((pos = strchr(service, '/')) != NULL) {
            *pos++ = '\0';
            instance = pos;
        }
    }

    printf("Connecting to '%s:%s/%s/%s'\n", host, app, service, instance);

    fd = portmux_connect(host, app, service, instance);

    if (fd < 0) {
        fprintf(stderr, "portmux_connect: errno=[%d] %s\n", errno,
                strerror(errno));
        exit(1);
    }

    printf("I am %s, ready to send.\n", __func__);
    if (idstr != NULL) {
        free(idstr);
    }
    while (fgets(line, sizeof(line), stdin)) {
        printf("client: %s", line);
        len = strlen(line);
        rc = tcpwritemsg(fd, line, len, 0);
        if (rc != len) {
            fprintf(stderr, "tcpwritemsg: errno=[%d] %s\n", errno,
                    strerror(errno));
            exit(1);
        }

        if (!strncmp(line, "exit", 4)) {
            exit(0);
        }
    }

    close(fd);
}

static void alt_client(char *server_name)
{
    int fd;
    char line[128];
    int len;
    int rc;

    char *app = server_name;
    char *service = server_name;
    char *instance = server_name;
    char *idstr = NULL;

    if (strchr(server_name, '/') != NULL) {
        idstr = strdup(server_name);
        app = idstr;
        char *pos = strchr(app, '/');
        *pos++ = '\0';
        service = pos;
        if ((pos = strchr(service, '/')) != NULL) {
            *pos++ = '\0';
            instance = pos;
        }
    }
    printf("Connecting to '%s:%s/%s/%s'\n", host, app, service, instance);

    int port = portmux_get(host, app, service, instance);
    if (port <= 0) {
        fprintf(stderr, "portmux_get: errno=[%d] %s\n", errno, strerror(errno));
        exit(1);
    }

    fd = tcpconnecth(host, port, 0);
    if (fd < 0) {
        fprintf(stderr, "tcpconnecth: errno=[%d] %s\n", errno, strerror(errno));
        exit(1);
    }

    portmux_denagle(fd);
    if (idstr != NULL) {
        free(idstr);
    }

    printf("I am %s, ready to send.\n", __func__);
    while (fgets(line, sizeof(line), stdin)) {
        printf("client: %s", line);
        len = strlen(line);
        rc = tcpwritemsg(fd, line, len, 0);
        if (rc != len) {
            fprintf(stderr, "tcpwritemsg: errno=[%d] %s\n", errno,
                    strerror(errno));
            exit(1);
        }

        if (!strncmp(line, "exit", 4)) {
            exit(0);
        }
    }

    close(fd);
}

#define V_SIZE 10
static portmux_fd_t *portmux_hndl[V_SIZE];

int server_accept_hndl_v(int which, int fd, void *userdata)
{
    pthread_t tid;
    int rc;

    portmux_fd_t *fds = portmux_hndl[which];
    printf("I have accepted a connection on fd %d for portmux_hndl %d "
           "(%s/%s/%s)\n",
           fd, which, fds->app, fds->service, fds->instance);
    // ok, spawn a client thread and go wait for somebody else
    rc = pthread_create(&tid, &attr, (void *(*)(void *))client_thr, (void *)fd);
    if (rc) {
        fprintf(stderr, "accept_hndl:pthread_create(): errno=[%d] %s\n", rc,
                strerror(rc));
        close(fd); // close fd
    }
    return 0;
}

static void server_v(char *server_name, int timeoutms, int pure_one_port_mode)
{
    Pthread_attr_init(&attr);
    Pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    uint32_t options;

    options = pure_one_port_mode ? PORTMUX_PORT_SUPPRESS : 0;
    for (int ii = 0; ii < V_SIZE; ++ii) {
        char instance[8];

        snprintf(instance, sizeof(instance), "%d", ii);
        portmux_hndl[ii] = portmux_listen_options_setup(
            server_name, server_name, instance, options);
        if (portmux_hndl[ii] == NULL) {
            fprintf(stderr, "portmux_listen_options_setup returned NULL\n");
            exit(1);
        }
    }

    printf("I am %s, ready to accept for:\n", __func__);
    for (int ii = 0; ii < V_SIZE; ++ii) {
        printf("%s/%s/%s ", portmux_hndl[ii]->app, portmux_hndl[ii]->service,
               portmux_hndl[ii]->instance);
        if ((options & PORTMUX_PORT_SUPPRESS) == 0) {
            printf("on TCP port %d\n", portmux_hndl[ii]->port);
        } else {
            printf("(no TCP port requested)\n");
        }
    }

    if (timeoutms <= 0) {
        timeoutms = -1;
    }

    int result;
    while ((result = portmux_acceptv(portmux_hndl, V_SIZE, timeoutms,
                                     server_accept_hndl_v, NULL, NULL)) >= 0) {
        if (result == 0 && errno == ETIMEDOUT) {
            printf("portmux_acceptv timeout. Continuing\n");
        } else {
            printf("portmux_acceptv returned %d. Continuing.\n", result);
        }
    }
    fprintf(stderr, "portmux_listen: errno=[%d] %s\n", errno, strerror(errno));

    exit(1);
}

static const char *usage_text[] = {
    "Usage: -[bh] -[sum] [-a r<host>] <name>", "-h: help",
    "-s: specify server mode.", "-u: don't request a TCP port number, use 1 "
                                "port mode for communication. (server only)",
    "-m: listen on multiple instances. (server only)",
    "-a: use deprecated API calls.",
    "-r: specify <host> to connect to. (client only)", "-t: specify timeoutms",
    "\nTo use a specific app/service/instance give this as <name>.", NULL};

static void usage(FILE *fh)
{
    int ii;
    for (ii = 0; usage_text[ii]; ii++) {
        fprintf(fh, "%s\n", usage_text[ii]);
    }
}

int main(int argc, char *argv[])
{
    extern char *optarg;
    extern int optind, optopt;
    int server_mode = 0;
    int alt_mode = 0;
    int multi_mode = 0;
    int pure_one_port_mode = 0;
    int timeoutms = portmux_default_timeout;

    int c;

    while ((c = getopt(argc, argv, "ahsr:umt:")) != EOF) {
        switch (c) {
        case 'a':
            alt_mode = 1;
            break;

        case 'h':
            usage(stdout);
            exit(0);
            break;

        case 'r':
            if (strlen(optarg) >= sizeof(host)) {
                fprintf(stderr, "Hostname too long: %s\n", optarg);
                exit(2);
            }
            strnpy(host, optarg, sizeof(host));
            break;

        case 's':
            server_mode = 1;
            break;

        case 'u':
            pure_one_port_mode = 1;
            break;

        case 'm':
            multi_mode = 1;
            break;

        case 't':
            timeoutms = atoi(optarg);
            break;

        case '?':
            fprintf(stderr, "Unrecognised option: -%c\n", optopt);
            usage(stderr);
            exit(2);
        }
    }

    argc -= optind;
    argv += optind;

    if (argc < 1) {
        usage(stdout);
        exit(0);
    }

    if (set_portmux_bind_path(NULL)) {
        exit(1);
    }

    if (server_mode) {
        if (multi_mode) {
            server_v(argv[0], timeoutms, pure_one_port_mode);
        } else if (alt_mode) {
            server_no_callback(argv[0], pure_one_port_mode);
        } else {
            server(argv[0], pure_one_port_mode);
        }
    } else {
        printf("will try to connect to portmux@%s\n", host);
        if (alt_mode) {
            alt_client(argv[0]);
        } else {
            client(argv[0]);
        }
    }
    return 0;
}

#endif
