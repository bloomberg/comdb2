/*
   Copyright 2015, 2017, Bloomberg Finance L.P.

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

#include <sbuf2.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <sys/time.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <limits.h>

#include "cdb2api.h"

#include "sqlquery.pb-c.h"
#include "sqlresponse.pb-c.h"

/*
*******************************************************************************
** WARNING: If you add any internal configuration state to this file, please
**          update the reset_the_configuration() function as well to include
**          it.
*******************************************************************************
*/

#define SOCKPOOL_SOCKET_NAME "/tmp/sockpool.socket"
#define COMDB2DB "comdb2db"
#define COMDB2DB_NUM 32432

#define CDB2DBCONFIG_NOBBENV_DEFAULT "/opt/bb/etc/cdb2/config/comdb2db.cfg"
static char CDB2DBCONFIG_NOBBENV[512] = CDB2DBCONFIG_NOBBENV_DEFAULT;

/* The real path is COMDB2_ROOT + CDB2DBCONFIG_NOBBENV_PATH  */
#define CDB2DBCONFIG_NOBBENV_PATH_DEFAULT "/etc/cdb2/config.d/"
static char CDB2DBCONFIG_NOBBENV_PATH[] = CDB2DBCONFIG_NOBBENV_PATH_DEFAULT; /* READ-ONLY */

#define CDB2DBCONFIG_TEMP_BB_BIN_DEFAULT "/bb/bin/comdb2db.cfg"
static char CDB2DBCONFIG_TEMP_BB_BIN[512] = "/bb/bin/comdb2db.cfg";

static char *CDB2DBCONFIG_BUF = NULL;

static char cdb2_default_cluster[64] = "";
static char cdb2_comdb2dbname[32] = "";
static char cdb2_dnssuffix[255] = "";
static char cdb2_machine_room[16] = "";

#define CDB2_PORTMUXPORT_DEFAULT 5105
static int CDB2_PORTMUXPORT = CDB2_PORTMUXPORT_DEFAULT;

#define MAX_RETRIES_DEFAULT 21
static int MAX_RETRIES = MAX_RETRIES_DEFAULT; /* We are looping each node twice. */

#define MIN_RETRIES_DEFAULT 3
static int MIN_RETRIES = MIN_RETRIES_DEFAULT;

#define CDB2_CONNECT_TIMEOUT_DEFAULT 100
static int CDB2_CONNECT_TIMEOUT = CDB2_CONNECT_TIMEOUT_DEFAULT;

#define CDB2_AUTO_CONSUME_TIMEOUT_MS_DEFAULT 2
static int CDB2_AUTO_CONSUME_TIMEOUT_MS = CDB2_AUTO_CONSUME_TIMEOUT_MS_DEFAULT;

#define COMDB2DB_TIMEOUT_DEFAULT 500
static int COMDB2DB_TIMEOUT = COMDB2DB_TIMEOUT_DEFAULT;

#define CDB2_TCPBUFSZ_DEFAULT 0
static int cdb2_tcpbufsz = CDB2_TCPBUFSZ_DEFAULT;

#ifndef WITH_SSL
#  define WITH_SSL 1
#endif

#if WITH_SSL
static ssl_mode cdb2_c_ssl_mode = SSL_ALLOW;

static char cdb2_sslcertpath[PATH_MAX];
static char cdb2_sslcert[PATH_MAX];
static char cdb2_sslkey[PATH_MAX];
static char cdb2_sslca[PATH_MAX];
#if HAVE_CRL
static char cdb2_sslcrl[PATH_MAX];
#endif

#ifdef NID_host /* available as of RFC 4524 */
#define CDB2_NID_DBNAME_DEFAULT NID_host
#else
#define CDB2_NID_DBNAME_DEFAULT NID_commonName
#endif
int cdb2_nid_dbname = CDB2_NID_DBNAME_DEFAULT;

#define CDB2_CACHE_SSL_SESS_DEFAULT 0
static int cdb2_cache_ssl_sess = CDB2_CACHE_SSL_SESS_DEFAULT;

static pthread_mutex_t cdb2_ssl_sess_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct cdb2_ssl_sess_list cdb2_ssl_sess_list;
static void cdb2_free_ssl_sessions(cdb2_ssl_sess_list *sessions);
static cdb2_ssl_sess_list *cdb2_get_ssl_sessions(cdb2_hndl_tp *hndl);
static int cdb2_set_ssl_sessions(cdb2_hndl_tp *hndl,
                                 cdb2_ssl_sess_list *sessions);
#endif

#define CDB2_ALLOW_PMUX_ROUTE_DEFAULT 0
static int cdb2_allow_pmux_route = CDB2_ALLOW_PMUX_ROUTE_DEFAULT;

static int _PID; /* ONE-TIME */
static int _MACHINE_ID; /* ONE-TIME */
static char *_ARGV0; /* ONE-TIME */

#define DB_TZNAME_DEFAULT "America/New_York"

#define MAX_NODES 128
#define MAX_CONTEXTS 10 /* Maximum stack size for storing context messages */
#define MAX_CONTEXT_LEN 100 /* Maximum allowed length of a context message */

#define MAX_STACK 512 /* Size of call-stack which opened the handle */

pthread_mutex_t cdb2_sockpool_mutex = PTHREAD_MUTEX_INITIALIZER;

#include <netdb.h>

static pthread_once_t init_once = PTHREAD_ONCE_INIT;
static int log_calls = 0; /* ONE-TIME */

static void reset_sockpool(void);

/*
** NOTE: This function is designed to reset the internal state of this module,
**       related to the configuration, back to initial defaults.  It should
**       allow for the subsequent reconfiguration using different parameters.
**       Currently, it is surfaced via passing a NULL value to the public APIs
**       cdb2_set_comdb2db_config() and cdb2_set_comdb2db_info().
*/
static void reset_the_configuration(void)
{
    if (log_calls)
        fprintf(stderr, "%p> %s()\n", (void *)pthread_self(), __func__);

    memset(CDB2DBCONFIG_NOBBENV, 0, sizeof(CDB2DBCONFIG_NOBBENV));
    strncpy(CDB2DBCONFIG_NOBBENV, CDB2DBCONFIG_NOBBENV_DEFAULT, 511);

    memset(CDB2DBCONFIG_TEMP_BB_BIN, 0, sizeof(CDB2DBCONFIG_TEMP_BB_BIN));
    strncpy(CDB2DBCONFIG_TEMP_BB_BIN, CDB2DBCONFIG_TEMP_BB_BIN_DEFAULT, 511);

    if (CDB2DBCONFIG_BUF != NULL) {
        free(CDB2DBCONFIG_BUF);
        CDB2DBCONFIG_BUF = NULL;
    }

    memset(cdb2_default_cluster, 0, sizeof(cdb2_default_cluster));
    memset(cdb2_comdb2dbname, 0, sizeof(cdb2_comdb2dbname));
    memset(cdb2_dnssuffix, 0, sizeof(cdb2_dnssuffix));
    memset(cdb2_machine_room, 0, sizeof(cdb2_machine_room));

    CDB2_PORTMUXPORT = CDB2_PORTMUXPORT_DEFAULT;
    MAX_RETRIES = MAX_RETRIES_DEFAULT;
    MIN_RETRIES = MIN_RETRIES_DEFAULT;
    CDB2_CONNECT_TIMEOUT = CDB2_CONNECT_TIMEOUT_DEFAULT;
    CDB2_AUTO_CONSUME_TIMEOUT_MS = CDB2_AUTO_CONSUME_TIMEOUT_MS_DEFAULT;
    COMDB2DB_TIMEOUT = COMDB2DB_TIMEOUT_DEFAULT;
    cdb2_tcpbufsz = CDB2_TCPBUFSZ_DEFAULT;

    cdb2_allow_pmux_route = CDB2_ALLOW_PMUX_ROUTE_DEFAULT;

#if WITH_SSL
    cdb2_c_ssl_mode = SSL_ALLOW;

    memset(cdb2_sslcertpath, 0, sizeof(cdb2_sslcertpath));
    memset(cdb2_sslcert, 0, sizeof(cdb2_sslcert));
    memset(cdb2_sslkey, 0, sizeof(cdb2_sslkey));
    memset(cdb2_sslca, 0, sizeof(cdb2_sslca));
    memset(cdb2_sslcrl, 0, sizeof(cdb2_sslcrl));

    cdb2_nid_dbname = CDB2_NID_DBNAME_DEFAULT;
    cdb2_cache_ssl_sess = CDB2_CACHE_SSL_SESS_DEFAULT;
#endif

    reset_sockpool();
}

#if defined(__APPLE__)
#include <libproc.h>

static char *apple_getargv0(void)
{
    static char argv0[PATH_MAX];
    int ret = proc_pidpath(_PID, argv0, sizeof(argv0));
    if (ret <= 0) {
        fprintf(stderr, "%s proc_pidpath returns %d\n", __func__, ret);
        return NULL;
    }
    return argv0;
}
#endif

#if defined(_SUN_SOURCE) || defined(_LINUX_SOURCE)

static char *proc_cmdline_getargv0(void)
{
    char procname[64];
    static char argv0[PATH_MAX];

    snprintf(procname, sizeof(procname), "/proc/self/cmdline");
    FILE *f = fopen(procname, "r");
    if (f == NULL) {
        fprintf(stderr, "%s cannot open %s, %s\n", __func__, procname,
                strerror(errno));
        return NULL;
    }

    if (fgets(argv0, PATH_MAX, f) == NULL) {
        fprintf(stderr, "%s error reading from %s, %s\n", __func__, procname,
                strerror(errno));
        fclose(f);
        return NULL;
    }

    fclose(f);
    return argv0;
}
#endif

#if defined(_IBM_SOURCE)

#include <sys/procfs.h>
#include <procinfo.h>

static char *ibm_getargv0(void)
{
    struct procsinfo p;
    static char argv0[PATH_MAX];
    pid_t idx = _PID;
    int rc;

    if (1 == (rc = getprocs(&p, sizeof(p), NULL, 0, &idx, 1)) &&
        _PID == p.pi_pid) {
        strncpy(argv0, p.pi_comm, PATH_MAX);
        argv0[PATH_MAX - 1] = '\0';
    } else {
        fprintf(stderr, "%s getprocs returns %d for pid %d\n", __func__, _PID);
        return NULL;
    }

    return argv0;
}
#endif

#define SQLCACHEHINT "/*+ RUNCOMDB2SQL "
#define SQLCACHEHINTLENGTH 17

static inline const char *cdb2_skipws(const char *str)
{
    while (*str && isspace(*str))
        str++;
    return str;
}

static char *getargv0(void)
{
#if defined(__APPLE__)
    return apple_getargv0();
#elif defined(_LINUX_SOURCE) || defined(_SUN_SOURCE)
    return proc_cmdline_getargv0();
#elif defined(_IBM_SOURCE)
    return ibm_getargv0();
#else
    fprintf(stderr, "%s unsupported architecture\n", __func__);
    return NULL;
#endif
}

static void do_init_once(void)
{
    char *do_log = getenv("CDB2_LOG_CALLS");
    if (do_log)
        log_calls = 1;
    char *config = getenv("CDB2_CONFIG_FILE");
    if (config) {
        /* can't call back cdb2_set_comdb2db_config from do_init_once */
        strncpy(CDB2DBCONFIG_NOBBENV, config, 511);
    }
    _PID = getpid();
    _MACHINE_ID = gethostid();
    _ARGV0 = getargv0();
}

/* if sqlstr is a read stmt will return 1 otherwise return 0
 * returns -1 if sqlstr is null
 */
static int is_sql_read(const char *sqlstr)
{
    const char get[] = "GET";
    const char sp_exec[] = "EXEC";
    const char with[] = "WITH";
    const char sel[] = "SELECT";
    const char explain[] = "EXPLAIN";

    if (sqlstr == NULL)
        return -1;
    sqlstr = cdb2_skipws(sqlstr);
    int slen = strlen(sqlstr);
    if (slen) {
        if (slen < sizeof(get) - 1)
            return 0;
        if (!strncasecmp(sqlstr, get, sizeof(get) - 1))
            return 1;
        if (slen < sizeof(sp_exec) - 1)
            return 0;
        if (!strncasecmp(sqlstr, sp_exec, sizeof(sp_exec) - 1))
            return 1;
        if (!strncasecmp(sqlstr, with, sizeof(with) - 1))
            return 1;
        if (slen < sizeof(sel) - 1)
            return 0;
        if (!strncasecmp(sqlstr, sel, sizeof(sel) - 1))
            return 1;
        if (slen < sizeof(explain) - 1)
            return 0;
        if (!strncasecmp(sqlstr, explain, sizeof(explain) - 1))
            return 1;
    }
    return 0;
}

/* PASSFD CODE */
#if defined(_IBM_SOURCE) || defined(_LINUX_SOURCE)
#define HAVE_MSGHDR_MSG_CONTROL
#endif

enum {
    PASSFD_SUCCESS = 0,
    PASSFD_RECVMSG = -1, /* error with recvmsg() */
    PASSFD_EOF = -2,     /* eof before message completely read */
    PASSFD_2FDS = -3,    /* received more than one file descriptor */
    PASSFD_BADCTRL = -4, /* received bad control message */
    PASSFD_TIMEOUT = -5, /* timed out */
    PASSFD_POLL = -6,    /* error with poll() */
    PASSFD_SENDMSG = -7  /* error with sendmsg() */
};

static int recv_fd_int(int sockfd, void *data, size_t nbytes, int *fd_recvd)
{
    ssize_t rc;
    size_t bytesleft;
    char *cdata;
    struct msghdr msg;
    struct iovec iov[1];
    int recvfd;
#ifdef HAVE_MSGHDR_MSG_CONTROL
    union {
        struct cmsghdr cm;
        char control[CMSG_SPACE(sizeof(int))];
    } control_un;
    struct cmsghdr *cmsgptr;
#endif

    *fd_recvd = -1;
    cdata = data;
    bytesleft = nbytes;

    while (bytesleft > 0) {
#ifdef HAVE_MSGHDR_MSG_CONTROL
        msg.msg_control = control_un.control;
        msg.msg_controllen = sizeof(control_un.control);
        msg.msg_flags = 0;
#else
        msg.msg_accrights = (caddr_t)&recvfd;
        msg.msg_accrightslen = sizeof(int);
#endif
        msg.msg_name = NULL;
        msg.msg_namelen = 0;
        msg.msg_iov = iov;
        msg.msg_iovlen = 1;
        iov[0].iov_base = cdata;
        iov[0].iov_len = bytesleft;

        rc = recvmsg(sockfd, &msg, 0);

        if (rc == -1) {
            if (errno == EINTR || errno == EAGAIN)
                continue;
            return PASSFD_RECVMSG;
        }

        if (rc == 0) {
            /* Premature eof */
            return PASSFD_EOF;
        }

        cdata += rc;
        bytesleft -= rc;

/* See if we got a descriptor with this message */
#ifdef HAVE_MSGHDR_MSG_CONTROL
        cmsgptr = CMSG_FIRSTHDR(&msg);
        if (cmsgptr) {
            if (cmsgptr->cmsg_len != CMSG_LEN(sizeof(int)) ||
                cmsgptr->cmsg_level != SOL_SOCKET ||
                cmsgptr->cmsg_type != SCM_RIGHTS) {
                return PASSFD_BADCTRL;
            }
            recvfd = *((int *)CMSG_DATA(cmsgptr));
            if (*fd_recvd != -1) {
                if (close(recvfd) == -1) {
                    fprintf(stderr, "%s: error closing second fd %d: %d %s\n",
                            __func__, recvfd, errno, strerror(errno));
                }
                return PASSFD_2FDS;
            }
            *fd_recvd = recvfd;

            if (CMSG_NXTHDR(&msg, cmsgptr)) {
                return PASSFD_BADCTRL;
            }
        }
#else
        if (msg.msg_accrightslen == sizeof(int)) {
            if (*fd_recvd != -1) {
                if (close(recvfd) == -1) {
                    fprintf(stderr, "%s: error closing second fd %d: %d %s\n",
                            __func__, recvfd, errno, strerror(errno));
                }
                return PASSFD_2FDS;
            }
            *fd_recvd = recvfd;
        }
#endif
    }

    return PASSFD_SUCCESS;
}

/* This wrapper ensures that on error we close any file descriptor that we
 * may have received before the error occured.  Alse we make sure that we
 * preserve the value of errno which may be needed if the error was
 * PASSFD_RECVMSG. */
static int recv_fd(int sockfd, void *data, size_t nbytes, int *fd_recvd)
{
    int rc;
    rc = recv_fd_int(sockfd, data, nbytes, fd_recvd);
    if (rc != 0 && *fd_recvd != -1) {
        int errno_save = errno;
        if (close(*fd_recvd) == -1) {
            fprintf(stderr, "%s: close(%d) error: %d %s\n", __func__, *fd_recvd,
                    errno, strerror(errno));
        }
        *fd_recvd = -1;
        errno = errno_save;
    }
    return rc;
}

static int send_fd_to(int sockfd, const void *data, size_t nbytes,
                      int fd_to_send, int timeoutms)
{
    ssize_t rc;
    size_t bytesleft;
    struct msghdr msg;
    struct iovec iov[1];
    const char *cdata;
#ifdef HAVE_MSGHDR_MSG_CONTROL
    union {
        struct cmsghdr cm;
        char control[CMSG_SPACE(sizeof(int))];
    } control_un;
    struct cmsghdr *cmsgptr;
#endif

    bytesleft = nbytes;
    cdata = data;

    while (bytesleft > 0) {
        if (timeoutms > 0) {
            struct pollfd pol;
            int pollrc;
            pol.fd = sockfd;
            pol.events = POLLOUT;
            pollrc = poll(&pol, 1, timeoutms);
            if (pollrc == 0) {
                return PASSFD_TIMEOUT;
            } else if (pollrc == -1) {
                /* error will be in errno */
                return PASSFD_POLL;
            }
        }

        if (fd_to_send != -1) {
#ifdef HAVE_MSGHDR_MSG_CONTROL
            msg.msg_control = control_un.control;
            msg.msg_controllen = sizeof(control_un.control);
            msg.msg_flags = 0;
            cmsgptr = CMSG_FIRSTHDR(&msg);
            cmsgptr->cmsg_len = CMSG_LEN(sizeof(int));
            cmsgptr->cmsg_level = SOL_SOCKET;
            cmsgptr->cmsg_type = SCM_RIGHTS;
            *((int *)CMSG_DATA(cmsgptr)) = fd_to_send;
#else
            msg.msg_accrights = (caddr_t)&fd_to_send;
            msg.msg_accrightslen = sizeof(int);
#endif
        } else {
#ifdef HAVE_MSGHDR_MSG_CONTROL
            msg.msg_control = NULL;
            msg.msg_controllen = 0;
            msg.msg_flags = 0;
#else
            msg.msg_accrights = NULL;
            msg.msg_accrightslen = 0;
#endif
        }
        msg.msg_name = NULL;
        msg.msg_namelen = 0;
        msg.msg_iov = iov;
        msg.msg_iovlen = 1;
        iov[0].iov_base = (caddr_t)cdata;
        iov[0].iov_len = bytesleft;

        rc = sendmsg(sockfd, &msg, 0);
        if (rc == -1) {
            if (errno == EINTR || errno == EAGAIN)
                continue;
            return PASSFD_SENDMSG;
        }

        if (rc == 0) {
            return PASSFD_EOF;
        }

        /* We didn't get an error, so the fd must have been sent. */
        fd_to_send = -1;

        cdata += rc;
        bytesleft -= rc;
    }

    return PASSFD_SUCCESS;
}

static int send_fd(int sockfd, const void *data, size_t nbytes, int fd_to_send)
{
    return send_fd_to(sockfd, data, nbytes, fd_to_send, 0);
}

static int cdb2_tcpresolve(const char *host, struct in_addr *in, int *port)
{
    /*RESOLVE AN ADDRESS*/
    in_addr_t inaddr;

    int len;
    char tmp[8192];
    int tmplen = 8192;
    int herr;
    struct hostent hostbuf, *hp = NULL;
    char tok[128], *cc;
    cc = strchr(host, (int)':');
    if (cc == 0) {
        len = strlen(host);
        if (len >= sizeof(tok))
            return -2;
        memcpy(tok, host, len);
        tok[len] = 0;
    } else {
        *port = atoi(cc + 1);
        len = (int)(cc - host);
        if (len >= sizeof(tok))
            return -2;
        memcpy(tok, host, len);
        tok[len] = 0;
    }
    if ((inaddr = inet_addr(tok)) != (in_addr_t)-1) {
        /* it's dotted-decimal */
        memcpy(&in->s_addr, &inaddr, sizeof(inaddr));
    } else {
#ifdef __APPLE__
        hp = gethostbyname(tok);
#elif _LINUX_SOURCE
        gethostbyname_r(tok, &hostbuf, tmp, tmplen, &hp, &herr);
#elif _SUN_SOURCE
        hp = gethostbyname_r(tok, &hostbuf, tmp, tmplen, &herr);
#else
        hp = gethostbyname(tok);
#endif
        if (hp == NULL) {
            fprintf(stderr, "%s:gethostbyname(%s): errno=%d err=%s\n", __func__,
                    tok, errno, strerror(errno));
            return -1;
        }
        memcpy(&in->s_addr, hp->h_addr, hp->h_length);
    }
    return 0;
}

#include <fcntl.h>
static int lclconn(int s, const struct sockaddr *name, int namelen,
                   int timeoutms)
{
    /* connect with timeout */
    struct pollfd pfd;
    int flags, rc;
    int err;
    socklen_t len;
    if (timeoutms <= 0)
        return connect(s, name, namelen); /*no timeout specified*/
    flags = fcntl(s, F_GETFL, 0);
    if (flags < 0)
        return -1;
    if (fcntl(s, F_SETFL, flags | O_NONBLOCK) < 0) {
        return -1;
    }

    rc = connect(s, name, namelen);
    if (rc == -1 && errno == EINPROGRESS) {
        /*wait for connect event */
        pfd.fd = s;
        pfd.events = POLLOUT;
        rc = poll(&pfd, 1, timeoutms);
        if (rc == 0) {
            /*timeout*/
            /*fprintf(stderr,"connect timed out\n");*/
            return -2;
        }
        if (rc != 1) { /*poll failed?*/
            return -1;
        }
        if ((pfd.revents & POLLOUT) == 0) { /*wrong event*/
            /*fprintf(stderr,"poll event %d\n",pfd.revents);*/
            return -1;
        }
    } else if (rc == -1) {
        /*connect failed?*/
        return -1;
    }
    if (fcntl(s, F_SETFL, flags) < 0) {
        return -1;
    }
    len = sizeof(err);
    if (getsockopt(s, SOL_SOCKET, SO_ERROR, &err, &len)) {
        return -1;
    }
    errno = err;
    if (errno != 0)
        return -1;
    return 0;
}

static int cdb2_do_tcpconnect(struct in_addr in, int port, int myport,
                              int timeoutms)
{
    int sockfd, rc;
    int sendbuff;
    struct sockaddr_in tcp_srv_addr; /* server's Internet socket addr */
    struct sockaddr_in my_addr;      /* my Internet address */
    bzero((char *)&tcp_srv_addr, sizeof tcp_srv_addr);
    tcp_srv_addr.sin_family = AF_INET;
    if (port <= 0) {
        return -1;
    }
    tcp_srv_addr.sin_port = htons(port);
    memcpy(&tcp_srv_addr.sin_addr, &in.s_addr, sizeof(in.s_addr));
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        fprintf(stderr, "tcpconnect_to: can't create TCP socket\n");
        return -1;
    }
#if 0
 	/* Allow connect port to be re-used */
 	sendbuff = 1;		/* enable option */
 	if (setsockopt( sockfd, SOL_SOCKET, SO_REUSEADDR, (char *)&sendbuff,
 	sizeof sendbuff ) < 0)
 	{
 		fprintf(stderr,"tcpconnect_to: setsockopt failure\n" );
 		close( sockfd );
 		return -1;
 	}
#endif
    sendbuff = 1; /* enable option */
    if (setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char *)&sendbuff,
                   sizeof sendbuff) < 0) {
        fprintf(stderr, "tcpconnect_to: setsockopt failure\n");
        close(sockfd);
        return -1;
    }
    struct linger ling;
    ling.l_onoff = 1;
    ling.l_linger = 0;
    if (setsockopt(sockfd, SOL_SOCKET, SO_LINGER, (char *)&ling, sizeof(ling)) <
        0) {
        fprintf(stderr, "tcpconnect_to: setsockopt failure:%s",
                strerror(errno));
        close(sockfd);
        return -1;
    }

    if (cdb2_tcpbufsz) {
        int tcpbufsz = cdb2_tcpbufsz;
        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, (char *)&tcpbufsz,
                       sizeof(tcpbufsz)) < 0) {
            fprintf(stderr, "tcpconnect_to: setsockopt failure:%s",
                    strerror(errno));
            close(sockfd);
            return -1;
        }
    }

    if (myport > 0) { /* want to use specific port on local host */
        bzero((char *)&my_addr, sizeof my_addr);
        my_addr.sin_family = AF_INET;
        my_addr.sin_addr.s_addr = INADDR_ANY;
        my_addr.sin_port = htons((u_short)myport);
        if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof my_addr) < 0) {
            fprintf(stderr, "tcpconnect_to: bind failed on local port %d: %s",
                    myport, strerror(errno));
            close(sockfd);
            return -1;
        }
    }
    /* Connect to the server.  */
    rc = lclconn(sockfd, (struct sockaddr *)&tcp_srv_addr, sizeof(tcp_srv_addr),
                 timeoutms);

    if (rc < 0) {
        close(sockfd);
        return rc;
    }
    return (sockfd); /* all OK */
}

static int cdb2_tcpconnecth_to(const char *host, int port, int myport,
                               int timeoutms)
{
    int rc;
    struct in_addr in;
    if ((rc = cdb2_tcpresolve(host, &in, &port)) != 0)
        return rc;
    return cdb2_do_tcpconnect(in, port, myport, timeoutms);
}

struct context_messages {
    char *message[MAX_CONTEXTS];
    int count;
    int has_changed;
};

/* Forward declarations. */
static void cdb2_init_context_msgs(cdb2_hndl_tp *hndl);
static int cdb2_free_context_msgs(cdb2_hndl_tp *hndl);

/* Make it equal to FSQL header. */
struct newsqlheader {
    int type;
    int compression;
    int dummy;
    int length;
};

typedef struct cdb2_query_list_item {
    void *buf;
    int len;
    int is_read;
    char *sql;
    struct cdb2_query_list_item *next;
} cdb2_query_list;

#if WITH_SSL
typedef struct cdb2_ssl_sess {
    char host[64];
    SSL_SESSION *sess;
} cdb2_ssl_sess;

struct cdb2_ssl_sess_list {
    cdb2_ssl_sess_list *next;
    char dbname[64];
    char cluster[64];
    int ref;
    int n;
    /* We need to malloc the list separately as
       the list may change due to SSL re-negotiation
       or database migration. */
    cdb2_ssl_sess *list;
};
static cdb2_ssl_sess_list cdb2_ssl_sess_cache;
#endif

#define MAX_CNONCE_LEN 100

struct cdb2_hndl {
    char dbname[64];
    char cluster[64];
    char type[64];
    char hosts[MAX_NODES][64];
    uint64_t timestampus; // client query timestamp of first try
    int ports[MAX_NODES];
    int hosts_connected[MAX_NODES];
    SBUF2 *sb;
    int dbnum;
    int num_hosts;
    int num_hosts_sameroom;
    int node_seq;
    int in_trans;
    int temp_trans;
    int is_retry;
    char newsql_typestr[128];
    char policy[24];
    int master;
    int connected_host;
    char *query;
    char *query_hint;
    char *hint;
    int use_hint;
    int flags;
    char errstr[1024];
    char cnonce[MAX_CNONCE_LEN];
    int cnonce_len;
    char *sql;
    int ntypes;
    int *types;
    uint8_t *last_buf;
    CDB2SQLRESPONSE *lastresponse;
    uint8_t *first_buf;
    CDB2SQLRESPONSE *firstresponse;
    int error_in_trans;
    int client_side_error;
    int n_bindvars;
    CDB2SQLQUERY__Bindvalue **bindvars;
    cdb2_query_list *query_list;
    int snapshot_file;
    int snapshot_offset;
    int query_no;
    int retry_all;
    int num_set_commands;
    int num_set_commands_sent;
    int is_read;
    unsigned long long rows_read;
    int read_intrans_results;
    int first_record_read;
    char **commands;
    int ack;
    int is_hasql;
    int clear_snap_line;
    int debug_trace;
    int max_retries;
    int min_retries;
#if WITH_SSL
    ssl_mode c_sslmode; /* client SSL mode */
    peer_ssl_mode s_sslmode; /* server SSL mode */
    int sslerr; /* 1 if unrecoverable SSL error. */
    char *sslpath; /* SSL certificates */
    char *cert;
    char *key;
    char *ca;
    char *crl;
    int cache_ssl_sess;
    cdb2_ssl_sess_list *sess_list;
    int nid_dbname;
#endif
    struct context_messages context_msgs;
    char *env_tz;
    int sent_client_info;
    char stack[MAX_STACK];
    int send_stack;
};

void cdb2_set_min_retries(int min_retries)
{
    if (min_retries > 0) {
        MIN_RETRIES = min_retries;
    }
}

void cdb2_set_max_retries(int max_retries)
{
    if (max_retries > 0) {
        MAX_RETRIES = max_retries;
    }
}

void cdb2_hndl_set_min_retries(cdb2_hndl_tp *hndl, int min_retries)
{
    if (min_retries > 0) {
        hndl->min_retries = min_retries;
    }
}

void cdb2_hndl_set_max_retries(cdb2_hndl_tp *hndl, int max_retries)
{
    if (max_retries > 0) {
        hndl->max_retries = max_retries;
    }
}

void cdb2_set_comdb2db_config(const char *cfg_file)
{
    pthread_once(&init_once, do_init_once);
    if (log_calls)
        fprintf(stderr, "%p> %s(\"%s\")\n", (void *)pthread_self(), __func__,
                cfg_file);
    memset(CDB2DBCONFIG_NOBBENV, 0, sizeof(CDB2DBCONFIG_NOBBENV) /* 512 */);
    if (cfg_file != NULL) {
        strncpy(CDB2DBCONFIG_NOBBENV, cfg_file, 511);
    } else {
        reset_the_configuration();
    }
}

void cdb2_set_comdb2db_info(const char *cfg_info)
{
    int len;
    pthread_once(&init_once, do_init_once);
    if (log_calls)
        fprintf(stderr, "%p> cdb2_set_comdb2db_info(\"%s\")\n",
                (void *)pthread_self(), cfg_info);
    if (CDB2DBCONFIG_BUF != NULL) {
        free(CDB2DBCONFIG_BUF);
        CDB2DBCONFIG_BUF = NULL;
    }
    if (cfg_info == NULL) {
        reset_the_configuration();
        return;
    }
    len = strlen(cfg_info) + 1;
    CDB2DBCONFIG_BUF = malloc(len);
    strncpy(CDB2DBCONFIG_BUF, cfg_info, len);
}

static inline int get_char(FILE *fp, const char *buf, int *chrno)
{
    int ch;
    if (fp) {
        ch = getc(fp);
    } else {
        ch = buf[*chrno];
        *chrno += 1;
    }
    return ch;
}

static int read_line(char *line, int maxlen, FILE *fp, const char *buf,
                     int *chrno)
{
    int ch = get_char(fp, buf, chrno);
    while (ch == ' ' || ch == '\n')
        ch = get_char(fp, buf, chrno); // consume empty lines

    int count = 0;
    while ((ch != '\n') && (ch != EOF) && (ch != '\0')) {
        line[count] = ch;
        count++;
        if (count >= maxlen)
            return count;
        ch = get_char(fp, buf, chrno);
    }
    if (count == 0)
        return -1;
    line[count + 1] = '\0';
    return count + 1;
}

int is_valid_int(const char *str)
{
    while (*str) {
        if (!isdigit(*str))
            return 0;
        else
            ++str;
    }
    return 1;
}

#if WITH_SSL
static ssl_mode ssl_string_to_mode(const char *s, int *nid_dbname)
{
    if (strcasecmp(SSL_MODE_REQUIRE, s) == 0)
        return SSL_REQUIRE;
    if (strcasecmp(SSL_MODE_VERIFY_CA, s) == 0)
        return SSL_VERIFY_CA;
    if (strcasecmp(SSL_MODE_VERIFY_HOST, s) == 0)
        return SSL_VERIFY_HOSTNAME;
    if (strncasecmp(SSL_MODE_VERIFY_DBNAME, s,
                    sizeof(SSL_MODE_VERIFY_DBNAME) - 1) == 0) {
        s += sizeof(SSL_MODE_VERIFY_DBNAME);
        if (nid_dbname != NULL) {
            s = cdb2_skipws(s);
            *nid_dbname = (*s != '\0') ? OBJ_txt2nid(s) : cdb2_nid_dbname;
        }
        return SSL_VERIFY_DBNAME;
    }
    return SSL_ALLOW;
}
#endif

static void read_comdb2db_cfg(cdb2_hndl_tp *hndl, FILE *fp,
                              const char *comdb2db_name, const char *buf,
                              char comdb2db_hosts[][64], int *num_hosts,
                              int *comdb2db_num, const char *dbname,
                              char db_hosts[][64], int *num_db_hosts,
                              int *dbnum, int *dbname_found,
                              int *comdb2db_found, int *stack_at_open)
{
    char line[PATH_MAX > 2048 ? PATH_MAX : 2048] = {0};
    int line_no = 0;

    if (hndl && hndl->debug_trace) {
        fprintf(stderr, "td %u %s:%d \n", (uint32_t)pthread_self(), __func__,
                __LINE__);
    }
    while (read_line((char *)&line, sizeof(line), fp, buf, &line_no) != -1) {
        char *last = NULL;
        char *tok = NULL;
        tok = strtok_r(line, " :", &last);
        if (tok == NULL)
            continue;
        else if (comdb2db_name && strcasecmp(comdb2db_name, tok) == 0) {
            tok = strtok_r(NULL, " :,", &last);
            if (tok && is_valid_int(tok)) {
                *comdb2db_num = atoi(tok);
                tok = strtok_r(NULL, " :,", &last);
            }
            while (tok != NULL) {
                strcpy(comdb2db_hosts[*num_hosts], tok);
                (*num_hosts)++;
                tok = strtok_r(NULL, " :,", &last);
                *comdb2db_found = 1;
            }
        } else if (dbname && (strcasecmp(dbname, tok) == 0)) {
            tok = strtok_r(NULL, " :,", &last);
            if (tok && is_valid_int(tok)) {
                *dbnum = atoi(tok);
                tok = strtok_r(NULL, " :,", &last);
            }
            while (tok != NULL) {
                strcpy(db_hosts[*num_db_hosts], tok);
                tok = strtok_r(NULL, " :,", &last);
                (*num_db_hosts)++;
                *dbname_found = 1;
            }
        } else if (strcasecmp("comdb2_config", tok) == 0) {
            tok = strtok_r(NULL, " =:,", &last);
            if (tok == NULL) continue;
            pthread_mutex_lock(&cdb2_sockpool_mutex);
            if (strcasecmp("default_type", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    if (hndl && (strcasecmp(hndl->cluster, "default") == 0)) {
                        strcpy(hndl->cluster, tok);
                    } else if (!hndl) {
                        strcpy(cdb2_default_cluster, tok);
                    }
                }
            } else if (strcasecmp("room", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    strcpy(cdb2_machine_room, tok);
            } else if (strcasecmp("portmuxport", tok) == 0 || strcasecmp("pmuxport", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    CDB2_PORTMUXPORT = atoi(tok);
            } else if (strcasecmp("connect_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    CDB2_CONNECT_TIMEOUT = atoi(tok);
            } else if (strcasecmp("auto_consume_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    CDB2_AUTO_CONSUME_TIMEOUT_MS = atoi(tok);
            } else if (strcasecmp("comdb2db_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    COMDB2DB_TIMEOUT = atoi(tok);
            } else if (strcasecmp("comdb2dbname", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    strcpy(cdb2_comdb2dbname, tok);
            } else if (strcasecmp("tcpbufsz", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    cdb2_tcpbufsz = atoi(tok);
            } else if (strcasecmp("dnssufix", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    strcpy(cdb2_dnssuffix, tok);
            } else if (strcasecmp("stack_at_open", tok) == 0 && stack_at_open) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    if (strncasecmp(tok, "true", 4) == 0) {
                        *stack_at_open = 1;
                    } else {
                        *stack_at_open = 0;
                    }
                }
#if WITH_SSL
            } else if (strcasecmp("ssl_mode", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok != NULL) {
                    if (strcasecmp(SSL_MODE_ALLOW, tok) == 0)
                        cdb2_c_ssl_mode = SSL_ALLOW;
                    else if (strcasecmp(SSL_MODE_REQUIRE, tok) == 0)
                        cdb2_c_ssl_mode = SSL_REQUIRE;
                    else if (strcasecmp(SSL_MODE_VERIFY_CA, tok) == 0)
                        cdb2_c_ssl_mode = SSL_VERIFY_CA;
                    else if (strcasecmp(SSL_MODE_VERIFY_HOST, tok) == 0)
                        cdb2_c_ssl_mode = SSL_VERIFY_HOSTNAME;
                    else if (strcasecmp(SSL_MODE_VERIFY_DBNAME, tok) == 0) {
                        cdb2_c_ssl_mode = SSL_VERIFY_DBNAME;
                        tok = strtok_r(NULL, " :,", &last);
                        if (tok != NULL)
                            cdb2_nid_dbname = OBJ_txt2nid(tok);
                    }
                }
            } else if (strcasecmp(SSL_CERT_PATH_OPT, tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    strncpy(cdb2_sslcertpath, tok, PATH_MAX);
                    cdb2_sslcertpath[PATH_MAX - 1] = '\0';
                }
            } else if (strcasecmp(SSL_CERT_OPT, tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    strncpy(cdb2_sslcert, tok, PATH_MAX);
                    cdb2_sslcert[PATH_MAX - 1] = '\0';
                }
            } else if (strcasecmp(SSL_KEY_OPT, tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    strncpy(cdb2_sslkey, tok, PATH_MAX);
                    cdb2_sslkey[PATH_MAX - 1] = '\0';
                }
            } else if (strcasecmp(SSL_CA_OPT, tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    strncpy(cdb2_sslca, tok, PATH_MAX);
                    cdb2_sslca[PATH_MAX - 1] = '\0';
                }
#if HAVE_CRL
            } else if (strcasecmp(SSL_CRL_OPT, tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    strncpy(cdb2_sslcrl, tok, PATH_MAX);
                    cdb2_sslcrl[PATH_MAX - 1] = '\0';
                }
#endif /* HAVE_CRL */
            } else if (strcasecmp("ssl_session_cache", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    cdb2_cache_ssl_sess = !!atoi(tok);
#endif /* WITH_SSL */
            } else if (strcasecmp("allow_pmux_route", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    if (strncasecmp(tok, "true", 4) == 0) {
                        cdb2_allow_pmux_route = 1;
                    } else {
                        cdb2_allow_pmux_route = 0;
                    }
                }
            }
            pthread_mutex_unlock(&cdb2_sockpool_mutex);
        }
        bzero(line, sizeof(line));
    }
}

static int cdb2_dbinfo_query(cdb2_hndl_tp *hndl, const char *type,
                             const char *dbname, int dbnum, const char *host,
                             char valid_hosts[][64], int *valid_ports,
                             int *master_node, int *num_valid_hosts,
                             int *num_valid_sameroom_hosts);
#define QUOTE_(x) #x
#define QUOTE(x) QUOTE_(x)
static int get_config_file(const char *dbname, char *f, size_t s)
{
    char *root = getenv("COMDB2_ROOT");
    if (root == NULL)
        root = QUOTE(COMDB2_ROOT);
    size_t n;
    n = snprintf(f, s, "%s%s%s.cfg", root, CDB2DBCONFIG_NOBBENV_PATH, dbname);
    if (n >= s)
        return -1;
    return 0;
}

/* read all available comdb2 configuration files
 */
static int read_available_comdb2db_configs(
    cdb2_hndl_tp *hndl, char comdb2db_hosts[][64], const char *comdb2db_name,
    int *num_hosts, int *comdb2db_num, const char *dbname, char db_hosts[][64],
    int *num_db_hosts, int *dbnum, int *comdb2db_found, int *dbname_found)
{
    char filename[PATH_MAX];
    FILE *fp;
    int fallback_on_bb_bin = 1;

    if (hndl && hndl->debug_trace) {
        fprintf(stderr, "td %u %s:%d \n", (uint32_t)pthread_self(), __func__,
                __LINE__);
    }

    if (get_config_file(dbname, filename, sizeof(filename)) != 0)
        return -1; // set error string?

    if (num_hosts)
        *num_hosts = 0;
    if (num_db_hosts)
        *num_db_hosts = 0;
    int *send_stack = hndl ? (&hndl->send_stack) : NULL;

    if (CDB2DBCONFIG_BUF != NULL) {
        read_comdb2db_cfg(NULL, NULL, comdb2db_name, CDB2DBCONFIG_BUF,
                          comdb2db_hosts, num_hosts, comdb2db_num, dbname,
                          db_hosts, num_db_hosts, dbnum, dbname_found,
                          comdb2db_found, send_stack);
        fallback_on_bb_bin = 0;
    }

    if (*CDB2DBCONFIG_NOBBENV != '\0') {
      fp = fopen(CDB2DBCONFIG_NOBBENV, "r");
      if (fp != NULL) {
          read_comdb2db_cfg(NULL, fp, comdb2db_name, NULL, comdb2db_hosts,
                            num_hosts, comdb2db_num, dbname, db_hosts,
                            num_db_hosts, dbnum, dbname_found, comdb2db_found,
                            send_stack);
          fclose(fp);
          fallback_on_bb_bin = 0;
      }
    }

    /* This is a temporary solution.  There's no clear plan for how comdb2db.cfg
     * will be deployed to non-dbini machines. In the meantime, we have
     * programmers who want to use the API on dbini/mini machines. So if we
     * can't find the file in any standard location, look at /bb/bin
     * Once deployment details for comdb2db.cfg solidify, this will go away. */
    if (fallback_on_bb_bin) {
        fp = fopen(CDB2DBCONFIG_TEMP_BB_BIN, "r");
        if (fp != NULL) {
            read_comdb2db_cfg(NULL, fp, comdb2db_name, NULL, comdb2db_hosts,
                              num_hosts, comdb2db_num, dbname, db_hosts,
                              num_db_hosts, dbnum, dbname_found, comdb2db_found,
                              send_stack);
            fclose(fp);
        }
    }

    fp = fopen(filename, "r");
    if (fp != NULL) {
        read_comdb2db_cfg(hndl, fp, comdb2db_name, NULL, comdb2db_hosts,
                          num_hosts, comdb2db_num, dbname, db_hosts,
                          num_db_hosts, dbnum, dbname_found, comdb2db_found,
                          send_stack);
        fclose(fp);
    }
    return 0;
}

/* populate comdb2db_hosts based on hostname info of comdb2db_name
 * returns -1 if error or no osts wa found
 * returns 0 if hosts were found
 * this function has functionality similar to cdb2_tcpresolve()
 */
static int get_host_by_name(const char *comdb2db_name,
                            char comdb2db_hosts[][64], int *num_hosts)
{
    char tmp[8192];
    int tmplen = 8192;
    int herr;
    struct hostent hostbuf, *hp = NULL;
    char dns_name[256];

    if (cdb2_default_cluster[0] == '\0') {
        snprintf(dns_name, 256, "%s.%s", comdb2db_name, cdb2_dnssuffix);
    } else {
        snprintf(dns_name, 256, "%s-%s.%s", cdb2_default_cluster, comdb2db_name,
                 cdb2_dnssuffix);
    }
#ifdef __APPLE__
    hp = gethostbyname(dns_name);
#elif _LINUX_SOURCE
    gethostbyname_r(dns_name, &hostbuf, tmp, tmplen, &hp, &herr);
#elif _SUN_SOURCE
    hp = gethostbyname_r(dns_name, &hostbuf, tmp, tmplen, &herr);
#else
    hp = gethostbyname(dns_name);
#endif
    if (!hp) {
        fprintf(stderr, "%s:gethostbyname(%s): errno=%d err=%s\n", __func__,
                dns_name, errno, strerror(errno));
        return -1;
    }

    int rc = -1;
    struct in_addr **addr_list = (struct in_addr **)hp->h_addr_list;
    for (int i = 0; addr_list[i] != NULL; i++) {
        strcpy(comdb2db_hosts[i], inet_ntoa(*addr_list[i]));
        (*num_hosts)++;
        rc = 0;
    }
    return rc;
}

static int get_comdb2db_hosts(cdb2_hndl_tp *hndl, char comdb2db_hosts[][64],
                              int *comdb2db_ports, int *master,
                              const char *comdb2db_name, int *num_hosts,
                              int *comdb2db_num, const char *dbname,
                              char *dbtype, char db_hosts[][64],
                              int *num_db_hosts, int *dbnum, int just_defaults)
{
    int rc;
    int comdb2db_found = 0;
    int dbname_found = 0;

    if (hndl && hndl->debug_trace) {
        fprintf(stderr, "td %u %s:%d \n", (uint32_t)pthread_self(), __func__,
                __LINE__);
    }

    rc = read_available_comdb2db_configs(
        hndl, comdb2db_hosts, comdb2db_name, num_hosts, comdb2db_num, dbname,
        db_hosts, num_db_hosts, dbnum, &comdb2db_found, &dbname_found);
    if (rc == -1)
        return rc;

    if (master)
        *master = -1;

    if (just_defaults || comdb2db_found || dbname_found)
        return 0;

    rc = cdb2_dbinfo_query(hndl, cdb2_default_cluster, comdb2db_name,
                           *comdb2db_num, NULL, comdb2db_hosts, comdb2db_ports,
                           master, num_hosts, NULL);
    if (rc == 0)
        return 0;

    rc = get_host_by_name(comdb2db_name, comdb2db_hosts, num_hosts);
    return rc;
}

/* SOCKPOOL CODE START */

#define SOCKPOOL_ENABLED_DEFAULT 1
static int sockpool_enabled = SOCKPOOL_ENABLED_DEFAULT;

#define SOCKPOOL_FAIL_TIME_DEFAULT 0
static time_t sockpool_fail_time = SOCKPOOL_FAIL_TIME_DEFAULT;

static int sockpool_fd = -1;

struct sockaddr_sun {
    short sun_family;
    char sun_path[108];
};

struct sockpool_hello {
    char magic[4];
    int protocol_version;
    int pid;
    int slot;
};

struct sockpool_msg_vers0 {
    unsigned char request;
    char padding[3];
    int dbnum;
    int timeout;
    char typestr[48];
};

enum { SOCKPOOL_DONATE = 0, SOCKPOOL_REQUEST = 1 };

static int open_sockpool_ll(void)
{
    struct sockpool_hello hello;
    const char *ptr;
    size_t bytesleft;
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd == -1) {
        fprintf(stderr, "%s:socket: %d %s\n", __func__, errno, strerror(errno));
        return -1;
    }

    struct sockaddr_sun addr = {0};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, SOCKPOOL_SOCKET_NAME, sizeof(addr.sun_path));

    if (connect(fd, (const struct sockaddr *)&addr, sizeof(addr)) == -1) {
        close(fd);
        return -1;
    }

    /* Connected - write hello message */
    memcpy(hello.magic, "SQLP", 4);
    hello.protocol_version = 0;
    hello.pid = _PID;
    hello.slot = 0;

    ptr = (const char *)&hello;
    bytesleft = sizeof(hello);
    while (bytesleft > 0) {
        ssize_t nbytes;
        nbytes = write(fd, ptr, bytesleft);
        if (nbytes == -1) {
            fprintf(stderr, "%s:error writing hello: %d %s\n", __func__, errno,
                    strerror(errno));
            close(fd);
            return -1;
        } else if (nbytes == 0) {
            fprintf(stderr, "%s:unexpected eof writing hello\n", __func__);
            close(fd);
            return -1;
        }
        bytesleft -= nbytes;
        ptr += nbytes;
    }

    return fd;
}

void cdb2_enable_sockpool()
{
    pthread_mutex_lock(&cdb2_sockpool_mutex);
    sockpool_enabled = 1;
    pthread_mutex_unlock(&cdb2_sockpool_mutex);
}

static void cdb2_maybe_disable_sockpool(int forceClose, int enabled)
{
    pthread_mutex_lock(&cdb2_sockpool_mutex);
    /* Close sockpool fd */
    if (forceClose || (sockpool_enabled == 1)) {
        if (sockpool_fd != -1) {
            close(sockpool_fd);
            sockpool_fd = -1;
        }
    }
    sockpool_enabled = enabled;
    pthread_mutex_unlock(&cdb2_sockpool_mutex);
}

/* Disable sockpool and close sockpool socket */
void cdb2_disable_sockpool()
{
    cdb2_maybe_disable_sockpool(0, -1);
}

static void reset_sockpool(void)
{
    cdb2_maybe_disable_sockpool(1, SOCKPOOL_ENABLED_DEFAULT);
    sockpool_fail_time = SOCKPOOL_FAIL_TIME_DEFAULT;
}

// cdb2_socket_pool_get_ll: lockless
static int cdb2_socket_pool_get_ll(const char *typestr, int dbnum, int *port)
{
    if (sockpool_enabled == 0) {
        time_t current_time = time(NULL);
        /* Check every 10 seconds. */
        if ((current_time - sockpool_fail_time) > 10) {
            sockpool_enabled = 1;
        }
    }

    if (sockpool_enabled != 1) {
        return -1;
    }

    if (sockpool_fd == -1) {
        sockpool_fd = open_sockpool_ll();
        if (sockpool_fd == -1) {
            sockpool_enabled = 0;
            sockpool_fail_time = time(NULL);
            return -1;
        }
    }

    struct sockpool_msg_vers0 msg = {0};
    if (strlen(typestr) >= sizeof(msg.typestr)) {
        return -1;
    }
    /* Please may I have a file descriptor */
    msg.request = SOCKPOOL_REQUEST;
    msg.dbnum = dbnum;
    strncpy(msg.typestr, typestr, sizeof(msg.typestr) - 1);

    errno = 0;
    int rc = send_fd(sockpool_fd, &msg, sizeof(msg), -1);
    if (rc != PASSFD_SUCCESS) {
        fprintf(stderr, "%s: send_fd rc %d errno %d %s\n", __func__, rc, errno,
                strerror(errno));
        close(sockpool_fd);
        sockpool_fd = -1;
        return -1;
    }

    int fd;
    /* Read reply from server.  It can legitimately not send
     * us a file descriptor. */
    errno = 0;
    rc = recv_fd(sockpool_fd, &msg, sizeof(msg), &fd);
    if (rc != PASSFD_SUCCESS) {
        fprintf(stderr, "%s: recv_fd rc %d errno %d %s\n", __func__, rc, errno,
                strerror(errno));
        close(sockpool_fd);
        sockpool_fd = -1;
        fd = -1;
    }
    if (fd == -1 && port) {
        short gotport;
        memcpy((char *)&gotport, (char *)&msg.padding[1], 2);
        *port = ntohs(gotport);
    }
    return fd;
}

/* Get the file descriptor of a socket matching the given type string from
 * the pool.  Returns -1 if none is available or the file descriptor on
 * success. */
int cdb2_socket_pool_get(const char *typestr, int dbnum, int *port)
{
    pthread_mutex_lock(&cdb2_sockpool_mutex);
    int rc = cdb2_socket_pool_get_ll(typestr, dbnum, port);
    pthread_mutex_unlock(&cdb2_sockpool_mutex);
    if (log_calls)
        fprintf(stderr, "%s(%s,%d): fd=%d\n", __func__, typestr, dbnum, rc);
    return rc;
}

void cdb2_socket_pool_donate_ext(const char *typestr, int fd, int ttl,
                                 int dbnum, int flags, void *destructor,
                                 void *voidarg)
{
    pthread_mutex_lock(&cdb2_sockpool_mutex);
    if (sockpool_enabled == 1) {
        /* Donate this socket to the global socket pool.  We know that the
         * mutex is held. */
        if (sockpool_fd == -1) {
            sockpool_fd = open_sockpool_ll();
            if (sockpool_fd == -1) {
                sockpool_enabled = 0;
                fprintf(stderr, "\n Sockpool not present");
            }
        }

        struct sockpool_msg_vers0 msg = {0};
        if (sockpool_fd != -1 && (strlen(typestr) < sizeof(msg.typestr))) {
            int rc;
            msg.request = SOCKPOOL_DONATE;
            msg.dbnum = dbnum;
            msg.timeout = ttl;
            strncpy(msg.typestr, typestr, sizeof(msg.typestr) - 1);

            errno = 0;
            rc = send_fd(sockpool_fd, &msg, sizeof(msg), fd);
            if (rc != PASSFD_SUCCESS) {
                fprintf(stderr, "%s: send_fd rc %d errno %d %s\n", __func__, rc,
                        errno, strerror(errno));
                close(sockpool_fd);
                sockpool_fd = -1;
            }
        }
    }

    if (close(fd) == -1) {
        fprintf(stderr, "%s: close error for '%s' fd %d: %d %s\n", __func__,
                typestr, fd, errno, strerror(errno));
    }
    pthread_mutex_unlock(&cdb2_sockpool_mutex);
}

/* SOCKPOOL CODE ENDS */

static inline int cdb2_hostid()
{
    return _MACHINE_ID;
}

static int send_reset(SBUF2 *sb)
{
    int rc = 0;
    struct newsqlheader hdr;
    hdr.type = ntohl(CDB2_REQUEST_TYPE__RESET);
    hdr.compression = 0;
    hdr.length = 0;
    rc = sbuf2fwrite((char *)&hdr, sizeof(hdr), 1, sb);
    if (rc != 1) {
        return -1;
    }
    return 0;
}

#if WITH_SSL
static int try_ssl(cdb2_hndl_tp *hndl, SBUF2 *sb, int indx)
{
    /*
     *                   |<---------------- CLIENT ---------------->|
     *                   |------------------------------------------|
     *                   |    REQUIRE    |    ALLOW    |    < R7    |
     * -------|-----------------------------------------------------|
     *   ^    | REQUIRE  |     SSL[1]    |    SSL[1]   |    X[2]    |
     *   |    |-----------------------------------------------------|
     * SERVER | ALLOW    |     SSL[1]    |  PLAINTEXT  |  PLAINTEXT |
     *   |    |-----------------------------------------------------|
     *   v    | < R7     |     X[3]      |  PLAINTEXT  |  PLAINTEXT |
     * -------|-----------------------------------------------------|
     *        [1] The client writes an SSL negotiation packet first.
     *        [2] Rejected by the server.
     *        [3] Rejected by the client API.
     */

    /* An application may use different certificates.
       So we allocate an SSL context for each handle. */
    SSL_CTX *ctx;
    int rc, i, dossl = 0;
    cdb2_ssl_sess *p;
    cdb2_ssl_sess_list *store;
    SSL_SESSION *sess;

    if (hndl->c_sslmode >= SSL_REQUIRE) {
        switch (hndl->s_sslmode) {
        case PEER_SSL_UNSUPPORTED:
            sprintf(hndl->errstr, "The database does not support SSL.");
            hndl->sslerr = 1;
            return -1;
        case PEER_SSL_ALLOW:
        case PEER_SSL_REQUIRE:
            dossl = 1;
            break;
        default:
            sprintf(hndl->errstr,
                    "Unrecognized peer SSL mode: %d", hndl->s_sslmode);
            hndl->sslerr = 1;
            return -1;
        }
    } else {
        switch (hndl->s_sslmode) {
        case PEER_SSL_ALLOW:
        case PEER_SSL_UNSUPPORTED:
            dossl = 0;
            break;
        case PEER_SSL_REQUIRE:
            dossl = 1;
            break;
        default:
            sprintf(hndl->errstr,
                    "Unrecognized peer SSL mode: %d", hndl->s_sslmode);
            hndl->sslerr = 1;
            return -1;
        }
    }

    hndl->sslerr = 0;

    /* fast return if SSL is not needed. */
    if (!dossl)
        return 0;

    if ((rc = cdb2_init_ssl(1, 1)) != 0) {
        hndl->sslerr = 1;
        return rc;
    }

    /* If negotiation fails, let API retry. */
    struct newsqlheader hdr;
    hdr.type = ntohl(CDB2_REQUEST_TYPE__SSLCONN);
    hdr.compression = 0;
    hdr.length = 0;
    rc = sbuf2fwrite((char *)&hdr, sizeof(hdr), 1, sb);
    if (rc != 1)
        return -1;
    if ((rc = sbuf2flush(sb)) < 0 || (rc = sbuf2getc(sb)) < 0)
        return rc;

    /* The node does not agree with dbinfo. This usually happens
       during the downgrade from SSL to non-SSL. */
    if (rc == 'N') {
        if (hndl->c_sslmode <= SSL_ALLOW)
            return 0;

        /* We reach here only if the server is mistakenly downgraded
           before the client. */
        sprintf(hndl->errstr, "The database does not support SSL.");
        hndl->sslerr = 1;
        return -1;
    }

    rc = ssl_new_ctx(&ctx, hndl->c_sslmode, hndl->sslpath, &hndl->cert,
                     &hndl->key, &hndl->ca, &hndl->crl, hndl->num_hosts, NULL,
                     hndl->errstr, sizeof(hndl->errstr));
    if (rc != 0) {
        hndl->sslerr = 1;
        return -1;
    }

    p = (hndl->sess_list == NULL) ? NULL : &(hndl->sess_list->list[indx]);

    rc = sslio_connect(sb, ctx, hndl->c_sslmode, hndl->dbname, hndl->nid_dbname,
                       hndl->errstr, sizeof(hndl->errstr),
                       ((p != NULL) ? p->sess : NULL), &hndl->sslerr);

    SSL_CTX_free(ctx);
    if (rc != 1) {
        /* If SSL_connect() fails, invalidate the session. */
        if (p != NULL)
            p->sess = NULL;
        return -1;
    }

    if (hndl->cache_ssl_sess) {
        if (hndl->sess_list == NULL) {
            hndl->sess_list = malloc(sizeof(cdb2_ssl_sess_list));
            if (hndl->sess_list == NULL)
                return ENOMEM;
            hndl->sess_list->list = NULL;
            strncpy(hndl->sess_list->dbname,
                    hndl->dbname, sizeof(hndl->dbname) - 1);
            hndl->sess_list->dbname[sizeof(hndl->dbname) - 1] = '\0';
            strncpy(hndl->sess_list->cluster,
                    hndl->cluster, sizeof(hndl->cluster) - 1);
            hndl->sess_list->cluster[sizeof(hndl->cluster) - 1] = '\0';
            hndl->sess_list->ref = 1;
            hndl->sess_list->n = hndl->num_hosts;

            /* Append it to our internal linkedlist. */
            rc = pthread_mutex_lock(&cdb2_ssl_sess_lock);
            if (rc != 0) {
                /* If we fail to lock (which is quite rare), don't error out. 
                   we lose the caching ability, and that's it. */
                free(hndl->sess_list);
                hndl->sess_list = NULL;
                hndl->cache_ssl_sess = 0;
                return 0;
            }

            /* move store to the last element. */
            for (store = &cdb2_ssl_sess_cache; store->next != NULL;
                 store = store->next) {
                /* right, blank. */
            };
            hndl->sess_list->next = NULL;
            store->next = hndl->sess_list;
            pthread_mutex_unlock(&cdb2_ssl_sess_lock);
        }

        if (hndl->sess_list->list == NULL) {
            p = malloc(sizeof(cdb2_ssl_sess) * hndl->num_hosts);
            if (p == NULL)
                return ENOMEM;
            hndl->sess_list->list = p;

            for (i = 0; i != hndl->num_hosts; ++i, ++p) {
                strncpy(p->host, hndl->hosts[i], sizeof(p->host));
                p->host[sizeof(p->host) - 1] = '\0';
                p->sess = NULL;
            }
        }

        /* Refresh in case of renegotiation. */
        p = &(hndl->sess_list->list[indx]);
        sess = p->sess;
        p->sess = SSL_get1_session(sslio_get_ssl(sb));
        if (sess != NULL) SSL_SESSION_free(sess);
    }
    return 0;
}
#endif

static int cdb2portmux_route(const char *remote_host, const char *app,
                             const char *service, const char *instance,
                             int debug)
{
    char name[128];
    char res[32];
    SBUF2 *ss = NULL;
    int rc, fd;
    rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name)) {
        if (debug)
            fprintf(stderr,
                    "ERROR: can not fit entire string into name '%s/%s/%s'\n",
                    app, service, instance);
        return -1;
    }

    if (debug)
        fprintf(stderr, "td %d %s name %s\n", (uint32_t)pthread_self(),
                __func__, name);

    fd = cdb2_tcpconnecth_to(remote_host, CDB2_PORTMUXPORT, 0,
                             CDB2_CONNECT_TIMEOUT);
    if (fd < 0)
        return -1;
    ss = sbuf2open(fd, 0);
    if (ss == 0) {
        close(fd);
        return -1;
    }
    sbuf2printf(ss, "rte %s\n", name);
    sbuf2flush(ss);
    res[0] = '\0';
    sbuf2gets(res, sizeof(res), ss);
    if (debug)
        fprintf(stderr, "rte '%s' returns res='%s'\n", name, res);
    if (res[0] != '0') { // character '0' is indication of success
        sbuf2close(ss);
        return -1;
    }
    sbuf2free(ss);
    return fd;
}

/* Tries to connect to specified node using sockpool.
 * If there is none, then makes a new socket connection.
 */
static int newsql_connect(cdb2_hndl_tp *hndl, char *host, int port, int myport,
                          int timeoutms, int indx)
{

    if (hndl->debug_trace) {
        fprintf(stderr, "td %u %s:%d entering, host '%s:%d'\n",
                (uint32_t)pthread_self(), __func__, __LINE__, host, port);
    }
    int fd = -1;
    SBUF2 *sb = NULL;
    int rc = snprintf(hndl->newsql_typestr, sizeof(hndl->newsql_typestr),
                      "comdb2/%s/%s/newsql/%s", hndl->dbname, hndl->type,
                      hndl->policy);
    if ((rc < 1 || rc >= sizeof(hndl->newsql_typestr)) && hndl->debug_trace) {
        fprintf(stderr, "ERROR: can not fit entire string into "
                        "'comdb2/%s/%s/newsql/%s' only %s\n",
                hndl->dbname, hndl->type, hndl->policy, hndl->newsql_typestr);
    }

    while ((fd = cdb2_socket_pool_get(hndl->newsql_typestr, hndl->dbnum,
                                      NULL)) > 0) {
        if ((sb = sbuf2open(fd, 0)) == 0) {
            close(fd);
            return -1;
        }
        if (send_reset(sb) == 0)
            break;      // connection is ready
        sbuf2close(sb); // retry newsql connect;
    }

    if (fd < 0) {
        if (!cdb2_allow_pmux_route) {
            fd = cdb2_tcpconnecth_to(host, port, 0, CDB2_CONNECT_TIMEOUT);
        } else {
            fd = cdb2portmux_route(host, "comdb2", "replication", hndl->dbname,
                                   hndl->debug_trace);
        }
        if (fd < 0)
            return -1;

        if ((sb = sbuf2open(fd, 0)) == 0) {
            close(fd);
            return -1;
        }
        sbuf2printf(sb, "newsql\n");
        sbuf2flush(sb);
    }

    sbuf2settimeout(sb, 5000, 5000);

#if WITH_SSL
    if (try_ssl(hndl, sb, indx) != 0) {
        sbuf2close(sb);
        return -1;
    }
#endif

    hndl->sb = sb;
    hndl->num_set_commands_sent = 0;
    hndl->sent_client_info = 0;
    return 0;
}

static int newsql_disconnect(cdb2_hndl_tp *hndl, SBUF2 *sb, int line)
{
    if (sb == NULL)
        return 0;

    if (hndl->debug_trace) {
        fprintf(stderr, "td %p %s from line %d disconnecting from %s\n",
                (void *)pthread_self(), __func__, line,
                hndl->hosts[hndl->connected_host]);
    }
    int fd = sbuf2fileno(sb);

    int timeoutms = 10 * 1000;
    if ((hndl->firstresponse &&
         (!hndl->lastresponse ||
          (hndl->lastresponse->response_type != RESPONSE_TYPE__LAST_ROW))) ||
        (!hndl->firstresponse) || hndl->in_trans) {
        sbuf2close(sb);
    } else {
        sbuf2free(sb);
        cdb2_socket_pool_donate_ext(hndl->newsql_typestr, fd, timeoutms / 1000,
                                    hndl->dbnum, 5, NULL, NULL);
    }
    hndl->use_hint = 0;
    hndl->sb = NULL;
    return 0;
}

/* returns port number, or -1 for error*/
static int cdb2portmux_get(const char *remote_host, const char *app,
                           const char *service, const char *instance, int debug)
{
    char name[128]; /* app/service/dbname */
    char res[32];
    SBUF2 *ss = NULL;
    int rc, fd, port;
    rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name)) {
        if (debug)
            fprintf(stderr,
                    "ERROR: can not fit entire string into name '%s/%s/%s'\n",
                    app, service, instance);
        return -1;
    }

    if (debug)
        fprintf(stderr, "td %d %s name %s\n", (uint32_t)pthread_self(),
                __func__, name);

    fd = cdb2_tcpconnecth_to(remote_host, CDB2_PORTMUXPORT, 0,
                             CDB2_CONNECT_TIMEOUT);
    if (fd < 0) {
        if (debug)
            fprintf(stderr, "cdb2_tcpconnecth_to returns fd=%d'\n", fd);
        return -1;
    }
    ss = sbuf2open(fd, 0);
    if (ss == 0) {
        close(fd);
        if (debug)
            fprintf(stderr, "sbuf2open returned 0\n");
        return -1;
    }
    sbuf2settimeout(ss, CDB2_CONNECT_TIMEOUT, CDB2_CONNECT_TIMEOUT);
    sbuf2printf(ss, "get %s\n", name);
    sbuf2flush(ss);
    res[0] = '\0';
    sbuf2gets(res, sizeof(res), ss);
    sbuf2close(ss);
    if (debug)
        fprintf(stderr, "get '%s' returns res='%s'\n", name, res);
    if (res[0] == '\0') {
        return -1;
    }
    port = atoi(res);
    if (port <= 0)
        port = -1;
    return port;
}

void cdb2_use_hint(cdb2_hndl_tp *hndl)
{
    pthread_once(&init_once, do_init_once);
    hndl->use_hint = 1;
    if (log_calls) {
        fprintf(stderr, "%p> cdb2_use_hint(%p)\n", (void *)pthread_self(),
                hndl);
    }
}

static inline int cdb2_try_on_same_room(cdb2_hndl_tp *hndl)
{
    for (int i = 0; i < hndl->num_hosts_sameroom; i++) {
        int try_node = (hndl->node_seq + i) % hndl->num_hosts_sameroom;
        if (try_node == hndl->master || hndl->ports[try_node] <= 0 ||
            try_node == hndl->connected_host || hndl->hosts_connected[i] == 1)
            continue;
        int ret = newsql_connect(hndl, hndl->hosts[try_node],
                                 hndl->ports[try_node], 0, 100, i);
        if (ret != 0)
            continue;
        hndl->hosts_connected[try_node] = 1;
        hndl->connected_host = try_node;
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u %s:%d connected_host=%s\n",
                    (uint32_t)pthread_self(), __func__, __LINE__,
                    hndl->hosts[try_node]);
        }
        return 0;
    }
    return -1;
}

/* try to connect to range of hosts starting at begin stopping at end */
static inline int cdb2_try_connect_range(cdb2_hndl_tp *hndl, int begin, int end)
{
    for (int i = begin; i < end; i++) {
        hndl->node_seq = i + 1;
        if (i == hndl->master || hndl->ports[i] <= 0 ||
            i == hndl->connected_host || hndl->hosts_connected[i] == 1)
            continue;
        int ret =
            newsql_connect(hndl, hndl->hosts[i], hndl->ports[i], 0, 100, i);
        if (ret != 0)
            continue;
        hndl->connected_host = i;
        hndl->hosts_connected[i] = 1;
        return 0;
    }
    return -1;
}

static int cdb2_get_dbhosts(cdb2_hndl_tp *hndl);

/* combine hashes similar to hash_combine from boost library */
static uint64_t val_combine(uint64_t lhs, uint64_t rhs)
{
    lhs ^= rhs + 0x9e3779b9 + (lhs << 6) + (lhs >> 2);
    return lhs;
}

static int cdb2_random_int()
{
    static __thread unsigned short rand_state[3] = {0};
    if (rand_state[0] == 0) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        /* Initialize rand_state once per thread
         * _PID will ensure that cnonce will be different accross processes

         * Get the initial random state by using thread id and time info. */
        uint32_t tmp[2];
        tmp[0] = tv.tv_sec;
        tmp[1] = tv.tv_usec;
        uint64_t hash = val_combine(*(uint64_t *)tmp, (uint64_t)pthread_self());
        rand_state[0] = hash;
        rand_state[1] = hash >> 16;
        rand_state[2] = hash >> 32;
    }
    return nrand48(rand_state);
}

static inline int cdb2_try_resolve_ports(cdb2_hndl_tp *hndl)
{
    for (int i = 0; i < hndl->num_hosts; i++) {
        if (hndl->ports[i] <= 0) {
            hndl->ports[i] =
                cdb2portmux_get(hndl->hosts[i], "comdb2", "replication",
                                hndl->dbname, hndl->debug_trace);
            if (hndl->ports[i] > 0) {
                return 1;
            }
        }
    }
    return 0;
}

static int cdb2_connect_sqlhost(cdb2_hndl_tp *hndl)
{
    if (hndl->sb) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
    }

    int requery_done = 0;

retry_connect:
    if (hndl->debug_trace) {
        fprintf(stderr, "td %u %s:%d node_seq=%d "
                        "flags=0x%x, num_hosts=%d, num_hosts_sameroom=%d\n",
                (uint32_t)pthread_self(), __func__, __LINE__, hndl->node_seq,
                hndl->flags, hndl->num_hosts, hndl->num_hosts_sameroom);
    }

    if ((hndl->node_seq == 0) &&
        ((hndl->flags & CDB2_RANDOM) || ((hndl->flags & CDB2_RANDOMROOM) &&
                                         (hndl->num_hosts_sameroom == 0)))) {
        hndl->node_seq = cdb2_random_int() % hndl->num_hosts;
    } else if ((hndl->flags & CDB2_RANDOMROOM) && (hndl->node_seq == 0) &&
               (hndl->num_hosts_sameroom > 0)) {
        hndl->node_seq = cdb2_random_int() % hndl->num_hosts_sameroom;
        /* First try on same room. */
        if (0 == cdb2_try_on_same_room(hndl))
            return 0;
    }

    /* have hosts but no ports?  try to resolve ports */
    if (hndl->flags & CDB2_DIRECT_CPU) {
        for (int i = 0; i < hndl->num_hosts; i++) {
            if (hndl->ports[i] <= 0) {
                if (!cdb2_allow_pmux_route) {
                    hndl->ports[i] =
                        cdb2portmux_get(hndl->hosts[i], "comdb2", "replication",
                                        hndl->dbname, hndl->debug_trace);
                } else {
                    hndl->ports[i] = CDB2_PORTMUXPORT;
                }
            }
        }
    }

    int start_seq = hndl->node_seq;
    if (0 == cdb2_try_connect_range(hndl, start_seq, hndl->num_hosts))
        return 0;

    if (0 == cdb2_try_connect_range(hndl, 0, start_seq))
        return 0;

    if (hndl->sb == NULL) {
        /* Can't connect to any of the non-master nodes, try connecting to
         * master.*/
        /* After this retry on other nodes. */
        bzero(hndl->hosts_connected, sizeof(hndl->hosts_connected));
        if (hndl->ports[hndl->master] > 0) {
            int ret = newsql_connect(hndl, hndl->hosts[hndl->master],
                                     hndl->ports[hndl->master], 0, 100,
                                     hndl->master);
            if (ret == 0) {
                hndl->connected_host = hndl->master;
                return 0;
            }
        }
    }

    /* have hosts but no ports?  try to resolve ports */
    if (hndl->flags & CDB2_DIRECT_CPU && cdb2_try_resolve_ports(hndl) == 1) {
        requery_done = 1;
        goto retry_connect;
    }

    /* Can't connect to any of the nodes, re-check information about db. */
    if (!(hndl->flags & CDB2_DIRECT_CPU) && requery_done == 0 &&
        cdb2_get_dbhosts(hndl) == 0) {
        requery_done = 1;
        goto retry_connect;
    }

    hndl->connected_host = -1;
    return -1;
}

static inline void ack(cdb2_hndl_tp *hndl)
{
    hndl->ack = 0;
    struct newsqlheader hdr = {0};
    hdr.type = htonl(RESPONSE_HEADER__SQL_RESPONSE_PONG);
    sbuf2write((void *)&hdr, sizeof(hdr), hndl->sb);
    sbuf2flush(hndl->sb);
}

static int cdb2_read_record(cdb2_hndl_tp *hndl, uint8_t **buf, int *len, int *type)
{
    /* Got response */
    SBUF2 *sb = hndl->sb;
    struct newsqlheader hdr;
    int b_read;

retry:
    if (hndl->debug_trace) {
        fprintf(stderr, "td %p %s line %d\n", (void *)pthread_self(), __func__,
                __LINE__);
    }

    b_read = sbuf2fread((char *)&hdr, 1, sizeof(hdr), sb);
    if (b_read != sizeof(hdr)) {
        if (hndl->debug_trace) {
            fprintf(stderr, "td %p %s:%d bad read or numbytes,"
                            " b_read=%d, sizeof(hdr)=(%lu):\n",
                    (void *)pthread_self(), __func__, __LINE__, b_read,
                    sizeof(hdr));
        }
        return -1;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.length = ntohl(hdr.length);
    hndl->ack = (hdr.type == RESPONSE_HEADER__SQL_RESPONSE_PING);

    /* Server requires SSL. Return the header type in `type'.
       We may reach here under DIRECT_CPU mode where we skip DBINFO lookup. */
    if (hdr.type == RESPONSE_HEADER__SQL_RESPONSE_SSL) {
        if (type == NULL)
            return -1;
        *type = hdr.type;
        return 0;
    }

    if (hdr.length == 0)
        goto retry;

    if (type)
        *type = hdr.type;

    *buf = realloc(*buf, hdr.length);
    if ((*buf) == NULL) {
        fprintf(stderr, "%s: out of memory realloc(%d)\n", __func__, hdr.length);
        return -1;
    }

    b_read = sbuf2fread((char *)(*buf), 1, hdr.length, sb);
    *len = hdr.length;
    if (b_read != *len) {
        if (hndl->debug_trace) {
            fprintf(stderr, "td %p %s:%d bad read or numbytes,"
                            " b_read(%d) != *len(%d) type(%d)\n",
                    (void *)pthread_self(), __func__, __LINE__, b_read, *len,
                    *type);
        }
        return -1;
    }
    if (hndl->debug_trace && type) {
        fprintf(stderr, "td %u %s:%d returning type=%d\n",
                (uint32_t)pthread_self(), __func__, __LINE__, *type);
    }
    if (hdr.type == RESPONSE_HEADER__SQL_RESPONSE_TRACE) {
        CDB2SQLRESPONSE *response =  cdb2__sqlresponse__unpack(NULL, hdr.length, *buf);
        if (response->response_type == RESPONSE_TYPE__SP_TRACE) {
            fprintf(stderr,"%s\n",response->info_string);
            cdb2__sqlresponse__free_unpacked(response, NULL);
        } else {
            fprintf(stderr,"%s",response->info_string);
            cdb2__sqlresponse__free_unpacked(response, NULL);
            char cmd[250];
            if (fgets(cmd, 250, stdin) == NULL ||
                strncasecmp(cmd, "quit", 4) == 0) {
                exit(0);
            }
            CDB2QUERY query = CDB2__QUERY__INIT;
            query.spcmd = cmd;
            int len = cdb2__query__get_packed_size(&query);
            unsigned char *locbuf = malloc(len + 1);

            cdb2__query__pack(&query, locbuf);

            struct newsqlheader hdr;

            hdr.type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY);
            hdr.compression = ntohl(0);
            hdr.length = ntohl(len);

            sbuf2write((char *)&hdr, sizeof(hdr), hndl->sb);
            sbuf2write((char *)locbuf, len, hndl->sb);

            int rc = sbuf2flush(hndl->sb);
            free(locbuf);
            if (rc < 0)
                return -1;
        }
        goto retry;
    }
    return 0;
}

static int cdb2_convert_error_code(int rc)
{
    switch (rc) {
    case CDB2__ERROR_CODE__DUP_OLD:
        return CDB2ERR_DUPLICATE;
    case CDB2__ERROR_CODE__PREPARE_ERROR_OLD:
        return CDB2ERR_PREPARE_ERROR;
    default:
        return rc;
    }
}

static void clear_responses(cdb2_hndl_tp *hndl)
{
    if (hndl->lastresponse) {
        cdb2__sqlresponse__free_unpacked(hndl->lastresponse, NULL);
        free((void *)hndl->last_buf);
        hndl->last_buf = NULL;
        hndl->lastresponse = NULL;
    }

    if (hndl->firstresponse) {
        cdb2__sqlresponse__free_unpacked(hndl->firstresponse, NULL);
        free((void *)hndl->first_buf);
        hndl->first_buf = NULL;
        hndl->firstresponse = NULL;
    }
}

static int cdb2_effects_request(cdb2_hndl_tp *hndl)
{
    if (hndl && !hndl->in_trans) {
        return -1;
    }

    if (hndl->error_in_trans)
        return -1;

    clear_responses(hndl);
    CDB2QUERY query = CDB2__QUERY__INIT;
    CDB2DBINFO dbinfoquery = CDB2__DBINFO__INIT;
    dbinfoquery.dbname = hndl->dbname;
    dbinfoquery.has_want_effects = 1;
    dbinfoquery.want_effects = 1;
    query.sqlquery = NULL;
    query.dbinfo = &dbinfoquery;

    int len = cdb2__query__get_packed_size(&query);
    unsigned char *buf = malloc(len + 1);

    cdb2__query__pack(&query, buf);

    struct newsqlheader hdr;

    hdr.type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY);
    hdr.compression = ntohl(0);
    hdr.length = ntohl(len);

    sbuf2write((char *)&hdr, sizeof(hdr), hndl->sb);
    sbuf2write((char *)buf, len, hndl->sb);

    int rc = sbuf2flush(hndl->sb);
    free(buf);
    if (rc < 0)
        return -1;

    int type;

retry_read:
    rc = cdb2_read_record(hndl, &hndl->first_buf, &len, &type);
    if (rc) {
        free((void *)hndl->first_buf);
        hndl->first_buf = NULL;
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        return -1;
    }
    if (type ==
        RESPONSE_HEADER__SQL_RESPONSE) { /* This might be the error that
                                            happened within transaction. */
        hndl->firstresponse =
            cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
        hndl->error_in_trans = 
            cdb2_convert_error_code(hndl->firstresponse->error_code);
        strcpy(hndl->errstr, hndl->firstresponse->error_string);
        goto retry_read;
    }

    if (type == RESPONSE_HEADER__SQL_EFFECTS && hndl->error_in_trans)
        return -1;

    if (type != RESPONSE_HEADER__SQL_EFFECTS) {
        free((void *)hndl->first_buf);
        hndl->first_buf = NULL;
        return -1;
    }

    if (hndl->first_buf == NULL) {
        fprintf(stderr, "td %u %s: Can't read response from the db\n",
                (uint32_t)pthread_self(), __func__);
        return -1;
    }
    hndl->firstresponse = cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
    return 0;
}

static int cdb2_send_query(cdb2_hndl_tp *hndl, SBUF2 *sb, const char *dbname,
                           const char *sql, int n_set_commands,
                           int n_set_commands_sent, char **set_commands,
                           int n_bindvars, CDB2SQLQUERY__Bindvalue **bindvars,
                           int ntypes, int *types, int is_begin, int skip_nrows,
                           int retries_done, int do_append, int fromline)
{
    if (log_calls) {
        fprintf(stderr, "td %p %s line %d\n", (void *)pthread_self(), __func__,
                __LINE__);
    }

    int n_features = 0;
    int features[10]; // Max 10 client features??
    CDB2QUERY query = CDB2__QUERY__INIT;
    CDB2SQLQUERY sqlquery = CDB2__SQLQUERY__INIT;

    // This should be sent once right after we connect, not with every query
    CDB2SQLQUERY__Cinfo cinfo = CDB2__SQLQUERY__CINFO__INIT;

    if (!hndl || !hndl->sent_client_info) {
        cinfo.pid = _PID;
        cinfo.th_id = pthread_self();
        cinfo.host_id = cdb2_hostid();
        cinfo.argv0 = _ARGV0;
        if (hndl && hndl->send_stack)
            cinfo.stack = hndl->stack;
        sqlquery.client_info = &cinfo;
        if (hndl)
            hndl->sent_client_info = 1;
    }
    sqlquery.dbname = (char *)dbname;
    sqlquery.sql_query = (char *)cdb2_skipws(sql);
#if _LINUX_SOURCE
    sqlquery.little_endian = 1;
#else
    sqlquery.little_endian = 0;
#endif

    sqlquery.n_bindvars = n_bindvars;
    sqlquery.bindvars = bindvars;
    sqlquery.n_types = ntypes;
    sqlquery.types = types;
    sqlquery.tzname = (hndl) ? hndl->env_tz : DB_TZNAME_DEFAULT;
    sqlquery.mach_class = cdb2_default_cluster;


    if (hndl && hndl->debug_trace) {
        char *host = "NOT-CONNECTED";
        if (hndl->connected_host >= 0)
            host = hndl->hosts[hndl->connected_host];

        fprintf(stderr, "td %u %s:%d sending to %s '%s' from-line %d retries is"
                        " %d do_append is %d\n",
                (uint32_t)pthread_self(), __func__, __LINE__, host, sql,
                fromline, retries_done, do_append);
    }

    query.sqlquery = &sqlquery;

    sqlquery.n_set_flags = n_set_commands - n_set_commands_sent;
    if (sqlquery.n_set_flags)
        sqlquery.set_flags = &set_commands[n_set_commands_sent];

    if (hndl && hndl->is_retry) {
        sqlquery.has_retry = 1;
        sqlquery.retry = hndl->is_retry;
    }

    if (hndl && !(hndl->flags & CDB2_READ_INTRANS_RESULTS) && is_begin) {
        features[n_features] = CDB2_CLIENT_FEATURES__SKIP_INTRANS_RESULTS;
        n_features++;
    }

#if WITH_SSL
    features[n_features] = CDB2_CLIENT_FEATURES__SSL;
    n_features++;
#endif
    if (hndl) {
        features[n_features] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO;
        n_features++;
        if ((hndl->flags & CDB2_DIRECT_CPU) ||
            (retries_done >= (hndl->num_hosts * 2 - 1) && hndl->master ==
             hndl->connected_host)) {
            features[n_features] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC;
            n_features++;
        }
        if (retries_done >= hndl->num_hosts) {
            features[n_features] = CDB2_CLIENT_FEATURES__ALLOW_QUEUING;
            n_features++;
        }
    } else if (retries_done) {
        features[n_features] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO;
        n_features++;
        features[n_features] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC;
        n_features++;
        features[n_features] = CDB2_CLIENT_FEATURES__ALLOW_QUEUING;
        n_features++;
    }

    if (hndl && hndl->cnonce_len > 0) {
        /* Have a query id associated with each transaction/query */
        sqlquery.has_cnonce = 1;
        sqlquery.cnonce.data = (uint8_t*)hndl->cnonce;
        sqlquery.cnonce.len = hndl->cnonce_len;
    }

    CDB2SQLQUERY__Snapshotinfo snapshotinfo = CDB2__SQLQUERY__SNAPSHOTINFO__INIT;
    if (hndl && hndl->snapshot_file) {
        snapshotinfo.file = hndl->snapshot_file;
        snapshotinfo.offset = hndl->snapshot_offset;
        sqlquery.snapshot_info = &snapshotinfo;
    }

    if (n_features) {
        sqlquery.n_features = n_features;
        sqlquery.features = features;
    }

    if (skip_nrows) {
        sqlquery.has_skip_rows = 1;
        sqlquery.skip_rows = skip_nrows;
    }

    if (hndl && hndl->context_msgs.has_changed == 1 &&
        hndl->context_msgs.count > 0) {
        sqlquery.n_context = hndl->context_msgs.count;
        sqlquery.context = hndl->context_msgs.message;
        /* Reset the has_changed flag. */
        hndl->context_msgs.has_changed = 0;
    }

    CDB2SQLQUERY__Reqinfo req_info = CDB2__SQLQUERY__REQINFO__INIT;
    req_info.timestampus = (hndl ? hndl->timestampus : 0);
    req_info.num_retries = retries_done;
    sqlquery.req_info = &req_info;

    int len = cdb2__query__get_packed_size(&query);
    unsigned char *buf = malloc(len + 1);

    cdb2__query__pack(&query, buf);

    struct newsqlheader hdr;
    hdr.type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY);
    hdr.compression = ntohl(0);
    hdr.length = ntohl(len);

    // finally send header and query
    sbuf2write((char *)&hdr, sizeof(hdr), sb);
    sbuf2write((char *)buf, len, sb);

    int rc = sbuf2flush(sb);

    if (rc < 0) {
        free(buf);
        return -1;
    }

    if (hndl && hndl->in_trans && do_append) {
        /* Retry number of transaction is different from that of query.*/
        cdb2_query_list *item = malloc(sizeof(cdb2_query_list));
        item->buf = buf;
        item->len = len;
        item->is_read = hndl->is_read;
        item->next = NULL;
        item->sql = strdup(sql);
        cdb2_query_list *last = hndl->query_list;
        if (last == NULL) {
            hndl->query_list = item;
        } else {
            while (last->next != NULL)
                last = last->next;
            last->next = item;
        }
    } else {
        free(buf);
    }

    return 0;
}

/* All "soft" errors are retryable .. constraint violation are not */
static int is_retryable(cdb2_hndl_tp *hndl, int err_val)
{
    switch (err_val) {
    case CDB2ERR_CHANGENODE:
    case CDB2ERR_NOMASTER:
    case CDB2ERR_TRAN_IO_ERROR:
    case CDB2ERR_REJECTED:
    case CDB2__ERROR_CODE__MASTER_TIMEOUT:
        return 1;

    default:
        return 0;
    }
}

static int retry_queries_and_skip(cdb2_hndl_tp *hndl, int num_retry,
                                  int skip_nrows);

#define PRINT_RETURN(rcode)                                                    \
    {                                                                          \
        if (hndl->debug_trace)                                                 \
            fprintf(stderr, "%std %u %s:%d cnonce '%s' [%d][%d] "              \
                            "returning %d\n",                                  \
                    rcode == 0 ? "" : "XXX ", (uint32_t)pthread_self(),        \
                    __func__, __LINE__, hndl->cnonce ? hndl->cnonce : "(nil)", \
                    hndl->snapshot_file, hndl->snapshot_offset, rcode);        \
        return (rcode);                                                        \
    }
#define PRINT_RETURN_OK(rcode)                                                 \
    {                                                                          \
        if (hndl->debug_trace)                                                 \
            fprintf(stderr, "td %u %s:%d cnonce '%s' [%d][%d] "                \
                            "returning %d\n",                                  \
                    (uint32_t)pthread_self(), __func__, __LINE__,              \
                    hndl->cnonce ? hndl->cnonce : "(nil)",                     \
                    hndl->snapshot_file, hndl->snapshot_offset, rcode);        \
        return (rcode);                                                        \
    }

static int cdb2_next_record_int(cdb2_hndl_tp *hndl, int shouldretry)
{
    int len;
    int rc;
    int num_retry = 0;

    if (hndl->ack)
        ack(hndl);

retry_next_record:
    if (hndl->first_buf == NULL || hndl->sb == NULL)
        PRINT_RETURN_OK(CDB2_OK_DONE);

    if (hndl->firstresponse->error_code)
        PRINT_RETURN_OK(hndl->firstresponse->error_code);

    if (hndl->lastresponse) {
        if (hndl->lastresponse->response_type == RESPONSE_TYPE__LAST_ROW) {
            PRINT_RETURN_OK(CDB2_OK_DONE);
        }

        if (hndl->lastresponse->response_type == RESPONSE_TYPE__COLUMN_VALUES &&
                hndl->lastresponse->error_code != 0) {
            int rc = cdb2_convert_error_code(hndl->lastresponse->error_code);
            if (hndl->in_trans) {
                /* Give the same error for every query until commit/rollback */
                hndl->error_in_trans = rc;
            }
            PRINT_RETURN_OK(rc);
        }
    }

    rc = cdb2_read_record(hndl, &hndl->last_buf, &len, NULL);
    if (rc) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        sprintf(hndl->errstr, "%s: Timeout while reading response from server",
                __func__);
    retry:
        if (hndl->debug_trace) {
            fprintf(stderr,
                    "td %p %s:%d retry: shouldretry=%d, "
                    "hndl->snapshot_file=%d, num_retry=%d\n",
                    (void *)pthread_self(), __func__, __LINE__, shouldretry,
                    hndl->snapshot_file, num_retry);
        }
        if (shouldretry && hndl->snapshot_file && num_retry < hndl->max_retries) {
            num_retry++;
            if (num_retry > hndl->num_hosts) {
                int tmsec;
                tmsec = (num_retry - hndl->num_hosts) * 100;
                if (tmsec > 1000)
                    tmsec = 1000;
                poll(NULL, 0, tmsec);
            }
            cdb2_connect_sqlhost(hndl);
            if (hndl->sb == NULL) {
#if WITH_SSL
                if (hndl->sslerr != 0)
                    PRINT_RETURN_OK(-1);
#endif
                goto retry;
            }
            rc = retry_queries_and_skip(hndl, num_retry, hndl->rows_read);
            if (rc) {
                newsql_disconnect(hndl, hndl->sb, __LINE__);
                goto retry;
            }
            goto retry_next_record;
        }
        PRINT_RETURN_OK(-1);
    }

    if (hndl->last_buf == NULL) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        sprintf(hndl->errstr, "%s: No response from server", __func__);
        PRINT_RETURN_OK(-1);
    }

    if (hndl->lastresponse)
        cdb2__sqlresponse__free_unpacked(hndl->lastresponse, NULL);

    hndl->lastresponse = cdb2__sqlresponse__unpack(NULL, len, hndl->last_buf);

    if (hndl->lastresponse->snapshot_info &&
        hndl->lastresponse->snapshot_info->file) {
        hndl->snapshot_file = hndl->lastresponse->snapshot_info->file;
        hndl->snapshot_offset = hndl->lastresponse->snapshot_info->offset;
    }

    if (hndl->lastresponse->response_type == RESPONSE_TYPE__COLUMN_VALUES) {
        // "Good" rcodes are not retryable
        if (is_retryable(hndl, hndl->lastresponse->error_code) &&
            hndl->snapshot_file) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
            sprintf(hndl->errstr,
                    "%s: Timeout while reading response from server", __func__);
            goto retry;
        }

        hndl->rows_read++;
        if (hndl->in_trans) {
            /* Give the same error for every query until commit/rollback */
            hndl->error_in_trans =
                cdb2_convert_error_code(hndl->lastresponse->error_code);
        }

        if (hndl->debug_trace) {
            fprintf(stderr, "td %p %s line %d error_string=%s\n",
                    (void *)pthread_self(), __func__, __LINE__,
                    hndl->lastresponse->error_string);
        }
        rc = cdb2_convert_error_code(hndl->lastresponse->error_code);
        PRINT_RETURN_OK(rc);
    }

    if (hndl->lastresponse->response_type == RESPONSE_TYPE__LAST_ROW) {
        int ii = 0;

        // check for begin that couldn't retrieve the durable lsn from master
        if (is_retryable(hndl, hndl->lastresponse->error_code)) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
            sprintf(hndl->errstr,
                    "%s: Timeout while reading response from server", __func__);
            return hndl->lastresponse->error_code;
        }

        if (hndl->num_set_commands) {
            hndl->num_set_commands_sent = hndl->num_set_commands;
        }
        for (ii = 0; ii < hndl->lastresponse->n_features; ii++) {
            if (hndl->in_trans && (CDB2_SERVER_FEATURES__SKIP_INTRANS_RESULTS ==
                                   hndl->lastresponse->features[ii]))
                hndl->read_intrans_results = 0;
        }

        PRINT_RETURN_OK(CDB2_OK_DONE);
    }

    PRINT_RETURN_OK(-1);
}

int cdb2_next_record(cdb2_hndl_tp *hndl)
{
    int rc = 0;

    pthread_once(&init_once, do_init_once);

    if (hndl->in_trans && !hndl->read_intrans_results && !hndl->is_read) {
        rc = CDB2_OK_DONE;
    } else if (hndl->lastresponse && hndl->first_record_read == 0) {
        hndl->first_record_read = 1;
        if (hndl->lastresponse->response_type == RESPONSE_TYPE__COLUMN_VALUES) {
            rc = hndl->lastresponse->error_code;
        } else if (hndl->lastresponse->response_type ==
                   RESPONSE_TYPE__LAST_ROW) {
            if (hndl->num_set_commands) {
                hndl->num_set_commands_sent = hndl->num_set_commands;
            }
            rc = CDB2_OK_DONE;
        } else {
            rc = -1;
        }
    } else {
        rc = cdb2_next_record_int(hndl, 1);
    }

    if (log_calls)
        fprintf(stderr, "%p> cdb2_next_record(%p) = %d\n",
                (void *)pthread_self(), hndl, rc);
    return rc;
}

int cdb2_get_effects(cdb2_hndl_tp *hndl, cdb2_effects_tp *effects)
{
    int rc = 0;
    pthread_once(&init_once, do_init_once);

    while (cdb2_next_record_int(hndl, 0) == CDB2_OK)
        ;

    if (hndl->lastresponse == NULL) {
        int lrc = cdb2_effects_request(hndl);
        if (lrc) {
            rc = -1;
        } else if (hndl->firstresponse && hndl->firstresponse->effects) {
            effects->num_affected = hndl->firstresponse->effects->num_affected;
            effects->num_selected = hndl->firstresponse->effects->num_selected;
            effects->num_updated = hndl->firstresponse->effects->num_updated;
            effects->num_deleted = hndl->firstresponse->effects->num_deleted;
            effects->num_inserted = hndl->firstresponse->effects->num_inserted;
            cdb2__sqlresponse__free_unpacked(hndl->firstresponse, NULL);
            free((void *)hndl->first_buf);
            hndl->first_buf = NULL;
            hndl->firstresponse = NULL;
            rc = 0;
        } else {
            rc = -1;
        }
    } else if (hndl->lastresponse->effects) {
        effects->num_affected = hndl->lastresponse->effects->num_affected;
        effects->num_selected = hndl->lastresponse->effects->num_selected;
        effects->num_updated = hndl->lastresponse->effects->num_updated;
        effects->num_deleted = hndl->lastresponse->effects->num_deleted;
        effects->num_inserted = hndl->lastresponse->effects->num_inserted;
        rc = 0;
    } else {
        rc = -1;
    }

    if (log_calls) {
        fprintf(stderr, "%p> cdb_get_effects(%p) = %d", (void *)pthread_self(),
                hndl, rc);
        if (rc == 0)
            fprintf(stderr, " => affected %d, selected %d, updated %d, deleted "
                            "%d, inserted %d\n",
                    effects->num_affected, effects->num_selected,
                    effects->num_updated, effects->num_deleted,
                    effects->num_inserted);
    }

    return rc;
}

int cdb2_close(cdb2_hndl_tp *hndl)
{
    pthread_once(&init_once, do_init_once);
    if (log_calls)
        fprintf(stderr, "%p> cdb2_close(%p)\n", (void *)pthread_self(), hndl);

    if (!hndl)
        return 0;

    if (hndl->ack)
        ack(hndl);

    if (hndl->sb && !hndl->in_trans && hndl->firstresponse &&
        (!hndl->lastresponse ||
         (hndl->lastresponse->response_type != RESPONSE_TYPE__LAST_ROW))) {
        int nrec = 0;
        sbuf2settimeout(hndl->sb, CDB2_AUTO_CONSUME_TIMEOUT_MS,
                        CDB2_AUTO_CONSUME_TIMEOUT_MS);
        struct timeval tv;
        gettimeofday(&tv, NULL);
        uint64_t starttimems = ((uint64_t)tv.tv_sec) * 1000 + tv.tv_usec / 1000;
        while (cdb2_next_record_int(hndl, 0) == CDB2_OK) {
            nrec++;
            gettimeofday(&tv, NULL);
            uint64_t curr = ((uint64_t)tv.tv_sec) * 1000 + tv.tv_usec / 1000;
            /* auto consume for up to CDB2_AUTO_CONSUME_TIMEOUT_MS */
            if (curr - starttimems >= CDB2_AUTO_CONSUME_TIMEOUT_MS)
                break;
        }
        if (hndl->debug_trace) {
            gettimeofday(&tv, NULL);
            uint64_t curr = ((uint64_t)tv.tv_sec) * 1000 + tv.tv_usec / 1000;
            fprintf(stderr, "%s: auto consume %d records took %lu ms\n",
                    __func__, nrec, curr - starttimems);
        }
    }

    if (hndl->sb)
        newsql_disconnect(hndl, hndl->sb, __LINE__);

    if (hndl->firstresponse) {
        cdb2__sqlresponse__free_unpacked(hndl->firstresponse, NULL);
        free((void *)hndl->first_buf);
        hndl->first_buf = NULL;
    }

    if (hndl->lastresponse) {
        cdb2__sqlresponse__free_unpacked(hndl->lastresponse, NULL);
        free((void *)hndl->last_buf);
    }
    if (hndl->num_set_commands) {
        while (hndl->num_set_commands) {
            hndl->num_set_commands--;
            free(hndl->commands[hndl->num_set_commands]);
        }
        free(hndl->commands);
        hndl->commands = NULL;
    }

    if (hndl->query)
        free(hndl->query);

    if (hndl->query_hint)
        free(hndl->query_hint);

    if (hndl->hint)
        free(hndl->hint);

    cdb2_clearbindings(hndl);
    cdb2_free_context_msgs(hndl);
#if WITH_SSL
    free(hndl->sslpath);
    free(hndl->cert);
    free(hndl->key);
    free(hndl->ca);
    free(hndl->crl);
    if (hndl->sess_list) {
        /* This is correct - we don't have to do it under lock. */
        hndl->sess_list->ref = 0;
    }
#endif

    free(hndl);
    return 0;
}

/* make_random_str() will return a randomly generated string
 * this is used to get a cnonce, composed of four components:
 * the first part is the id of this host machine
 * the second part is the PID of this client process
 * the third part is the current time usec portion
 * the fourth part is a [pseudo]random number
 */
static void make_random_str(char *str, size_t max_len, int *len)
{
    static __thread char cached_portion[23] = {0}; // 2*10 digits + 2 '-' + '\n'
    static __thread size_t cached_portion_len = 0;
    if (cached_portion_len == 0) {
        cached_portion_len =
            snprintf(cached_portion, sizeof(cached_portion) - 1, "%d-%d-",
                     cdb2_hostid(), _PID);
    }
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int randval = cdb2_random_int();
    strncpy(str, cached_portion, cached_portion_len);
    *len = cached_portion_len;
    *len += snprintf(str + cached_portion_len, max_len - cached_portion_len,
                     "%d-%d", (int)tv.tv_usec, randval);
    return;
}

static int cdb2_query_with_hint(cdb2_hndl_tp *hndl, const char *sqlquery,
                                char *short_identifier, char **hint,
                                char **query_hint)
{
    const char *sqlstr = cdb2_skipws(sqlquery);
    const char *sql_start = sqlstr;
    int len = strlen(sqlstr);
    int len_id = strlen(short_identifier);
    if (len_id > 128) {
        sprintf(hndl->errstr, "Short identifier is too long.");
        return -1;
    }

    int fw_end = 1;
    while (*sql_start != '\0' && *sql_start != ' ') {
        fw_end++;
        sql_start++;
    }

    /* short string will be something like this
       select <* RUNCOMDB2SQL <short_identifier> *>
       */
    *hint = malloc(fw_end + SQLCACHEHINTLENGTH + 4 + len_id + 1);
    strncpy(*hint, sqlstr, fw_end);
    /* Add the SQL HINT */
    strncpy(*hint + fw_end, SQLCACHEHINT, SQLCACHEHINTLENGTH);
    strncpy(*hint + fw_end + SQLCACHEHINTLENGTH, short_identifier, len_id);
    strncpy(*hint + fw_end + SQLCACHEHINTLENGTH + len_id, " */ ", 5);
    /* short string will be something like this
       select <* RUNCOMDB2SQL <short_identifier> *> <rest of the sql>
       */
    *query_hint = malloc(len + SQLCACHEHINTLENGTH + 4 + len_id + 1);
    strncpy(*query_hint, *hint, fw_end + SQLCACHEHINTLENGTH + 4 + len_id);
    strcpy(*query_hint + fw_end + SQLCACHEHINTLENGTH + 4 + len_id,
           sqlstr + fw_end);
    return 0;
}

int cdb2_run_statement(cdb2_hndl_tp *hndl, const char *sql)
{
    return cdb2_run_statement_typed(hndl, sql, 0, NULL);
}

static void parse_dbresponse(CDB2DBINFORESPONSE *dbinfo_response,
                             char valid_hosts[][64], int *valid_ports,
                             int *master_node, int *num_valid_hosts,
                             int *num_valid_sameroom_hosts
#if WITH_SSL
                             , peer_ssl_mode *s_mode
#endif
                             )
{
    if (log_calls)
        fprintf(stderr, "td %d %s:%d\n", (uint32_t)pthread_self(), __func__,
                __LINE__);
    int num_hosts = dbinfo_response->n_nodes;
    *num_valid_hosts = 0;
    if (num_valid_sameroom_hosts)
        *num_valid_sameroom_hosts = 0;
    int myroom = 0;
    int i = 0;
    for (i = 0; i < num_hosts; i++) {
        CDB2DBINFORESPONSE__Nodeinfo *currnode = dbinfo_response->nodes[i];
        if (!myroom) {
            if (currnode->has_room) {
                myroom = currnode->room;
            } else {
                myroom = -1;
            }
        }
        if (currnode->incoherent)
            continue;

        strcpy(valid_hosts[*num_valid_hosts], currnode->name);
        if (currnode->has_port) {
            valid_ports[*num_valid_hosts] = currnode->port;
        } else {
            valid_ports[*num_valid_hosts] = -1;
        }
        if (strcmp(currnode->name, dbinfo_response->master->name) == 0)
            *master_node = *num_valid_hosts;

        if (log_calls)
            fprintf(stderr, "td %d %s:%d, %d) host=%s(%d)%s\n",
                    (uint32_t)pthread_self(), __func__, __LINE__,
                    *num_valid_hosts, valid_hosts[*num_valid_hosts],
                    valid_ports[*num_valid_hosts],
                    (*master_node == *num_valid_hosts) ? "*" : "");

        if (num_valid_sameroom_hosts && (myroom == currnode->room))
            (*num_valid_sameroom_hosts)++;

        (*num_valid_hosts)++;
    }

    /* Add incoherent nodes too, don't count them for same room hosts. */
    for (i = 0; i < num_hosts; i++) {
        CDB2DBINFORESPONSE__Nodeinfo *currnode = dbinfo_response->nodes[i];
        if (!currnode->incoherent)
            continue;
        strcpy(valid_hosts[*num_valid_hosts], currnode->name);
        if (currnode->has_port) {
            valid_ports[*num_valid_hosts] = currnode->port;
        } else {
            valid_ports[*num_valid_hosts] = -1;
        }
        if (currnode->number == dbinfo_response->master->number)
            *master_node = *num_valid_hosts;

        (*num_valid_hosts)++;
    }

#if WITH_SSL
    if (!dbinfo_response->has_require_ssl)
        *s_mode = PEER_SSL_UNSUPPORTED;
    else if (dbinfo_response->require_ssl)
        *s_mode = PEER_SSL_REQUIRE;
    else
        *s_mode = PEER_SSL_ALLOW;
#endif
}

static int retry_query_list(cdb2_hndl_tp *hndl, int num_retry, int run_last)
{
    if (hndl->debug_trace) {
        fprintf(stderr, "td %u %s():%d, retry_all %d, intran %d\n",
                (uint32_t)pthread_self(), __func__, __LINE__, hndl->retry_all,
                hndl->in_trans);
    }

    if (!hndl->retry_all || !hndl->in_trans)
        return 0;

    int rc = 0;
    if (!(hndl->snapshot_file || hndl->query_no <= 1)) {
        if (hndl->debug_trace) {
            fprintf(stderr,
                    "td %u %s:%d in_trans=%d snapshot_file=%d query_no=%d\n",
                    (uint32_t)pthread_self(), __func__, __LINE__,
                    hndl->in_trans, hndl->snapshot_file, hndl->query_no);
        }
        sprintf(hndl->errstr, "%s: Database disconnected while in transaction.",
                __func__);
        return CDB2ERR_TRAN_IO_ERROR; /* Fail if disconnect happens in
                                         transaction which doesn't have snapshot
                                         info.*/
    }

    /* Replay all the queries. */
    char *host = "NOT-CONNECTED";
    if (hndl->connected_host >= 0)
        host = hndl->hosts[hndl->connected_host];

    /*Send Begin. */
    hndl->is_retry = num_retry;

    clear_responses(hndl);
    hndl->read_intrans_results = 1;

    hndl->in_trans = 0;
    if (hndl->debug_trace) {
        fprintf(stderr, "td %u %s:%d setting in_trans to 0\n",
                (uint32_t)pthread_self(), __func__, __LINE__);
        fprintf(stderr, "td %u %s:%d sending 'begin' to %s\n",
                (uint32_t)pthread_self(), __func__, __LINE__, host);
    }
    rc = cdb2_send_query(hndl, hndl->sb, hndl->dbname, "begin",
                         hndl->num_set_commands, hndl->num_set_commands_sent,
                         hndl->commands, 0, NULL, 0, NULL, 1, 0, num_retry, 0,
                         __LINE__);
    hndl->in_trans = 1;
    if (hndl->debug_trace) {
        fprintf(stderr, "td %u %s:%d setting in_trans to 1\n",
                (uint32_t)pthread_self(), __func__, __LINE__);
    }

    if (rc != 0) {
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u %s:%d send_query rc=%d returning 1\n",
                    (uint32_t)pthread_self(), host, __LINE__, rc);
        }
        return 1;
    }
    int len = 0;
    int type = 0;
    rc = cdb2_read_record(hndl, &hndl->first_buf, &len, &type);

    if (hndl->debug_trace) {
        fprintf(stderr, "td %u line %d reading response from %s rc=%d\n",
                (uint32_t)pthread_self(), __LINE__, host, rc);
    }

    if (rc) {
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        if (hndl->debug_trace) {
            fprintf(
                stderr,
                "td %u line %d reading response from %s rc=%d returning 1\n",
                (uint32_t)pthread_self(), __LINE__, host, rc);
        }
        return 1;
    }
    if (type == RESPONSE_HEADER__DBINFO_RESPONSE) {
        if (hndl->flags & CDB2_DIRECT_CPU) {
            if (hndl->debug_trace) {
                fprintf(stderr, "td %u %s() directcpu will ignore dbinfo\n",
                        (uint32_t)pthread_self(), __func__);
            }
            return 1;
        }
        /* The master sent info about nodes that might be coherent. */
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        CDB2DBINFORESPONSE *dbinfo_response = NULL;
        dbinfo_response =
            cdb2__dbinforesponse__unpack(NULL, len, hndl->first_buf);
        parse_dbresponse(dbinfo_response, hndl->hosts, hndl->ports,
                         &hndl->master, &hndl->num_hosts,
                         &hndl->num_hosts_sameroom
#if WITH_SSL
                         ,
                         &hndl->s_sslmode
#endif
                         );
        cdb2__dbinforesponse__free_unpacked(dbinfo_response, NULL);

        if (hndl->debug_trace) {
            fprintf(stderr, "td %u host %s:%d type=%d returning 1\n",
                    (uint32_t)pthread_self(), host, __LINE__, type);
        }

#if WITH_SSL
        /* Clear cached SSL sessions - Hosts may have changed. */
        if (hndl->sess_list != NULL) {
            cdb2_ssl_sess_list *sl = hndl->sess_list;
            for (int i = 0; i != sl->n; ++i)
                SSL_SESSION_free(sl->list[i].sess);
            free(sl->list);
            sl->list = NULL;
        }
#endif
        return 1;
    }
    if (hndl->first_buf != NULL) {
        hndl->firstresponse =
            cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
    } else {
        fprintf(stderr, "td %u %s:%d: Can't read response from DB\n",
                (uint32_t)pthread_self(), __func__, __LINE__);
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        return 1;
    }
    while ((rc = cdb2_next_record_int(hndl, 0)) == CDB2_OK)
        ;

    if (hndl->sb == NULL) {
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u %s:%d: sb is NULL, next_record "
                            "returns %d, returning 1\n",
                    (uint32_t)pthread_self(), __func__, __LINE__, rc);
        }
        return 1;
    }

    cdb2_query_list *item = hndl->query_list;
    int i = 0;
    while (item != NULL) { /* Send all but the last query. */

        /* This is the case when we got disconnected while reading the
           query.
           In that case retry all the queries and skip their results,
           except the last one. */
        if (run_last == 0 && item->next == NULL)
            break;

        struct newsqlheader hdr;
        hdr.type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY);
        hdr.compression = ntohl(0);
        hdr.length = ntohl(item->len);
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u %s:%d resending '%s' to %s\n",
                    (uint32_t)pthread_self(), __func__, __LINE__, item->sql,
                    host);
        }
        sbuf2write((char *)&hdr, sizeof(hdr), hndl->sb);
        sbuf2write((char *)item->buf, item->len, hndl->sb);
        sbuf2flush(hndl->sb);

        clear_responses(hndl);

        int len = 0;
        i++;

        if (!hndl->read_intrans_results && !item->is_read) {
            item = item->next;
            continue;
        }
        /* This is for select queries, we send just the last row. */
        rc = cdb2_read_record(hndl, &hndl->first_buf, &len, NULL);
        if (rc) {
            if (hndl->debug_trace) {
                fprintf(stderr, "td %u %s:%d: Can't read response "
                                "from the db node %s\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, host);
            }
            sbuf2close(hndl->sb);
            hndl->sb = NULL;
            return 1;
        }
        if (hndl->first_buf != NULL) {
            hndl->firstresponse =
                cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
        } else {
            if (hndl->debug_trace) {
                fprintf(stderr, "td %u %s:%d: Can't read response "
                                "from the db node %s\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, host);
            }
            sbuf2close(hndl->sb);
            hndl->sb = NULL;
            return 1;
        }
        int num_read = 0;
        int read_rc;

        while ((read_rc = cdb2_next_record_int(hndl, 0)) == CDB2_OK) {
            num_read++;
        }

        if (hndl->sb == NULL) {
            if (hndl->debug_trace) {
                fprintf(stderr,
                        "td %u %s:%d: sb is NULL, next_record_int returns "
                        "%d, returning 1\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, read_rc);
            }
            return 1;
        }

        item = item->next;
    }
    clear_responses(hndl);
    return 0;
}

static int retry_queries_and_skip(cdb2_hndl_tp *hndl, int num_retry,
                                  int skip_nrows)
{
    if (hndl->debug_trace) {
        fprintf(stderr, "td %p %s line %d num_retry=%d, skip_nrows=%d\n",
                (void *)pthread_self(), __func__, __LINE__, num_retry,
                skip_nrows);
    }

    int rc = 0, len;
    if (!(hndl->snapshot_file))
        return -1;

    hndl->retry_all = 1;

    if (hndl->in_trans) {
        rc = retry_query_list(hndl, num_retry, 0);
        if (rc) {
            PRINT_RETURN_OK(rc);
        }
    }
    hndl->is_retry = num_retry;

    rc = cdb2_send_query(hndl, hndl->sb, hndl->dbname, hndl->sql,
                         hndl->num_set_commands, hndl->num_set_commands_sent,
                         hndl->commands, hndl->n_bindvars, hndl->bindvars,
                         hndl->ntypes, hndl->types, 0, skip_nrows, num_retry, 0,
                         __LINE__);
    if (rc) {
        PRINT_RETURN_OK(rc);
    }

    rc = cdb2_read_record(hndl, &hndl->first_buf, &len, NULL);

    if (rc) {
        PRINT_RETURN_OK(rc);
    }

    if (hndl->first_buf != NULL) {
        hndl->firstresponse =
            cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
    }

    PRINT_RETURN_OK(rc);
}

static int is_hasql(const char *set_command, int *value)
{
    const char *p = &set_command[0];

    assert(strncasecmp(p, "set", 3) == 0);

    while (*p && *p != ' ')
        p++;

    while (*p && *p == ' ')
        p++;

    if (strncasecmp(p, "hasql", 5))
        return 0;

    while (*p && *p != ' ')
        p++;

    while (*p && *p == ' ')
        p++;

    if (!strncasecmp(p, "on", 2))
        *value = 1;
    else
        *value = 0;

    return 1;
}

#if WITH_SSL
/*
 *  0 - Processed an SSL set
 * >0 - Failed to process an SSL set
 * <0 - Not an SSL set
 */
static int process_ssl_set_command(cdb2_hndl_tp *hndl, const char *cmd)
{
    int rc = 0;
    const char *p = &cmd[sizeof("SET") - 1];
    p = cdb2_skipws(p);

    if (strncasecmp(p, "SSL_MODE", sizeof("SSL_MODE") - 1) == 0) {
        p += sizeof("SSL_MODE");
        p = cdb2_skipws(p);
        hndl->c_sslmode = ssl_string_to_mode(p, &hndl->nid_dbname);
    } else if (strncasecmp(p, SSL_CERT_PATH_OPT,
                           sizeof(SSL_CERT_PATH_OPT) - 1) == 0) {
        p += sizeof(SSL_CERT_PATH_OPT);
        p = cdb2_skipws(p);
        free(hndl->sslpath);
        hndl->sslpath = strdup(p);
        if (hndl->sslpath == NULL)
            rc = ENOMEM;
    } else if (strncasecmp(p, SSL_CERT_OPT, sizeof(SSL_CERT_OPT) - 1) == 0) {
        p += sizeof(SSL_CERT_OPT);
        p = cdb2_skipws(p);
        free(hndl->cert);
        hndl->cert = strdup(p);
        if (hndl->cert == NULL)
            rc = ENOMEM;
    } else if (strncasecmp(p, SSL_KEY_OPT, sizeof(SSL_KEY_OPT) - 1) == 0) {
        p += sizeof(SSL_KEY_OPT);
        p = cdb2_skipws(p);
        free(hndl->key);
        hndl->key = strdup(p);
        if (hndl->key == NULL)
            rc = ENOMEM;
    } else if (strncasecmp(p, SSL_CA_OPT, sizeof(SSL_CA_OPT) - 1) == 0) {
        p += sizeof(SSL_CA_OPT);
        p = cdb2_skipws(p);
        free(hndl->ca);
        hndl->ca = strdup(p);
        if (hndl->ca == NULL)
            rc = ENOMEM;
#if HAVE_CRL
    } else if (strncasecmp(p, SSL_CRL_OPT, sizeof(SSL_CRL_OPT) - 1) == 0) {
        p += sizeof(SSL_CRL_OPT);
        p = cdb2_skipws(p);
        free(hndl->crl);
        hndl->crl = strdup(p);
        if (hndl->crl == NULL)
            rc = ENOMEM;
#endif /* HAVE_CRL */
    } else if (strncasecmp(p, "SSL_SESSION_CACHE",
                           sizeof("SSL_SESSION_CACHE") - 1) == 0) {
        p += sizeof("SSL_SESSION_CACHE");
        p = cdb2_skipws(p);
        hndl->cache_ssl_sess = (strncasecmp(p, "ON", 2) == 0);
        if (hndl->cache_ssl_sess)
            cdb2_set_ssl_sessions(hndl, cdb2_get_ssl_sessions(hndl));
    } else {
        rc = -1;
    }

    if (rc == 0) {
        /* Reset ssl error flag. */
        hndl->sslerr = 0;
        /* Refresh connection if SSL config has changed. */
        if (hndl->sb != NULL) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
            hndl->sb = NULL;
        }
    }

    return rc;
}
#endif /* WITH_SSL */

static inline void cleanup_query_list(cdb2_hndl_tp *hndl,
                                      cdb2_query_list *commit_query_list,
                                      int line)
{
    if (hndl->debug_trace && line)
        fprintf(stderr, "%s:%d called from line %d\n", __func__, __LINE__,
                line);
    hndl->read_intrans_results = 1;
    hndl->snapshot_file = 0;
    hndl->snapshot_offset = 0;
    hndl->is_retry = 0;
    hndl->error_in_trans = 0;
    hndl->in_trans = 0;
    if (hndl->debug_trace) {
        fprintf(stderr, "td %u %s:%d setting in_trans to 0\n",
                (uint32_t)pthread_self(), __func__, __LINE__);
    }

    cdb2_query_list *item = hndl->query_list;
    while (item != NULL) {
        cdb2_query_list *ditem = item;
        item = item->next;
        free(ditem->sql);
        free(ditem->buf);
        free(ditem);
    }

    item = commit_query_list;
    while (item != NULL) {
        cdb2_query_list *ditem = item;
        item = item->next;
        free(ditem->sql);
        free(ditem->buf);
        free(ditem);
    }

    hndl->query_list = NULL;
}

static inline void clear_snapshot_info(cdb2_hndl_tp *hndl, int line)
{
    hndl->clear_snap_line = line;
    hndl->snapshot_file = 0;
    hndl->snapshot_offset = 0;
    hndl->is_retry = 0;
}

static int process_set_command(cdb2_hndl_tp *hndl, const char *sql)
{
    int i, j, k;

    if (hndl->in_trans) {
        sprintf(hndl->errstr, "Can't run set query inside transaction.");
        hndl->error_in_trans = CDB2ERR_BADREQ;
        hndl->client_side_error = 1;
        return CDB2ERR_BADREQ;
    }

#if WITH_SSL
    int rc = process_ssl_set_command(hndl, sql);
    if (rc >= 0)
        return rc;
#endif

    i = hndl->num_set_commands;
    if (i > 0) {
        int skip_len = 4;
        char *dup_sql = strdup(sql + skip_len);
        char *rest;
        char *set_tok = strtok_r(dup_sql, " ", &rest);
        /* special case for spversion */
        if (set_tok && strcasecmp(set_tok, "spversion") == 0) {
            skip_len += 10;
            set_tok = strtok_r(rest, " ", &rest);
        }
        if (!set_tok) {
            free(dup_sql);
            return 0;
        }
        int len = strlen(set_tok);

        for (j = 0; j < i; j++) {
            /* If this matches any of the previous commands. */
            if ((strncasecmp(&hndl->commands[j][skip_len], set_tok, len) ==
                 0) &&
                (hndl->commands[j][len + skip_len] == ' ')) {
                free(dup_sql);
                if (j == (i - 1)) {
                    if (strcmp(hndl->commands[j], sql) == 0) {
                        /* Do Nothing. */
                    } else {
                        hndl->commands[i - 1] =
                            realloc(hndl->commands[i - 1], strlen(sql) + 1);
                        strcpy(hndl->commands[i - 1], sql);
                    }
                } else {
                    char *cmd = hndl->commands[j];
                    /* Move all the commands down the array. */
                    for (k = j; k < i - 1; k++) {
                        hndl->commands[k] = hndl->commands[k + 1];
                    }
                    if (strcmp(cmd, sql) == 0) {
                        hndl->commands[i - 1] = cmd;
                    } else {
                        hndl->commands[i - 1] = realloc(cmd, strlen(sql) + 1);
                        strcpy(hndl->commands[i - 1], sql);
                    }
                }
                if (hndl->num_set_commands_sent)
                    hndl->num_set_commands_sent--;
                return 0;
            }
        }
        free(dup_sql);
    }
    hndl->num_set_commands++;
    hndl->commands =
        realloc(hndl->commands, sizeof(char *) * hndl->num_set_commands);
    hndl->commands[i] = malloc(strlen(sql) + 1);
    strcpy(hndl->commands[i], sql);
    int hasql_val;
    if (is_hasql(sql, &hasql_val)) {
        hndl->is_hasql = hasql_val;
    }
    return 0;
}

static inline void consume_previous_query(cdb2_hndl_tp *hndl)
{
    while (cdb2_next_record_int(hndl, 0) == CDB2_OK)
        ;

    clear_responses(hndl);
    hndl->rows_read = 0;
}

#define GOTO_RETRY_QUERIES()                                                   \
    do {                                                                       \
        if (hndl->debug_trace) {                                               \
            fprintf(stderr, "td %u %s:%d goto retry_queries\n",                \
                    (uint32_t)pthread_self(), __func__, __LINE__);             \
        }                                                                      \
        goto retry_queries;                                                    \
    } while (0);

static int cdb2_run_statement_typed_int(cdb2_hndl_tp *hndl, const char *sql,
                                        int ntypes, int *types, int line)
{
    int return_value;
    int using_hint = 0;
    int rc = 0;
    int is_begin = 0;
    int is_commit = 0;
    int is_hasql_commit = 0;
    int commit_file = 0;
    int commit_offset = 0;
    int commit_is_retry = 0;
    cdb2_query_list *commit_query_list = NULL;
    int is_rollback = 0;
    int retries_done = 0;

    if (hndl->debug_trace) {
        fprintf(stderr, "%s running '%s' from line %d\n", __func__, sql, line);
    }

    consume_previous_query(hndl);
    if (!sql)
        return 0;

    /* sniff out 'set hasql on' here */
    if (strncasecmp(sql, "set", 3) == 0) {
        return process_set_command(hndl, sql);
    }

    if (strncasecmp(sql, "begin", 5) == 0) {
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u:%d setting is_begin flag\n",
                    (uint32_t)pthread_self(), __LINE__);
        }
        is_begin = 1;
    } else if (strncasecmp(sql, "commit", 6) == 0) {
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u:%d setting is_commit flag\n",
                    (uint32_t)pthread_self(), __LINE__);
        }
        is_commit = 1;
    } else if (strncasecmp(sql, "rollback", 8) == 0) {
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u:%d setting is_commit & is_rollback "
                            "flag for rollback\n",
                    (uint32_t)pthread_self(), __LINE__);
        }
        is_commit = 1;
        is_rollback = 1;
    }
    if (hndl->client_side_error == 1 && hndl->in_trans) {
        if (!is_commit)
            return hndl->error_in_trans;
        else
            sql = "rollback";
    }

    if ((is_begin && hndl->in_trans) || (is_commit && !hndl->in_trans)) {
        if (hndl->debug_trace) {
            if (is_commit && !hndl->in_trans) {
                fprintf(
                    stderr,
                    "XXX td %u line %d: i am committing but not 'in-trans'\n",
                    (uint32_t)pthread_self(), __LINE__);
            } else {
                fprintf(stderr,
                        "XXX td %u line %d i am beginning but not 'in-trans'\n",
                        (uint32_t)pthread_self(), __LINE__);
            }
        }

        sprintf(hndl->errstr, "Wrong sql handle state");
        PRINT_RETURN(CDB2ERR_BADSTATE);
    }

    hndl->is_read = is_sql_read(sql);
    struct timeval tv;
    gettimeofday(&tv, NULL);
    hndl->timestampus = ((uint64_t)tv.tv_sec) * 1000000 + tv.tv_usec;

    if (hndl->use_hint) {
        if (hndl->query && (strcmp(hndl->query, sql) == 0)) {
            sql = hndl->hint;
            using_hint = 1;
        } else {

            if (hndl->query) {
                free(hndl->query);
                hndl->query = NULL;
            }

            if (hndl->query_hint) {
                free(hndl->query_hint);
                hndl->query_hint = NULL;
            }

            if (hndl->hint) {
                free(hndl->hint);
                hndl->hint = NULL;
            }

            int len = strlen(sql);
            if (len > 100) {
                hndl->query = malloc(len + 1);
                strcpy(hndl->query, sql);

                char c_hint[128];
                int length;
                make_random_str(c_hint, sizeof(c_hint), &length);

                cdb2_query_with_hint(hndl, sql, c_hint, &hndl->hint,
                                     &hndl->query_hint);

                sql = hndl->query_hint;
            }
        }
    }

    if (!hndl->in_trans) { /* only one cnonce for a transaction. */
        clear_snapshot_info(hndl, __LINE__);
        make_random_str(hndl->cnonce, MAX_CNONCE_LEN, &hndl->cnonce_len);
    }
    hndl->retry_all = 1;
    int run_last = 1;

retry_queries:
    if (hndl->debug_trace) {
        fprintf(stderr, "td %u %s:%d retry_queries: hndl->host=%d (%s)\n",
                (uint32_t)pthread_self(), __func__, __LINE__,
                hndl->connected_host,
                (hndl->connected_host >= 0 ? hndl->hosts[hndl->connected_host]
                                           : ""));
    }

    hndl->first_record_read = 0;

    retries_done++;

#if WITH_SSL
    if (hndl->sslerr != 0)
        PRINT_RETURN(CDB2ERR_CONNECT_ERROR);
#endif

    if (retries_done > hndl->max_retries) {
        sprintf(hndl->errstr, "%s: Maximum number of retries done.", __func__);
        if (is_hasql_commit) {
            cleanup_query_list(hndl, commit_query_list, __LINE__);
        }
        if (is_begin) {
            hndl->in_trans = 0;
        }
        PRINT_RETURN(CDB2ERR_TRAN_IO_ERROR);
    }

    if (!hndl->sb) {
        if (is_rollback) {
            cleanup_query_list(hndl, NULL, __LINE__);
            if (hndl->debug_trace) {
                fprintf(stderr,
                        "td %u %s:%d returning 0 on unconnected rollback\n",
                        (uint32_t)pthread_self(), __func__, __LINE__);
            }
            PRINT_RETURN(0);
        }

        if (retries_done > hndl->num_hosts) {
            if (!hndl->is_hasql && (retries_done > hndl->min_retries)) {
                if (hndl->debug_trace) {
                    fprintf(stderr, "td %u %s:%d returning cannot-connect, "
                                    "retries_done=%d, num_hosts=%d\n",
                            (uint32_t)pthread_self(), __func__, __LINE__,
                            retries_done, hndl->num_hosts);
                }
                sprintf(hndl->errstr, "%s: Cannot connect to db", __func__);
                PRINT_RETURN(CDB2ERR_CONNECT_ERROR);
            }

            int tmsec = (retries_done - hndl->num_hosts) * 100;
            if (tmsec >= 1000) {
                tmsec = 1000;
                if (!hndl->debug_trace) {
                    fprintf(stderr, "%s: cannot connect: sleep on retry\n", 
                            __func__);
                }
            }

            if (hndl->debug_trace) {
                fprintf(stderr, "td %u %s:%d polling for %d ms\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, tmsec);
            }

            poll(NULL, 0, tmsec);
        }
        cdb2_connect_sqlhost(hndl);
        if (hndl->sb == NULL) {
            if (hndl->debug_trace) {
                fprintf(stderr,
                        "td %u %s:%d rc=%d goto retry_queries on connect "
                        "failure\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, rc);
            }
            goto retry_queries;
        }
        if (!is_begin) {
            hndl->retry_all = 1;
            rc = retry_query_list(hndl, (retries_done - 1), run_last);
            if (rc > 0) {
                newsql_disconnect(hndl, hndl->sb, __LINE__);
                hndl->retry_all = 1;
                if (hndl->debug_trace) {
                    fprintf(stderr, "td %u %s:%d rc=%d goto retry_queries\n",
                            (uint32_t)pthread_self(), __func__, __LINE__, rc);
                }
                goto retry_queries;
            }
            else if (rc < 0) {
                sprintf(hndl->errstr, "Can't retry query to db");
                PRINT_RETURN(rc);
            }
        }
    }

    hndl->sql = (char *)sql;
    hndl->ntypes = ntypes;
    hndl->types = types;

    if (!hndl->in_trans || is_begin) {
        hndl->query_no = 0;
        rc = cdb2_send_query(
            hndl, hndl->sb, hndl->dbname, (char *)sql, hndl->num_set_commands,
            hndl->num_set_commands_sent, hndl->commands, hndl->n_bindvars,
            hndl->bindvars, ntypes, types, is_begin, 0, retries_done - 1,
            is_begin ? 0 : run_last, __LINE__);
    } else {
        hndl->query_no += run_last;
        rc = cdb2_send_query(hndl, hndl->sb, hndl->dbname, (char *)sql, 0, 0,
                             NULL, hndl->n_bindvars, hndl->bindvars, ntypes,
                             types, 0, 0, 0, run_last, __LINE__);
        if (rc != 0) 
            hndl->query_no -= run_last;
    }
    if (rc != 0) {
        sprintf(hndl->errstr, "%s: Can't send query to the db", __func__);
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        hndl->retry_all = 1;
        GOTO_RETRY_QUERIES();
    }
    run_last = 0;

    int len;
    int type = 0;
    int err_val = hndl->error_in_trans;
    int read_intrans_results = hndl->read_intrans_results;

    if (is_rollback || is_commit) {
        if (is_commit && hndl->snapshot_file) {
            commit_file = hndl->snapshot_file;
            commit_offset = hndl->snapshot_offset;
            commit_is_retry = hndl->is_retry;
            commit_query_list = hndl->query_list;
            hndl->query_list = NULL;
            is_hasql_commit = 1;
        }
        hndl->read_intrans_results = 1;
        clear_snapshot_info(hndl, __LINE__);
        hndl->error_in_trans = 0;
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u %s:%d setting in_trans to 0\n",
                    (uint32_t)pthread_self(), __func__, __LINE__);
        }
        hndl->in_trans = 0;

        cdb2_query_list *item = hndl->query_list;
        while (item != NULL) {
            cdb2_query_list *ditem = item;
            item = item->next;
            free(ditem->sql);
            free(ditem->buf);
            free(ditem);
        }
        hndl->query_list = NULL;

        if (!read_intrans_results && !hndl->client_side_error) {
            if (err_val) {
                if (is_rollback) {
                    PRINT_RETURN(0);
                } else {
                    PRINT_RETURN(err_val);
                }
            }
        } else if (err_val) {
            hndl->client_side_error = 0;
            /* With read_intrans_results on, we need to read the 1st response
               of commit/rollback even if there is an in-trans error. */
            goto read_record;
        }
    }

    if (err_val) {
        PRINT_RETURN(err_val);
    }

    if (!hndl->read_intrans_results && !hndl->is_read && hndl->in_trans) {
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u %s:%d in_trans=%d is_hasql=%d\n",
                    (uint32_t)pthread_self(), __func__, __LINE__,
                    hndl->in_trans, hndl->is_hasql);
        }
        return (0);
    }

read_record:

    rc = cdb2_read_record(hndl, &hndl->first_buf, &len, &type);
    if (hndl->debug_trace) {
        char *host = "NOT-CONNECTED";
        if (hndl && hndl->connected_host >= 0)
            host = hndl->hosts[hndl->connected_host];
        fprintf(stderr, "td %p line %d reading from %s rc=%d type:%d\n",
                (void *)pthread_self(), __LINE__, host, rc, type);
    }

    if (type == RESPONSE_HEADER__SQL_RESPONSE_SSL) {
#if WITH_SSL
        hndl->s_sslmode = PEER_SSL_REQUIRE;
        /* server wants us to use ssl so turn ssl on in same connection */
        try_ssl(hndl, hndl->sb, hndl->connected_host);

        /* Decrement retry counter: It is not a real retry. */
        --retries_done;
        GOTO_RETRY_QUERIES();
#else
        sprintf(hndl->errstr, "%s: The database requires SSL connections.",
                __func__);
        PRINT_RETURN(-1);
#endif
    }

    /* Dbinfo .. go to new node */
    if (type == RESPONSE_HEADER__DBINFO_RESPONSE) {
        if (hndl->flags & CDB2_DIRECT_CPU) {
            /* direct cpu should not do anything with dbinfo, just retry */
            GOTO_RETRY_QUERIES();
        }
        /* We got back info about nodes that might be coherent. */
        CDB2DBINFORESPONSE *dbinfo_resp = NULL;
        dbinfo_resp = cdb2__dbinforesponse__unpack(NULL, len, hndl->first_buf);
        parse_dbresponse(dbinfo_resp, hndl->hosts, hndl->ports, &hndl->master,
                         &hndl->num_hosts, &hndl->num_hosts_sameroom
#if WITH_SSL
                         ,
                         &hndl->s_sslmode
#endif
                         );
        cdb2__dbinforesponse__free_unpacked(dbinfo_resp, NULL);

        newsql_disconnect(hndl, hndl->sb, __LINE__);
        hndl->connected_host = -1;
        hndl->retry_all = 1;

#if WITH_SSL
        /* Clear cached SSL sessions - Hosts may have changed. */
        if (hndl->sess_list != NULL) {
            cdb2_ssl_sess_list *sl = hndl->sess_list;
            for (int i = 0; i != sl->n; ++i)
                SSL_SESSION_free(sl->list[i].sess);
            free(sl->list);
            sl->list = NULL;
        }
#endif

        GOTO_RETRY_QUERIES();
    }

    if (rc) {
        if (err_val) {
            /* we get here because skip feature is off
               and the sql is either commit or rollback.
               don't retry because the transaction would
               fail anyway. Also if the sql is rollback,
               suppress any error. */
            if (is_rollback) {
                PRINT_RETURN(0);
            } else if (is_retryable(hndl, err_val) &&
                       (hndl->snapshot_file ||
                        (!hndl->in_trans && !is_commit) || commit_file)) {
                hndl->error_in_trans = 0;
                newsql_disconnect(hndl, hndl->sb, __LINE__);
                hndl->retry_all=1;
                if (commit_file) {
                    if (hndl->debug_trace) {
                        fprintf(stderr,
                                "td %u:%d: i am retrying, retries_done %d\n",
                                (uint32_t)pthread_self(), __LINE__,
                                retries_done);
                        fprintf(stderr, "td %u %s:%d setting in_trans to 1\n",
                                (uint32_t)pthread_self(), __func__, __LINE__);
                    }
                    hndl->in_trans = 1;
                    hndl->snapshot_file = commit_file;
                    hndl->snapshot_offset = commit_offset;
                    hndl->is_retry = commit_is_retry;
                    hndl->query_list = commit_query_list;
                    commit_query_list = NULL;
                    commit_file = 0;
                }
                if (hndl->debug_trace) {
                    fprintf(stderr, "td %u %s:%d goto retry_queries "
                                    "err_val=%d\n",
                            (uint32_t)pthread_self(), __func__, __LINE__,
                            err_val);
                }
                goto retry_queries;
            } else {
                if (is_commit) {
                    cleanup_query_list(hndl, commit_query_list, __LINE__);
                }
                sprintf(hndl->errstr,
                        "%s: Timeout while reading response from server", __func__);
                PRINT_RETURN(err_val);
            }
        }

        if (!is_commit || hndl->snapshot_file) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
            hndl->sb = NULL;
            hndl->retry_all = 1;
            if (hndl->debug_trace) {
                fprintf(stderr, "td %u %s:%d goto retry_queries read-record "
                                "rc=%d err_val=%d\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, rc,
                        err_val);
            }
            goto retry_queries;
        }
        newsql_disconnect(hndl, hndl->sb, __LINE__);

        if (hndl->is_hasql || commit_file) {
            if (commit_file) {
                if (hndl->debug_trace) {
                    fprintf(stderr,
                            "td %u:%d: i am retrying, retries_done %d\n",
                            (uint32_t)pthread_self(), __LINE__, retries_done);
                    fprintf(stderr, "td %u %s:%d setting in_trans to 1\n",
                            (uint32_t)pthread_self(), __func__, __LINE__);
                }
                hndl->in_trans = 1;
                hndl->snapshot_file = commit_file;
                hndl->snapshot_offset = commit_offset;
                hndl->is_retry = commit_is_retry;
                hndl->query_list = commit_query_list;
                commit_query_list = NULL;
                commit_file = 0;
            }
            hndl->retry_all = 1;
            if (hndl->debug_trace) {
                fprintf(stderr,
                        "td %u %s:%d goto retry_queries rc=%d, err_val=%d\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, rc,
                        err_val);
            }
            goto retry_queries;
        }

        if (is_hasql_commit) {
            cleanup_query_list(hndl, commit_query_list, __LINE__);
        }
        sprintf(hndl->errstr,
                "%s: Timeout while reading response from server", __func__);
        if (hndl->debug_trace) {
            fprintf(stderr, "%s:%d returning, clear_snap_line is %d\n",
                    __func__, __LINE__, hndl->clear_snap_line);
        }
        PRINT_RETURN(-1);
    }
    if (hndl->first_buf != NULL) {
        hndl->firstresponse =
            cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
        if (err_val) {
            /* we've read the 1st response of commit/rollback.
               that is all we need so simply return here. 
               I dont think we should get here normally */
            if (hndl->debug_trace) {
                fprintf(stderr, "td %u %s:%d: err_val is %d\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, err_val);
            }
            if (is_rollback) {
                PRINT_RETURN(0);
            } else {
                if (is_hasql_commit) {
                    cleanup_query_list(hndl, commit_query_list, __LINE__);
                }
                PRINT_RETURN(err_val);
            }
        }
    } else {
        if (err_val) {
            if (hndl->debug_trace) {
                fprintf(stderr, "td %u %s:%d: err_val is %d on null "
                                "first_buf\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, err_val);
            }

            if (is_rollback) {
                PRINT_RETURN(0);
            } else if (is_retryable(hndl, err_val) &&
                       (hndl->snapshot_file ||
                        (!hndl->in_trans && !is_commit) || commit_file)) {
                hndl->error_in_trans = 0;
                newsql_disconnect(hndl, hndl->sb, __LINE__);
                hndl->retry_all=1;
                if (commit_file) {
                    if (hndl->debug_trace) {
                        fprintf(stderr,
                                "td %u:%d: i am retrying, retries_done %d\n",
                                (uint32_t)pthread_self(), __LINE__,
                                retries_done);
                        fprintf(stderr, "td %u %s:%d setting in_trans to 1\n",
                                (uint32_t)pthread_self(), __func__, __LINE__);
                    }
                    hndl->in_trans = 1;
                    hndl->snapshot_file = commit_file;
                    hndl->snapshot_offset = commit_offset;
                    hndl->is_retry = commit_is_retry;
                    hndl->query_list = commit_query_list;
                    commit_query_list = NULL;
                    commit_file = 0;
                }
                if (hndl->debug_trace) {
                    fprintf(stderr, "td %u %s:%d goto retry_queries "
                                    "err_val=%d\n",
                            (uint32_t)pthread_self(), __func__, __LINE__,
                            err_val);
                }
                goto retry_queries;
            } else {
                if (is_hasql_commit) {
                    cleanup_query_list(hndl, commit_query_list, __LINE__);
                }
                PRINT_RETURN(err_val);
            }
        }
        if (!is_commit || hndl->snapshot_file) {
            if (hndl->debug_trace) {
                fprintf(stderr, "td %u %s:%d: disconnect & retry on null "
                                "first_buf\n",
                        (uint32_t)pthread_self(), __func__, __LINE__);
            }
            newsql_disconnect(hndl, hndl->sb, __LINE__);
            hndl->sb = NULL;
            hndl->retry_all = 1;
            if (hndl->debug_trace) {
                fprintf(stderr, "td %u %s:%d goto retry_queries err_val=%d\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, err_val);
            }
            goto retry_queries;
        }
        /* Changes here to retry commit and goto retry queries. */
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u %s:%d: Can't read response from the db\n",
                    (uint32_t)pthread_self(), __func__, __LINE__);
        }
        sprintf(hndl->errstr, "%s: Can't read response from the db", 
                __func__);
        if (is_hasql_commit) {
            cleanup_query_list(hndl, commit_query_list, __LINE__);
        }
        PRINT_RETURN(-1);
    }

    if (using_hint) {
        if (hndl->firstresponse->error_code ==
                CDB2__ERROR_CODE__PREPARE_ERROR_OLD ||
            hndl->firstresponse->error_code ==
                CDB2__ERROR_CODE__PREPARE_ERROR) {
            sql = hndl->query;
            hndl->retry_all = 1;
            if (hndl->debug_trace) {
                fprintf(stderr,
                        "td %u %s:%d goto retry_queries error_code=%d\n",
                        (uint32_t)pthread_self(), __func__, __LINE__,
                        hndl->firstresponse->error_code);
            }
            goto retry_queries;
        }
    } else if (hndl->firstresponse->error_code == CDB2__ERROR_CODE__WRONG_DB && !hndl->in_trans) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        hndl->sb = NULL;
        hndl->retry_all = 1;
        for (int i = 0; i < hndl->num_hosts; i++) {
            hndl->ports[i] = -1;
        }
        if (retries_done < MAX_RETRIES) {
            GOTO_RETRY_QUERIES();
        }
    }

    if ((hndl->firstresponse->error_code == CDB2__ERROR_CODE__MASTER_TIMEOUT ||
         hndl->firstresponse->error_code == CDB2ERR_CHANGENODE) &&
        (hndl->snapshot_file || (!hndl->in_trans && !is_commit) ||
         commit_file)) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        hndl->sb = NULL;
        hndl->retry_all = 1;
        if (commit_file) {
            if (hndl->debug_trace) {
                fprintf(stderr, "td %u %s:%d setting in_trans to 1\n",
                        (uint32_t)pthread_self(), __func__, __LINE__);
            }
            hndl->in_trans = 1;
            hndl->snapshot_file = commit_file;
            hndl->snapshot_offset = commit_offset;
            hndl->is_retry = commit_is_retry;
            hndl->query_list = commit_query_list;
            commit_query_list = NULL;
            commit_file = 0;
        }
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u %s:%d goto retry_queries error_code=%d\n",
                    (uint32_t)pthread_self(), __func__, __LINE__,
                    hndl->firstresponse->error_code);
        }
        goto retry_queries;
    }

    if (is_begin) {
        if (hndl->debug_trace) {
            fprintf(stderr, "td %u %s:%d setting in_trans to 1\n",
                    (uint32_t)pthread_self(), __func__, __LINE__);
        }
        hndl->in_trans = 1;
    } else if (!is_hasql_commit && (is_rollback || is_commit)) {
        cleanup_query_list(hndl, commit_query_list, __LINE__);
    }

    hndl->node_seq = 0;
    bzero(hndl->hosts_connected, sizeof(hndl->hosts_connected));

    if (is_commit) {
        clear_snapshot_info(hndl, __LINE__);
    }

    if (hndl->firstresponse->response_type == RESPONSE_TYPE__COLUMN_NAMES) {
        /* Handle rejects from Server. */
        if (is_retryable(hndl, hndl->firstresponse->error_code) &&
            (hndl->snapshot_file || (!hndl->in_trans && !is_commit) ||
             commit_file)) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
            hndl->sb = NULL;
            hndl->retry_all = 1;

            if (commit_file) {
                hndl->in_trans = 1;
                hndl->snapshot_file = commit_file;
                hndl->snapshot_offset = commit_offset;
                hndl->is_retry = commit_is_retry;
                hndl->query_list = commit_query_list;
                commit_query_list = NULL;
                commit_file = 0;
            }
            if (hndl->debug_trace) {
                fprintf(stderr,
                        "td %d %s:%d: goto retry_queries error_code=%d\n",
                        (uint32_t)pthread_self(), __func__, __LINE__,
                        hndl->firstresponse->error_code);
            }

            goto retry_queries;
        }

        if (hndl->firstresponse->error_code) {
            if (is_begin) {
                hndl->in_trans = 0;
            } else if (hndl->in_trans) {
                /* Give the same error for every query until commit/rollback */
                hndl->error_in_trans =
                    cdb2_convert_error_code(hndl->firstresponse->error_code);
            }
            return_value =
                cdb2_convert_error_code(hndl->firstresponse->error_code);
            if (is_hasql_commit)
                cleanup_query_list(hndl, commit_query_list, __LINE__);
            PRINT_RETURN(return_value);
        }
        int rc = cdb2_next_record_int(hndl, 1);
        if (rc == CDB2_OK_DONE || rc == CDB2_OK) {
            return_value = 
                cdb2_convert_error_code(hndl->firstresponse->error_code);
            if (is_hasql_commit)
                cleanup_query_list(hndl, commit_query_list, __LINE__);
            PRINT_RETURN(return_value);
        }

        if (hndl->is_hasql && (((is_retryable(hndl, rc) && hndl->snapshot_file) ||
            is_begin) || (!hndl->sb && ((hndl->in_trans && hndl->snapshot_file)
            || commit_file)))) {

            if (hndl->sb)
                sbuf2close(hndl->sb);

            hndl->sb = NULL;

            if (commit_file) {
                if (hndl->debug_trace) {
                    fprintf(stderr, "td %u %s:%d setting in_trans to 1\n",
                            (uint32_t)pthread_self(), __func__, __LINE__);
                }
                hndl->in_trans = 1;
                hndl->snapshot_file = commit_file;
                hndl->snapshot_offset = commit_offset;
                hndl->is_retry = commit_is_retry;
                hndl->query_list = commit_query_list;
                commit_query_list = NULL;
                commit_file = 0;
            }

            hndl->retry_all = 1;

            if (hndl->debug_trace) {
                fprintf(stderr, "td %d %s:%d: goto retry_queries retry-begin, "
                                "error_code=%d\n",
                        (uint32_t)pthread_self(), __func__, __LINE__,
                        hndl->firstresponse->error_code);
            }

            clear_responses(hndl);
            goto retry_queries;
        }

        return_value = cdb2_convert_error_code(rc);

        if (is_hasql_commit)
            cleanup_query_list(hndl, commit_query_list, __LINE__);

        PRINT_RETURN(return_value);
    }

    sprintf(hndl->errstr, "%s: Unknown response type %d", __func__,
            hndl->firstresponse->response_type);
    if (is_hasql_commit)
        cleanup_query_list(hndl, commit_query_list, __LINE__);
    PRINT_RETURN(-1);
}

static char *cdb2_type_str(int type)
{
    switch (type) {
    case CDB2_INTEGER:
        return "CDB2_INTEGER";
    case CDB2_REAL:
        return "CDB2_REAL";
    case CDB2_CSTRING:
        return "CDB2_CSTRING";
    case CDB2_BLOB:
        return "CDB2_BLOB";
    case CDB2_DATETIME:
        return "CDB2_DATETIME";
    case CDB2_INTERVALYM:
        return "CDB2_INTERVALYM";
    case CDB2_INTERVALDS:
        return "CDB2_INTERVALDS";
    case CDB2_DATETIMEUS:
        return "CDB2_DATETIMEUS";
    case CDB2_INTERVALDSUS:
        return "CDB2_INTERVALDSUS";
    default:
        return "???";
    }
}

int cdb2_run_statement_typed(cdb2_hndl_tp *hndl, const char *sql, int ntypes,
                             int *types)
{
    int rc = 0, commit_rc;

    pthread_once(&init_once, do_init_once);

    if (hndl->temp_trans && hndl->in_trans) {
        cdb2_run_statement_typed_int(hndl, "rollback", 0, NULL, __LINE__);
    }

    hndl->temp_trans = 0;

    if (hndl->is_hasql && !hndl->in_trans &&
        (strncasecmp(sql, "set", 3) != 0 && strncasecmp(sql, "begin", 5) != 0 &&
         strncasecmp(sql, "commit", 6) != 0 &&
         strncasecmp(sql, "rollback", 8) != 0)) {
        rc = cdb2_run_statement_typed_int(hndl, "begin", 0, NULL, __LINE__);
        if (rc != 0) {
            return rc;
        }
        hndl->temp_trans = 1;
    }

    sql = cdb2_skipws(sql);
    rc = cdb2_run_statement_typed_int(hndl, sql, ntypes, types, __LINE__);

    // XXX This code does not work correctly for WITH statements
    // (they can be either read or write)
    if (hndl->temp_trans && !is_sql_read(sql)) {
        if (rc == 0) {
            commit_rc =
                cdb2_run_statement_typed_int(hndl, "commit", 0, NULL, __LINE__);
            rc = commit_rc;
        } else {
            cdb2_run_statement_typed_int(hndl, "rollback", 0, NULL, __LINE__);
        }
        hndl->temp_trans = 0;
    }
    if (log_calls) {
        if (ntypes == 0)
            fprintf(stderr, "%p> cdb2_run_statement(%p, \"%s\") = %d\n",
                    (void *)pthread_self(), hndl, sql, rc);
        else {
            fprintf(stderr, "%p> cdb2_run_statement_typed(%p, \"%s\", [",
                    (void *)pthread_self(), hndl, sql);
            for (int i = 0; i < ntypes; i++) {
                fprintf(stderr, "%s%s", cdb2_type_str(types[i]),
                        i == ntypes - 1 ? "" : ", ");
            }
            fprintf(stderr, "] = %d\n", rc);
        }
    }
    return rc;
}

int cdb2_numcolumns(cdb2_hndl_tp *hndl)
{
    int rc;
    pthread_once(&init_once, do_init_once);
    if (hndl->firstresponse == NULL)
        rc = 0;
    else
        rc = hndl->firstresponse->n_value;
    if (log_calls) {
        fprintf(stderr, "%p> cdb2_numcolumns(%p) = %d\n",
                (void *)pthread_self(), hndl, rc);
    }
    return rc;
}

const char *cdb2_column_name(cdb2_hndl_tp *hndl, int col)
{
    const char *ret;
    pthread_once(&init_once, do_init_once);
    if (hndl->firstresponse == NULL)
        ret = NULL;
    else
        ret = (const char *)hndl->firstresponse->value[col]->value.data;
    if (log_calls)
        fprintf(stderr, "%p> cdb2_column_name(%p, %d) = \"%s\"\n",
                (void *)pthread_self(), hndl, col, ret == NULL ? "NULL" : ret);
    return ret;
}

int cdb2_snapshot_file(cdb2_hndl_tp *hndl, int *snapshot_file,
                       int *snapshot_offset)
{
    char *ret;

    if (hndl == NULL) {
        (*snapshot_file) = -1;
        (*snapshot_offset) = -1;
        return -1;
    }

    (*snapshot_file) = hndl->snapshot_file;
    (*snapshot_offset) = hndl->snapshot_offset;
    return 0;
}

void cdb2_getinfo(cdb2_hndl_tp *hndl, int *intrans, int *hasql)
{
    (*intrans) = hndl->in_trans;
    (*hasql) = hndl->is_hasql;
}

void cdb2_set_debug_trace(cdb2_hndl_tp *hndl) 
{ 
    hndl->debug_trace = 1; 
}

void cdb2_dump_ports(cdb2_hndl_tp *hndl, FILE *out)
{
    int i;
    for (i = 0; i < hndl->num_hosts; i++) {
        fprintf(out, "%s %d\n", hndl->hosts[i], hndl->ports[i]);
    }
}

void cdb2_cluster_info(cdb2_hndl_tp *hndl, char **cluster, int *ports, int max,
                       int *count)
{
    int i, target;
    if (count)
        *count = hndl->num_hosts;

    target = (max < hndl->num_hosts ? max : hndl->num_hosts);
    for (i = 0; i < target; i++) {
        if (cluster)
            cluster[i] = strdup(hndl->hosts[i]);
        if (ports)
            (ports[i]) = hndl->ports[i];
    }
}

const char *cdb2_cnonce(cdb2_hndl_tp *hndl)
{
    char *ret;

    if (hndl == NULL)
        return "unallocated cdb2 handle";

    return hndl->cnonce;
}

const char *cdb2_errstr(cdb2_hndl_tp *hndl)
{
    char *ret;

    pthread_once(&init_once, do_init_once);

    if (hndl == NULL)
        ret = "unallocated cdb2 handle";
    else if (hndl->firstresponse == NULL) {
        ret = hndl->errstr;
    } else if (hndl->lastresponse == NULL) {
        ret = hndl->firstresponse->error_string;
    } else {
        ret = hndl->lastresponse->error_string;
    }

    if (!ret)
        ret = hndl->errstr;
    if (log_calls)
        fprintf(stderr, "%p> cdb2_errstr(%p) = \"%s\"\n",
                (void *)pthread_self(), hndl, ret ? ret : "NULL");
    return ret;
}

int cdb2_column_type(cdb2_hndl_tp *hndl, int col)
{
    int ret;
    if (hndl->firstresponse == NULL)
        ret = 0;
    else
        ret = hndl->firstresponse->value[col]->type;
    if (log_calls) {
        fprintf(stderr, "%p> cdb2_column_type(%p, %d) = %s\n",
                (void *)pthread_self(), hndl, col, cdb2_type_str(ret));
    }
    return ret;
}

int cdb2_column_size(cdb2_hndl_tp *hndl, int col)
{
    if (hndl->lastresponse == NULL)
        return -1;
    return hndl->lastresponse->value[col]->value.len;
}

void *cdb2_column_value(cdb2_hndl_tp *hndl, int col)
{
    if (hndl->lastresponse == NULL)
        return NULL;
    if (hndl->lastresponse->value[col]->value.len == 0 &&
        hndl->lastresponse->value[col]->has_isnull != 1 &&
        hndl->lastresponse->value[col]->isnull != 1) {
        return (void *)"";
    }
    return hndl->lastresponse->value[col]->value.data;
}

int cdb2_bind_param(cdb2_hndl_tp *hndl, const char *varname, int type,
                    const void *varaddr, int length)
{
    pthread_once(&init_once, do_init_once);
    hndl->n_bindvars++;
    hndl->bindvars = realloc(hndl->bindvars, sizeof(CDB2SQLQUERY__Bindvalue *) *
                                                 hndl->n_bindvars);
    CDB2SQLQUERY__Bindvalue *bindval = malloc(sizeof(CDB2SQLQUERY__Bindvalue));
    cdb2__sqlquery__bindvalue__init(bindval);
    bindval->type = type;
    bindval->varname = (char *)varname;
    bindval->value.data = (void *)varaddr;
    if (varaddr == NULL) {
        bindval->value.len = 0;
        bindval->has_isnull = 1;
        bindval->isnull = 1;
    } else if (type == CDB2_CSTRING && length == 0) {
        bindval->value.data = (unsigned char *)"";
        bindval->value.len = 1;
    } else if (type == CDB2_BLOB && length == 0) {
        bindval->value.data = (unsigned char *)"";
        bindval->value.len = 0;
        bindval->has_isnull = 1;
        bindval->isnull = 0;
    } else {
        bindval->value.len = length;
    }
    hndl->bindvars[hndl->n_bindvars - 1] = bindval;
    if (log_calls)
        fprintf(stderr, "%p> cdb2_bind_param(%p, \"%s\", %s, %p, %d) = 0\n",
                (void *)pthread_self(), hndl, varname, cdb2_type_str(type),
                varaddr, length);
    return 0;
}

int cdb2_bind_index(cdb2_hndl_tp *hndl, int index, int type,
                    const void *varaddr, int length)
{
    pthread_once(&init_once, do_init_once);
    if (log_calls)
        fprintf(stderr, "%p> cdb2_bind_index(%p, %d, %s, %p, %d)\n",
                (void *)pthread_self(), hndl, index, cdb2_type_str(type),
                varaddr, length);

    if (index <= 0) {
        sprintf(hndl->errstr, "%s: bind index starts at value 1", __func__);
        return -1;
    }
    hndl->n_bindvars++;
    hndl->bindvars = realloc(hndl->bindvars, sizeof(CDB2SQLQUERY__Bindvalue *) *
                                                 hndl->n_bindvars);
    CDB2SQLQUERY__Bindvalue *bindval = malloc(sizeof(CDB2SQLQUERY__Bindvalue));
    cdb2__sqlquery__bindvalue__init(bindval);
    bindval->type = type;
    bindval->varname = NULL;
    bindval->value.data = (void *)varaddr;
    bindval->has_index = 1;
    bindval->index = index;
    if (varaddr == NULL) {
        bindval->value.len = 0;
        bindval->has_isnull = 1;
        bindval->isnull = 1;
    } else if (type == CDB2_CSTRING && length == 0) {
        bindval->value.data = (unsigned char *)"";
        bindval->value.len = 1;
    } else if (type == CDB2_BLOB && length == 0) {
        bindval->value.data = (unsigned char *)"";
        bindval->value.len = 0;
        bindval->has_isnull = 1;
        bindval->isnull = 0;
    } else {
        bindval->value.len = length;
    }
    hndl->bindvars[hndl->n_bindvars - 1] = bindval;

    return 0;
}

int cdb2_clearbindings(cdb2_hndl_tp *hndl)
{
    pthread_once(&init_once, do_init_once);
    if (log_calls)
        fprintf(stderr, "%p> cdb2_clearbindings(%p)\n", (void *)pthread_self(),
                hndl);
    if (hndl->bindvars == NULL)
        return 0;
    for (int i = 0; i < hndl->n_bindvars; i++) {
        free(hndl->bindvars[i]);
    }
    free(hndl->bindvars);
    hndl->bindvars = NULL;
    hndl->n_bindvars = 0;
    return 0;
}

static int comdb2db_get_dbhosts(cdb2_hndl_tp *hndl, const char *comdb2db_name,
                                int comdb2db_num, const char *host, int port,
                                char hosts[][64], int *num_hosts,
                                const char *dbname, char *cluster, int *dbnum,
                                int *num_same_room, int num_retries)
{
    char sql_query[256];
    *dbnum = 0;
    int n_bindvars = 3;
    sprintf(sql_query, "select M.name, D.dbnum, M.room from machines M join "
                       "databases D where M.cluster IN (select cluster_machs "
                       "from clusters where name=@dbname and "
                       "cluster_name=@cluster) and D.name=@dbname order by "
                       "(room = @room) desc");
    CDB2SQLQUERY__Bindvalue **bindvars =
        malloc(sizeof(CDB2SQLQUERY__Bindvalue *) * n_bindvars);
    CDB2SQLQUERY__Bindvalue *bind_dbname =
        malloc(sizeof(CDB2SQLQUERY__Bindvalue));
    CDB2SQLQUERY__Bindvalue *bind_cluster =
        malloc(sizeof(CDB2SQLQUERY__Bindvalue));
    CDB2SQLQUERY__Bindvalue *bind_room =
        malloc(sizeof(CDB2SQLQUERY__Bindvalue));

    cdb2__sqlquery__bindvalue__init(bind_dbname);
    cdb2__sqlquery__bindvalue__init(bind_cluster);
    cdb2__sqlquery__bindvalue__init(bind_room);

    bind_dbname->type = CDB2_CSTRING;
    bind_dbname->varname = "dbname";
    bind_dbname->value.data = (unsigned char *)dbname;
    bind_dbname->value.len = strlen(dbname);

    bind_cluster->type = CDB2_CSTRING;
    bind_cluster->varname = "cluster";
    bind_cluster->value.data = (unsigned char *)cluster;
    bind_cluster->value.len = strlen(cluster);

    bind_room->type = CDB2_CSTRING;
    bind_room->varname = "room";
    bind_room->value.data = (unsigned char *)cdb2_machine_room;
    bind_room->value.len = strlen(cdb2_machine_room);

    bindvars[0] = bind_dbname;
    bindvars[1] = bind_cluster;
    bindvars[2] = bind_room;
    char newsql_typestr[128];
    int is_sockfd = 1;
    int i = 0;

    if (num_same_room)
        *num_same_room = 0;

    int rc = snprintf(newsql_typestr, sizeof(newsql_typestr),
                      "comdb2/%s/%s/newsql/%s", comdb2db_name, cluster,
                      hndl->policy);
    if ((rc < 1 || rc >= sizeof(newsql_typestr)) && hndl->debug_trace) {
        fprintf(stderr,
                "ERROR: can not fit entire string 'comdb2/%s/%s/newsql/%s'\n",
                comdb2db_name, cluster, hndl->policy);
    }

    int fd = cdb2_socket_pool_get(newsql_typestr, comdb2db_num, NULL);
    if (fd < 0) {
        if (!cdb2_allow_pmux_route) {
            fd = cdb2_tcpconnecth_to(host, port, 0, CDB2_CONNECT_TIMEOUT);
        } else {
            fd = cdb2portmux_route(host, "comdb2", "replication", comdb2db_name,
                                   hndl->debug_trace);
        }
        is_sockfd = 0;
    }

    if (fd < 0) {
        i = 0;
        for (i = 0; i < 3; i++) {
            free(bindvars[i]);
        }
        free(bindvars);
        return -1;
    }
    SBUF2 *ss = sbuf2open(fd, 0);
    if (ss == 0) {
        close(fd);
        i = 0;
        for (i = 0; i < n_bindvars; i++) {
            free(bindvars[i]);
        }
        free(bindvars);
        return -1;
    }
    sbuf2settimeout(ss, 5000, 5000);
    if (is_sockfd == 0) {
        sbuf2printf(ss, "newsql\n");
        sbuf2flush(ss);
    } else {
        rc = send_reset(ss);
        if (rc != 0) {
            goto free_vars;
        }
    }
    rc = cdb2_send_query(NULL, ss, comdb2db_name, sql_query, 0, 0, NULL, 3,
                         bindvars, 0, NULL, 0, 0, num_retries, 0, __LINE__);
free_vars:
    i = 0;
    for (i = 0; i < 3; i++) {
        free(bindvars[i]);
    }
    free(bindvars);

    if (rc != 0) {
        sprintf(hndl->errstr, "%s: Can't send query to comdb2db", __func__);
        sbuf2close(ss);
        return -1;
    }
    uint8_t *p = NULL;
    int len;
    CDB2SQLRESPONSE *sqlresponse = NULL;
    cdb2_hndl_tp tmp = {.sb = ss};
    rc = cdb2_read_record(&tmp, &p, &len, NULL);
    if (rc) {
        sbuf2close(ss);
        return -1;
    }
    if ((p != NULL) && (len != 0)) {
        sqlresponse =
            cdb2__sqlresponse__unpack(NULL, len, (const unsigned char *)p);
    }
    if ((len == 0) || (sqlresponse == NULL) || (sqlresponse->error_code != 0) ||
        (sqlresponse->response_type != RESPONSE_TYPE__COLUMN_NAMES &&
         sqlresponse->n_value != 1 && sqlresponse->value[0]->has_type != 1 &&
         sqlresponse->value[0]->type != 3)) {
        sprintf(hndl->errstr,
                "%s: Got bad response for comdb2db query. Reply len: %d",
                __func__, len);
        sbuf2close(ss);
        return -1;
    }

    *num_hosts = 0;
    while (sqlresponse->response_type <= RESPONSE_TYPE__COLUMN_VALUES) {
        cdb2__sqlresponse__free_unpacked(sqlresponse, NULL);
        rc = cdb2_read_record(&tmp, &p, &len, NULL);
        if (rc) {
            sbuf2close(ss);
            return -1;
        }
        if (p != NULL) {
            sqlresponse =
                cdb2__sqlresponse__unpack(NULL, len, (const unsigned char *)p);
        }
        if (sqlresponse->error_code)
            break;
        if (sqlresponse->response_type == RESPONSE_TYPE__COLUMN_VALUES &&
            (sqlresponse->value != NULL)) {
            strcpy(hosts[*num_hosts],
                   (const char *)sqlresponse->value[0]->value.data);
            if (*dbnum == 0) {
                *dbnum = *((long long *)sqlresponse->value[1]->value.data);
            }
            if (num_same_room && sqlresponse->value[2]->value.data &&
                strcasecmp(cdb2_machine_room,
                           sqlresponse->value[2]->value.data) == 0) {
                (*num_same_room)++;
            }
            (*num_hosts)++;
        }
    }
    cdb2__sqlresponse__free_unpacked(sqlresponse, NULL);
    free(p);
    int timeoutms = 10 * 1000;
    cdb2_socket_pool_donate_ext(newsql_typestr, fd, timeoutms / 1000,
                                comdb2db_num, 5, NULL, NULL);

    sbuf2free(ss);
    return 0;
}

/* get dbinfo
 * returns -1 on error
 * returns 0 if number of hosts it finds is > 0
 */
static int cdb2_dbinfo_query(cdb2_hndl_tp *hndl, const char *type,
                             const char *dbname, int dbnum, const char *host,
                             char valid_hosts[][64], int *valid_ports,
                             int *master_node, int *num_valid_hosts,
                             int *num_valid_sameroom_hosts)
{
    char newsql_typestr[128];
    SBUF2 *sb = NULL;

    if (hndl->debug_trace)
        fprintf(stderr, "td %d %s:%d\n", (uint32_t)pthread_self(), __func__,
                __LINE__);

    int rc = snprintf(newsql_typestr, sizeof(newsql_typestr),
                      "comdb2/%s/%s/newsql/%s", dbname, type, hndl->policy);
    if (rc < 1 || rc >= sizeof(newsql_typestr)) {
        if (hndl->debug_trace)
            fprintf(
                stderr,
                "ERROR: can not fit entire string 'comdb2/%s/%s/newsql/%s'\n",
                dbname, type, hndl->policy);
        return -1;
    }
    int port = 0;
    int fd = cdb2_socket_pool_get(newsql_typestr, dbnum, NULL);
    if (hndl->debug_trace)
        fprintf(stderr, "td %d %s:%d, cdb2_socket_pool_get fd %d, host '%s'\n",
                (uint32_t)pthread_self(), __func__, __LINE__, fd, host);
    if (fd < 0) {
        if (host == NULL)
            return -1;

        if (!cdb2_allow_pmux_route) {
            if (!port) {
                port = cdb2portmux_get(host, "comdb2", "replication", dbname,
                                       hndl->debug_trace);
                if (hndl->debug_trace)
                    fprintf(stderr, "cdb2portmux_get port=%d'\n", port);
            }
            if (port < 0)
                return -1;
            fd = cdb2_tcpconnecth_to(host, port, 0, CDB2_CONNECT_TIMEOUT);
        } else {
            fd = cdb2portmux_route(host, "comdb2", "replication", dbname,
                                   hndl->debug_trace);
            if (hndl->debug_trace)
                fprintf(stderr, "cdb2portmux_route fd=%d'\n", fd);
        }
        if (fd < 0)
            return -1;
        sb = sbuf2open(fd, 0);
        if (sb == 0) {
            close(fd);
            return -1;
        }
        sbuf2printf(sb, "newsql\n");
        sbuf2flush(sb);
    } else {
        sb = sbuf2open(fd, 0);
        if (sb == 0) {
            close(fd);
            return -1;
        }
    }

    sbuf2settimeout(sb, COMDB2DB_TIMEOUT, COMDB2DB_TIMEOUT);

    CDB2QUERY query = CDB2__QUERY__INIT;

    CDB2DBINFO dbinfoquery = CDB2__DBINFO__INIT;
    dbinfoquery.dbname = (char *)dbname;
    query.dbinfo = &dbinfoquery;

    int len = cdb2__query__get_packed_size(&query);
    unsigned char *buf = malloc(len + 1);
    cdb2__query__pack(&query, buf);

    struct newsqlheader hdr;

    hdr.type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY);
    hdr.compression = ntohl(0);
    hdr.length = ntohl(len);

    sbuf2write((char *)&hdr, sizeof(hdr), sb);
    sbuf2write((char *)buf, len, sb);

    sbuf2flush(sb);
    free(buf);

    rc = sbuf2fread((char *)&hdr, 1, sizeof(hdr), sb);
    if (rc != sizeof(hdr)) {
        sbuf2close(sb);
        return -1;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.length = ntohl(hdr.length);

    char *p = malloc(hdr.length);
    if (!p) {
        sprintf(hndl->errstr, "%s: out of memory", __func__);
        sbuf2close(sb);
        return -1;
    }

    rc = sbuf2fread(p, 1, hdr.length, sb);
    if (rc != hdr.length) {
        sbuf2close(sb);
        free(p);
        return -1;
    }
    CDB2DBINFORESPONSE *dbinfo_response = cdb2__dbinforesponse__unpack(
        NULL, hdr.length, (const unsigned char *)p);

    if (dbinfo_response == NULL) {
        sprintf(hndl->errstr, "%s: Got no dbinfo response from comdb2 database",
                __func__);
        sbuf2close(sb);
        free(p);
        return -1;
    }

    parse_dbresponse(dbinfo_response, valid_hosts, valid_ports, master_node,
                     num_valid_hosts, num_valid_sameroom_hosts
#if WITH_SSL
                     , &hndl->s_sslmode
#endif
                     );

    cdb2__dbinforesponse__free_unpacked(dbinfo_response, NULL);

    free(p);

    int timeoutms = 10 * 1000;

    cdb2_socket_pool_donate_ext(newsql_typestr, fd, timeoutms / 1000, dbnum, 5,
                                NULL, NULL);

    sbuf2free(sb);
    if ((*num_valid_hosts) > 0)
        return 0;

    return -1;
}

static inline void only_read_config()
{
    read_available_comdb2db_configs(NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                    NULL, NULL, NULL, NULL);
}

static int cdb2_get_dbhosts(cdb2_hndl_tp *hndl)
{
    char comdb2db_hosts[MAX_NODES][64];
    int comdb2db_ports[MAX_NODES];
    int num_comdb2db_hosts;
    int master = -1, rc = 0;
    int num_retry = 0;
    int comdb2db_num = COMDB2DB_NUM;
    char comdb2db_name[32] = COMDB2DB;

    if (hndl->debug_trace)
        fprintf(stderr, "td %d %s:%d\n", (uint32_t)pthread_self(), __func__,
                __LINE__);

    /* Try dbinfo query without any host info. */
    if (cdb2_dbinfo_query(hndl, hndl->type, hndl->dbname, hndl->dbnum, NULL,
                          hndl->hosts, hndl->ports, &hndl->master,
                          &hndl->num_hosts, &hndl->num_hosts_sameroom) == 0) {
        /* We get a plaintext socket from sockpool.
           We still need to read SSL config */
        only_read_config();
        return 0;
    }

    get_comdb2db_hosts(hndl, comdb2db_hosts, comdb2db_ports, &master,
                       comdb2db_name, &num_comdb2db_hosts, &comdb2db_num,
                       hndl->dbname, hndl->cluster, hndl->hosts,
                       &(hndl->num_hosts), &hndl->dbnum, 1);

    if ((cdb2_default_cluster[0] != '\0') && (cdb2_comdb2dbname[0] != '\0')) {
        strcpy(comdb2db_name, cdb2_comdb2dbname);
    }

    if (strcasecmp(hndl->cluster, "default") == 0) {
        if (cdb2_default_cluster[0] == '\0') {
            sprintf(hndl->errstr, "cdb2_get_dbhosts: no default_type "
                                  "entry in comdb2db config.");
            return -1;
        }
        strncpy(hndl->cluster, cdb2_default_cluster, sizeof(hndl->cluster) - 1);
    }

    if (strcasecmp(hndl->cluster, "local") == 0) {
        hndl->num_hosts = 1;
        strcpy(hndl->hosts[0], "localhost");
        hndl->ports[0] = cdb2portmux_get("localhost", "comdb2", "replication",
                                         hndl->dbname, hndl->debug_trace);
        hndl->flags |= CDB2_DIRECT_CPU;
    } else {
        rc = get_comdb2db_hosts(
            hndl, comdb2db_hosts, comdb2db_ports, &master, comdb2db_name,
            &num_comdb2db_hosts, &comdb2db_num, hndl->dbname, hndl->cluster,
            hndl->hosts, &(hndl->num_hosts), &hndl->dbnum, 0);
        if (rc != 0 || (num_comdb2db_hosts == 0 && hndl->num_hosts == 0)) {
            sprintf(hndl->errstr, "cdb2_get_dbhosts: no %s hosts found.",
                    comdb2db_name);
            return -1;
        }
    }

retry:
    if (rc) {
        if (num_retry >= MAX_RETRIES)
            return rc;

        num_retry++;
        poll(NULL, 0, 250); // Sleep for 250ms everytime and total of 5 seconds
        rc = 0;
    }
    if (hndl->debug_trace)
        fprintf(stderr, "td %d %s:%d: num_retry=%d hndl->num_hosts=%d "
                        "num_comdb2db_hosts=%d\n",
                (uint32_t)pthread_self(), __func__, __LINE__, num_retry,
                hndl->num_hosts, num_comdb2db_hosts);

    if (hndl->num_hosts == 0) {
        if (master == -1) {
            for (int i = 0; i < num_comdb2db_hosts; i++) {
                rc = cdb2_dbinfo_query(
                    hndl, cdb2_default_cluster, comdb2db_name, comdb2db_num,
                    comdb2db_hosts[i], comdb2db_hosts, comdb2db_ports, &master,
                    &num_comdb2db_hosts, NULL);
                if (rc == 0) {
                    break;
                }
            }
            if (rc != 0) {
                sprintf(hndl->errstr, "cdb2_get_dbhosts: can't do dbinfo "
                                      "query on comdb2db hosts.");
                goto retry;
            }
        }

        rc = -1;
        for (int i = 0; i < num_comdb2db_hosts; i++) {
            if (i == master)
                continue;
            rc = comdb2db_get_dbhosts(hndl, comdb2db_name, comdb2db_num,
                                      comdb2db_hosts[i], comdb2db_ports[i],
                                      hndl->hosts, &hndl->num_hosts,
                                      hndl->dbname, hndl->cluster, &hndl->dbnum,
                                      &hndl->num_hosts_sameroom, num_retry);
            if (rc == 0) {
                break;
            }
        }
        if (rc == -1) {
            rc = comdb2db_get_dbhosts(
                hndl, comdb2db_name, comdb2db_num, comdb2db_hosts[master],
                comdb2db_ports[master], hndl->hosts, &hndl->num_hosts,
                hndl->dbname, hndl->cluster, &hndl->dbnum,
                &hndl->num_hosts_sameroom, num_retry);
        }

        if (rc != 0) {
            sprintf(hndl->errstr,
                    "cdb2_get_dbhosts: can't do newsql query on %s hosts.",
                    comdb2db_name);
            goto retry;
        }
    }

    if (hndl->num_hosts == 0) {
        sprintf(hndl->errstr, "cdb2_get_dbhosts: comdb2db has no entry of "
                              "db %s of cluster type %s.",
                hndl->dbname, hndl->cluster);
        return -1;
    }

    rc = -1;
    int i = 0;
    int node_seq = 0;
    if ((hndl->flags & CDB2_RANDOM) ||
        ((hndl->flags & CDB2_RANDOMROOM) && (hndl->num_hosts_sameroom == 0))) {
        node_seq = cdb2_random_int() % hndl->num_hosts;
    } else if ((hndl->flags & CDB2_RANDOMROOM) &&
               (hndl->num_hosts_sameroom > 0)) {
        node_seq = cdb2_random_int() % hndl->num_hosts_sameroom;
        /* Try dbinfo on same room first */
        for (i = 0; i < hndl->num_hosts_sameroom; i++) {
            int try_node = (node_seq + i) % hndl->num_hosts_sameroom;
            rc = cdb2_dbinfo_query(hndl, hndl->type, hndl->dbname, hndl->dbnum,
                                   hndl->hosts[try_node], hndl->hosts,
                                   hndl->ports, &hndl->master, &hndl->num_hosts,
                                   &hndl->num_hosts_sameroom);
            if (rc == 0) {
                goto done;
            }
        }
    }

    /* Try everything now */
    for (i = 0; i < hndl->num_hosts; i++) {
        int try_node = (node_seq + i) % hndl->num_hosts;
        rc = cdb2_dbinfo_query(hndl, hndl->type, hndl->dbname, hndl->dbnum,
                               hndl->hosts[try_node], hndl->hosts, hndl->ports,
                               &hndl->master, &hndl->num_hosts,
                               &hndl->num_hosts_sameroom);
        if (rc == 0) {
            break;
        }
    }

done:
    if (rc != 0) {
        sprintf(hndl->errstr,
                "cdb2_get_dbhosts: can't do dbinfo query on %s hosts.",
                hndl->dbname);
        if (hndl->num_hosts > 1) goto retry;
    }
    return rc;
}

const char *cdb2_dbname(cdb2_hndl_tp *hndl)
{
    if (hndl)
        return hndl->dbname;
    return NULL;
}

int cdb2_clone(cdb2_hndl_tp **handle, cdb2_hndl_tp *c_hndl)
{
    cdb2_hndl_tp *hndl;
    pthread_once(&init_once, do_init_once);
    *handle = hndl = calloc(1, sizeof(cdb2_hndl_tp));
    strncpy(hndl->dbname, c_hndl->dbname, sizeof(hndl->dbname) - 1);
    strncpy(hndl->cluster, c_hndl->cluster, sizeof(hndl->cluster) - 1);
    strncpy(hndl->type, c_hndl->type, sizeof(hndl->type) - 1);
    hndl->num_hosts = c_hndl->num_hosts;
    hndl->dbnum = c_hndl->dbnum;
    int i = 0;
    for (i = 0; i < c_hndl->num_hosts; i++) {
        strncpy(hndl->hosts[i], c_hndl->hosts[i], sizeof(hndl->hosts[i]) - 1);
        hndl->ports[i] = c_hndl->ports[i];
    }
    hndl->master = c_hndl->master;
    if (log_calls)
        fprintf(stderr, "%p> cdb2_clone(%p) => %p\n", (void *)pthread_self(),
                c_hndl, hndl);
    return 0;
}

static inline int is_machine_list(const char *type)
{
    const char *s = cdb2_skipws(type);
    return *s == '@';
}

struct machine {
    char *host;
    int port;
    int ourdc;
};

static int our_dc_first(const void *mp1, const void *mp2)
{
    const struct machine *m1 = (struct machine *)mp1,
                         *m2 = (struct machine *)mp2;
    if (m1->ourdc) {
        if (m2->ourdc)
            return 0;
        else
            return -1;
    } else if (m2->ourdc)
        return 1;
    else
        return 0;
}

/* wll configure comdb2 hosts based on cmdline parameters eg:
 *   @machine:port=123:dc=ZONE1,machine2:port=456:dc=ZONE2
 */
static int configure_from_literal(cdb2_hndl_tp *hndl, const char *type)
{
    char *type_copy = strdup(cdb2_skipws(type));
    char *eomachine;
    char *eooptions;
    int rc = 0;
    int port;
    char *dc;
    struct machine m[MAX_NODES];
    int num_hosts = 0;

    assert(type_copy[0] == '@');
    char *s = type_copy + 1; // advance past the '@'

    only_read_config();

    char *machine;
    machine = strtok_r(s, ",", &eomachine);
    while (machine) {
        char *options;
        char *hostname;

        port = -1;
        dc = NULL;

        hostname = strtok_r(machine, ":", &eooptions);
        if (hostname == NULL) {
            fprintf(stderr, "no machine name specified?\n");
            rc = 1;
            goto done;
        }
        options = strtok_r(NULL, ":", &eooptions);
        while (options) {
            char *option, *value, *eos;

            option = strtok_r(options, "=", &eos);
            if (option == NULL) {
                fprintf(stderr, "no option set, port or dc required.\n");
                rc = 1;
                goto done;
            }
            if (strcmp(option, "port") != 0 && strcmp(option, "dc") != 0) {
                fprintf(stderr, "port or dc expected instead of %s\n", option);
                rc = 1;
                goto done;
            }
            value = strtok_r(NULL, "=", &eos);
            if (value == NULL) {
                fprintf(stderr, "no value set for %s?\n", option);
                rc = 1;
                goto done;
            }

            if (strcmp(option, "port") == 0) {
                port = atoi(value);
            } else {
                dc = value;
            }

            options = strtok_r(NULL, ":", &eooptions);
        }

        if (num_hosts < MAX_NODES) {
            if (strlen(hostname) >= sizeof(hndl->hosts[0]))
                fprintf(stderr, "Hostname \"%s\" is too long, max %lu\n",
                        hostname, sizeof(hndl->hosts[0]));
            else if (port < -1 || port > USHRT_MAX)
                fprintf(stderr, "Hostname \"%s\" invalid port number %d\n",
                        hostname, port);
            else {
                m[num_hosts].host = hostname;
                m[num_hosts].port = port;
                if (dc)
                    m[num_hosts].ourdc =
                        strcmp(dc, cdb2_machine_room) == 0 ? 1 : 0;
                else
                    m[num_hosts].ourdc = 0;
                num_hosts++;
            }
        }

        machine = strtok_r(NULL, ",", &eomachine);
    }
    qsort(m, num_hosts, sizeof(struct machine), our_dc_first);
    for (int i = 0; i < num_hosts; i++) {
        strcpy(hndl->hosts[i], m[i].host);
        hndl->ports[i] = m[i].port;
        hndl->num_hosts++;
        if (m[i].ourdc)
            hndl->num_hosts_sameroom++;

        if (hndl && hndl->debug_trace)
            fprintf(stderr, "td %u %s host %s port %d\n",
                    (uint32_t)pthread_self(), __func__, m[i].host, m[i].port);
    }

    hndl->flags |= CDB2_DIRECT_CPU;

done:
    free(type_copy);
    if (log_calls)
        fprintf(stderr, "%p> %s() hosts=%d\n", (void *)pthread_self(), __func__,
                num_hosts);
    return rc;
}

#if WITH_SSL
#include <ssl_support.h>
static int set_up_ssl_params(cdb2_hndl_tp *hndl)
{
    /* In case that the application connects to multiple databases
       and uses different certificates, we must copy the global SSL
       parameters to the handle and reset them. It does not make
       cdb2_open() reentrant, but is better than nothing.
     */
    char *sslenv;

    if ((sslenv = getenv("SSL_MODE")) != NULL && sslenv[0] != '\0')
        hndl->c_sslmode = ssl_string_to_mode(sslenv, &hndl->nid_dbname);
    else {
        hndl->c_sslmode = cdb2_c_ssl_mode;
        hndl->nid_dbname = cdb2_nid_dbname;
    }

    if ((sslenv = getenv("SSL_CERT_PATH")) != NULL && sslenv[0] != '\0') {
        hndl->sslpath = strdup(sslenv);
        if (hndl->sslpath == NULL)
            return ENOMEM;
    } else if (cdb2_sslcertpath[0] != '\0') {
        hndl->sslpath = strdup(cdb2_sslcertpath);
        if (hndl->sslpath == NULL)
            return ENOMEM;
    }

    if ((sslenv = getenv("SSL_CERT")) != NULL && sslenv[0] != '\0') {
        hndl->cert = strdup(sslenv);
        if (hndl->cert == NULL)
            return ENOMEM;
    } else if (cdb2_sslcert[0] != '\0') {
        hndl->cert = strdup(cdb2_sslcert);
        if (hndl->cert == NULL)
            return ENOMEM;
    }

    if ((sslenv = getenv("SSL_KEY")) != NULL && sslenv[0] != '\0') {
        hndl->key = strdup(sslenv);
        if (hndl->key == NULL)
            return ENOMEM;
    } else if (cdb2_sslkey[0] != '\0') {
        hndl->key = strdup(cdb2_sslkey);
        if (hndl->key == NULL)
            return ENOMEM;
    }

    if ((sslenv = getenv("SSL_CA")) != NULL && sslenv[0] != '\0') {
        hndl->ca = strdup(sslenv);
        if (hndl->ca == NULL)
            return ENOMEM;
    } else if (cdb2_sslca[0] != '\0') {
        hndl->ca = strdup(cdb2_sslca);
        if (hndl->ca == NULL)
            return ENOMEM;
    }

#if HAVE_CRL
    if ((sslenv = getenv("SSL_CRL")) != NULL && sslenv[0] != '\0') {
        hndl->crl = strdup(sslenv);
        if (hndl->crl == NULL)
            return ENOMEM;
    } else if (cdb2_sslcrl[0] != '\0') {
        hndl->crl = strdup(cdb2_sslcrl);
        if (hndl->crl == NULL)
            return ENOMEM;
    }
#endif

    /* Set up SSL sessions. */
    if ((sslenv = getenv("SSL_SESSION_CACHE")) != NULL)
        hndl->cache_ssl_sess = !!atoi(sslenv);
    else
        hndl->cache_ssl_sess = cdb2_cache_ssl_sess;
    if (hndl->cache_ssl_sess)
        cdb2_set_ssl_sessions(hndl, cdb2_get_ssl_sessions(hndl));

    /* Reset for next cdb2_open() */
    cdb2_c_ssl_mode = SSL_ALLOW;
    cdb2_sslcertpath[0] = '\0';
    cdb2_sslcert[0] = '\0';
    cdb2_sslkey[0] = '\0';
    cdb2_sslca[0] = '\0';
    cdb2_sslcrl[0] = '\0';

    cdb2_nid_dbname = CDB2_NID_DBNAME_DEFAULT;
    cdb2_cache_ssl_sess = CDB2_CACHE_SSL_SESS_DEFAULT;
    return 0;
}

static int cdb2_called_ssl_init = 0;
pthread_mutex_t fend_ssl_init_lock = PTHREAD_MUTEX_INITIALIZER;
int cdb2_init_ssl(int init_libssl, int init_libcrypto)
{
    int rc = 0;
    if (cdb2_called_ssl_init == 0 &&
        (rc = pthread_mutex_lock(&fend_ssl_init_lock)) == 0) {
        if (cdb2_called_ssl_init == 0) {
            rc = ssl_init(init_libssl, init_libcrypto,
                          0, NULL, 0);
            cdb2_called_ssl_init = 1;
        }
        if (rc == 0)
            rc = pthread_mutex_unlock(&fend_ssl_init_lock);
        else
            pthread_mutex_unlock(&fend_ssl_init_lock);
    }
    return rc;
}

int cdb2_is_ssl_encrypted(cdb2_hndl_tp *hndl)
{
    return hndl->sb == NULL ? 0 : sslio_has_ssl(hndl->sb);
}

static cdb2_ssl_sess_list *cdb2_get_ssl_sessions(cdb2_hndl_tp *hndl)
{
    cdb2_ssl_sess_list *pos;
    int rc = pthread_mutex_lock(&cdb2_ssl_sess_lock);
    if (rc != 0)
        return NULL;

    for (pos = cdb2_ssl_sess_cache.next; pos != NULL; pos = pos->next) {
        if (strcasecmp(hndl->dbname, pos->dbname) == 0 &&
            strcasecmp(hndl->cluster, pos->cluster) == 0) {
            /* Don't return if being used. */
            if (pos->ref)
                pos = NULL;
            else
                pos->ref = 1;
            break;
        }
    }

    pthread_mutex_unlock(&cdb2_ssl_sess_lock);
    return pos;
}

static int cdb2_set_ssl_sessions(cdb2_hndl_tp *hndl, cdb2_ssl_sess_list *arg)
{
    /* Worst practices of variable naming. */
    int i, j;
    cdb2_ssl_sess *p, *q, *r;

    if (arg == NULL)
        return EINVAL;

    /* Disallow if sess_list not nil to avoid any confusion. */
    if (hndl->sess_list != NULL)
        return EPERM;

    /* Transfer valid SSL sessions to the new list
       in case that the hosts have changed (re-ordering, migration and etc). */
    r = malloc(sizeof(cdb2_ssl_sess) * hndl->num_hosts);
    if (r == NULL)
        return ENOMEM;

    for (i = 0, p = r; i != hndl->num_hosts; ++i, ++p) {
        strncpy(p->host, hndl->hosts[i], sizeof(p->host));
        p->host[sizeof(p->host) - 1] = '\0';
        p->sess = NULL;
        for (j = 0, q = arg->list; j != arg->n; ++q) {
            if (strcasecmp(p->host, q->host) == 0) {
                p->sess = q->sess;
                break;
            }
        }
    }

    free(arg->list);
    arg->n = hndl->num_hosts;
    arg->list = r;

    hndl->sess_list = arg;

    return 0;
}

static void cdb2_free_ssl_sessions(cdb2_ssl_sess_list *p)
{
    int i, rc;
    cdb2_ssl_sess_list *pos;

    if (p == NULL)
        return;

    if (p->ref != 0)
        return;

    /* Remove from the linkedlist first. */
    rc = pthread_mutex_lock(&cdb2_ssl_sess_lock);
    if (rc != 0)
        return;

    if (p->ref == 0) {
        /* pos points to the element before p. */
        for (pos = &cdb2_ssl_sess_cache; pos->next != NULL; pos = pos->next) {
            if (pos->next == p) {
                pos->next = p->next;
                break;
            }
        }
    }

    pthread_mutex_unlock(&cdb2_ssl_sess_lock);

    for (i = 0; i != p->n; ++i)
        SSL_SESSION_free(p->list[i].sess);
    free(p->list);
    free(p);
}
#else /* WITH_SSL */
int cdb2_init_ssl(int init_libssl, int init_libcrypto)
{
    return 0;
}

int cdb2_is_ssl_encrypted(cdb2_hndl_tp *hndl)
{
    return 0;
}
#endif /* !WITH_SSL */

int comdb2_cheapstack_char_array(char *str, int maxln);

int cdb2_open(cdb2_hndl_tp **handle, const char *dbname, const char *type,
              int flags)
{
    cdb2_hndl_tp *hndl;
    int rc = 0;

    pthread_once(&init_once, do_init_once);

    *handle = hndl = calloc(1, sizeof(cdb2_hndl_tp));
    strncpy(hndl->dbname, dbname, sizeof(hndl->dbname) - 1);
    strncpy(hndl->cluster, type, sizeof(hndl->cluster) - 1);
    strncpy(hndl->type, type, sizeof(hndl->type) - 1);
    hndl->flags = flags;
    hndl->dbnum = 1;
    hndl->connected_host = -1;
    hndl->send_stack = 1;
    hndl->read_intrans_results = 1;
#if WITH_SSL
    /* We don't do dbinfo if DIRECT_CPU. So we'd default peer SSL mode to
       ALLOW. We will find it out later when we send SSL negotitaion packet
       to the server. */
    hndl->s_sslmode = PEER_SSL_ALLOW;
#endif

    hndl->max_retries = MAX_RETRIES;
    hndl->min_retries = MIN_RETRIES;

    hndl->env_tz = getenv("COMDB2TZ");

    if (hndl->env_tz == NULL)
        hndl->env_tz = getenv("TZ");

    if (hndl->env_tz == NULL)
        hndl->env_tz = DB_TZNAME_DEFAULT;


    cdb2_init_context_msgs(hndl);

    if (getenv("CDB2_DEBUG")) {
        hndl->debug_trace = 1;
        fprintf(stderr, "td %u %s %d debug trace enabled\n",
                (uint32_t)pthread_self(), __func__, __LINE__);
    }

    if (hndl->flags & CDB2_RANDOM) {
        strcpy(hndl->policy, "random");
    } else if (hndl->flags & CDB2_RANDOMROOM) {
        strcpy(hndl->policy, "random_room");
    } else if (hndl->flags & CDB2_ROOM) {
        strcpy(hndl->policy, "room");
    } else {
        hndl->flags |= CDB2_RANDOMROOM;
        strcpy(hndl->policy, "random_room");
    }

    if (hndl->flags & CDB2_DIRECT_CPU) {
        hndl->num_hosts = 1;
        /* Get defaults from comdb2db.cfg */
        only_read_config();
        strncpy(hndl->hosts[0], type, sizeof(hndl->hosts[0]) - 1);
        char *p = strchr(hndl->hosts[0], ':');
        if (p) {
            *p = '\0';
            hndl->ports[0] = atoi(p + 1);
        } else {
            if (!cdb2_allow_pmux_route) {
                hndl->ports[0] = cdb2portmux_get(type, "comdb2", "replication",
                                                 dbname, hndl->debug_trace);
            } else {
                hndl->ports[0] = CDB2_PORTMUXPORT;
            }
        }
        if (hndl && hndl->debug_trace)
            fprintf(stderr, "td %u %s:%d host %s port %d\n",
                    (uint32_t)pthread_self(), __func__, __LINE__,
                    hndl->hosts[0], hndl->ports[0]);
    } else if (is_machine_list(type)) {
        rc = configure_from_literal(hndl, type);
    } else {
        rc = cdb2_get_dbhosts(hndl);
    }

#if WITH_SSL
    if (rc == 0)
        rc = set_up_ssl_params(hndl);
#endif

    if (hndl->send_stack)
        comdb2_cheapstack_char_array(hndl->stack, MAX_STACK);

    if (log_calls) {
        fprintf(stderr, "%p> cdb2_open(dbname: \"%s\", type: \"%s\", flags: "
                        "%x) = %d => %p\n",
                (void *)pthread_self(), dbname, type, hndl->flags, rc, *handle);
    }
    return rc;
}

/*
  Initialize the context messages object.
*/
static void cdb2_init_context_msgs(cdb2_hndl_tp *hndl)
{
    memset((void *)&hndl->context_msgs, 0, sizeof(struct context_messages));
}

/*
  Free the alloc-ed context messages.
*/
static int cdb2_free_context_msgs(cdb2_hndl_tp *hndl)
{
    int i = 0;

    while (i < hndl->context_msgs.count) {
        free(hndl->context_msgs.message[i]);
        hndl->context_msgs.message[i] = 0;
        i++;
    }

    hndl->context_msgs.count = 0;
    hndl->context_msgs.has_changed = 1;

    return 0;
}

/*
  Store the specified message in the handle. Return error if
  MAX_CONTEXTS number of messages have already been stored.

  @param hndl [IN]   Connection handle
  @param msg  [IN]   Context message

  @return
    0                Success
    1                Error
*/
int cdb2_push_context(cdb2_hndl_tp *hndl, const char *msg)
{
    /* Check for overflow. */
    if (hndl->context_msgs.count >= MAX_CONTEXTS) {
        return 1;
    }

    hndl->context_msgs.message[hndl->context_msgs.count] =
        strndup(msg, MAX_CONTEXT_LEN);
    hndl->context_msgs.count++;
    hndl->context_msgs.has_changed = 1;
    return 0;
}

/*
  Remove the last stored context message.
*/
int cdb2_pop_context(cdb2_hndl_tp *hndl)
{
    /* Check for underflow. */
    if (hndl->context_msgs.count == 0) {
        return 1;
    }

    hndl->context_msgs.count--;
    free(hndl->context_msgs.message[hndl->context_msgs.count]);
    hndl->context_msgs.message[hndl->context_msgs.count] = 0;
    hndl->context_msgs.has_changed = 1;

    return 0;
}

/*
  Clear/free all the stored context messages.
*/
int cdb2_clear_contexts(cdb2_hndl_tp *hndl)
{
    return cdb2_free_context_msgs(hndl);
}

/*
  Clear ack flag so cdb2_close will not consume event
*/
int cdb2_clear_ack(cdb2_hndl_tp* hndl)
{
    if (hndl) {
        hndl->ack = 0;
    }
    return 0;
}
