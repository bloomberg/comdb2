/*
   Copyright 2015, 2023, Bloomberg Finance L.P.

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

#include <inttypes.h>
#include <alloca.h>
#include <stdarg.h>
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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "cdb2api.h"

#include "sqlquery.pb-c.h"
#include "sqlresponse.pb-c.h"

#include "str_util.h" /* QUOTE */

/*
*******************************************************************************
** WARNING: If you add any internal configuration state to this file, please
**          update the reset_the_configuration() function as well to include
**          it.
*******************************************************************************
*/

#define SOCKPOOL_SOCKET_NAME "/tmp/sockpool.socket"
static char *SOCKPOOL_OTHER_NAME = NULL;
#define COMDB2DB "comdb2db"
#define COMDB2DB_NUM 32432
#define MAX_BUFSIZE_ONSTACK 8192
#define CDB2HOSTNAME_LEN 128
static char COMDB2DB_OVERRIDE[32] = {0};
static int COMDB2DB_NUM_OVERRIDE = 0;

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

#define API_DRIVER_NAME open_cdb2api
static char api_driver_name[] = QUOTE(API_DRIVER_NAME);

#ifndef API_DRIVER_VERSION
#define API_DRIVER_VERSION latest
#endif
static char api_driver_version[] = QUOTE(API_DRIVER_VERSION);

#ifndef CDB2_DNS_SUFFIX
#define CDB2_DNS_SUFFIX
#endif
static char cdb2_dnssuffix[255] = QUOTE(CDB2_DNS_SUFFIX);
static char cdb2_machine_room[16] = "";

#define CDB2_PORTMUXPORT_DEFAULT 5105
static int CDB2_PORTMUXPORT = CDB2_PORTMUXPORT_DEFAULT;

#define MAX_RETRIES_DEFAULT 21
static int MAX_RETRIES = MAX_RETRIES_DEFAULT; /* We are looping each node twice. */

#define MIN_RETRIES_DEFAULT 16
static int MIN_RETRIES = MIN_RETRIES_DEFAULT;

#define CDB2_CONNECT_TIMEOUT_DEFAULT 100
static int CDB2_CONNECT_TIMEOUT = CDB2_CONNECT_TIMEOUT_DEFAULT;

#define CDB2_AUTO_CONSUME_TIMEOUT_MS_DEFAULT 0
static int CDB2_AUTO_CONSUME_TIMEOUT_MS = CDB2_AUTO_CONSUME_TIMEOUT_MS_DEFAULT;

#define COMDB2DB_TIMEOUT_DEFAULT 2000
static int COMDB2DB_TIMEOUT = COMDB2DB_TIMEOUT_DEFAULT;

#define CDB2_API_CALL_TIMEOUT_DEFAULT 120000 /* defaults to 2 minute */
static int CDB2_API_CALL_TIMEOUT = CDB2_API_CALL_TIMEOUT_DEFAULT;

#define CDB2_SOCKET_TIMEOUT_DEFAULT 5000
static int CDB2_SOCKET_TIMEOUT = CDB2_SOCKET_TIMEOUT_DEFAULT;

#define CDB2_POLL_TIMEOUT_DEFAULT 250
static int CDB2_POLL_TIMEOUT = CDB2_POLL_TIMEOUT_DEFAULT;

#define CDB2_PROTOBUF_SIZE_DEFAULT 4096
static int CDB2_PROTOBUF_SIZE = CDB2_PROTOBUF_SIZE_DEFAULT;

#define CDB2_TCPBUFSZ_DEFAULT 0
static int cdb2_tcpbufsz = CDB2_TCPBUFSZ_DEFAULT;

#define CDB2CFG_OVERRIDE_DEFAULT 0
static int cdb2cfg_override = CDB2CFG_OVERRIDE_DEFAULT;

#define CDB2_REQUEST_FP_DEFAULT 0
static int CDB2_REQUEST_FP = CDB2_REQUEST_FP_DEFAULT;

#define CDB2_GET_HOSTNAME_FROM_SOCKPOOL_FD_DEFAULT 1
static int CDB2_GET_HOSTNAME_FROM_SOCKPOOL_FD = CDB2_GET_HOSTNAME_FROM_SOCKPOOL_FD_DEFAULT;

#include <openssl/conf.h>
#include <openssl/crypto.h>
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

#define CDB2_MIN_TLS_VER_DEFAULT 0
static double cdb2_min_tls_ver = CDB2_MIN_TLS_VER_DEFAULT;

static pthread_mutex_t cdb2_ssl_sess_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct cdb2_ssl_sess cdb2_ssl_sess;
static cdb2_ssl_sess *cdb2_get_ssl_sessions(cdb2_hndl_tp *hndl);
static int cdb2_set_ssl_sessions(cdb2_hndl_tp *hndl,
                                 cdb2_ssl_sess *sessions);
static int cdb2_add_ssl_session(cdb2_hndl_tp *hndl);

static pthread_mutex_t cdb2_cfg_lock = PTHREAD_MUTEX_INITIALIZER;

#define CDB2_ALLOW_PMUX_ROUTE_DEFAULT 0
static int cdb2_allow_pmux_route = CDB2_ALLOW_PMUX_ROUTE_DEFAULT;

static int _PID; /* ONE-TIME */
static int _MACHINE_ID; /* ONE-TIME */
static char *_ARGV0; /* ONE-TIME */

static int iam_identity = 0;

#define DB_TZNAME_DEFAULT "America/New_York"

#define MAX_NODES 128
#define MAX_CONTEXTS 10 /* Maximum stack size for storing context messages */
#define MAX_CONTEXT_LEN 100 /* Maximum allowed length of a context message */

#define MAX_STACK 512 /* Size of call-stack which opened the handle */

pthread_mutex_t cdb2_sockpool_mutex = PTHREAD_MUTEX_INITIALIZER;
#define MAX_SOCKPOOL_FDS 8

#include <netdb.h>

static pthread_once_t init_once = PTHREAD_ONCE_INIT;
static int log_calls = 0; /* ONE-TIME */

static void reset_sockpool(void);

struct cdb2_event {
    cdb2_event_type types;
    cdb2_event_ctrl ctrls;
    cdb2_event_callback cb;
    int global;
    void *user_arg;
    cdb2_event *next;
    int argc;
    cdb2_event_arg argv[1];
};

static pthread_mutex_t cdb2_event_mutex = PTHREAD_MUTEX_INITIALIZER;
static cdb2_event cdb2_gbl_events;
static int cdb2_gbl_event_version;
static cdb2_event *cdb2_next_callback(cdb2_hndl_tp *, cdb2_event_type,
                                      cdb2_event *);
static void *cdb2_invoke_callback(cdb2_hndl_tp *, cdb2_event *, int, ...);
static int refresh_gbl_events_on_hndl(cdb2_hndl_tp *);

#define PROCESS_EVENT_CTRL_BEFORE(h, e, rc, callbackrc, ovwrrc)                \
    do {                                                                       \
        if (e->ctrls & CDB2_OVERWRITE_RETURN_VALUE) {                          \
            ovwrrc = 1;                                                        \
            rc = (int)(intptr_t)callbackrc;                                    \
        }                                                                      \
        if (e->ctrls & CDB2_AS_HANDLE_SPECIFIC_ARG)                            \
            h->user_arg = callbackrc;                                          \
    } while (0)

#define PROCESS_EVENT_CTRL_AFTER(h, e, rc, callbackrc)                         \
    do {                                                                       \
        if (e->ctrls & CDB2_OVERWRITE_RETURN_VALUE) {                          \
            rc = (int)(intptr_t)callbackrc;                                    \
        }                                                                      \
        if (e->ctrls & CDB2_AS_HANDLE_SPECIFIC_ARG)                            \
            h->user_arg = callbackrc;                                          \
    } while (0)

typedef void (*cdb2_init_t)(void);

/* Undocumented compile-time library installation/uninstallation routine. */
#ifndef CDB2_INSTALL_LIBS
#define CDB2_INSTALL_LIBS NULL
#else
extern void CDB2_INSTALL_LIBS(void);
#endif
void (*cdb2_install)(void) = CDB2_INSTALL_LIBS;

#ifndef CDB2_UNINSTALL_LIBS
#define CDB2_UNINSTALL_LIBS NULL
#else
extern void CDB2_UNINSTALL_LIBS(void);
#endif
void (*cdb2_uninstall)(void) = CDB2_UNINSTALL_LIBS;

#ifndef CDB2_IDENTITY_CALLBACKS
    struct cdb2_identity *identity_cb = NULL;
#else
    extern struct cdb2_identity CDB2_IDENTITY_CALLBACKS;
    struct cdb2_identity *identity_cb = &CDB2_IDENTITY_CALLBACKS;
#endif

#ifndef WITH_DL_LIBS
#define WITH_DL_LIBS 0
#endif

#if WITH_DL_LIBS
#include <dlfcn.h>
void cdb2_set_install_libs(void (*ptr)(void)) { cdb2_install = ptr; }
void cdb2_set_uninstall_libs(void (*ptr)(void)) { cdb2_uninstall = ptr; }
#endif

#define debugprint(fmt, args...)                                               \
    do {                                                                       \
        if (hndl && hndl->debug_trace)                                         \
            fprintf(stderr, "td 0x%p %s:%d " fmt, (void *)pthread_self(),      \
                    __func__, __LINE__, ##args);                               \
    } while (0);

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

    memset(COMDB2DB_OVERRIDE, 0, sizeof(COMDB2DB_OVERRIDE));
    COMDB2DB_NUM_OVERRIDE = 0;

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
    CDB2_API_CALL_TIMEOUT = CDB2_API_CALL_TIMEOUT_DEFAULT;
    CDB2_SOCKET_TIMEOUT = CDB2_SOCKET_TIMEOUT_DEFAULT;
    CDB2_POLL_TIMEOUT = CDB2_POLL_TIMEOUT_DEFAULT;
    CDB2_AUTO_CONSUME_TIMEOUT_MS = CDB2_AUTO_CONSUME_TIMEOUT_MS_DEFAULT;
    COMDB2DB_TIMEOUT = COMDB2DB_TIMEOUT_DEFAULT;
    CDB2_API_CALL_TIMEOUT = CDB2_API_CALL_TIMEOUT_DEFAULT;
    CDB2_SOCKET_TIMEOUT = CDB2_SOCKET_TIMEOUT_DEFAULT;
    CDB2_PROTOBUF_SIZE = CDB2_PROTOBUF_SIZE_DEFAULT;
    cdb2_tcpbufsz = CDB2_TCPBUFSZ_DEFAULT;

    cdb2_allow_pmux_route = CDB2_ALLOW_PMUX_ROUTE_DEFAULT;
    cdb2cfg_override = CDB2CFG_OVERRIDE_DEFAULT;
    CDB2_REQUEST_FP = CDB2_REQUEST_FP_DEFAULT;
    CDB2_GET_HOSTNAME_FROM_SOCKPOOL_FD = CDB2_GET_HOSTNAME_FROM_SOCKPOOL_FD_DEFAULT;

    cdb2_c_ssl_mode = SSL_ALLOW;

    memset(cdb2_sslcertpath, 0, sizeof(cdb2_sslcertpath));
    memset(cdb2_sslcert, 0, sizeof(cdb2_sslcert));
    memset(cdb2_sslkey, 0, sizeof(cdb2_sslkey));
    memset(cdb2_sslca, 0, sizeof(cdb2_sslca));
    memset(cdb2_sslcrl, 0, sizeof(cdb2_sslcrl));

    cdb2_nid_dbname = CDB2_NID_DBNAME_DEFAULT;
    cdb2_cache_ssl_sess = CDB2_CACHE_SSL_SESS_DEFAULT;
    cdb2_min_tls_ver = CDB2_MIN_TLS_VER_DEFAULT;

    reset_sockpool();
}

static SBUF2 *sbuf2openread(const char *filename)
{
    int fd;
    SBUF2 *s;

    if ((fd = open(filename, O_RDONLY, 0)) < 0 ||
        (s = sbuf2open(fd, 0)) == NULL) {
        if (fd >= 0)
            close(fd);
        return NULL;
    }
    return s;
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
    SBUF2 *s = sbuf2openread(procname);
    if (s == NULL) {
        fprintf(stderr, "%s cannot open %s, %s\n", __func__, procname,
                strerror(errno));
        return NULL;
    }

    if ((sbuf2gets(argv0, PATH_MAX, s)) < 0) {
        fprintf(stderr, "%s error reading from %s, %s\n", __func__, procname,
                strerror(errno));
        sbuf2close(s);
        return NULL;
    }

    sbuf2close(s);
    return argv0;
}
#endif

#define SQLCACHEHINT "/*+ RUNCOMDB2SQL "
#define SQLCACHEHINTLENGTH 17

static int value_on_off(const char *value) {
    if (strcasecmp("on", value) == 0) {
        return 1;
    } else if (strcasecmp("off", value) == 0) {
        return 0;
    } else if (strcasecmp("no", value) == 0) {
        return 0;
    } else if (strcasecmp("yes", value) == 0) {
        return 1;
    }
    return atoi(value);
}

static inline const char *cdb2_skipws(const char *str)
{
    while (*str && isspace(*str))
        str++;
    return str;
}

char *cdb2_getargv0(void)
{
#if defined(__APPLE__)
    return apple_getargv0();
#elif defined(_LINUX_SOURCE) || defined(_SUN_SOURCE)
    return proc_cmdline_getargv0();
#else
    fprintf(stderr, "%s unsupported architecture\n", __func__);
    return NULL;
#endif
}

static void atfork_prepare(void) {
    pthread_mutex_lock(&cdb2_sockpool_mutex);
    if (identity_cb)
        identity_cb->resetIdentity_start();
}

static void atfork_me(void) {
    if (identity_cb)
        identity_cb->resetIdentity_end(1);
    pthread_mutex_unlock(&cdb2_sockpool_mutex);
}

static void sockpool_close_all(void);
static void atfork_child(void) {
    sockpool_close_all();
    _PID = getpid();
    if (identity_cb)
        identity_cb->resetIdentity_end(0);
    pthread_mutex_unlock(&cdb2_sockpool_mutex);
}

static int init_once_has_run = 0;

static void do_init_once(void)
{
    static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
    if (init_once_has_run == 0) {
        pthread_mutex_lock(&lk);
        if (!init_once_has_run) {
            srandom(time(0));
            _PID = getpid();
            _MACHINE_ID = gethostid();
            _ARGV0 = cdb2_getargv0();
            pthread_atfork(atfork_prepare, atfork_me, atfork_child);
            init_once_has_run = 1;
        }
        pthread_mutex_unlock(&lk);
    }

    char *do_log = getenv("CDB2_LOG_CALLS");
    if (do_log)
        log_calls = 1;
    char *config = getenv("CDB2_CONFIG_FILE");
    if (config) {
        /* can't call back cdb2_set_comdb2db_config from do_init_once */
        strncpy(CDB2DBCONFIG_NOBBENV, config, 511);
    }
    char *cdb2db_override = getenv("CDB2_COMDB2DB_OVERRIDE");
    if (cdb2db_override) {
        strncpy(COMDB2DB_OVERRIDE, cdb2db_override, 31);
    }
    char *cdb2db_dbnum_override = getenv("CDB2_COMDB2DB_DBNUM_OVERRIDE");
    if (cdb2db_dbnum_override) {
        COMDB2DB_NUM_OVERRIDE = atoi(cdb2db_dbnum_override);
    }
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
#if defined(_LINUX_SOURCE)
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
        unsigned char control[CMSG_SPACE(sizeof(int))];
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
        unsigned char control[CMSG_SPACE(sizeof(int))];
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
    struct hostent *hp = NULL;
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
        int herr;
        char tmp[8192];
        int tmplen = 8192;
        struct hostent hostbuf;
        gethostbyname_r(tok, &hostbuf, tmp, tmplen, &hp, &herr);
#elif _SUN_SOURCE
        int herr;
        char tmp[8192];
        int tmplen = 8192;
        struct hostent hostbuf;
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
    int state; /* query state */
    int length;
};

typedef struct cdb2_query_list_item {
    void *buf;
    int len;
    int is_read;
    char *sql;
    struct cdb2_query_list_item *next;
} cdb2_query_list;

struct cdb2_ssl_sess {
    struct cdb2_ssl_sess *next;
    char dbname[64];
    char cluster[64];
    int ref;
    SSL_SESSION *sessobj;
};

static cdb2_ssl_sess cdb2_ssl_sess_cache;

/* A cnonce is composed of
   - 32 bits of machine ID
   - 32 bits of process PID
   - 32 or 64 bits of handle address
   - 52 bits for the epoch time in microseconds
   - 12 bits for the sequence number

   The allocation allows a handle to run at a maximum transaction rate of
   4096 txn/us (~4 billion transactions per second) till September 17, 2112.

   See next_cnonce() for details. */
#define CNONCE_STR_FMT "%lx-%x-%llx-"
#define CNONCE_STR_SZ 60 /* 16 + 1 + 8 + 1 + 16 + 1 + 16 + 1 (NUL) */

#define CNT_BITS 12
#define TIME_MASK (-1ULL << CNT_BITS)
#define CNT_MASK (-1ULL ^ TIME_MASK)

typedef struct cnonce {
    long hostid;
    int pid;
    struct cdb2_hndl *hndl;
    uint64_t seq;
    int ofs;
    char str[CNONCE_STR_SZ];
} cnonce_t;

#define DBNAME_LEN 64
#define TYPE_LEN 64
#define POLICY_LEN 24

struct cdb2_hndl {
    char dbname[DBNAME_LEN];
    char cluster[64];
    char type[TYPE_LEN];
    char hosts[MAX_NODES][CDB2HOSTNAME_LEN];
    uint64_t timestampus; // client query timestamp of first try
    int ports[MAX_NODES];
    int hosts_connected[MAX_NODES];
    char cached_host[CDB2HOSTNAME_LEN]; /* hostname of a sockpool connection */
    int cached_port;      /* port of a sockpool connection */
    SBUF2 *sb;
    int dbnum;
    int num_hosts;          /* total number of hosts */
    int num_hosts_sameroom; /* number of hosts that are in my datacenter (aka room) */
    int node_seq;           /* fail over to the `node_seq'-th host */
    int in_trans;
    int temp_trans;
    int is_retry;
    char newsql_typestr[DBNAME_LEN + TYPE_LEN + POLICY_LEN + 16];
    char policy[POLICY_LEN];
    int master;
    int connected_host;
    char *query;
    char *query_hint;
    char *hint;
    int use_hint;
    int flags;
    char errstr[1024];
    cnonce_t cnonce;
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
    int is_admin;
    int clear_snap_line;
    int debug_trace;
    int max_retries;
    int min_retries;
    ssl_mode c_sslmode; /* client SSL mode */
    peer_ssl_mode s_sslmode; /* server SSL mode */
    int sslerr; /* 1 if unrecoverable SSL error. */
    char *sslpath; /* SSL certificates */
    char *cert;
    char *key;
    char *ca;
    char *crl;
    int cache_ssl_sess;
    double min_tls_ver;
    cdb2_ssl_sess *sess;
    int nid_dbname;
    /* 1 if it's a newly established session which needs to be cached. */
    int newsess;
    struct context_messages context_msgs;
    char *env_tz;
    int sent_client_info;
    char stack[MAX_STACK];
    int send_stack;
    void *user_arg;
    int *gbl_event_version; /* Cached global event version */
    int api_call_timeout;
    int connect_timeout;
    int comdb2db_timeout;
    int socket_timeout;
    int request_fp; /* 1 if requesting the fingerprint; 0 otherwise. */
    cdb2_event *events;
    // Protobuf allocator data used only for row data i.e. lastresponse
    void *protobuf_data;
    int protobuf_size;
    int protobuf_offset;
    ProtobufCAllocator allocator;
    int auto_consume_timeout_ms;
    struct cdb2_hndl *fdb_hndl;
    int is_child_hndl;
    CDB2SQLQUERY__IdentityBlob *id_blob;
};

static void *cdb2_protobuf_alloc(void *allocator_data, size_t size)
{
    struct cdb2_hndl *hndl = allocator_data;
    void *p = NULL;
    if (size <= hndl->protobuf_size - hndl->protobuf_offset) {
        p = hndl->protobuf_data + hndl->protobuf_offset;
        if (size%8) {
            hndl->protobuf_offset += size + (8 - size%8);
        } else {
            hndl->protobuf_offset += size;
        }
        if (hndl->protobuf_offset > hndl->protobuf_size)
            hndl->protobuf_offset = hndl->protobuf_size;
    } else {
        p = malloc(size);
    }
    return p;
}
void cdb2_protobuf_free(void *allocator_data, void *p)
{
    struct cdb2_hndl *hndl = allocator_data;
    if (p < hndl->protobuf_data ||
        p >= (hndl->protobuf_data + hndl->protobuf_size)) {
        free(p);
    }
}

static int cdb2_tcpconnecth_to(cdb2_hndl_tp *hndl, const char *host, int port,
                               int myport, int timeoutms)
{
    int rc = 0;
    struct in_addr in;

    void *callbackrc;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;

    while ((e = cdb2_next_callback(hndl, CDB2_BEFORE_TCP_CONNECT, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 2, CDB2_HOSTNAME, host,
                                          CDB2_PORT, port);
        PROCESS_EVENT_CTRL_BEFORE(hndl, e, rc, callbackrc, overwrite_rc);
    }

    if (overwrite_rc)
        goto after_callback;

    if ((rc = cdb2_tcpresolve(host, &in, &port)) != 0)
        goto after_callback;

    rc = cdb2_do_tcpconnect(in, port, myport, timeoutms);

after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_TCP_CONNECT, e)) != NULL) {
        callbackrc =
            cdb2_invoke_callback(hndl, e, 3, CDB2_HOSTNAME, host, CDB2_PORT,
                                 port, CDB2_RETURN_VALUE, (intptr_t)rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }
    return rc;
}

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
    pthread_mutex_lock(&cdb2_cfg_lock);
    pthread_once(&init_once, do_init_once);
    if (log_calls)
        fprintf(stderr, "%p> %s(\"%s\")\n", (void *)pthread_self(), __func__,
                cfg_file);
    memset(CDB2DBCONFIG_NOBBENV, 0, sizeof(CDB2DBCONFIG_NOBBENV) /* 512 */);
    if (cfg_file != NULL) {
        cdb2cfg_override = 1;
        strncpy(CDB2DBCONFIG_NOBBENV, cfg_file, 511);
    } else {
        reset_the_configuration();
    }
    pthread_mutex_unlock(&cdb2_cfg_lock);
}

void cdb2_set_comdb2db_info(const char *cfg_info)
{
    int len;
    pthread_mutex_lock(&cdb2_cfg_lock);
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
        pthread_mutex_unlock(&cdb2_cfg_lock);
        return;
    }
    cdb2cfg_override = 1;
    len = strlen(cfg_info) + 1;
    CDB2DBCONFIG_BUF = malloc(len);
    strcpy(CDB2DBCONFIG_BUF, cfg_info);
    pthread_mutex_unlock(&cdb2_cfg_lock);
}

void cdb2_set_sockpool(const char *sp_path)
{
    if (SOCKPOOL_OTHER_NAME)
        free(SOCKPOOL_OTHER_NAME);
    SOCKPOOL_OTHER_NAME = strdup(sp_path);
}

static inline int get_char(SBUF2 *s, const char *buf, int *chrno)
{
    int ch;
    if (s) {
        ch = sbuf2getc(s);
    } else {
        ch = buf[*chrno];
        *chrno += 1;
    }
    return ch;
}

static int read_line(char *line, int maxlen, SBUF2 *s, const char *buf,
                     int *chrno)
{
    int ch = get_char(s, buf, chrno);
    while (ch == ' ' || ch == '\n')
        ch = get_char(s, buf, chrno); // consume empty lines

    int count = 0;
    while ((ch != '\n') && (ch != EOF) && (ch != '\0')) {
        line[count] = ch;
        count++;
        if (count >= maxlen)
            return count;
        ch = get_char(s, buf, chrno);
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

static ssl_mode ssl_string_to_mode(const char *s, int *nid_dbname)
{
    if (strcasecmp(SSL_MODE_PREFER, s) == 0)
        return SSL_PREFER;
    if (strcasecmp(SSL_MODE_PREFER_VERIFY_CA, s) == 0)
        return SSL_PREFER_VERIFY_CA;
    if (strcasecmp(SSL_MODE_PREFER_VERIFY_HOST, s) == 0)
        return SSL_PREFER_VERIFY_HOSTNAME;
    if (strcasecmp(SSL_MODE_PREFER_VERIFY_DBNAME, s) == 0)
        return SSL_PREFER_VERIFY_DBNAME;
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

static void only_read_config(cdb2_hndl_tp *, int, int); /* FORWARD */

static void read_comdb2db_cfg(cdb2_hndl_tp *hndl, SBUF2 *s, const char *comdb2db_name, const char *buf,
                              char comdb2db_hosts[][CDB2HOSTNAME_LEN], int *num_hosts, int *comdb2db_num,
                              const char *dbname, char db_hosts[][CDB2HOSTNAME_LEN], int *num_db_hosts, int *dbnum,
                              int *stack_at_open)
{
    char line[PATH_MAX > 2048 ? PATH_MAX : 2048] = {0};
    int line_no = 0;

    debugprint("entering\n");
    while (read_line((char *)&line, sizeof(line), s, buf, &line_no) != -1) {
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
                strncpy(comdb2db_hosts[*num_hosts], tok, CDB2HOSTNAME_LEN - 1);
                (*num_hosts)++;
                tok = strtok_r(NULL, " :,", &last);
            }
        } else if (dbname && (strcasecmp(dbname, tok) == 0)) {
            tok = strtok_r(NULL, " :,", &last);
            if (tok && is_valid_int(tok)) {
                *dbnum = atoi(tok);
                tok = strtok_r(NULL, " :,", &last);
            }
            while (tok != NULL) {
                strncpy(db_hosts[*num_db_hosts], tok, CDB2HOSTNAME_LEN - 1);
                tok = strtok_r(NULL, " :,", &last);
                (*num_db_hosts)++;
            }
        } else if (strcasecmp("comdb2_feature", tok) == 0) {
            tok = strtok_r(NULL, " =:,", &last);
            if ((strcasecmp("iam_identity_v6",tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok)
                    iam_identity = value_on_off(tok);
            }
        } else if (strcasecmp("comdb2_config", tok) == 0) {
            tok = strtok_r(NULL, " =:,", &last);
            if (tok == NULL) continue;
            pthread_mutex_lock(&cdb2_sockpool_mutex);
            if (strcasecmp("default_type", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    if (hndl && (strcasecmp(hndl->cluster, "default") == 0)) {
                        strncpy(hndl->cluster, tok, sizeof(cdb2_default_cluster) - 1);
                    } else if (!hndl) {
                        strncpy(cdb2_default_cluster, tok, sizeof(cdb2_default_cluster) - 1);
                    }
                }
            } else if (strcasecmp("room", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    strncpy(cdb2_machine_room, tok, sizeof(cdb2_machine_room) - 1);
            } else if (strcasecmp("portmuxport", tok) == 0 || strcasecmp("pmuxport", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    CDB2_PORTMUXPORT = atoi(tok);
            } else if (strcasecmp("connect_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl && tok)
                    hndl->connect_timeout = atoi(tok);
                else if (tok)
                    CDB2_CONNECT_TIMEOUT = atoi(tok);
            } else if (strcasecmp("api_call_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl && tok)
                    hndl->api_call_timeout = atoi(tok);
                else if (tok)
                    CDB2_API_CALL_TIMEOUT = atoi(tok);
            } else if (strcasecmp("auto_consume_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    CDB2_AUTO_CONSUME_TIMEOUT_MS = atoi(tok);
            } else if (strcasecmp("comdb2db_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl && tok)
                    hndl->comdb2db_timeout = atoi(tok);
                else if (tok)
                    COMDB2DB_TIMEOUT = atoi(tok);
            } else if (strcasecmp("socket_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl && tok)
                    hndl->socket_timeout = atoi(tok);
                else if (tok)
                    CDB2_SOCKET_TIMEOUT = atoi(tok);
            } else if (strcasecmp("protobuf_size", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl)
                    hndl->protobuf_size = atoi(tok);
                else
                    CDB2_PROTOBUF_SIZE = atoi(tok);
            } else if (strcasecmp("comdb2dbname", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    strncpy(cdb2_comdb2dbname, tok, sizeof(cdb2_comdb2dbname) - 1);
            } else if (strcasecmp("tcpbufsz", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    cdb2_tcpbufsz = atoi(tok);
            } else if (strcasecmp("dnssufix", tok) == 0 ||
                       strcasecmp("dnssuffix", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    strncpy(cdb2_dnssuffix, tok, sizeof(cdb2_dnssuffix) - 1);
            } else if (strcasecmp("stack_at_open", tok) == 0 && stack_at_open) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    if (strncasecmp(tok, "true", 4) == 0) {
                        *stack_at_open = 1;
                    } else {
                        *stack_at_open = 0;
                    }
                }
            } else if (strcasecmp("ssl_mode", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok != NULL) {
                    if (strcasecmp(SSL_MODE_ALLOW, tok) == 0)
                        cdb2_c_ssl_mode = SSL_ALLOW;
                    else if (strcasecmp(SSL_MODE_PREFER, tok) == 0)
                        cdb2_c_ssl_mode = SSL_PREFER;
                    else if (strcasecmp(SSL_MODE_PREFER_VERIFY_CA, tok) == 0)
                        cdb2_c_ssl_mode = SSL_PREFER_VERIFY_CA;
                    else if (strcasecmp(SSL_MODE_PREFER_VERIFY_HOST, tok) == 0)
                        cdb2_c_ssl_mode = SSL_PREFER_VERIFY_HOSTNAME;
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
                    } else if (strcasecmp(SSL_MODE_PREFER_VERIFY_DBNAME, tok) == 0) {
                        cdb2_c_ssl_mode = SSL_PREFER_VERIFY_DBNAME;
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
            } else if (strcasecmp(SSL_MIN_TLS_VER_OPT, tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    cdb2_min_tls_ver = atof(tok);
            } else if (strcasecmp("allow_pmux_route", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    if (strncasecmp(tok, "true", 4) == 0) {
                        cdb2_allow_pmux_route = 1;
                    } else {
                        cdb2_allow_pmux_route = 0;
                    }
                }
            } else if (strcasecmp("install_static_libs_v2", tok) == 0 ||
                       strcasecmp("enable_static_libs", tok) == 0) {
                if (cdb2_install != NULL)
                    (*cdb2_install)();
            } else if (strcasecmp("uninstall_static_libs_v2", tok) == 0 ||
                       strcasecmp("disable_static_libs", tok) == 0) {
                /* Provide a way to disable statically installed (via
                 * CDB2_INSTALL_LIBS) libraries. */
                if (cdb2_uninstall != NULL)
                    (*cdb2_uninstall)();
#if WITH_DL_LIBS
            } else if (strcasecmp("lib", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    void *handle =
                        dlopen(tok, RTLD_NOLOAD | RTLD_NOW | RTLD_GLOBAL);
                    if (handle == NULL) {
                        handle = dlopen(tok, RTLD_NOW | RTLD_GLOBAL);
                        if (handle == NULL)
                            fprintf(stderr, "dlopen(%s) failed: %s\n", tok,
                                    dlerror());
                        else {
                            cdb2_init_t libinit =
                                (cdb2_init_t)dlsym(handle, "cdb2_lib_init");
                            if (libinit == NULL)
                                fprintf(stderr,
                                        "dlsym(cdb2_lib_init) failed: %s\n",
                                        dlerror());
                            else
                                (*libinit)();
                        }
                    }
                }
#endif
            } else if (strcasecmp("include_defaults", tok) == 0) {
                pthread_mutex_unlock(&cdb2_sockpool_mutex);
                only_read_config(NULL, 1, 1);
                pthread_mutex_lock(&cdb2_sockpool_mutex);
            } else if (strcasecmp("request_fingerprint", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    CDB2_REQUEST_FP = (strncasecmp(tok, "true", 4) == 0);
            } else if (strcasecmp("get_hostname_from_sockpool_fd", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    CDB2_GET_HOSTNAME_FROM_SOCKPOOL_FD = (strncasecmp(tok, "true", 4) == 0);
            }
            pthread_mutex_unlock(&cdb2_sockpool_mutex);
        }
        bzero(line, sizeof(line));
    }
}

static int cdb2_dbinfo_query(cdb2_hndl_tp *hndl, const char *type, const char *dbname, int dbnum, const char *host,
                             char valid_hosts[][CDB2HOSTNAME_LEN], int *valid_ports, int *master_node,
                             int *num_valid_hosts, int *num_valid_sameroom_hosts);
static int get_config_file(const char *dbname, char *f, size_t s)
{
    char *root = getenv("COMDB2_ROOT");

    /* `dbname' is NULL if we're only reading defaults from comdb2db.cfg.
       Formatting a NULL pointer with %s specifier is undefined behavior,
       and whatever file path it produces (in glibc, it's "(null).cfg")
       does not make sense. Return an error. */
    if (dbname == NULL)
        return -1;

    if (root == NULL)
        root = QUOTE(COMDB2_ROOT);
    size_t n;
    n = snprintf(f, s, "%s%s%s.cfg", root, CDB2DBCONFIG_NOBBENV_PATH, dbname);
    if (n >= s)
        return -1;
    return 0;
}

static void set_cdb2_timeouts(cdb2_hndl_tp *hndl)
{
    if (!hndl)
        return;
    if (!hndl->api_call_timeout)
        hndl->api_call_timeout = CDB2_API_CALL_TIMEOUT;
    if (!hndl->connect_timeout)
        hndl->connect_timeout = CDB2_CONNECT_TIMEOUT;
    if (!hndl->comdb2db_timeout)
        hndl->comdb2db_timeout = COMDB2DB_TIMEOUT;
    if (!hndl->socket_timeout)
        hndl->socket_timeout = CDB2_SOCKET_TIMEOUT;
    if (!hndl->auto_consume_timeout_ms)
        hndl->auto_consume_timeout_ms = CDB2_AUTO_CONSUME_TIMEOUT_MS;
}

/* Read all available comdb2 configuration files.
   The function returns -1 if the config file path is longer than PATH_MAX;
   returns 0 otherwise. */
static int read_available_comdb2db_configs(cdb2_hndl_tp *hndl, char comdb2db_hosts[][CDB2HOSTNAME_LEN],
                                           const char *comdb2db_name, int *num_hosts, int *comdb2db_num,
                                           const char *dbname, char db_hosts[][CDB2HOSTNAME_LEN], int *num_db_hosts,
                                           int *dbnum, int noLock, int defaultOnly)
{
    char filename[PATH_MAX];
    SBUF2 *s;
    int fallback_on_bb_bin = 1;

    if (hndl)
        debugprint("entering\n");

    if (!noLock) pthread_mutex_lock(&cdb2_cfg_lock);

    if (num_hosts)
        *num_hosts = 0;
    if (num_db_hosts)
        *num_db_hosts = 0;
    int *send_stack = hndl ? (&hndl->send_stack) : NULL;

    if (!defaultOnly && CDB2DBCONFIG_BUF != NULL) {
        read_comdb2db_cfg(NULL, NULL, comdb2db_name, CDB2DBCONFIG_BUF,
                          comdb2db_hosts, num_hosts, comdb2db_num, dbname,
                          db_hosts, num_db_hosts, dbnum, send_stack);
        fallback_on_bb_bin = 0;
    } else {
        if (!defaultOnly && *CDB2DBCONFIG_NOBBENV != '\0') {
            s = sbuf2openread(CDB2DBCONFIG_NOBBENV);
            if (s != NULL) {
                read_comdb2db_cfg(NULL, s, comdb2db_name, NULL, comdb2db_hosts,
                                  num_hosts, comdb2db_num, dbname, db_hosts,
                                  num_db_hosts, dbnum, send_stack);
                sbuf2close(s);
                fallback_on_bb_bin = 0;
            }
        }
    }

    /* This is a temporary solution.  There's no clear plan for how comdb2db.cfg
     * will be deployed to non-dbini machines. In the meantime, we have
     * programmers who want to use the API on dbini/mini machines. So if we
     * can't find the file in any standard location, look at /bb/bin
     * Once deployment details for comdb2db.cfg solidify, this will go away. */
    if (fallback_on_bb_bin) {
        s = sbuf2openread(CDB2DBCONFIG_TEMP_BB_BIN);
        if (s != NULL) {
            read_comdb2db_cfg(NULL, s, comdb2db_name, NULL, comdb2db_hosts,
                              num_hosts, comdb2db_num, dbname, db_hosts,
                              num_db_hosts, dbnum, send_stack);
            sbuf2close(s);
        }
    }

    if (get_config_file(dbname, filename, sizeof(filename)) == 0) {
        s = sbuf2openread(filename);
        if (s != NULL) {
            read_comdb2db_cfg(hndl, s, comdb2db_name, NULL, comdb2db_hosts, num_hosts, comdb2db_num, dbname, db_hosts,
                              num_db_hosts, dbnum, send_stack);
            sbuf2close(s);
        }
    }
    if (!noLock) pthread_mutex_unlock(&cdb2_cfg_lock);
    return 0;
}

int cdb2_get_comdb2db(char **comdb2dbname)
{
    if (!strlen(cdb2_comdb2dbname)) {
        read_available_comdb2db_configs(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0);
    }
    (*comdb2dbname) = strdup(cdb2_comdb2dbname);
    return 0;
}

/* populate comdb2db_hosts based on hostname info of comdb2db_name
 * returns -1 if error or no osts wa found
 * returns 0 if hosts were found
 * this function has functionality similar to cdb2_tcpresolve()
 */
static int get_host_by_name(const char *comdb2db_name, char comdb2db_hosts[][CDB2HOSTNAME_LEN], int *num_hosts)
{
    struct hostent *hp = NULL;
    char dns_name[512];

    if (cdb2_default_cluster[0] == '\0') {
        snprintf(dns_name, sizeof(dns_name), "%s.%s", comdb2db_name, cdb2_dnssuffix);
    } else {
        snprintf(dns_name, sizeof(dns_name), "%s-%s.%s", cdb2_default_cluster, comdb2db_name, cdb2_dnssuffix);
    }
#ifdef __APPLE__
    hp = gethostbyname(dns_name);
#elif _LINUX_SOURCE
    int herr;
    char tmp[8192];
    int tmplen = sizeof(tmp);
    struct hostent hostbuf;
    gethostbyname_r(dns_name, &hostbuf, tmp, tmplen, &hp, &herr);
#elif _SUN_SOURCE
    int herr;
    char tmp[8192];
    int tmplen = sizeof(tmp);
    struct hostent hostbuf;
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

static int get_comdb2db_hosts(cdb2_hndl_tp *hndl, char comdb2db_hosts[][CDB2HOSTNAME_LEN], int *comdb2db_ports,
                              int *master, const char *comdb2db_name, int *num_hosts, int *comdb2db_num,
                              const char *dbname, char db_hosts[][CDB2HOSTNAME_LEN], int *num_db_hosts, int *dbnum,
                              int dbinfo_or_dns)
{
    int rc;

    if (hndl)
        debugprint("entering\n");

    rc = read_available_comdb2db_configs(hndl, comdb2db_hosts, comdb2db_name, num_hosts, comdb2db_num, dbname, db_hosts,
                                         num_db_hosts, dbnum, 0, 0);
    if (rc == -1)
        return rc;
    if (master)
        *master = -1;
    set_cdb2_timeouts(hndl);

    if (dbinfo_or_dns) {
        /* If previous call to the function successfully retrieved hosts,
           return 0. */
        if (*num_hosts > 0 || *num_db_hosts > 0)
            return 0;
        /* If we have a cached connection to comdb2db in the sockpool, use it to
           get comdb2db dbinfo. */
        rc = cdb2_dbinfo_query(hndl, cdb2_default_cluster, comdb2db_name,
                               *comdb2db_num, NULL, comdb2db_hosts,
                               comdb2db_ports, master, num_hosts, NULL);
        /* DNS lookup comdb2db hosts. */
        if (rc != 0)
            rc = get_host_by_name(comdb2db_name, comdb2db_hosts, num_hosts);
    }

    return rc;
}

/* SOCKPOOL CODE START */

#define SOCKPOOL_ENABLED_DEFAULT 1
static int sockpool_enabled = SOCKPOOL_ENABLED_DEFAULT;

#define SOCKPOOL_FAIL_TIME_DEFAULT 0
static time_t sockpool_fail_time = SOCKPOOL_FAIL_TIME_DEFAULT;

struct sockpool_fd_list {
    int sockpool_fd;
    int in_use;
};

static struct sockpool_fd_list *sockpool_fds = NULL;
static int sockpool_fd_count = 0;
static int sockpool_generation = 0;

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
    if (SOCKPOOL_OTHER_NAME)
        strncpy(addr.sun_path, SOCKPOOL_OTHER_NAME, sizeof(addr.sun_path) - 1);
    else
        strncpy(addr.sun_path, SOCKPOOL_SOCKET_NAME, sizeof(addr.sun_path) - 1);

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
    if (forceClose || (sockpool_enabled != -1)) {
        sockpool_generation++;
        for (int i = 0; i < sockpool_fd_count; i++) {
            struct sockpool_fd_list *sp = &sockpool_fds[i];
            if (sp->in_use == 0) {
                if (sp->sockpool_fd > -1)
                    close(sp->sockpool_fd);
                sp->sockpool_fd = -1;
            }
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

/* The sockpool mutex must be locked at this point */
static int sockpool_get_from_pool(void)
{
    int fd = -1;
    for (int i = 0; i < sockpool_fd_count; i++) {
        struct sockpool_fd_list *sp = &sockpool_fds[i];
        if (sp->in_use == 0 && sp->sockpool_fd > -1) {
            fd = sp->sockpool_fd;
            sp->in_use = 1;
            break;
        }
    }
    return fd;
}

/* The sockpool mutex must be locked at this point */
static int sockpool_place_fd_in_pool(int fd)
{
    int found = 0, empty_ix = -1, rc = -1;
    for (int i = 0; i < sockpool_fd_count; i++) {
        struct sockpool_fd_list *sp = &sockpool_fds[i];
        if (sp->sockpool_fd == fd) {
            assert(sp->in_use == 1);
            sp->in_use = 0;
            found = 1;
            rc = 0;
            break;
        }
        if (sp->sockpool_fd < 0 && empty_ix == -1) {
            assert(sp->in_use == 0);
            empty_ix = i;
        }
    }

    if (found == 0) {
        if (empty_ix != -1) {
            struct sockpool_fd_list *sp = &sockpool_fds[empty_ix];
            sp->in_use = 0;
            sp->sockpool_fd = fd;
            rc = 0;
        } else if (sockpool_fd_count < MAX_SOCKPOOL_FDS) {
            sockpool_fds =
                realloc(sockpool_fds, (sockpool_fd_count + 1) *
                                          sizeof(struct sockpool_fd_list));
            sockpool_fds[sockpool_fd_count].sockpool_fd = fd;
            sockpool_fds[sockpool_fd_count].in_use = 0;
            sockpool_fd_count++;
            rc = 0;
        }
    }
    return rc;
}

static void sockpool_remove_fd(int fd)
{
    for (int i = 0; i < sockpool_fd_count; i++) {
        struct sockpool_fd_list *sp = &sockpool_fds[i];
        if (sp->sockpool_fd == fd) {
            assert(sp->in_use == 1);
            sp->sockpool_fd = -1;
            sp->in_use = 0;
            break;
        }
    }
}

static void sockpool_close_all(void)
{
    for (int i = 0; i < sockpool_fd_count; i++) {
        struct sockpool_fd_list *sp = &sockpool_fds[i];
        if (sp->sockpool_fd != -1) {
            close(sp->sockpool_fd);
            sp->sockpool_fd = -1;
            sp->in_use = 0;
        }
    }
}

// cdb2_socket_pool_get_ll: low-level
static int cdb2_socket_pool_get_ll(const char *typestr, int dbnum, int *port)
{
    int sockpool_fd = -1;
    int enabled = 0;
    int sp_generation = -1;
    int fd = -1;

    pthread_mutex_lock(&cdb2_sockpool_mutex);
    if (sockpool_enabled == 0) {
        time_t current_time = time(NULL);
        /* Check every 10 seconds. */
        if ((current_time - sockpool_fail_time) > 10) {
            sockpool_enabled = 1;
        }
    }
    enabled = sockpool_enabled;
    if (enabled == 1) {
        sp_generation = sockpool_generation;
        sockpool_fd = sockpool_get_from_pool();
    }
    pthread_mutex_unlock(&cdb2_sockpool_mutex);

    if (enabled != 1) {
        return -1;
    }

    if (sockpool_fd == -1) {
        sockpool_fd = open_sockpool_ll();
        if (sockpool_fd == -1) {
            pthread_mutex_lock(&cdb2_sockpool_mutex);
            sockpool_enabled = 0;
            sockpool_fail_time = time(NULL);
            pthread_mutex_unlock(&cdb2_sockpool_mutex);
            return -1;
        }
    }

    struct sockpool_msg_vers0 msg = {0};
    if (strlen(typestr) >= sizeof(msg.typestr)) {
        int closeit = 0;
        pthread_mutex_lock(&cdb2_sockpool_mutex);
        if (sp_generation == sockpool_generation) {
            if ((sockpool_place_fd_in_pool(sockpool_fd)) != 0) {
                closeit = 1;
            }
        } else {
            sockpool_remove_fd(sockpool_fd);
            closeit = 1;
        }
        pthread_mutex_unlock(&cdb2_sockpool_mutex);
        if (closeit)
            close(sockpool_fd);
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
        pthread_mutex_lock(&cdb2_sockpool_mutex);
        sockpool_remove_fd(sockpool_fd);
        pthread_mutex_unlock(&cdb2_sockpool_mutex);
        close(sockpool_fd);
        sockpool_fd = -1;
        return -1;
    }

    /* Read reply from server.  It can legitimately not send
     * us a file descriptor. */
    errno = 0;
    rc = recv_fd(sockpool_fd, &msg, sizeof(msg), &fd);
    if (rc != PASSFD_SUCCESS) {
        fprintf(stderr, "%s: recv_fd rc %d errno %d %s\n", __func__, rc, errno,
                strerror(errno));
        pthread_mutex_lock(&cdb2_sockpool_mutex);
        sockpool_remove_fd(sockpool_fd);
        pthread_mutex_unlock(&cdb2_sockpool_mutex);
        close(sockpool_fd);
        sockpool_fd = -1;
        fd = -1;
    }
    if (fd == -1 && port) {
        short gotport;
        memcpy((char *)&gotport, (char *)&msg.padding[1], 2);
        *port = ntohs(gotport);
    }
    if (sockpool_fd != -1) {
        int closeit = 0;
        pthread_mutex_lock(&cdb2_sockpool_mutex);
        if (sp_generation == sockpool_generation) {
            if ((sockpool_place_fd_in_pool(sockpool_fd)) != 0) {
                closeit = 1;
            }
        } else {
            sockpool_remove_fd(sockpool_fd);
            closeit = 1;
        }
        pthread_mutex_unlock(&cdb2_sockpool_mutex);
        if (closeit)
            close(sockpool_fd);
    }
    return fd;
}

/* Get the file descriptor of a socket matching the given type string from
 * the pool.  Returns -1 if none is available or the file descriptor on
 * success. */
int cdb2_socket_pool_get(const char *typestr, int dbnum, int *port)
{
    int rc = cdb2_socket_pool_get_ll(typestr, dbnum, port);
    if (log_calls)
        fprintf(stderr, "%s(%s,%d): fd=%d\n", __func__, typestr, dbnum, rc);
    return rc;
}

void cdb2_socket_pool_donate_ext(const char *typestr, int fd, int ttl,
                                 int dbnum)
{
    int enabled = 0;
    int sockpool_fd = -1;
    int sp_generation = -1;

    pthread_mutex_lock(&cdb2_sockpool_mutex);
    enabled = sockpool_enabled;
    if (enabled == 1) {
        sockpool_fd = sockpool_get_from_pool();
        sp_generation = sockpool_generation;
    }
    pthread_mutex_unlock(&cdb2_sockpool_mutex);

    if (enabled == 1) {
        /* Donate this socket to the global socket pool.  We know that the
         * mutex is held. */
        if (sockpool_fd == -1) {
            sockpool_fd = open_sockpool_ll();
            if (sockpool_fd == -1) {
                pthread_mutex_lock(&cdb2_sockpool_mutex);
                sockpool_enabled = 0;
                pthread_mutex_unlock(&cdb2_sockpool_mutex);
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
                pthread_mutex_lock(&cdb2_sockpool_mutex);
                sockpool_remove_fd(sockpool_fd);
                pthread_mutex_unlock(&cdb2_sockpool_mutex);
                close(sockpool_fd);
                sockpool_fd = -1;
            }
        }
        if (sockpool_fd != -1) {
            pthread_mutex_lock(&cdb2_sockpool_mutex);
            int closeit = 0;
            if (sp_generation == sockpool_generation) {
                if ((sockpool_place_fd_in_pool(sockpool_fd)) != 0) {
                    closeit = 1;
                }
            } else {
                sockpool_remove_fd(sockpool_fd);
                closeit = 1;
            }
            pthread_mutex_unlock(&cdb2_sockpool_mutex);
            if (closeit)
                close(sockpool_fd);
            sockpool_fd = -1;
        }
    }

    if (close(fd) == -1) {
        fprintf(stderr, "%s: close error for '%s' fd %d: %d %s\n", __func__,
                typestr, fd, errno, strerror(errno));
    }
}

/* SOCKPOOL CODE ENDS */

static inline int cdb2_hostid()
{
    return _MACHINE_ID;
}

typedef enum {
    NEWSQL_STATE_NONE = 0,
    /* Query is making progress. Applicable only when type is HEARTBEAT */
    NEWSQL_STATE_ADVANCING,
    /* RESET from in-process cache. Applicable only when type is RESET */
    NEWSQL_STATE_LOCALCACHE
} newsql_state;

static int send_reset(SBUF2 *sb)
{
    int rc = 0;
    struct newsqlheader hdr = {
        .type = ntohl(CDB2_REQUEST_TYPE__RESET),
        .state = ntohl(NEWSQL_STATE_NONE)
    };
    rc = sbuf2fwrite((char *)&hdr, sizeof(hdr), 1, sb);
    if (rc != 1) {
        return -1;
    }
    return 0;
}

static int try_ssl(cdb2_hndl_tp *hndl, SBUF2 *sb)
{
    /*
     *                   |<----------------------- CLIENT ----------------------->|
     *                   |--------------------------------------------------------|
     *                   |    REQUIRE    |    PREFER   |    ALLOW    |   others   |
     * -------|-------------------------------------------------------------------|
     *   ^    | REQUIRE  |     SSL[1]    |    SSL[1]   |    SSL[1]   |    X[2]    |
     *   |    |-------------------------------------------------------------------|
     *   |    | PREFER   |     SSL[1]    |    SSL[1]   |    SSL[1]   |  PLAINTEXT |
     *   |    |-------------------------------------------------------------------|
     * SERVER | ALLOW    |     SSL[1]    |    SSL[1]   |  PLAINTEXT  |  PLAINTEXT |
     *   |    |-------------------------------------------------------------------|
     *   v    | others   |     X[3]      |  PLAINTEXT  |  PLAINTEXT  |  PLAINTEXT |
     * -------|-------------------------------------------------------------------|
     *        [1] The client writes an SSL negotiation packet first.
     *        [2] Rejected by the server.
     *        [3] Rejected by the client API.
     */

    /* An application may use different certificates.
       So we allocate an SSL context for each handle. */
    SSL_CTX *ctx;
    int rc, dossl = 0;
    cdb2_ssl_sess *p;

    if (SSL_IS_REQUIRED(hndl->c_sslmode)) {
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
    } else if (SSL_IS_PREFERRED(hndl->c_sslmode)) {
        dossl = 1;
    } else { /* hndl->c_sslmode == SSL_ALLOW */
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
    struct newsqlheader hdr = {.type = ntohl(CDB2_REQUEST_TYPE__SSLCONN)};
    rc = sbuf2fwrite((char *)&hdr, sizeof(hdr), 1, sb);
    if (rc != 1)
        return -1;
    if ((rc = sbuf2flush(sb)) < 0 || (rc = sbuf2getc(sb)) < 0) {
        /* If SSL is optional (this includes ALLOW and PREFER), change
           my mode to ALLOW so that we can reconnect in plaintext. */
        if (SSL_IS_OPTIONAL(hndl->c_sslmode))
            hndl->c_sslmode = SSL_ALLOW;
        return rc;
    }

    /* The node does not agree with dbinfo. This usually happens
       during the downgrade from SSL to non-SSL. */
    if (rc != 'Y') {
        if (SSL_IS_OPTIONAL(hndl->c_sslmode)) {
            hndl->c_sslmode = SSL_ALLOW;
            /* if server sends back 'N', reuse this plaintext connection;
               force reconnecting for an unexpected byte */
            return (rc == 'N') ? 0 : -1;
        }

        /* We reach here only if the server is mistakenly downgraded
           before the client. */
        sprintf(hndl->errstr, "The database does not support SSL.");
        hndl->sslerr = 1;
        return -1;
    }

    rc = ssl_new_ctx(&ctx, hndl->c_sslmode, hndl->sslpath, &hndl->cert,
                     &hndl->key, &hndl->ca, &hndl->crl, hndl->num_hosts, NULL,
                     hndl->min_tls_ver, hndl->errstr, sizeof(hndl->errstr));
    if (rc != 0) {
        hndl->sslerr = 1;
        return -1;
    }

    p = hndl->sess;
    rc = sslio_connect(sb, ctx, hndl->c_sslmode, hndl->dbname, hndl->nid_dbname, ((p != NULL) ? p->sessobj : NULL));

    SSL_CTX_free(ctx);
    if (rc != 1) {
        hndl->sslerr = sbuf2lasterror(sb, hndl->errstr, sizeof(hndl->errstr));
        /* If SSL_connect() fails, invalidate the session. */
        if (p != NULL)
            p->sessobj = NULL;
        return -1;
    }
    hndl->newsess = 1;
    return 0;
}

static int cdb2portmux_route(cdb2_hndl_tp *hndl, const char *remote_host,
                             const char *app, const char *service,
                             const char *instance)
{
    char name[128];
    char res[32];
    SBUF2 *ss = NULL;
    int rc, fd;
    rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name)) {
        debugprint("ERROR: can not fit entire string into name '%s/%s/%s'\n",
                   app, service, instance);
        return -1;
    }

    debugprint("name %s\n", name);

    fd = cdb2_tcpconnecth_to(hndl, remote_host, CDB2_PORTMUXPORT, 0,
                             hndl->connect_timeout);
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
    debugprint("rte '%s' returns res=%s", name, res);
    if (res[0] != '0') { // character '0' is indication of success
        sbuf2close(ss);
        return -1;
    }
    sbuf2free(ss);
    return fd;
}

static void get_host_and_port_from_fd(int fd, char *buf, size_t n, int *port)
{
    int rc;
    struct sockaddr_in addr;
    socklen_t addr_size = sizeof(struct sockaddr_in);

    if (!CDB2_GET_HOSTNAME_FROM_SOCKPOOL_FD)
        return;

    if (fd == -1)
        return;

    rc = getpeername(fd, (struct sockaddr *)&addr, &addr_size);
    if (rc == 0) {
        *port = addr.sin_port;
        /* Request a short host name. Set buf to empty on error. */
        if (getnameinfo((struct sockaddr *)&addr, addr_size, buf, n, NULL, 0, NI_NOFQDN))
            buf[0] = '\0';
    }
}

static int newsql_connect_via_fd(cdb2_hndl_tp *hndl)
{
    int rc = 0;
    SBUF2 *sb = NULL;
    void *callbackrc;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;

    /* Handle BEFRE_NEWSQL_CONNECT callbacks */
    while ((e = cdb2_next_callback(hndl, CDB2_BEFORE_NEWSQL_CONNECT, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 2, CDB2_HOSTNAME, NULL, CDB2_PORT, -1);
        PROCESS_EVENT_CTRL_BEFORE(hndl, e, rc, callbackrc, overwrite_rc);
    }
    if (overwrite_rc)
        goto after_callback;

    char *endptr;
    int fd = strtol(hndl->type, &endptr, 10);
    if (*endptr != 0 || fd < 3) { /* shouldn't be stdin, stdout, stderr */
        debugprint("ERROR: %s:%d invalid fd:%s", __func__, __LINE__, hndl->type);
    } else {
        if ((sb = sbuf2open(fd, SBUF2_NO_CLOSE_FD)) == 0) {
            rc = -1;
            goto after_callback;
        }
        sbuf2printf(sb, hndl->is_admin ? "@newsql\n" : "newsql\n");
        sbuf2flush(sb);
    }

    sbuf2settimeout(sb, hndl->socket_timeout, hndl->socket_timeout);

    if (try_ssl(hndl, sb) != 0) {
        sbuf2close(sb);
        rc = -1;
        goto after_callback;
    }

    hndl->sb = sb;
    hndl->num_set_commands_sent = 0;
    hndl->sent_client_info = 0;
    hndl->connected_host = 0;
    hndl->hosts_connected[hndl->connected_host] = 1;
    debugprint("connected_host=%s\n", hndl->hosts[hndl->connected_host]);

after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_NEWSQL_CONNECT, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 3, CDB2_HOSTNAME, NULL, CDB2_PORT, -1, CDB2_RETURN_VALUE, rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }
    return rc;
}

static int cdb2portmux_get(cdb2_hndl_tp *hndl, const char *type, const char *remote_host, const char *app,
                           const char *service, const char *instance);

/* Tries to connect to specified node using sockpool.
 * If there is none, then makes a new socket connection.
 */
static int newsql_connect(cdb2_hndl_tp *hndl, int node_indx)
{
    const char *host = hndl->hosts[node_indx];
    int port = hndl->ports[node_indx];
    debugprint("entering, host '%s:%d'\n", host, port);
    int fd = -1;
    int rc = 0;
    int nprinted;
    SBUF2 *sb = NULL;
    void *callbackrc;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;

    /* Handle BEFRE_NEWSQL_CONNECT callbacks */
    while ((e = cdb2_next_callback(hndl, CDB2_BEFORE_NEWSQL_CONNECT, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 2, CDB2_HOSTNAME, host, CDB2_PORT, port);
        PROCESS_EVENT_CTRL_BEFORE(hndl, e, rc, callbackrc, overwrite_rc);
    }
    if (overwrite_rc)
        goto after_callback;

    nprinted = snprintf(hndl->newsql_typestr, sizeof(hndl->newsql_typestr), "comdb2/%s/%s/newsql/%s", hndl->dbname,
                        hndl->type, hndl->policy);
    if (nprinted < 0) {
        debugprint("ERROR: %s:%d error in snprintf", __func__, __LINE__);
    } else if (nprinted >= sizeof(hndl->newsql_typestr)) {
        debugprint("ERROR: can not fit entire string into "
                   "'comdb2/%s/%s/newsql/%s' only %s\n",
                   hndl->dbname, hndl->type, hndl->policy,
                   hndl->newsql_typestr);
    }

    while (!hndl->is_admin && !(hndl->flags & CDB2_MASTER) &&
           (fd = cdb2_socket_pool_get(hndl->newsql_typestr, hndl->dbnum, NULL)) > 0) {
        get_host_and_port_from_fd(fd, hndl->cached_host, sizeof(hndl->cached_host), &hndl->cached_port);
        if ((sb = sbuf2open(fd, 0)) == 0) {
            close(fd);
            rc = -1;
            goto after_callback;
        }
        if (send_reset(sb) == 0) {
            break;      // connection is ready
        }
        sbuf2close(sb); // retry newsql connect;
    }

    if (fd < 0) {
        if (!cdb2_allow_pmux_route) {
            if (port <= 0) {
                port = cdb2portmux_get(hndl, hndl->type, host, "comdb2", "replication", hndl->dbname);
                hndl->ports[node_indx] = port;
            }
            fd = cdb2_tcpconnecth_to(hndl, host, port, 0, hndl->connect_timeout);
        } else {
            if (port <= 0) {
                /* cdb2portmux_route() works without the assignment. We do it here to make it clear
                   that we will be connecting directly to portmux on this host. */
                hndl->ports[node_indx] = CDB2_PORTMUXPORT;
            }
            fd = cdb2portmux_route(hndl, host, "comdb2", "replication",
                                   hndl->dbname);
        }
        if (fd < 0) {
            rc = -1;
            goto after_callback;
        }

        if ((sb = sbuf2open(fd, 0)) == 0) {
            close(fd);
            rc = -1;
            goto after_callback;
        }
        sbuf2printf(sb, hndl->is_admin ? "@newsql\n" : "newsql\n");
        sbuf2flush(sb);
    }

    sbuf2settimeout(sb, hndl->socket_timeout, hndl->socket_timeout);

    if (try_ssl(hndl, sb) != 0) {
        sbuf2close(sb);
        rc = -1;
        goto after_callback;
    }

    hndl->sb = sb;
    hndl->num_set_commands_sent = 0;
    hndl->sent_client_info = 0;
    hndl->connected_host = node_indx;
    hndl->hosts_connected[hndl->connected_host] = 1;
    debugprint("connected_host=%s\n", hndl->hosts[hndl->connected_host]);

after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_NEWSQL_CONNECT, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 3, CDB2_HOSTNAME, host, CDB2_PORT, port, CDB2_RETURN_VALUE, rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }
    return rc;
}

static void newsql_disconnect(cdb2_hndl_tp *hndl, SBUF2 *sb, int line)
{
    if (sb == NULL)
        return;

    debugprint("disconnecting from %s, line %d\n",
               hndl->hosts[hndl->connected_host], line);
    int fd = sbuf2fileno(sb);

    int timeoutms = 10 * 1000;
    if (hndl->is_admin ||
        (hndl->firstresponse &&
         (!hndl->lastresponse ||
          (hndl->lastresponse->response_type != RESPONSE_TYPE__LAST_ROW))) ||
        (!hndl->firstresponse) ||
        (hndl->in_trans) ||
        ((hndl->flags & CDB2_TYPE_IS_FD) != 0)) {
        sbuf2close(sb);
    } else if (sbuf2free(sb) == 0) {
        cdb2_socket_pool_donate_ext(hndl->newsql_typestr, fd, timeoutms / 1000,
                                    hndl->dbnum);
    }
    hndl->use_hint = 0;
    hndl->sb = NULL;
    return;
}

/* returns port number, or -1 for error*/
static int cdb2portmux_get(cdb2_hndl_tp *hndl, const char *type,
                           const char *remote_host, const char *app,
                           const char *service, const char *instance)
{
    char name[128]; /* app/service/dbname */
    char res[32];
    SBUF2 *ss = NULL;
    int rc, fd, port = -1;

    void *callbackrc;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;

    while ((e = cdb2_next_callback(hndl, CDB2_BEFORE_PMUX, e)) != NULL) {
        callbackrc =
            cdb2_invoke_callback(hndl, e, 2, CDB2_HOSTNAME, remote_host,
                                 CDB2_PORT, CDB2_PORTMUXPORT);
        PROCESS_EVENT_CTRL_BEFORE(hndl, e, port, callbackrc, overwrite_rc);
    }

    if (overwrite_rc)
        goto after_callback;

    rc = snprintf(name, sizeof(name), "%s/%s/%s", app, service, instance);
    if (rc < 1 || rc >= sizeof(name)) {
        debugprint("ERROR: can not fit entire string into name '%s/%s/%s'\n",
                   app, service, instance);
        port = -1;
        goto after_callback;
    }

    debugprint("name %s\n", name);

    fd = cdb2_tcpconnecth_to(hndl, remote_host, CDB2_PORTMUXPORT, 0,
                             hndl->connect_timeout);
    if (fd < 0) {
        debugprint("cdb2_tcpconnecth_to returns fd=%d'\n", fd);
        snprintf(
            hndl->errstr, sizeof(hndl->errstr),
            "%s:%d Can't connect to portmux port dbname: %s tier: %s host: %s "
            "port %d. "
            "Err(%d): %s. Portmux down on remote machine or firewall issue.",
            __func__, __LINE__, instance, type, remote_host, CDB2_PORTMUXPORT,
            errno, strerror(errno));
        port = -1;
        goto after_callback;
    }
    ss = sbuf2open(fd, 0);
    if (ss == 0) {
        snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d out of memory\n",
                 __func__, __LINE__);
        close(fd);
        debugprint("sbuf2open returned 0\n");
        port = -1;
        goto after_callback;
    }
    sbuf2settimeout(ss, hndl->connect_timeout, hndl->connect_timeout);
    sbuf2printf(ss, "get %s\n", name);
    sbuf2flush(ss);
    res[0] = '\0';
    sbuf2gets(res, sizeof(res), ss);
    sbuf2close(ss);
    debugprint("get '%s' returns res='%s'\n", name, res);
    if (res[0] == '\0') {
        snprintf(hndl->errstr, sizeof(hndl->errstr),
                 "%s:%d Invalid response from portmux.\n", __func__, __LINE__);
        port = -1;
        goto after_callback;
    }
    port = atoi(res);
    if (port <= 0) {
        snprintf(hndl->errstr, sizeof(hndl->errstr),
                 "%s:%d Invalid response from portmux.\n", __func__, __LINE__);
        port = -1;
    }
after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_PMUX, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(
            hndl, e, 3, CDB2_HOSTNAME, remote_host, CDB2_PORT, CDB2_PORTMUXPORT,
            CDB2_RETURN_VALUE, (intptr_t)port);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, port, callbackrc);
    }
    return port;
}

void cdb2_use_hint(cdb2_hndl_tp *hndl)
{
    hndl->use_hint = 1;
    if (log_calls) {
        fprintf(stderr, "%p> cdb2_use_hint(%p)\n", (void *)pthread_self(),
                hndl);
    }
}

/* try to connect to range from low (inclusive) to high (exclusive) starting with begin */
static inline int cdb2_try_connect_range(cdb2_hndl_tp *hndl, int begin, int low, int high)
{
    int max = high - low;
    /* Starting from `begin', try up to `max' times */
    for (int j = 0, i = begin; j < max; j++, i++) {
        /* if we've reached the end of the range, start from the beginning */
        if (i == high)
            i = low;

        hndl->node_seq = i + 1;
        if (i == hndl->master || i == hndl->connected_host || hndl->hosts_connected[i] == 1)
            continue;
        if (newsql_connect(hndl, i) == 0)
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

static __thread unsigned short rand_state[3] = {0};
static __thread unsigned short do_once = 0;

static int cdb2_random_int()
{
    if (!do_once) {
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
        do_once = 1;
    }
    return nrand48(rand_state);
}

/* Get random value from range min to max-1 excluding 1 value */
static int getRandomExclude(int min, int max, int exclude)
{
    int val = 0;
    /* If the range has only 1 value, return it */
    if (max - min < 2)
        return min;
    for (int i = 0; i < 10; i++) {
        val = cdb2_random_int() % (max - min) + min;
        if (val != exclude)
            return val;
    }
    return val;
}

static int cdb2_connect_sqlhost(cdb2_hndl_tp *hndl)
{
    if (hndl->flags & CDB2_TYPE_IS_FD) {
        return newsql_connect_via_fd(hndl);
    }

    if (hndl->sb) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
    }

    int requery_done = 0;

retry_connect:
    debugprint("node_seq=%d flags=0x%x, num_hosts=%d, num_hosts_sameroom=%d\n", hndl->node_seq, hndl->flags,
               hndl->num_hosts, hndl->num_hosts_sameroom);

    if ((hndl->flags & CDB2_MASTER)) {
        bzero(hndl->hosts_connected, sizeof(hndl->hosts_connected));
        if (newsql_connect(hndl, hndl->master) == 0) {
            return 0;
        }
        hndl->connected_host = -1;
        return -1;
    }

    if ((hndl->node_seq == 0) &&
        ((hndl->flags & CDB2_RANDOM) || ((hndl->flags & CDB2_RANDOMROOM) &&
                                         (hndl->num_hosts_sameroom == 0)))) {
        hndl->node_seq = getRandomExclude(0, hndl->num_hosts, hndl->master);
    } else if ((hndl->flags & CDB2_RANDOMROOM) && (hndl->node_seq == 0) &&
               (hndl->num_hosts_sameroom > 0)) {
        hndl->node_seq = getRandomExclude(0, hndl->num_hosts_sameroom, hndl->master);
        /* First try on same room. */
        if (0 == cdb2_try_connect_range(hndl, hndl->node_seq, 0, hndl->num_hosts_sameroom))
            return 0;
        /* If we fail to connect to any of the nodes in our DC, choose a random start point from the other DC */
        hndl->node_seq = getRandomExclude(hndl->num_hosts_sameroom, hndl->num_hosts, hndl->master);
    }

    if (0 == cdb2_try_connect_range(hndl, hndl->node_seq, 0, hndl->num_hosts))
        return 0;

    if (hndl->sb == NULL) {
        /* Can't connect to any of the non-master nodes, try connecting to
         * master.*/
        /* After this retry on other nodes. */
        bzero(hndl->hosts_connected, sizeof(hndl->hosts_connected));
        if (newsql_connect(hndl, hndl->master) == 0)
            return 0;
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
    struct newsqlheader hdr = {.type =
                                   htonl(RESPONSE_HEADER__SQL_RESPONSE_PONG)};

    sbuf2write((void *)&hdr, sizeof(hdr), hndl->sb);
    sbuf2flush(hndl->sb);
}

static int cdb2_read_record(cdb2_hndl_tp *hndl, uint8_t **buf, int *len, int *type)
{
    /* Got response */
    SBUF2 *sb = hndl->sb;
    struct newsqlheader hdr = {0};
    int b_read;
    int rc = 0; /* Make compilers happy. */

    void *callbackrc;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;

    while ((e = cdb2_next_callback(hndl, CDB2_BEFORE_READ_RECORD, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 0);
        PROCESS_EVENT_CTRL_BEFORE(hndl, e, rc, callbackrc, overwrite_rc);
    }

    if (overwrite_rc)
        goto after_callback;

retry:
    b_read = sbuf2fread((char *)&hdr, 1, sizeof(hdr), sb);
    debugprint("READ HDR b_read=%d, sizeof(hdr)=(%zu):\n", b_read, sizeof(hdr));

    if (b_read != sizeof(hdr)) {
        debugprint("bad read or numbytes, b_read=%d, sizeof(hdr)=(%zu):\n",
                   b_read, sizeof(hdr));
        rc = -1;
        /* In TLS 1.3, client authentication happens after handshake (RFC 8446).
           An invalid client (e.g., a revoked cert) may see a successful
           handshake but encounter an error when reading data from the server.
           Catch the error here. */
        if ((hndl->sslerr = sbuf2lasterror(sb, NULL, 0)))
            sbuf2lasterror(sb, hndl->errstr, sizeof(hndl->errstr));
        goto after_callback;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.state = ntohl(hdr.state);
    hdr.length = ntohl(hdr.length);
    hndl->ack = (hdr.type == RESPONSE_HEADER__SQL_RESPONSE_PING);

    /* Server requires SSL. Return the header type in `type'.
       We may reach here under DIRECT_CPU mode where we skip DBINFO lookup. */
    if (hdr.type == RESPONSE_HEADER__SQL_RESPONSE_SSL) {
        if (type == NULL) {
            rc = -1;
            goto after_callback;
        }
        *type = hdr.type;
        rc = 0;
        goto after_callback;
    }

    if (hdr.length == 0) {
        debugprint("hdr length (0) from mach %s - going to retry\n",
                   hndl->hosts[hndl->connected_host]);

        /* If we have an AT_RECEIVE_HEARTBEAT event, invoke it now. */
        cdb2_event *e = NULL;
        void *callbackrc;
        while ((e = cdb2_next_callback(hndl, CDB2_AT_RECEIVE_HEARTBEAT, e)) !=
               NULL) {
            int unused;
            (void)unused;
            callbackrc =
                cdb2_invoke_callback(hndl, e, 1, CDB2_QUERY_STATE, hdr.state);
            PROCESS_EVENT_CTRL_AFTER(hndl, e, unused, callbackrc);
        }
        goto retry;
    }

    if (type)
        *type = hdr.type;

    *buf = realloc(*buf, hdr.length);
    if ((*buf) == NULL) {
        fprintf(stderr, "%s: out of memory realloc(%d)\n", __func__, hdr.length);
        rc = -1;
        goto after_callback;
    }

    b_read = sbuf2fread((char *)(*buf), 1, hdr.length, sb);
    debugprint("READ MSG b_read(%d) hdr.length(%d) type(%d)\n", b_read,
               hdr.length, hdr.type);

    *len = hdr.length;
    if (b_read != *len) {
        debugprint("bad read or numbytes, b_read(%d) != *len(%d) type(%d)\n",
                   b_read, *len, *type);
        rc = -1;
        goto after_callback;
    }
    if (hdr.type == RESPONSE_HEADER__SQL_RESPONSE_TRACE) {
        CDB2SQLRESPONSE *response =
            cdb2__sqlresponse__unpack(NULL, hdr.length, *buf);
        if (response->response_type == RESPONSE_TYPE__SP_TRACE) {
            fprintf(stderr, "%s\n", response->info_string);
            cdb2__sqlresponse__free_unpacked(response, NULL);
        } else {
            fprintf(stderr, "%s", response->info_string);
            cdb2__sqlresponse__free_unpacked(response, NULL);
            char cmd[250];
            if (fgets(cmd, 250, stdin) == NULL ||
                strncasecmp(cmd, "quit", 4) == 0) {
                exit(0);
            }
            CDB2QUERY query = CDB2__QUERY__INIT;
            query.spcmd = cmd;
            int loc_len = cdb2__query__get_packed_size(&query);
            unsigned char *locbuf = malloc(loc_len + 1);

            cdb2__query__pack(&query, locbuf);

            struct newsqlheader hdr = {.type =
                                           ntohl(CDB2_REQUEST_TYPE__CDB2QUERY),
                                       .compression = ntohl(0),
                                       .length = ntohl(loc_len)};

            sbuf2write((char *)&hdr, sizeof(hdr), hndl->sb);
            sbuf2write((char *)locbuf, loc_len, hndl->sb);

            int rc = sbuf2flush(hndl->sb);
            free(locbuf);
            if (rc < 0) {
                rc = -1;
                goto after_callback;
            }
        }
        debugprint("- going to retry\n");
        goto retry;
    }

    rc = 0;
after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_READ_RECORD, e)) != NULL) {
        callbackrc =
            cdb2_invoke_callback(hndl, e, 1, CDB2_RETURN_VALUE, (intptr_t)rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }
    return rc;
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
        cdb2__sqlresponse__free_unpacked(hndl->lastresponse,
                                         &hndl->allocator);
        hndl->protobuf_offset = 0;
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

int cdb2_send_2pc(cdb2_hndl_tp *hndl, char *dbname, char *pname, char *ptier, char *cmaster, unsigned int op,
                  char *dist_txnid, int rcode, int outrc, char *errmsg, int async)
{
    if (!hndl->sb) {
        cdb2_connect_sqlhost(hndl);
        if (!hndl->sb) {
            return -1;
        }
    }

    if (op != CDB2_DIST__PREPARE && op != CDB2_DIST__DISCARD && op != CDB2_DIST__PREPARED &&
        op != CDB2_DIST__FAILED_PREPARE && op != CDB2_DIST__COMMIT && op != CDB2_DIST__ABORT &&
        op != CDB2_DIST__PROPAGATED && op != CDB2_DIST__HEARTBEAT) {
        return -1;
    }

    CDB2QUERY query = CDB2__QUERY__INIT;
    CDB2DISTTXN distquery = CDB2__DISTTXN__INIT;
    CDB2DISTTXN__Disttxn disttxn = CDB2__DISTTXN__DISTTXN__INIT;

    /* TODO: hand-craft each msg-type on a switch */
    disttxn.operation = op;
    disttxn.async = async;
    disttxn.txnid = dist_txnid;
    disttxn.name = pname;
    disttxn.tier = ptier;
    disttxn.master = cmaster;
    disttxn.has_rcode = 1;
    disttxn.rcode = rcode;
    disttxn.has_outrc = 1;
    disttxn.outrc = outrc;
    disttxn.errmsg = errmsg;

    distquery.dbname = dbname;
    distquery.disttxn = &disttxn;
    query.disttxn = &distquery;

    int len = cdb2__query__get_packed_size(&query);
    unsigned char *buf = malloc(len + 1);

    cdb2__query__pack(&query, buf);

    struct newsqlheader hdr = {
        .type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY), .compression = ntohl(0), .length = ntohl(len)};

    sbuf2write((char *)&hdr, sizeof(hdr), hndl->sb);
    sbuf2write((char *)buf, len, hndl->sb);

    int rc = sbuf2flush(hndl->sb);
    free(buf);

    if (rc < 0) {
        sprintf(hndl->errstr, "%s: Error writing to other master", __func__);
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        return -1;
    }

    if (async) {
        return 0;
    }

    rc = sbuf2fread((char *)&hdr, 1, sizeof(hdr), hndl->sb);
    if (rc != sizeof(hdr)) {
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        return -1;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.length = ntohl(hdr.length);
    char *p = malloc(hdr.length);
    rc = sbuf2fread(p, 1, hdr.length, hndl->sb);

    if (rc != hdr.length) {
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        free(p);
        return -1;
    }
    CDB2DISTTXNRESPONSE *disttxn_response = cdb2__disttxnresponse__unpack(NULL, hdr.length, (const unsigned char *)p);

    if (disttxn_response == NULL) {
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        free(p);
        return -1;
    }

    int response_rcode = disttxn_response->rcode;

    cdb2__disttxnresponse__free_unpacked(disttxn_response, NULL);

    return response_rcode;
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

    struct newsqlheader hdr = {.type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY),
                               .compression = ntohl(0),
                               .length = ntohl(len)};

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
        if (hndl->firstresponse->error_string)
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
        fprintf(stderr, "td 0x%p %s: Can't read response from the db\n",
                (void *)pthread_self(), __func__);
        return -1;
    }
    hndl->firstresponse = cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
    return 0;
}

static int cdb2_send_query(cdb2_hndl_tp *hndl, cdb2_hndl_tp *event_hndl,
                           SBUF2 *sb, const char *dbname, const char *sql,
                           int n_set_commands, int n_set_commands_sent,
                           char **set_commands, int n_bindvars,
                           CDB2SQLQUERY__Bindvalue **bindvars, int ntypes,
                           int *types, int is_begin, int skip_nrows,
                           int retries_done, int do_append, int fromline)
{
    int rc = 0;
    void *callbackrc;
    CDB2SQLQUERY__IdentityBlob *id_blob = NULL;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;

    while ((e = cdb2_next_callback(event_hndl, CDB2_BEFORE_SEND_QUERY, e)) !=
           NULL) {
        callbackrc = cdb2_invoke_callback(event_hndl, e, 1, CDB2_SQL, sql);
        PROCESS_EVENT_CTRL_BEFORE(event_hndl, e, rc, callbackrc, overwrite_rc);
    }

    if (overwrite_rc)
        goto after_callback;

    if (log_calls) {
        fprintf(stderr, "td 0x%p %s line %d\n", (void *)pthread_self(),
                __func__, __LINE__);
    }

    int n_features = 0;
    int features[10]; // Max 10 client features??
    CDB2QUERY query = CDB2__QUERY__INIT;
    CDB2SQLQUERY sqlquery = CDB2__SQLQUERY__INIT;
    CDB2SQLQUERY__Snapshotinfo snapshotinfo;

    // This should be sent once right after we connect, not with every query
    CDB2SQLQUERY__Cinfo cinfo = CDB2__SQLQUERY__CINFO__INIT;

    if (!hndl || !hndl->sent_client_info) {
        cinfo.pid = _PID;
        cinfo.th_id = (uint64_t)pthread_self();
        cinfo.host_id = cdb2_hostid();
        cinfo.argv0 = _ARGV0;
        cinfo.api_driver_name = api_driver_name;
        cinfo.api_driver_version = api_driver_version;
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

    if (hndl && hndl->id_blob) {
        sqlquery.identity = hndl->id_blob;
    } else if (iam_identity && identity_cb && ((hndl->flags & CDB2_SQL_ROWS) == 0)) {
        id_blob = identity_cb->getIdentity();
        if (id_blob->data.data) {
            sqlquery.identity = id_blob;
        }
    }

    query.sqlquery = &sqlquery;

    sqlquery.n_set_flags = n_set_commands - n_set_commands_sent;
    if (sqlquery.n_set_flags)
        sqlquery.set_flags = &set_commands[n_set_commands_sent];

    /* hndl is NULL when we query comdb2db in comdb2db_get_dbhosts(). */
    if (hndl) {
        features[n_features++] = CDB2_CLIENT_FEATURES__SSL;
        if (hndl->request_fp) /* Request server to send back query fingerprint */
            features[n_features++] = CDB2_CLIENT_FEATURES__REQUEST_FP;
        /* Request server to send back row data flat, instead of storing it in
           a nested data structure. This helps reduce server's memory footprint. */
        features[n_features++] = CDB2_CLIENT_FEATURES__FLAT_COL_VALS;

        features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO;
        if ((hndl->flags & (CDB2_DIRECT_CPU | CDB2_MASTER)) ||
            (retries_done >= (hndl->num_hosts * 2 - 1) && hndl->master == hndl->connected_host)) {
            features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC;
        }
        if (retries_done >= hndl->num_hosts) {
            features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_QUEUING;
        }
        features[n_features++] = CDB2_CLIENT_FEATURES__CAN_REDIRECT_FDB;

        debugprint("sending to %s '%s' from-line %d retries is"
                   " %d do_append is %d\n",
                   hndl->connected_host >= 0 ? hndl->hosts[hndl->connected_host]
                                             : "NOT-CONNECTED",
                   sql, fromline, retries_done, do_append);

        if (hndl->is_retry) {
            sqlquery.has_retry = 1;
            sqlquery.retry = hndl->is_retry;
        }

        if ( !(hndl->flags & CDB2_READ_INTRANS_RESULTS) && is_begin) {
            features[n_features++] = CDB2_CLIENT_FEATURES__SKIP_INTRANS_RESULTS;
        }

        /* Have a query id associated with each transaction/query */
        sqlquery.has_cnonce = 1;
        sqlquery.cnonce.data = (uint8_t *)hndl->cnonce.str;
        sqlquery.cnonce.len = strlen(hndl->cnonce.str);

        if (hndl->snapshot_file) {
            cdb2__sqlquery__snapshotinfo__init(&snapshotinfo);
            snapshotinfo.file = hndl->snapshot_file;
            snapshotinfo.offset = hndl->snapshot_offset;
            sqlquery.snapshot_info = &snapshotinfo;
        }
    } else if (retries_done) {
        features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO;
        features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC;
        features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_QUEUING;
    }

    if (hndl && hndl->flags & CDB2_SQL_ROWS) {
        features[n_features++] = CDB2_CLIENT_FEATURES__SQLITE_ROW_FORMAT;
    }

    if (hndl && (hndl->flags & CDB2_REQUIRE_FASTSQL) != 0) {
        features[n_features++] = CDB2_CLIENT_FEATURES__REQUIRE_FASTSQL;
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

    uint8_t trans_append = hndl && hndl->in_trans && do_append;
    CDB2SQLQUERY__Reqinfo req_info = CDB2__SQLQUERY__REQINFO__INIT;
    req_info.timestampus = (hndl ? hndl->timestampus : 0);
    req_info.num_retries = retries_done;
    sqlquery.req_info = &req_info;

    int len = cdb2__query__get_packed_size(&query);

    unsigned char *buf;
    int on_heap = 1;
    if (trans_append || len > MAX_BUFSIZE_ONSTACK) {
        buf = malloc(len + 1);
    } else {
        buf = alloca(len + 1);
        on_heap = 0;
    }

    cdb2__query__pack(&query, buf);

    struct newsqlheader hdr = {.type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY),
                               .compression = ntohl(0),
                               .length = ntohl(len)};

    // finally send header and query
    rc = sbuf2write((char *)&hdr, sizeof(hdr), sb);
    if (rc != sizeof(hdr))
        debugprint("sbuf2write rc = %d\n", rc);

    rc = sbuf2write((char *)buf, len, sb);
    if (rc != len)
        debugprint("sbuf2write rc = %d (len = %d)\n", rc, len);

    rc = sbuf2flush(sb);
    if (rc < 0) {
        debugprint("sbuf2flush rc = %d\n", rc);
        if (on_heap)
            free(buf);
        rc = -1;
        goto after_callback;
    }

    if (trans_append && hndl->snapshot_file > 0) {
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
    } else if (on_heap) {
        free(buf);
    }

    rc = 0;

after_callback:
    if (iam_identity && identity_cb && id_blob) {
        if (id_blob->principal)
            free(id_blob->principal);
        if (id_blob->data.data)
            free(id_blob->data.data);
        free(id_blob);
    }
    while ((e = cdb2_next_callback(event_hndl, CDB2_AFTER_SEND_QUERY, e)) !=
           NULL) {
        callbackrc = cdb2_invoke_callback(event_hndl, e, 2, CDB2_SQL, sql,
                                          CDB2_RETURN_VALUE, (intptr_t)rc);
        PROCESS_EVENT_CTRL_AFTER(event_hndl, e, rc, callbackrc);
    }
    return rc;
}

/* All "soft" errors are retryable .. constraint violation are not */
static int is_retryable(int err_val)
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

#define PRINT_AND_RETURN(rcode)                                                \
    do {                                                                       \
        debugprint("%s: cnonce '%s' [%d][%d] "                                 \
                   "returning %d\n",                                           \
                   rcode == 0 ? "" : "XXX ", hndl->cnonce.str,                 \
                   hndl->snapshot_file, hndl->snapshot_offset, rcode);         \
        return (rcode);                                                        \
    } while (0)

#define PRINT_AND_RETURN_OK(rcode)                                             \
    do {                                                                       \
        debugprint("cnonce '%s' [%d][%d] "                                     \
                   "returning %d\n",                                           \
                   hndl->cnonce.str, hndl->snapshot_file,                      \
                   hndl->snapshot_offset, rcode);                              \
        return (rcode);                                                        \
    } while (0)

static int cdb2_next_record_int(cdb2_hndl_tp *hndl, int shouldretry)
{
    int len;
    int rc;
    int num_retry = 0;

    if (hndl->ack)
        ack(hndl);

retry_next_record:
    if (hndl->first_buf == NULL || hndl->sb == NULL)
        PRINT_AND_RETURN_OK(CDB2_OK_DONE);

    if (hndl->firstresponse->error_code)
        PRINT_AND_RETURN_OK(hndl->firstresponse->error_code);

    if (hndl->lastresponse) {
        if (hndl->lastresponse->response_type == RESPONSE_TYPE__LAST_ROW) {
            PRINT_AND_RETURN_OK(CDB2_OK_DONE);
        }

        if ((hndl->lastresponse->response_type ==
                 RESPONSE_TYPE__COLUMN_VALUES ||
             hndl->lastresponse->response_type == RESPONSE_TYPE__SQL_ROW) &&
            hndl->lastresponse->error_code != 0) {
            int rc = cdb2_convert_error_code(hndl->lastresponse->error_code);
            if (hndl->in_trans) {
                /* Give the same error for every query until commit/rollback */
                hndl->error_in_trans = rc;
            }
            PRINT_AND_RETURN_OK(rc);
        }
    }

    rc = cdb2_read_record(hndl, &hndl->last_buf, &len, NULL);
    if (rc) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        sprintf(hndl->errstr, "%s: Timeout while reading response from server",
                __func__);
    retry:
        debugprint("retry: shouldretry=%d, snapshot_file=%d, num_retry=%d\n",
                   shouldretry, hndl->snapshot_file, num_retry);
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
                if (hndl->sslerr != 0)
                    PRINT_AND_RETURN_OK(-1);
                goto retry;
            }
            rc = retry_queries_and_skip(hndl, num_retry, hndl->rows_read);
            if (rc) {
                newsql_disconnect(hndl, hndl->sb, __LINE__);
                goto retry;
            }
            goto retry_next_record;
        }
        PRINT_AND_RETURN_OK(-1);
    }

    if (hndl->last_buf == NULL) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        sprintf(hndl->errstr, "%s: No response from server", __func__);
        PRINT_AND_RETURN_OK(-1);
    }

    /* free previous response */
    if (hndl->lastresponse) {
        cdb2__sqlresponse__free_unpacked(hndl->lastresponse,
                                         &hndl->allocator);
        hndl->protobuf_offset = 0;
    }

    hndl->lastresponse =
        cdb2__sqlresponse__unpack(&hndl->allocator, len, hndl->last_buf);
    debugprint("hndl->lastresponse->response_type=%d\n",
               hndl->lastresponse->response_type);

    if (hndl->flags & CDB2_SQL_ROWS &&
        hndl->lastresponse->response_type != RESPONSE_TYPE__LAST_ROW &&
        !hndl->lastresponse->has_sqlite_row) {
        debugprint("received regular row when asked for sqlite format\n");
        sprintf(hndl->errstr, "remote is R7 or lower");
        return -1;
    }

    if (hndl->lastresponse->snapshot_info &&
        hndl->lastresponse->snapshot_info->file) {
        hndl->snapshot_file = hndl->lastresponse->snapshot_info->file;
        hndl->snapshot_offset = hndl->lastresponse->snapshot_info->offset;
    }

    if (hndl->lastresponse->response_type == RESPONSE_TYPE__COLUMN_VALUES ||
        hndl->lastresponse->response_type == RESPONSE_TYPE__SQL_ROW) {
        // "Good" rcodes are not retryable
        if (is_retryable(hndl->lastresponse->error_code) &&
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

        debugprint("error_string=%s\n", hndl->lastresponse->error_string);
        rc = cdb2_convert_error_code(hndl->lastresponse->error_code);
        PRINT_AND_RETURN_OK(rc);
    }

    if (hndl->lastresponse->response_type == RESPONSE_TYPE__LAST_ROW) {
        int ii = 0;

        // check for begin that couldn't retrieve the durable lsn from master
        if (is_retryable(hndl->lastresponse->error_code)) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
            sprintf(hndl->errstr,
                    "%s: Timeout while reading response from server", __func__);
            debugprint("Timeout while reading response from server\n");
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

        PRINT_AND_RETURN_OK(CDB2_OK_DONE);
    }

    PRINT_AND_RETURN_OK(-1);
}

int cdb2_next_record(cdb2_hndl_tp *hndl)
{
    int rc = 0;

    void *callbackrc;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;

    if (hndl->fdb_hndl)
        hndl = hndl->fdb_hndl;

    while ((e = cdb2_next_callback(hndl, CDB2_AT_ENTER_NEXT_RECORD, e)) !=
           NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 0);
        PROCESS_EVENT_CTRL_BEFORE(hndl, e, rc, callbackrc, overwrite_rc);
    }

    if (overwrite_rc)
        goto after_callback;

    if (hndl->in_trans && !hndl->read_intrans_results && !hndl->is_read) {
        rc = CDB2_OK_DONE;
    } else if (hndl->lastresponse && hndl->first_record_read == 0) {
        hndl->first_record_read = 1;
        if (hndl->lastresponse->response_type == RESPONSE_TYPE__COLUMN_VALUES ||
            hndl->lastresponse->response_type == RESPONSE_TYPE__SQL_ROW) {
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

after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AT_EXIT_NEXT_RECORD, e)) !=
           NULL) {
        callbackrc =
            cdb2_invoke_callback(hndl, e, 1, CDB2_RETURN_VALUE, (intptr_t)rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }

    return rc;
}

int cdb2_get_effects(cdb2_hndl_tp *hndl, cdb2_effects_tp *effects)
{
    int rc = 0;

    if (hndl->fdb_hndl)
        hndl = hndl->fdb_hndl;

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

static void free_events(cdb2_hndl_tp *hndl)
{
    cdb2_event *curre, *preve;
    if (!hndl->events)
        return;
    curre = hndl->events->next;
    while (curre != NULL) {
        preve = curre;
        curre = curre->next;
        free(preve);
    }
    free(hndl->events);
    hndl->events = NULL;
    free(hndl->gbl_event_version);
    hndl->gbl_event_version = NULL;
}

static void free_query_list(cdb2_query_list *head)
{
    cdb2_query_list *ditem;
    while (head != NULL) {
        ditem = head;
        head = head->next;
        free(ditem->sql);
        free(ditem->buf);
        free(ditem);
    }
}

/* Free query list and reset the pointer. */
static inline void free_query_list_on_handle(cdb2_hndl_tp *hndl)
{
    free_query_list(hndl->query_list);
    hndl->query_list = NULL;
}

int cdb2_close(cdb2_hndl_tp *hndl)
{
    cdb2_event *curre;
    void *callbackrc;
    int rc = 0;

    if (log_calls)
        fprintf(stderr, "%p> cdb2_close(%p)\n", (void *)pthread_self(), hndl);

    if (!hndl)
        return 0;

    if (hndl->fdb_hndl) {
        cdb2_close(hndl->fdb_hndl);
        hndl->fdb_hndl = NULL;
    }

    if (hndl->ack)
        ack(hndl);

    if (hndl->auto_consume_timeout_ms > 0 && hndl->sb && !hndl->in_trans && hndl->firstresponse &&
        (!hndl->lastresponse || (hndl->lastresponse->response_type != RESPONSE_TYPE__LAST_ROW))) {
        int nrec = 0;
        sbuf2settimeout(hndl->sb, hndl->auto_consume_timeout_ms, hndl->auto_consume_timeout_ms);
        struct timeval tv;
        gettimeofday(&tv, NULL);
        uint64_t starttimems = ((uint64_t)tv.tv_sec) * 1000 + tv.tv_usec / 1000;
        while (cdb2_next_record_int(hndl, 0) == CDB2_OK) {
            nrec++;
            gettimeofday(&tv, NULL);
            uint64_t curr = ((uint64_t)tv.tv_sec) * 1000 + tv.tv_usec / 1000;
            /* auto consume for up to CDB2_AUTO_CONSUME_TIMEOUT_MS */
            if (curr - starttimems >= hndl->auto_consume_timeout_ms)
                break;
        }
        if (hndl->debug_trace) {
            gettimeofday(&tv, NULL);
            uint64_t curr = ((uint64_t)tv.tv_sec) * 1000 + tv.tv_usec / 1000;
            fprintf(stderr, "%s: auto consume %d records took %" PRIu64 " ms\n",
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
        cdb2__sqlresponse__free_unpacked(hndl->lastresponse,
                                         &hndl->allocator);
        hndl->protobuf_offset = 0;
        free((void *)hndl->last_buf);
    }

    if (hndl->protobuf_data)
        free(hndl->protobuf_data);

    if (hndl->num_set_commands && hndl->is_child_hndl) {
        // don't free memory for this, parent handle will free
        hndl->num_set_commands = 0;
        hndl->commands = NULL;
    } else if (hndl->num_set_commands) {
        while (hndl->num_set_commands) {
            hndl->num_set_commands--;
            free(hndl->commands[hndl->num_set_commands]);
        }
        free(hndl->commands);
        hndl->commands = NULL;
    }

    free(hndl->query);
    free(hndl->query_hint);
    free(hndl->hint);
    free(hndl->sql);

    cdb2_clearbindings(hndl);
    cdb2_free_context_msgs(hndl);
    free(hndl->sslpath);
    free(hndl->cert);
    free(hndl->key);
    free(hndl->ca);
    free(hndl->crl);
    if (hndl->sess) {
        /* This is correct - we don't have to do it under lock. */
        hndl->sess->ref = 0;
    }

    curre = NULL;
    while ((curre = cdb2_next_callback(hndl, CDB2_AT_CLOSE, curre)) != NULL) {
        callbackrc = cdb2_invoke_callback(hndl, curre, 1, CDB2_RETURN_VALUE,
                                          (intptr_t)rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, curre, rc, callbackrc);
    }

    if (!hndl->is_child_hndl) // parent handle will free
        free_events(hndl);
    free_query_list(hndl->query_list);

    free(hndl);
    return rc;
}

static int next_cnonce(cdb2_hndl_tp *hndl)
{
    /* 1. Get the current epoch in microseconds.
       2. If the epoch is equal to the time embedded in `seq',
          increment the sequence number. If the sequence number wraps
          around 0, return an error.
       3. If the epoch is greater than the time embedded in `seq',
          3.1 If the embedded time is 0, which means this is the 1st time
              a cnonce is generated, initialze `hostid', `pid' and `hndl'.
          embed the epoch to `seq' and reset the sequence number to 0.
       4. Otherwise, return an error. */

    int rc;
    struct timeval tv;
    uint64_t cnt, seq, tm, now;
    cnonce_t *c;

    static char hex[] = "0123456789abcdef";
    char *in, *out, *end;

    rc = gettimeofday(&tv, NULL);
    if (rc != 0)
        return rc;
    c = &hndl->cnonce;
    seq = c->seq;
    tm = (seq & TIME_MASK) >> CNT_BITS;
    now = tv.tv_sec * 1000000 + tv.tv_usec;
    if (now == tm) {
        cnt = ((seq & CNT_MASK) + 1) & CNT_MASK;
        if (cnt == 0) {
            snprintf(hndl->errstr, sizeof(hndl->errstr),
                     "Transaction rate too high.");
            rc = E2BIG;
        } else {
            c->seq = (seq & TIME_MASK) | cnt;
        }
    } else if (now > tm) {
        if (tm == 0) {
            c->hostid = _MACHINE_ID;
            c->pid = _PID;
            c->hndl = hndl;
            c->ofs = sprintf(c->str, CNONCE_STR_FMT, c->hostid, c->pid,
                             (unsigned long long)c->hndl);
        }
        c->seq = (now << CNT_BITS);
    } else {
        rc = EINVAL;
    }

    if (rc == 0) {
        in = (char *)&c->seq;
        end = in + sizeof(c->seq);
        out = c->str + c->ofs;

        while (in != end) {
            char i = *(in++);
            *(out++) = hex[(i & 0xf0) >> 4];
            *(out++) = hex[i & 0x0f];
        }
        *out = 0;
    }
    return rc;
}

static int cdb2_query_with_hint(cdb2_hndl_tp *hndl, const char *sqlquery,
                                int len, char *short_identifier, char **hint,
                                char **query_hint)
{
    int len_id = strlen(short_identifier);
    if (len_id > 128) {
        sprintf(hndl->errstr, "Short identifier is too long.");
        return -1;
    }
    const char *first = cdb2_skipws(sqlquery);
    const char *tail = first;
    while (*tail && !isspace(*tail)) {
        ++tail;
    }
    int first_len = tail - first;
    char pfx[] = " /*+ RUNCOMDB2SQL ";
    char sfx[] = " */";
    size_t sz;
    char *sql;

    sz = first_len + sizeof(pfx) + sizeof(sfx) + len_id + 1;
    sql = malloc(sz);
    snprintf(sql, sz, "%.*s%s%s%s", first_len, first, pfx, short_identifier, sfx);
    *hint = sql;

    sz = len + sizeof(pfx) + sizeof(sfx) + len_id + 1;
    sql = malloc(sz);
    snprintf(sql, sz, "%.*s%s%s%s%s", first_len, first, pfx, short_identifier, sfx, tail);
    *query_hint = sql;
    return 0;
}

int cdb2_run_statement(cdb2_hndl_tp *hndl, const char *sql)
{
    return cdb2_run_statement_typed(hndl, sql, 0, NULL);
}

static void parse_dbresponse(CDB2DBINFORESPONSE *dbinfo_response, char valid_hosts[][CDB2HOSTNAME_LEN],
                             int *valid_ports, int *master_node, int *num_valid_hosts, int *num_valid_sameroom_hosts,
                             int debug_trace, peer_ssl_mode *s_mode)
{
    if (log_calls)
        fprintf(stderr, "td %" PRIxPTR "%s:%d\n", (intptr_t)pthread_self(), __func__,
                __LINE__);
    int num_hosts = dbinfo_response->n_nodes;
    *num_valid_hosts = 0;
    if (num_valid_sameroom_hosts)
        *num_valid_sameroom_hosts = 0;
    int myroom = 0;
    int i = 0;

    if (debug_trace) {
        /* Print a list of nodes received via dbinforesponse. */
        fprintf(stderr, "dbinforesponse:\n%s (master)\n",
                dbinfo_response->master->name);
        for (i = 0; i < num_hosts; i++) {
            CDB2DBINFORESPONSE__Nodeinfo *currnode = dbinfo_response->nodes[i];
            if (strcmp(dbinfo_response->master->name, currnode->name) == 0) {
                continue;
            }
            fprintf(stderr, "%s\n", currnode->name);
        }
    }

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

        if (strlen(currnode->name) >= CDB2HOSTNAME_LEN)
            continue;

        strncpy(valid_hosts[*num_valid_hosts], currnode->name, CDB2HOSTNAME_LEN);
        if (currnode->has_port) {
            valid_ports[*num_valid_hosts] = currnode->port;
        } else {
            valid_ports[*num_valid_hosts] = -1;
        }
        if (strcmp(currnode->name, dbinfo_response->master->name) == 0)
            *master_node = *num_valid_hosts;

        if (log_calls)
            fprintf(stderr, "td %" PRIxPTR "%s:%d, %d) host=%s(%d)%s\n",
                    (intptr_t) pthread_self(), __func__, __LINE__,
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
        strncpy(valid_hosts[*num_valid_hosts], currnode->name,
                sizeof(valid_hosts[*num_valid_hosts]) - 1);
        if (currnode->has_port) {
            valid_ports[*num_valid_hosts] = currnode->port;
        } else {
            valid_ports[*num_valid_hosts] = -1;
        }
        if (currnode->number == dbinfo_response->master->number)
            *master_node = *num_valid_hosts;

        (*num_valid_hosts)++;
    }

    if (!dbinfo_response->has_require_ssl)
        *s_mode = PEER_SSL_UNSUPPORTED;
    else if (dbinfo_response->require_ssl)
        *s_mode = PEER_SSL_REQUIRE;
    else
        *s_mode = PEER_SSL_ALLOW;
}

static int retry_query_list(cdb2_hndl_tp *hndl, int num_retry, int run_last)
{
    debugprint("retry_all %d, intran %d\n", hndl->retry_all, hndl->in_trans);

    if (!hndl->retry_all || !hndl->in_trans)
        return 0;

    int rc = 0;
    if (!(hndl->snapshot_file || hndl->query_no <= 1)) {
        debugprint("in_trans=%d snapshot_file=%d query_no=%d\n", hndl->in_trans,
                   hndl->snapshot_file, hndl->query_no);
        sprintf(hndl->errstr, "Database disconnected while in transaction.");
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
    debugprint("sending 'begin' to %s\n", host);
    rc = cdb2_send_query(hndl, hndl, hndl->sb, hndl->dbname, "begin",
                         hndl->num_set_commands, hndl->num_set_commands_sent,
                         hndl->commands, 0, NULL, 0, NULL, 1, 0, num_retry, 0,
                         __LINE__);
    hndl->in_trans = 1;
    debugprint("cdb2_send_query rc = %d, setting in_trans to 1\n", rc);

    if (rc != 0) {
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        debugprint("send_query host=%s rc=%d; returning 1\n", host, rc);
        return 1;
    }
    int len = 0;
    int type = 0;
    rc = cdb2_read_record(hndl, &hndl->first_buf, &len, &type);
    if (rc) {
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        debugprint("cdb2_read_record from %s rc=%d, returning 1\n", host, rc);
        return 1;
    }

    if (type == RESPONSE_HEADER__DBINFO_RESPONSE) {
        if (hndl->flags & CDB2_DIRECT_CPU) {
            debugprint("directcpu will ignore dbinfo\n");
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
                         &hndl->num_hosts_sameroom, hndl->debug_trace,
                         &hndl->s_sslmode);
        cdb2__dbinforesponse__free_unpacked(dbinfo_response, NULL);
        debugprint("type=%d returning 1\n", type);

        /* Clear cached SSL sessions - Hosts may have changed. */
        if (hndl->sess != NULL) {
            SSL_SESSION_free(hndl->sess->sessobj);
            hndl->sess->sessobj = NULL;
        }
        return 1;
    }
    if (hndl->first_buf != NULL) {
        hndl->firstresponse =
            cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
    } else {
        fprintf(stderr, "td 0x%p %s:%d: Can't read response from DB\n",
                (void *)pthread_self(), __func__, __LINE__);
        sbuf2close(hndl->sb);
        hndl->sb = NULL;
        return 1;
    }
    while ((rc = cdb2_next_record_int(hndl, 0)) == CDB2_OK)
        ;

    if (hndl->sb == NULL) {
        debugprint("sb is NULL, next_record returns %d, returning 1\n", rc);
        return 1;
    }

    cdb2_query_list *item = hndl->query_list;
    while (item != NULL) { /* Send all but the last query. */

        /* This is the case when we got disconnected while reading the
           query.
           In that case retry all the queries and skip their results,
           except the last one. */
        if (run_last == 0 && item->next == NULL)
            break;

        struct newsqlheader hdr = {.type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY),
                                   .compression = ntohl(0),
                                   .length = ntohl(item->len)};
        debugprint("resending '%s' to %s\n", item->sql, host);

        sbuf2write((char *)&hdr, sizeof(hdr), hndl->sb);
        sbuf2write((char *)item->buf, item->len, hndl->sb);
        sbuf2flush(hndl->sb);

        clear_responses(hndl);

        int len = 0;

        if (!hndl->read_intrans_results && !item->is_read) {
            item = item->next;
            continue;
        }
        /* This is for select queries, we send just the last row. */
        rc = cdb2_read_record(hndl, &hndl->first_buf, &len, NULL);
        if (rc) {
            debugprint("Can't read response from the db node %s\n", host);
            free(hndl->first_buf);
            hndl->first_buf = NULL;
            sbuf2close(hndl->sb);
            hndl->sb = NULL;
            return 1;
        }
        if (hndl->first_buf != NULL) {
            hndl->firstresponse =
                cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
        } else {
            debugprint("Can't read response from the db node %s\n", host);
            sbuf2close(hndl->sb);
            hndl->sb = NULL;
            return 1;
        }
        int read_rc;

        while ((read_rc = cdb2_next_record_int(hndl, 0)) == CDB2_OK)
            ;

        if (hndl->sb == NULL) {
            debugprint("sb is NULL, next_record_int returns "
                       "%d, returning 1\n",
                       read_rc);
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
    debugprint("num_retry=%d, skip_nrows=%d\n", num_retry, skip_nrows);

    int rc = 0, len;
    if (!(hndl->snapshot_file))
        return -1;

    hndl->retry_all = 1;

    if (hndl->in_trans) {
        rc = retry_query_list(hndl, num_retry, 0);
        if (rc) {
            PRINT_AND_RETURN_OK(rc);
        }
    }
    hndl->is_retry = num_retry;

    rc = cdb2_send_query(hndl, hndl, hndl->sb, hndl->dbname, hndl->sql,
                         hndl->num_set_commands, hndl->num_set_commands_sent,
                         hndl->commands, hndl->n_bindvars, hndl->bindvars,
                         hndl->ntypes, hndl->types, 0, skip_nrows, num_retry, 0,
                         __LINE__);
    if (rc) {
        PRINT_AND_RETURN_OK(rc);
    }

    rc = cdb2_read_record(hndl, &hndl->first_buf, &len, NULL);

    if (rc) {
        free(hndl->first_buf);
        hndl->first_buf = NULL;
        PRINT_AND_RETURN_OK(rc);
    }

    if (hndl->first_buf != NULL) {
        hndl->firstresponse =
            cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
    }

    PRINT_AND_RETURN_OK(rc);
}

static void process_set_local(cdb2_hndl_tp *hndl, const char *set_command)
{
    const char *p = &set_command[sizeof("SET") - 1];

    p = cdb2_skipws(p);

    if (strncasecmp(p, "hasql", 5) == 0) {
        p += sizeof("HASQL");
        p = cdb2_skipws(p);
        if (strncasecmp(p, "on", 2) == 0)
            hndl->is_hasql = 1;
        else
            hndl->is_hasql = 0;
        return;
    }

    if (strncasecmp(p, "admin", 5) == 0) {
        p += sizeof("ADMIN");
        p = cdb2_skipws(p);
        if (strncasecmp(p, "on", 2) == 0)
            hndl->is_admin = 1;
        else
            hndl->is_admin = 0;
        return;
    }
}

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
    } else if (strncasecmp(p, SSL_MIN_TLS_VER_OPT,
                           sizeof(SSL_MIN_TLS_VER_OPT) - 1) == 0) {
        p += sizeof(SSL_MIN_TLS_VER_OPT);
        p = cdb2_skipws(p);
        hndl->min_tls_ver = atof(p);
    } else {
        rc = -1;
    }

    if (rc == 0) {
        /* Reset ssl error flag. */
        hndl->sslerr = 0;
        /* Refresh connection if SSL config has changed. */
        if (hndl->sb != NULL) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
        }
    }

    return rc;
}

static inline void cleanup_query_list(cdb2_hndl_tp *hndl,
                                      cdb2_query_list *commit_query_list,
                                      int line)
{
    debugprint("called from line %d\n", line);
    hndl->read_intrans_results = 1;
    hndl->snapshot_file = 0;
    hndl->snapshot_offset = 0;
    hndl->is_retry = 0;
    hndl->error_in_trans = 0;
    hndl->in_trans = 0;
    debugprint("setting in_trans to 0\n");

    free_query_list_on_handle(hndl);
    free_query_list(commit_query_list);
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

    int rc = process_ssl_set_command(hndl, sql);
    if (rc >= 0)
        return rc;

    i = hndl->num_set_commands;
    if (i > 0) {
        int skip_len = 4;
        char *dup_sql = strdup(sql + skip_len);
        char *rest = NULL;
        char *set_tok = strtok_r(dup_sql, " ", &rest);
        if (set_tok) {
            /* special case for spversion and temporal */
            if (strcasecmp(set_tok, "spversion") == 0) {
                skip_len += 10;
                set_tok = strtok_r(rest, " ", &rest);
            } else if (set_tok && strcasecmp(set_tok, "temporal") == 0) {
                skip_len += 9;
                set_tok = strtok_r(rest, " ", &rest);
            /* special case for transaction chunk */
            } else if (strncasecmp(set_tok, "transaction", 11) == 0) {
                char *set_tok2 = strtok_r(rest, " ", &rest);
                if (set_tok2 && strncasecmp(set_tok2, "chunk", 5) == 0) {
                    /* skip "transaction" if chunk, set we can set
                     * both transaction and chunk mode
                     */
                    skip_len += 12;
                    set_tok = set_tok2;
                    set_tok2 = NULL;
                }
            }
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

    process_set_local(hndl, sql);
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
        debugprint("goto retry_queries\n");                                    \
        goto retry_queries;                                                    \
    } while (0);

/*
Make newly opened child handle have same settings (references) as parent
NOTE: ONLY WORKS CURRENTLY FOR PERIOD OF TIME
      WHERE NUM BIND VARS, SET COMMANDS, AND CONTEXT MSGS
      DO NOT CHANGE IN PARENT HANDLE
*/
static void attach_to_handle(cdb2_hndl_tp *child, cdb2_hndl_tp *parent)
{
    child->is_child_hndl = 1;
    child->bindvars = parent->bindvars;
    child->n_bindvars = parent->n_bindvars;

    // TODO: Maybe shouldn't have authentication commands in here
    child->num_set_commands = parent->num_set_commands;
    child->num_set_commands_sent = 0; // we are opening a new handle (child) so need to send all set commands again
    child->commands = parent->commands;
    // process_set_local
    child->is_hasql = parent->is_hasql;
    child->is_admin = parent->is_admin;

    // process_ssl_set_command
    // set ssl vars to parent since process set command might have updated parent handle
    child->c_sslmode = parent->c_sslmode;
    child->nid_dbname = parent->nid_dbname;
    free(child->sslpath);
    child->sslpath = parent->sslpath ? strdup(parent->sslpath) : NULL;
    free(child->cert);
    child->cert = parent->cert ? strdup(parent->cert) : NULL;
    free(child->key);
    child->key = parent->key ? strdup(parent->key) : NULL;
    free(child->ca);
    child->ca = parent->ca ? strdup(parent->ca) : NULL;
    free(child->crl);
    child->crl = parent->crl ? strdup(parent->crl) : NULL;
    child->cache_ssl_sess = parent->cache_ssl_sess;
    if (child->cache_ssl_sess)
        cdb2_set_ssl_sessions(child, cdb2_get_ssl_sessions(child));
    child->min_tls_ver = parent->min_tls_ver;
    /* Reset ssl error flag. */
    child->sslerr = 0;
    /* Refresh connection if SSL config has changed. */
    if (child->sb != NULL) { // should already be NULL
        newsql_disconnect(child, child->sb, __LINE__);
    }

    child->min_retries = parent->min_retries;
    child->max_retries = parent->max_retries;

    child->debug_trace = parent->debug_trace;
    child->use_hint = parent->use_hint;

    free_events(child);
    child->events = parent->events;
    child->gbl_event_version = parent->gbl_event_version;

    memcpy(&child->context_msgs, &parent->context_msgs, sizeof(child->context_msgs));
    // this is a new handle so send if non-empty
    child->context_msgs.has_changed = child->context_msgs.count > 0;
}

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

    debugprint("running '%s' from line %d\n", sql, line);

    consume_previous_query(hndl);
    if (!sql)
        return 0;

    /* sniff out 'set hasql on' here */
    if (strncasecmp(sql, "set", 3) == 0) {
        return process_set_command(hndl, sql);
    }

    if (strncasecmp(sql, "begin", 5) == 0) {
        debugprint("setting is_begin flag\n");
        is_begin = 1;
    } else if (strncasecmp(sql, "commit", 6) == 0) {
        debugprint("setting is_commit flag\n");
        is_commit = 1;
    } else if (strncasecmp(sql, "rollback", 8) == 0) {
        debugprint("setting is_commit & is_rollback flag for rollback\n");
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
        debugprint("%s\n", is_begin && hndl->in_trans
                               ? "I am committing but not 'in-trans'"
                               : "I am beginning but 'in-trans'");
        sprintf(hndl->errstr, "Wrong sql handle state");
        PRINT_AND_RETURN(CDB2ERR_BADSTATE);
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

                if ((rc = next_cnonce(hndl)) != 0)
                    PRINT_AND_RETURN(rc);
                cdb2_query_with_hint(hndl, sql, len, hndl->cnonce.str, &hndl->hint,
                                     &hndl->query_hint);

                sql = hndl->query_hint;
            }
        }
    }

    if (!hndl->in_trans) { /* only one cnonce for a transaction. */
        clear_snapshot_info(hndl, __LINE__);
        if ((rc = next_cnonce(hndl)) != 0)
            PRINT_AND_RETURN(rc);
    }
    hndl->retry_all = 1;
    int run_last = 1;

    time_t max_time =
        time(NULL) + (hndl->api_call_timeout - hndl->connect_timeout) / 1000;
    if (max_time < 0)
        max_time = 0;
retry_queries:
    debugprint(
        "retry_queries: hndl->host=%d (%s)\n", hndl->connected_host,
        (hndl->connected_host >= 0 ? hndl->hosts[hndl->connected_host] : ""));

    hndl->first_record_read = 0;

    retries_done++;

    int tmsec = 0;

    if (!hndl->sb && (retries_done > hndl->num_hosts)) {
        tmsec = (retries_done - hndl->num_hosts) * 100;
    }

    if (hndl->sslerr != 0)
        PRINT_AND_RETURN(CDB2ERR_CONNECT_ERROR);

    if ((retries_done > 1) && ((retries_done > hndl->max_retries) ||
                               ((time(NULL) + (tmsec / 1000)) >= max_time))) {
        sprintf(hndl->errstr, "%s: Maximum number of retries done.", __func__);
        if (is_hasql_commit) {
            cleanup_query_list(hndl, commit_query_list, __LINE__);
        }
        if (is_begin) {
            hndl->in_trans = 0;
        }
        PRINT_AND_RETURN(CDB2ERR_TRAN_IO_ERROR);
    }

    if (!hndl->sb) {
        if (is_rollback) {
            cleanup_query_list(hndl, NULL, __LINE__);
            debugprint("returning 0 on unconnected rollback\n");
            PRINT_AND_RETURN(0);
        }

        if (retries_done > hndl->num_hosts) {
            if (!hndl->is_hasql && (retries_done > hndl->min_retries)) {
                debugprint("returning cannot-connect, "
                           "retries_done=%d, num_hosts=%d\n",
                           retries_done, hndl->num_hosts);
                sprintf(hndl->errstr, "%s: Cannot connect to db", __func__);
                PRINT_AND_RETURN(CDB2ERR_CONNECT_ERROR);
            }

            int tmsec = (retries_done - hndl->num_hosts) * 100;
            if (tmsec >= 1000) {
                tmsec = 1000;
                debugprint("%s: cannot connect: sleep on retry\n", __func__);
            }

            debugprint("polling for %d ms\n", tmsec);
            poll(NULL, 0, tmsec);
        }
        cdb2_connect_sqlhost(hndl);
        if (hndl->sb == NULL) {
            debugprint("rc=%d goto retry_queries on connect failure\n", rc);
            goto retry_queries;
        }
        if (!is_begin) {
            hndl->retry_all = 1;
            rc = retry_query_list(hndl, (retries_done - 1), run_last);
            if (rc > 0) {
                newsql_disconnect(hndl, hndl->sb, __LINE__);
                hndl->retry_all = 1;
                debugprint("rc=%d goto retry_queries\n", rc);
                goto retry_queries;
            }
            else if (rc < 0) {
                if (hndl->in_trans)
                    hndl->error_in_trans = rc;
                PRINT_AND_RETURN(rc);
            }
        }
    }

    clear_responses(hndl);

    hndl->ntypes = ntypes;
    hndl->types = types;

    if (!hndl->in_trans || is_begin) {
        hndl->query_no = 0;
        rc = cdb2_send_query(
            hndl, hndl, hndl->sb, hndl->dbname, (char *)sql,
            hndl->num_set_commands, hndl->num_set_commands_sent, hndl->commands,
            hndl->n_bindvars, hndl->bindvars, ntypes, types, is_begin, 0,
            retries_done - 1, is_begin ? 0 : run_last, __LINE__);
    } else {
        /* Latch the SQL only if we're in a SNAPSHOT HASQL txn. */
        if (hndl->snapshot_file != 0) {
            free(hndl->sql);
            hndl->sql = strdup(sql);
        }

        hndl->query_no += run_last;
        rc = cdb2_send_query(hndl, hndl, hndl->sb, hndl->dbname, (char *)sql, 0,
                             0, NULL, hndl->n_bindvars, hndl->bindvars, ntypes,
                             types, 0, 0, 0, run_last, __LINE__);
        if (rc != 0) 
            hndl->query_no -= run_last;
    }
    if (rc) {
        debugprint("cdb2_send_query rc = %d\n", rc);
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
        debugprint("setting in_trans to 0\n");

        hndl->in_trans = 0;

        free_query_list_on_handle(hndl);

        if (!read_intrans_results && !hndl->client_side_error) {
            if (err_val) {
                if (is_rollback) {
                    PRINT_AND_RETURN(0);
                } else {
                    PRINT_AND_RETURN(err_val);
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
        PRINT_AND_RETURN(err_val);
    }

    if (!hndl->read_intrans_results && !hndl->is_read && hndl->in_trans) {
        debugprint("returning because read_intrans_results=%d n_trans=%d "
                   "is_hasql=%d\n",
                   hndl->read_intrans_results, hndl->in_trans, hndl->is_hasql);
        return (0);
    }

read_record:

    rc = cdb2_read_record(hndl, &hndl->first_buf, &len, &type);
    debugprint("cdb2_read_record host=%s rc=%d type=%d\n",
               hndl->connected_host >= 0 ? hndl->hosts[hndl->connected_host]
                                         : "NOT-CONNECTED",
               rc, type);

    if (rc == 0) {
        if (type == RESPONSE_HEADER__SQL_RESPONSE_SSL) {
            free(hndl->first_buf);
            hndl->first_buf = NULL;
            hndl->s_sslmode = PEER_SSL_REQUIRE;
            /* server wants us to use ssl so turn ssl on in same connection */
            try_ssl(hndl, hndl->sb);

            /* Decrement retry counter: It is not a real retry. */
            --retries_done;
            /* Resend client info (argv0, cheapstack and etc.)
               over the SSL connection. */
            hndl->sent_client_info = 0;
            GOTO_RETRY_QUERIES();
        } else if (type == RESPONSE_HEADER__DBINFO_RESPONSE) {
            /* Dbinfo .. go to new node */
            if (hndl->flags & CDB2_DIRECT_CPU) {
                /* direct cpu should not do anything with dbinfo, just retry */
                GOTO_RETRY_QUERIES();
            }
            /* We got back info about nodes that might be coherent. */
            CDB2DBINFORESPONSE *dbinfo_resp = NULL;
            dbinfo_resp =
                cdb2__dbinforesponse__unpack(NULL, len, hndl->first_buf);
            parse_dbresponse(dbinfo_resp, hndl->hosts, hndl->ports,
                             &hndl->master, &hndl->num_hosts,
                             &hndl->num_hosts_sameroom, hndl->debug_trace,
                             &hndl->s_sslmode);
            cdb2__dbinforesponse__free_unpacked(dbinfo_resp, NULL);

            newsql_disconnect(hndl, hndl->sb, __LINE__);
            hndl->connected_host = -1;
            hndl->retry_all = 1;

            /* Clear cached SSL sessions - Hosts may have changed. */
            if (hndl->sess != NULL) {
                SSL_SESSION_free(hndl->sess->sessobj);
                hndl->sess->sessobj = NULL;
            }
            free(hndl->first_buf);
            hndl->first_buf = NULL;
            GOTO_RETRY_QUERIES();
        }

        /* We used to cache a session immediately after a handshake.
           However in TLSv1.3, a session is not established until a
           separate post-handshake message containing the session
           details from the server has been received by the client.
           So we do it here after we've successfully read the first
           response from the server. */
        if ((rc = cdb2_add_ssl_session(hndl)) != 0)
            PRINT_AND_RETURN(rc);
    } else {
        if (err_val) {
            /* we get here because skip feature is off
               and the sql is either commit or rollback.
               don't retry because the transaction would
               fail anyway. Also if the sql is rollback,
               suppress any error. */
            if (is_rollback) {
                PRINT_AND_RETURN(0);
            } else if (is_retryable(err_val) &&
                       (hndl->snapshot_file ||
                        (!hndl->in_trans && !is_commit) || commit_file)) {
                hndl->error_in_trans = 0;
                newsql_disconnect(hndl, hndl->sb, __LINE__);
                hndl->retry_all=1;
                if (commit_file) {
                    debugprint(
                        "I am retrying. retries_done %d. setting in_trans to 1",
                        retries_done);
                    hndl->in_trans = 1;
                    hndl->snapshot_file = commit_file;
                    hndl->snapshot_offset = commit_offset;
                    hndl->is_retry = commit_is_retry;
                    hndl->query_list = commit_query_list;
                    commit_query_list = NULL;
                    commit_file = 0;
                }
                debugprint("goto retry_queries err_val=%d\n", err_val);
                goto retry_queries;
            } else {
                if (is_commit) {
                    cleanup_query_list(hndl, commit_query_list, __LINE__);
                }
                sprintf(hndl->errstr,
                        "%s: Timeout while reading response from server", __func__);
                debugprint("Timeout while reading response from server\n");
                PRINT_AND_RETURN(err_val);
            }
        }

        if (!is_commit || hndl->snapshot_file) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
            hndl->retry_all = 1;
            debugprint("goto retry_queries read-record rc=%d err_val=%d\n", rc,
                       err_val);
            goto retry_queries;
        }
        newsql_disconnect(hndl, hndl->sb, __LINE__);

        if (hndl->is_hasql || commit_file) {
            if (commit_file) {
                debugprint(
                    "I am retrying. retries_done %d. setting in_trans to 1",
                    retries_done);
                hndl->in_trans = 1;
                hndl->snapshot_file = commit_file;
                hndl->snapshot_offset = commit_offset;
                hndl->is_retry = commit_is_retry;
                hndl->query_list = commit_query_list;
                commit_query_list = NULL;
                commit_file = 0;
            }
            hndl->retry_all = 1;
            debugprint("goto retry_queries rc=%d, err_val=%d\n", rc, err_val);
            goto retry_queries;
        }

        if (is_hasql_commit) {
            cleanup_query_list(hndl, commit_query_list, __LINE__);
        }
        sprintf(hndl->errstr,
                "%s: Timeout while reading response from server", __func__);
        debugprint("returning, clear_snap_line is %d\n", hndl->clear_snap_line);
        PRINT_AND_RETURN(-1);
    }

    if (hndl->first_buf == NULL) {
        if (err_val) {
            debugprint("err_val is %d on null first_buf\n", err_val);

            if (is_rollback) {
                PRINT_AND_RETURN(0);
            } else if (is_retryable(err_val) &&
                       (hndl->snapshot_file ||
                        (!hndl->in_trans && !is_commit) || commit_file)) {
                hndl->error_in_trans = 0;
                newsql_disconnect(hndl, hndl->sb, __LINE__);
                hndl->retry_all=1;
                if (commit_file) {
                    debugprint(
                        "I am retrying. retries_done %d. setting in_trans to 1",
                        retries_done);
                    hndl->in_trans = 1;
                    hndl->snapshot_file = commit_file;
                    hndl->snapshot_offset = commit_offset;
                    hndl->is_retry = commit_is_retry;
                    hndl->query_list = commit_query_list;
                    commit_query_list = NULL;
                    commit_file = 0;
                }
                debugprint("goto retry_queries err_val=%d\n", err_val);
                goto retry_queries;
            } else {
                if (is_hasql_commit) {
                    cleanup_query_list(hndl, commit_query_list, __LINE__);
                }
                PRINT_AND_RETURN(err_val);
            }
        }
        if (!is_commit || hndl->snapshot_file) {
            debugprint("disconnect & retry on null first_buf\n");
            newsql_disconnect(hndl, hndl->sb, __LINE__);
            hndl->retry_all = 1;
            debugprint("goto retry_queries err_val=%d\n", err_val);
            goto retry_queries;
        }
        /* Changes here to retry commit and goto retry queries. */
        debugprint("Can't read response from the db\n");
        sprintf(hndl->errstr, "%s: Can't read response from the db", __func__);
        if (is_hasql_commit) {
            cleanup_query_list(hndl, commit_query_list, __LINE__);
        }
        PRINT_AND_RETURN(-1);
    }

    // we have (hndl->first_buf != NULL)
    hndl->firstresponse = cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
    if (err_val) {
        /* we've read the 1st response of commit/rollback.
           that is all we need so simply return here.
           I dont think we should get here normally */
        debugprint("err_val is %d\n", err_val);
        if (is_rollback) {
            PRINT_AND_RETURN(0);
        } else {
            if (is_hasql_commit) {
                cleanup_query_list(hndl, commit_query_list, __LINE__);
            }
            PRINT_AND_RETURN(err_val);
        }
    }

    debugprint("Received message %d\n", hndl->firstresponse->response_type);

    if (using_hint) {
        if (hndl->firstresponse->error_code ==
                CDB2__ERROR_CODE__PREPARE_ERROR_OLD ||
            hndl->firstresponse->error_code ==
                CDB2__ERROR_CODE__PREPARE_ERROR) {
            sql = hndl->query;
            hndl->retry_all = 1;
            debugprint("goto retry_queries error_code=%d\n",
                       hndl->firstresponse->error_code);
            goto retry_queries;
        }
    } else if (hndl->firstresponse->error_code == CDB2__ERROR_CODE__WRONG_DB &&
               !hndl->in_trans) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        hndl->retry_all = 1;
        for (int i = 0; i < hndl->num_hosts; i++) {
            hndl->ports[i] = -1;
        }
        if (retries_done < MAX_RETRIES) {
            GOTO_RETRY_QUERIES();
        }
    }

    if ((hndl->firstresponse->error_code == CDB2__ERROR_CODE__MASTER_TIMEOUT ||
         hndl->firstresponse->error_code == CDB2__ERROR_CODE__CHANGENODE) &&
        (hndl->snapshot_file || (!hndl->in_trans && !is_commit) ||
         commit_file)) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        hndl->retry_all = 1;
        if (commit_file) {
            debugprint("setting in_trans to 1\n");
            hndl->in_trans = 1;
            hndl->snapshot_file = commit_file;
            hndl->snapshot_offset = commit_offset;
            hndl->is_retry = commit_is_retry;
            hndl->query_list = commit_query_list;
            commit_query_list = NULL;
            commit_file = 0;
        }
        debugprint("goto retry_queries error_code=%d\n",
                   hndl->firstresponse->error_code);
        goto retry_queries;
    }

    if (is_begin) {
        debugprint("setting in_trans to 1\n");
        hndl->in_trans = 1;
    } else if (!is_hasql_commit && (is_rollback || is_commit)) {
        cleanup_query_list(hndl, commit_query_list, __LINE__);
    }

    hndl->node_seq = 0;
    bzero(hndl->hosts_connected, sizeof(hndl->hosts_connected));

    if (is_commit) {
        clear_snapshot_info(hndl, __LINE__);
    }

    if (hndl->firstresponse->foreign_db) {
        int foreign_flags = hndl->firstresponse->foreign_policy_flag;
        foreign_flags |= (hndl->flags & CDB2_REQUIRE_FASTSQL);
        if (cdb2_open(&hndl->fdb_hndl, hndl->firstresponse->foreign_db, hndl->firstresponse->foreign_class, foreign_flags)) {
            cdb2_close(hndl->fdb_hndl);
            hndl->fdb_hndl = NULL;
            if (is_hasql_commit)
                cleanup_query_list(hndl, commit_query_list, __LINE__);

            sprintf(hndl->errstr, "%s: Can't open fdb %s:%s", __func__, hndl->firstresponse->foreign_db, hndl->firstresponse->foreign_class);
            debugprint("Can't open fdb %s:%s\n", hndl->firstresponse->foreign_db, hndl->firstresponse->foreign_class);
            clear_responses(hndl);  // signal to cdb2_next_record_int that we are done
            PRINT_AND_RETURN(-1);
        }
        attach_to_handle(hndl->fdb_hndl, hndl);

        return_value = cdb2_run_statement_typed(hndl->fdb_hndl, sql, ntypes, types);
        if (is_hasql_commit)
            cleanup_query_list(hndl, commit_query_list, __LINE__);

        clear_responses(hndl);  // signal to cdb2_next_record_int that we are done
        PRINT_AND_RETURN(return_value);
    }

    if (hndl->firstresponse->response_type == RESPONSE_TYPE__COLUMN_NAMES) {
        /* Handle rejects from Server. */
        if (is_retryable(hndl->firstresponse->error_code) &&
            (hndl->snapshot_file || (!hndl->in_trans && !is_commit) ||
             commit_file)) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
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
            debugprint("goto retry_queries error_code=%d\n",
                       hndl->firstresponse->error_code);
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
            PRINT_AND_RETURN(return_value);
        }
        int rc = cdb2_next_record_int(hndl, 1);
        if (rc == CDB2_OK_DONE || rc == CDB2_OK) {
            return_value = 
                cdb2_convert_error_code(hndl->firstresponse->error_code);
            if (is_hasql_commit)
                cleanup_query_list(hndl, commit_query_list, __LINE__);
            PRINT_AND_RETURN(return_value);
        }

        if (hndl->is_hasql &&
            (((is_retryable(rc) && hndl->snapshot_file) || is_begin) ||
             (!hndl->sb &&
              ((hndl->in_trans && hndl->snapshot_file) || commit_file)))) {

            if (hndl->sb)
                sbuf2close(hndl->sb);

            hndl->sb = NULL;

            if (commit_file) {
                debugprint("setting in_trans to 1\n");
                hndl->in_trans = 1;
                hndl->snapshot_file = commit_file;
                hndl->snapshot_offset = commit_offset;
                hndl->is_retry = commit_is_retry;
                hndl->query_list = commit_query_list;
                commit_query_list = NULL;
                commit_file = 0;
            }

            hndl->retry_all = 1;

            debugprint("goto retry_queries retry-begin error_code=%d\n",
                       hndl->firstresponse->error_code);
            clear_responses(hndl);
            goto retry_queries;
        }

        return_value = cdb2_convert_error_code(rc);

        if (is_hasql_commit)
            cleanup_query_list(hndl, commit_query_list, __LINE__);

        PRINT_AND_RETURN(return_value);
    }

    sprintf(hndl->errstr, "%s: Unknown response type %d", __func__,
            hndl->firstresponse->response_type);
    if (is_hasql_commit)
        cleanup_query_list(hndl, commit_query_list, __LINE__);
    PRINT_AND_RETURN(-1);
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
    int rc = 0;

    void *callbackrc;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;

    if (hndl->fdb_hndl) {
        cdb2_close(hndl->fdb_hndl);
        hndl->fdb_hndl = NULL;
    }

    while ((e = cdb2_next_callback(hndl, CDB2_AT_ENTER_RUN_STATEMENT, e)) !=
           NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 1, CDB2_SQL, sql);
        PROCESS_EVENT_CTRL_BEFORE(hndl, e, rc, callbackrc, overwrite_rc);
    }

    if (overwrite_rc)
        goto after_callback;

    if (hndl->temp_trans && hndl->in_trans) {
        cdb2_run_statement_typed_int(hndl, "rollback", 0, NULL, __LINE__);
    }

    hndl->temp_trans = 0;

    if (hndl->is_hasql && !hndl->in_trans &&
        (strncasecmp(sql, "set", 3) != 0 && strncasecmp(sql, "begin", 5) != 0 &&
         strncasecmp(sql, "commit", 6) != 0 &&
         strncasecmp(sql, "rollback", 8) != 0)) {
        rc = cdb2_run_statement_typed_int(hndl, "begin", 0, NULL, __LINE__);
        if (rc) {
            debugprint("cdb2_run_statement_typed_int rc = %d\n", rc);
            goto after_callback;
        }
        hndl->temp_trans = 1;
    }

    sql = cdb2_skipws(sql);
    rc = cdb2_run_statement_typed_int(hndl, sql, ntypes, types, __LINE__);
    if (rc)
        debugprint("rc = %d\n", rc);

    // XXX This code does not work correctly for WITH statements
    // (they can be either read or write)
    if (hndl->temp_trans && !is_sql_read(sql)) {
        if (rc == 0) {
            int commit_rc =
                cdb2_run_statement_typed_int(hndl, "commit", 0, NULL, __LINE__);
            debugprint("rc = %d\n", commit_rc);
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

after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AT_EXIT_RUN_STATEMENT, e)) !=
           NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 2, CDB2_SQL, sql,
                                          CDB2_RETURN_VALUE, (intptr_t)rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }

    return rc;
}

int cdb2_numcolumns(cdb2_hndl_tp *hndl)
{
    int rc;
    if (hndl->fdb_hndl)
        hndl = hndl->fdb_hndl;
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
    if (hndl->fdb_hndl)
        hndl = hndl->fdb_hndl;
    if ((hndl->firstresponse == NULL) || (hndl->firstresponse->value == NULL))
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
    if (hndl == NULL)
        return "unallocated cdb2 handle";
    if (hndl->fdb_hndl)
        hndl = hndl->fdb_hndl;

    return hndl->cnonce.str;
}

const char *cdb2_errstr(cdb2_hndl_tp *hndl)
{
    char *ret;

    if (hndl == NULL)
        ret = "unallocated cdb2 handle";
    else {
        if (hndl->fdb_hndl)
            hndl = hndl->fdb_hndl;
        if (hndl->firstresponse == NULL) {
            ret = hndl->errstr;
        } else if (hndl->lastresponse == NULL) {
            ret = hndl->firstresponse->error_string;
        } else {
            ret = hndl->lastresponse->error_string;
        }
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
    if (hndl->fdb_hndl)
        hndl = hndl->fdb_hndl;
    if ((hndl->firstresponse == NULL) || (hndl->firstresponse->value == NULL))
        ret = 0;
    else
        ret = hndl->firstresponse->value[col]->type;
    if (log_calls) {
        fprintf(stderr, "%p> cdb2_column_type(%p, %d) = %s\n",
                (void *)pthread_self(), hndl, col, cdb2_type_str(ret));
    }
    return ret;
}

static int col_values_flattened(CDB2SQLRESPONSE *resp)
{
    return (resp->has_flat_col_vals && resp->flat_col_vals);
}

int cdb2_column_size(cdb2_hndl_tp *hndl, int col)
{
    if (hndl->fdb_hndl)
        hndl = hndl->fdb_hndl;
    CDB2SQLRESPONSE *lastresponse = hndl->lastresponse;
    /* sanity check. just in case. */
    if (lastresponse == NULL)
        return -1;
    /* data came back in the child column structure */
    if (lastresponse->value != NULL)
        return lastresponse->value[col]->value.len;
    /* sqlite row */
    if (hndl->lastresponse->has_sqlite_row)
        return lastresponse->sqlite_row.len;
    /* data came back in the parent CDB2SQLRESPONSE structure */
    return (col_values_flattened(lastresponse)) ? lastresponse->values[col].len : -1;
}

void *cdb2_column_value(cdb2_hndl_tp *hndl, int col)
{
    if (hndl->fdb_hndl)
        hndl = hndl->fdb_hndl;
    CDB2SQLRESPONSE *lastresponse = hndl->lastresponse;
    /* sanity check. just in case. */
    if (lastresponse == NULL)
        return NULL;
    /* data came back in the child column structure */
    if (lastresponse->value != NULL) {
        /* handle empty values */
        if (lastresponse->value[col]->value.len == 0 && lastresponse->value[col]->has_isnull != 1 &&
            lastresponse->value[col]->isnull != 1) {
            return (void *)"";
        }
        return lastresponse->value[col]->value.data;
    }
    /* sqlite row */
    if (hndl->lastresponse->has_sqlite_row)
        return lastresponse->sqlite_row.data;
    /* data came back in the parent CDB2SQLRESPONSE structure */
    if (col_values_flattened(lastresponse)) {
        /* handle empty values */
        if (lastresponse->values[col].len == 0 && !lastresponse->isnulls[col])
            return (void *)"";
        return lastresponse->values[col].data;
    }
    return NULL;
}

int cdb2_bind_param(cdb2_hndl_tp *hndl, const char *varname, int type,
                    const void *varaddr, int length)
{
    hndl->n_bindvars++;
    hndl->bindvars = realloc(hndl->bindvars, sizeof(CDB2SQLQUERY__Bindvalue *) *
                                                 hndl->n_bindvars);
    CDB2SQLQUERY__Bindvalue *bindval = malloc(sizeof(CDB2SQLQUERY__Bindvalue));
    cdb2__sqlquery__bindvalue__init(bindval);
    bindval->type = type;
    bindval->varname = (char *)varname;
    bindval->value.data = (void *)varaddr;
    bindval->value.len = length;
    if (length == 0) {
        /* protobuf-c discards `data' if `len' is 0. A NULL value and a 0-length
           value would look the same from the server's perspective. Hence we
           need an addtional isnull flag to tell them apart. */
        bindval->has_isnull = 1;
        bindval->isnull = (varaddr == NULL);

        /* R6 and old R7 ignore isnull for cstring and treat a 0-length string
           as NULL. So we send 1 dummy byte here to be backward compatible with
           an old backend. */
        if (type == CDB2_CSTRING && !bindval->isnull) {
            bindval->value.data = (unsigned char *)"";
            bindval->value.len = 1;
        }
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
    bindval->value.len = length;
    bindval->has_index = 1;
    bindval->index = index;
    if (length == 0) {
        /* See comments in cdb2_bind_param(). */
        bindval->has_isnull = 1;
        bindval->isnull = (varaddr == NULL);
        if (type == CDB2_CSTRING && !bindval->isnull) {
            bindval->value.data = (unsigned char *)"";
            bindval->value.len = 1;
        }
    }
    hndl->bindvars[hndl->n_bindvars - 1] = bindval;

    return 0;
}


/* cdb2_bind_array -- bind c array to a parameter name
 * name is the variable name we used in the sql
 * type is the type of elements to bind ex. CDB2_INTEGER
 * varaddr is the array address
 * count is the number of items in the array we will bind
 * typelen is the size of the elements for integer-arrays sizeof(int32_t or int64_t)
 */
int cdb2_bind_array(cdb2_hndl_tp *hndl, const char *name, cdb2_coltype type, const void *varaddr, size_t count, size_t typelen)
{
    if (count <= 0 || count > CDB2_MAX_BIND_ARRAY) {
        sprintf(hndl->errstr, "%s: bad array length:%zd (max:%d)", __func__, count, CDB2_MAX_BIND_ARRAY);
        return -1;
    }

    CDB2SQLQUERY__Bindvalue__Array *carray = malloc(sizeof(*carray));
    cdb2__sqlquery__bindvalue__array__init(carray);

    switch(type) {
    case CDB2_INTEGER:
    if (typelen == sizeof(int32_t)) {
        CDB2SQLQUERY__Bindvalue__I32Array *i32 = malloc(sizeof(*i32));
        cdb2__sqlquery__bindvalue__i32_array__init(i32);
        i32->elements = (int32_t *)varaddr;
        i32->n_elements = count;
        carray->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_I32;
        carray->i32 = i32;
    } else if (typelen == sizeof(int64_t)) {
        CDB2SQLQUERY__Bindvalue__I64Array *i64 = malloc(sizeof(*i64));
        cdb2__sqlquery__bindvalue__i64_array__init(i64);
        i64->elements = (int64_t *)varaddr;
        i64->n_elements = count;
        carray->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_I64;
        carray->i64 = i64;
    } else {
        goto notsupported;
    }
    break;
    case CDB2_REAL: {
        CDB2SQLQUERY__Bindvalue__DblArray *dbl = malloc(sizeof(*dbl));
        cdb2__sqlquery__bindvalue__dbl_array__init(dbl);
        dbl->elements = (double *)varaddr;
        dbl->n_elements = count;
        carray->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_DBL;
        carray->dbl = dbl;
    }
    break;
    case CDB2_CSTRING: {
        CDB2SQLQUERY__Bindvalue__TxtArray *txt = malloc(sizeof(*txt));
        cdb2__sqlquery__bindvalue__txt_array__init(txt);
        txt->elements = (char **)varaddr;
        txt->n_elements = count;
        carray->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_TXT;
        carray->txt = txt;
    }
    break;
    case CDB2_BLOB: {
        CDB2SQLQUERY__Bindvalue__BlobArray *blob = malloc(sizeof(*blob));
        cdb2__sqlquery__bindvalue__blob_array__init(blob);
        blob->elements = (ProtobufCBinaryData *) varaddr;
        blob->n_elements = count;
        carray->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_BLOB;
        carray->blob = blob;
    }
    break;
    default: goto notsupported;
    }

    CDB2SQLQUERY__Bindvalue *bindval = malloc(sizeof(CDB2SQLQUERY__Bindvalue));
    cdb2__sqlquery__bindvalue__init(bindval);
    bindval->type = type;
    bindval->varname = (char *)name;
    bindval->carray = carray;

    hndl->n_bindvars++;
    hndl->bindvars = realloc(hndl->bindvars, sizeof(CDB2SQLQUERY__Bindvalue *) * hndl->n_bindvars);
    hndl->bindvars[hndl->n_bindvars - 1] = bindval;
    if (log_calls)
        fprintf(stderr, "%p> cdb2_bind_array(%p, \"%s\", %zu, %s, %p, %zu) = 0\n",
                (void *)pthread_self(), hndl, name, count,
                cdb2_type_str(type), varaddr, typelen);

    return 0;

notsupported:
    free(carray);
    sprintf(hndl->errstr, "%s: bind array type not supported", __func__);
    return -1;
}

int cdb2_clearbindings(cdb2_hndl_tp *hndl)
{
    if (hndl->is_child_hndl) {
        // don't free memory for this, parent handle will free
        hndl->bindvars = NULL;
        hndl->n_bindvars = 0;
        return 0;
    }
    if (log_calls)
        fprintf(stderr, "%p> cdb2_clearbindings(%p)\n", (void *)pthread_self(),
                hndl);
    if (hndl->bindvars == NULL)
        return 0;
    if (hndl->fdb_hndl)
        cdb2_clearbindings(hndl->fdb_hndl);
    for (int i = 0; i < hndl->n_bindvars; i++) {
        CDB2SQLQUERY__Bindvalue *val = hndl->bindvars[i];
        if (val->carray) {
            free(val->carray->i32);
            free(val->carray);
        }
        free(hndl->bindvars[i]);
    }
    free(hndl->bindvars);
    hndl->bindvars = NULL;
    hndl->n_bindvars = 0;
    return 0;
}

static int comdb2db_get_dbhosts(cdb2_hndl_tp *hndl, const char *comdb2db_name,
                                int comdb2db_num, const char *host, int port,
                                char hosts[][CDB2HOSTNAME_LEN], int *num_hosts,
                                const char *dbname, char *cluster, int *dbnum,
                                int *num_same_room, int num_retries)
{
    *dbnum = 0;
    int n_bindvars = 3;
    const char *sql_query = "select M.name, D.dbnum, M.room from machines M "
                            "join databases D where M.cluster IN (select "
                            "cluster_machs from clusters where name=@dbname "
                            "and cluster_name=@cluster) and D.name=@dbname "
                            "order by (room = @room) desc";
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
    if (rc < 1 || rc >= sizeof(newsql_typestr)) {
        debugprint(
            "ERROR: can not fit entire string 'comdb2/%s/%s/newsql/%s'\n",
            comdb2db_name, cluster, hndl->policy);
    }

    int fd = cdb2_socket_pool_get(newsql_typestr, comdb2db_num, NULL);
    get_host_and_port_from_fd(fd, hndl->cached_host, sizeof(hndl->cached_host), &hndl->cached_port);
    if (fd < 0) {
        if (!cdb2_allow_pmux_route) {
            fd =
                cdb2_tcpconnecth_to(hndl, host, port, 0, hndl->connect_timeout);
        } else {
            fd = cdb2portmux_route(hndl, host, "comdb2", "replication",
                                   comdb2db_name);
        }
        is_sockfd = 0;
    }

    if (fd < 0) {
        i = 0;
        for (i = 0; i < 3; i++) {
            free(bindvars[i]);
        }
        free(bindvars);
        snprintf(hndl->errstr, sizeof(hndl->errstr),
                 "%s: Can't connect to host %s port %d", __func__, host, port);
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
        snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d out of memory\n",
                 __func__, __LINE__);

        return -1;
    }
    sbuf2settimeout(ss, hndl->socket_timeout, hndl->socket_timeout);
    if (is_sockfd == 0) {
        if (hndl->is_admin)
            sbuf2printf(ss, "@");
        sbuf2printf(ss, "newsql\n");
        sbuf2flush(ss);
    } else {
        rc = send_reset(ss);
        if (rc != 0) {
            goto free_vars;
        }
    }
    rc = cdb2_send_query(NULL, hndl, ss, comdb2db_name, sql_query, 0, 0, NULL,
                         3, bindvars, 0, NULL, 0, 0, num_retries, 0, __LINE__);
    if (rc)
        debugprint("cdb2_send_query rc = %d\n", rc);

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
        debugprint("cdb2_read_record rc = %d\n", rc);
        sbuf2close(ss);
        snprintf(hndl->errstr, sizeof(hndl->errstr),
                 "%s:%d  Invalid sql response from db %s \n", __func__,
                 __LINE__, comdb2db_name);
        free_events(&tmp);
        return -1;
    }
    if ((p != NULL) && (len != 0)) {
        sqlresponse = cdb2__sqlresponse__unpack(NULL, len, p);
    }
    if ((len == 0) || (sqlresponse == NULL) || (sqlresponse->error_code != 0) ||
        (sqlresponse->response_type != RESPONSE_TYPE__COLUMN_NAMES &&
         sqlresponse->n_value != 1 && sqlresponse->value[0]->has_type != 1 &&
         sqlresponse->value[0]->type != 3)) {
        snprintf(hndl->errstr, sizeof(hndl->errstr),
                 "%s: Got bad response for %s query. Reply len: %d\n", __func__,
                 comdb2db_name, len);
        sbuf2close(ss);
        free_events(&tmp);
        return -1;
    }

    *num_hosts = 0;
    while (sqlresponse->response_type <= RESPONSE_TYPE__COLUMN_VALUES) {
        cdb2__sqlresponse__free_unpacked(sqlresponse, NULL);
        rc = cdb2_read_record(&tmp, &p, &len, NULL);
        if (rc) {
            snprintf(hndl->errstr, sizeof(hndl->errstr),
                     "%s: Can't read dbinfo response from %s \n", __func__,
                     comdb2db_name);
            sbuf2close(ss);
            free_events(&tmp);
            return -1;
        }
        if (p != NULL) {
            sqlresponse = cdb2__sqlresponse__unpack(NULL, len, p);
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
                           (const char *)sqlresponse->value[2]->value.data) ==
                    0) {
                (*num_same_room)++;
            }
            (*num_hosts)++;
        }
    }
    cdb2__sqlresponse__free_unpacked(sqlresponse, NULL);
    free(p);
    int timeoutms = 10 * 1000;
    if (sbuf2free(ss) == 0)
        cdb2_socket_pool_donate_ext(newsql_typestr, fd, timeoutms / 1000, comdb2db_num);
    free_events(&tmp);
    return 0;
}

/* get dbinfo
 * returns -1 on error
 * returns 0 if number of hosts it finds is > 0
 */
static int cdb2_dbinfo_query(cdb2_hndl_tp *hndl, const char *type, const char *dbname, int dbnum, const char *host,
                             char valid_hosts[][CDB2HOSTNAME_LEN], int *valid_ports, int *master_node,
                             int *num_valid_hosts, int *num_valid_sameroom_hosts)
{
    char newsql_typestr[128];
    SBUF2 *sb = NULL;
    int rc = 0; /* Make compilers happy. */
    int port = 0;

    void *callbackrc;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;

    while ((e = cdb2_next_callback(hndl, CDB2_BEFORE_DBINFO, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 2, CDB2_HOSTNAME, host,
                                          CDB2_PORT, port);
        PROCESS_EVENT_CTRL_BEFORE(hndl, e, rc, callbackrc, overwrite_rc);
    }

    if (overwrite_rc)
        goto after_callback;

    debugprint("entering\n");

    rc = snprintf(newsql_typestr, sizeof(newsql_typestr),
                  "comdb2/%s/%s/newsql/%s", dbname, type, hndl->policy);
    if (rc < 1 || rc >= sizeof(newsql_typestr)) {
        debugprint(
            "ERROR: can not fit entire string 'comdb2/%s/%s/newsql/%s'\n",
            dbname, type, hndl->policy);
        rc = -1;
        goto after_callback;
    }
    int fd = cdb2_socket_pool_get(newsql_typestr, dbnum, NULL);
    get_host_and_port_from_fd(fd, hndl->cached_host, sizeof(hndl->cached_host), &hndl->cached_port);
    debugprint("cdb2_socket_pool_get fd %d, host '%s'\n", fd, host);
    if (fd < 0) {
        if (host == NULL) {
            rc = -1;
            goto after_callback;
        }

        if (!cdb2_allow_pmux_route) {
            if (!port) {
                port = cdb2portmux_get(hndl, type, host, "comdb2",
                                       "replication", dbname);
                debugprint("cdb2portmux_get port=%d'\n", port);
            }
            if (port < 0) {
                rc = -1;
                goto after_callback;
            }
            fd =
                cdb2_tcpconnecth_to(hndl, host, port, 0, hndl->connect_timeout);
        } else {
            fd = cdb2portmux_route(hndl, host, "comdb2", "replication", dbname);
            debugprint("cdb2portmux_route fd=%d'\n", fd);
        }
        if (fd < 0) {
            snprintf(hndl->errstr, sizeof(hndl->errstr),
                     "%s: Can't connect to host %s port %d", __func__, host,
                     port);
            rc = -1;
            goto after_callback;
        }
        sb = sbuf2open(fd, 0);
        if (sb == 0) {
            snprintf(hndl->errstr, sizeof(hndl->errstr),
                     "%s:%d out of memory\n", __func__, __LINE__);
            close(fd);
            rc = -1;
            goto after_callback;
        }
        if (hndl->is_admin)
            sbuf2printf(sb, "@");
        sbuf2printf(sb, "newsql\n");
        sbuf2flush(sb);
    } else {
        sb = sbuf2open(fd, 0);
        if (sb == 0) {
            snprintf(hndl->errstr, sizeof(hndl->errstr),
                     "%s:%d out of memory\n", __func__, __LINE__);
            close(fd);
            rc = -1;
            goto after_callback;
        }
    }

    sbuf2settimeout(sb, hndl->comdb2db_timeout, hndl->comdb2db_timeout);

    CDB2QUERY query = CDB2__QUERY__INIT;

    CDB2DBINFO dbinfoquery = CDB2__DBINFO__INIT;
    dbinfoquery.dbname = (char *)dbname;
    query.dbinfo = &dbinfoquery;

    int len = cdb2__query__get_packed_size(&query);
    unsigned char *buf = malloc(len + 1);
    cdb2__query__pack(&query, buf);

    struct newsqlheader hdr = {.type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY),
                               .compression = ntohl(0),
                               .length = ntohl(len)};

    sbuf2write((char *)&hdr, sizeof(hdr), sb);
    sbuf2write((char *)buf, len, sb);

    sbuf2flush(sb);
    free(buf);

    rc = sbuf2fread((char *)&hdr, 1, sizeof(hdr), sb);
    if (rc != sizeof(hdr)) {
        sbuf2close(sb);
        rc = -1;
        goto after_callback;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.length = ntohl(hdr.length);

    char *p = malloc(hdr.length);
    if (!p) {
        sprintf(hndl->errstr, "%s: out of memory", __func__);
        sbuf2close(sb);
        rc = -1;
        goto after_callback;
    }

    rc = sbuf2fread(p, 1, hdr.length, sb);
    if (rc != hdr.length) {
        snprintf(hndl->errstr, sizeof(hndl->errstr),
                 "%s:%d  Invalid dbinfo response from db %s \n", __func__,
                 __LINE__, dbname);
        sbuf2close(sb);
        free(p);
        rc = -1;
        goto after_callback;
    }
    CDB2DBINFORESPONSE *dbinfo_response = cdb2__dbinforesponse__unpack(
        NULL, hdr.length, (const unsigned char *)p);

    if (dbinfo_response == NULL) {
        sprintf(hndl->errstr, "%s: Got no dbinfo response from comdb2 database",
                __func__);
        sbuf2close(sb);
        free(p);
        rc = -1;
        goto after_callback;
    }

    parse_dbresponse(dbinfo_response, valid_hosts, valid_ports, master_node,
                     num_valid_hosts, num_valid_sameroom_hosts,
                     hndl->debug_trace, &hndl->s_sslmode);

    cdb2__dbinforesponse__free_unpacked(dbinfo_response, NULL);

    free(p);

    int timeoutms = 10 * 1000;

    if (sbuf2free(sb) == 0)
        cdb2_socket_pool_donate_ext(newsql_typestr, fd, timeoutms / 1000, dbnum);

    rc = (*num_valid_hosts > 0) ? 0 : -1;

after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_DBINFO, e)) != NULL) {
        callbackrc =
            cdb2_invoke_callback(hndl, e, 3, CDB2_HOSTNAME, host, CDB2_PORT,
                                 port, CDB2_RETURN_VALUE, (intptr_t)rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }
    return rc;
}

static inline void only_read_config(cdb2_hndl_tp *hndl, int noLock,
                                    int defaultOnly)
{
    read_available_comdb2db_configs(NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                    NULL, NULL, noLock, defaultOnly);
    set_cdb2_timeouts(hndl);
}

static int cdb2_get_dbhosts(cdb2_hndl_tp *hndl)
{
    char comdb2db_hosts[MAX_NODES][CDB2HOSTNAME_LEN];
    int comdb2db_ports[MAX_NODES];
    int num_comdb2db_hosts;
    int master = -1, rc = 0;
    int num_retry = 0;
    int comdb2db_num = COMDB2DB_NUM_OVERRIDE ? COMDB2DB_NUM_OVERRIDE : COMDB2DB_NUM;
    char comdb2db_name[32] = COMDB2DB;
    if (COMDB2DB_OVERRIDE[0] != '\0') {
        strncpy(comdb2db_name, COMDB2DB_OVERRIDE, 31);
    }

    if (!cdb2cfg_override) {
        /* Try dbinfo query without any host info. */
        only_read_config(hndl, 0, 0);
        if (cdb2_dbinfo_query(hndl, hndl->type, hndl->dbname, hndl->dbnum, NULL,
                              hndl->hosts, hndl->ports, &hndl->master,
                              &hndl->num_hosts,
                              &hndl->num_hosts_sameroom) == 0) {
            /* We get a plaintext socket from sockpool.
               We still need to read SSL config */
            return 0;
        }
    }

    rc = get_comdb2db_hosts(hndl, comdb2db_hosts, comdb2db_ports, &master, comdb2db_name, &num_comdb2db_hosts,
                            &comdb2db_num, hndl->dbname, hndl->hosts, &(hndl->num_hosts), &hndl->dbnum, 0);

    /* Before database destination discovery */
    cdb2_event *e = NULL;
    void *callbackrc;
    while ((e = cdb2_next_callback(hndl, CDB2_BEFORE_DISCOVERY, e)) != NULL) {
        int unused;
        (void)unused;
        callbackrc = cdb2_invoke_callback(hndl, e, 0);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, unused, callbackrc);
    }

    if (rc != 0)
        goto after_callback;

    if ((cdb2_default_cluster[0] != '\0') && (cdb2_comdb2dbname[0] != '\0')) {
        strcpy(comdb2db_name, cdb2_comdb2dbname);
    }

    if (strcasecmp(hndl->cluster, "default") == 0) {
        if (cdb2_default_cluster[0] == '\0') {
            sprintf(hndl->errstr, "cdb2_get_dbhosts: no default_type "
                                  "entry in comdb2db config.");
            rc = -1;
            goto after_callback;
        }
        strncpy(hndl->cluster, cdb2_default_cluster, sizeof(hndl->cluster) - 1);
        if (cdb2cfg_override) {
            strncpy(hndl->type, cdb2_default_cluster, sizeof(hndl->type) - 1);
        }
    }

    if (strcasecmp(hndl->cluster, "local") == 0) {
        hndl->num_hosts = 1;
        strcpy(hndl->hosts[0], "localhost");
        hndl->flags |= CDB2_DIRECT_CPU;

        /* Skip dbinfo to avoid pulling other hosts in the cluster. */
        rc = 0;
        goto after_callback;
    } else {
        rc = get_comdb2db_hosts(hndl, comdb2db_hosts, comdb2db_ports, &master, comdb2db_name, &num_comdb2db_hosts,
                                &comdb2db_num, hndl->dbname, hndl->hosts, &(hndl->num_hosts), &hndl->dbnum, 1);
        if (rc != 0 || (num_comdb2db_hosts == 0 && hndl->num_hosts == 0)) {
            sprintf(hndl->errstr, "cdb2_get_dbhosts: no %s hosts found.",
                    comdb2db_name);
            rc = -1;
            goto after_callback;
        }
    }

    time_t max_time =
        time(NULL) +
        (hndl->api_call_timeout - (CDB2_POLL_TIMEOUT + hndl->connect_timeout)) /
            1000;
    if (max_time < 0)
        max_time = 0;

retry:
    if (rc) {
        if (num_retry >= MAX_RETRIES || time(NULL) > max_time)
            goto after_callback;

        num_retry++;
        poll(NULL, 0, CDB2_POLL_TIMEOUT); // Sleep for 250ms everytime and total
                                          // of 5 seconds
        rc = 0;
    }
    debugprint("num_retry=%d hndl->num_hosts=%d num_comdb2db_hosts=%d\n",
               num_retry, hndl->num_hosts, num_comdb2db_hosts);

    if (hndl->num_hosts == 0) {
        if (master == -1) {
            for (int i = 0; i < num_comdb2db_hosts; i++) {
                rc = cdb2_dbinfo_query(
                    hndl, cdb2_default_cluster, comdb2db_name, comdb2db_num,
                    comdb2db_hosts[i], comdb2db_hosts, comdb2db_ports, &master,
                    &num_comdb2db_hosts, NULL);
                if (rc == 0 || time(NULL) >= max_time) {
                    break;
                }
            }
            if (rc != 0) {
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
            if (rc == 0 || time(NULL) >= max_time) {
                break;
            }
        }
        if (rc == -1 && time(NULL) < max_time) {
            rc = comdb2db_get_dbhosts(
                hndl, comdb2db_name, comdb2db_num, comdb2db_hosts[master],
                comdb2db_ports[master], hndl->hosts, &hndl->num_hosts,
                hndl->dbname, hndl->cluster, &hndl->dbnum,
                &hndl->num_hosts_sameroom, num_retry);
        }

        if (rc != 0) {
            goto retry;
        }
    }

    if (hndl->num_hosts == 0) {
        sprintf(hndl->errstr, "cdb2_get_dbhosts: comdb2db has no entry of "
                              "db %s of cluster type %s.",
                hndl->dbname, hndl->cluster);
        rc = -1;
        goto after_callback;
    }

    rc = -1;
    int i = 0;
    int node_seq = 0;
    if ((hndl->flags & CDB2_RANDOM) ||
        ((hndl->flags & CDB2_RANDOMROOM) && (hndl->num_hosts_sameroom == 0))) {
        node_seq = rand() % hndl->num_hosts;
    } else if ((hndl->flags & CDB2_RANDOMROOM) &&
               (hndl->num_hosts_sameroom > 0)) {
        node_seq = rand() % hndl->num_hosts_sameroom;
        /* Try dbinfo on same room first */
        for (i = 0; i < hndl->num_hosts_sameroom; i++) {
            int try_node = (node_seq + i) % hndl->num_hosts_sameroom;
            rc = cdb2_dbinfo_query(hndl, hndl->type, hndl->dbname, hndl->dbnum,
                                   hndl->hosts[try_node], hndl->hosts,
                                   hndl->ports, &hndl->master, &hndl->num_hosts,
                                   &hndl->num_hosts_sameroom);
            if (rc == 0) {
                goto after_callback;
            }
        }

        /* If there's another datacenter, choose a random starting point from there. */
        if (hndl->num_hosts > hndl->num_hosts_sameroom)
            node_seq = rand() % (hndl->num_hosts - hndl->num_hosts_sameroom) + hndl->num_hosts_sameroom;
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

    if (rc != 0) {
        sprintf(hndl->errstr,
                "cdb2_get_dbhosts: can't do dbinfo query on %s hosts.",
                hndl->dbname);
        if (hndl->num_hosts > 1) goto retry;
    }
after_callback: /* We are going to exit the function in this label. */
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_DISCOVERY, e)) != NULL) {
        int unused;
        (void)unused;
        callbackrc = cdb2_invoke_callback(hndl, e, 0);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, unused, callbackrc);
    }
    return rc;
}

const char *cdb2_dbname(cdb2_hndl_tp *hndl)
{
    if (hndl)
        return hndl->dbname;
    return NULL;
}

const char *cdb2_host(cdb2_hndl_tp *hndl)
{
    if (hndl && hndl->connected_host >= 0) {
        return hndl->hosts[hndl->connected_host];
    }
    return NULL;
}

int cdb2_clone(cdb2_hndl_tp **handle, cdb2_hndl_tp *c_hndl)
{
    cdb2_hndl_tp *hndl;
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
    // don't copy fdb_hndl
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
    char *eomachine = NULL;
    char *eooptions = NULL;
    int rc = 0;
    int port;
    char *dc;
    struct machine m[MAX_NODES];
    int num_hosts = 0;

    assert(type_copy[0] == '@');
    char *s = type_copy + 1; // advance past the '@'

    only_read_config(hndl, 0, 0);

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
                fprintf(stderr, "Hostname \"%s\" is too long, max %zu\n",
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

        debugprint("host %s port %d\n", m[i].host, m[i].port);
    }

    hndl->flags |= CDB2_DIRECT_CPU;

done:
    free(type_copy);
    if (log_calls)
        fprintf(stderr, "%p> %s() hosts=%d\n", (void *)pthread_self(), __func__,
                num_hosts);
    return rc;
}

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

    if ((sslenv = getenv("SSL_MIN_TLS_VER")) != NULL)
        hndl->min_tls_ver = atof(sslenv);
    else
        hndl->min_tls_ver = cdb2_min_tls_ver;
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
    cdb2_min_tls_ver = CDB2_MIN_TLS_VER_DEFAULT;
    return 0;
}

#ifdef my_ssl_eprintln
#undef my_ssl_eprintln
#endif

#define my_ssl_eprintln(fmt, ...)                                              \
    ssl_eprintln("cdb2api", "%s: " fmt, __func__, ##__VA_ARGS__)

#ifndef CRYPTO_num_locks
/* Callbacks for OpenSSL locking. OpenSSL >= 1.1.0 has its own locking.
   CRYPTO_num_locks is a function in OpenSSL < 1.1.0 but is made a macro
   in OpenSSL >= 1.1.0. So we assume that if CRYPTO_num_locks is not
   defined, we still need to implement our own locking. */
static pthread_mutex_t *ssl_locks = NULL;

#ifdef OPENSSL_NO_DEPRECATED
static void ssl_threadid(CRYPTO_THREADID *thd)
{
    CRYPTO_THREADID_set_numeric(thd, (intptr_t)pthread_self());
}
#endif /* OPENSSL_NO_DEPRECATED */

/* For OpenSSL < 1.0.0. */
static unsigned long ssl_threadid_deprecated()
{
    return (unsigned long)pthread_self();
}

static void ssl_lock(int mode, int type, const char *file, int line)
{
    int rc;
    if (mode & CRYPTO_LOCK) {
        if ((rc = pthread_mutex_lock(&ssl_locks[type])) != 0)
            my_ssl_eprintln("Failed to lock pthread mutex: rc = %d.", rc);
    } else {
        if ((rc = pthread_mutex_unlock(&ssl_locks[type])) != 0)
            my_ssl_eprintln("Failed to unlock pthread mutex: rc = %d.", rc);
    }
}
#endif /* CRYPTO_num_locks */

static int ssl_init(int init_openssl, int init_crypto)
{
    if (init_openssl) {
#ifndef OPENSSL_THREADS
        /* OpenSSL is not configured for threaded applications. */
        ssl_sfeprint(NULL, 0, my_ssl_eprintln, "OpenSSL is not configured with thread support.");
        return EPERM;
#endif /* OPENSSL_THREADS */
    }

    /* Initialize OpenSSL only once. */
#ifndef CRYPTO_num_locks
    if (init_crypto) {
        /* Configure SSL locking.
           This is only required for OpenSSL < 1.1.0. */
        int nlocks = CRYPTO_num_locks();
        ssl_locks = malloc(sizeof(pthread_mutex_t) * nlocks);
        if (ssl_locks == NULL) {
            ssl_sfeprint(NULL, 0, my_ssl_eprintln, "Failed to allocate SSL locks.");
            return ENOMEM;
        }

        int rc = 0;
        for (int ii = 0; ii < nlocks; ++ii) {
            if ((rc = pthread_mutex_init(&ssl_locks[ii], NULL)) != 0) {
                /* Whoops - roll back all we have done. */
                while (ii > 0) {
                    --ii;
                    pthread_mutex_destroy(&ssl_locks[ii]);
                }
                free(ssl_locks);
                ssl_locks = NULL;
                ssl_sfeprint(NULL, 0, my_ssl_eprintln, "Failed to initialize mutex: %s", strerror(rc));
                return rc;
            }
        }
#ifdef OPENSSL_NO_DEPRECATED
        CRYPTO_THREADID_set_callback(ssl_threadid);
#else
        /* Use deprecated functions for OpenSSL < 1.0.0. */
        CRYPTO_set_id_callback(ssl_threadid_deprecated);
#endif /* OPENSSL_NO_DEPRECATED */
        CRYPTO_set_locking_callback(ssl_lock);
    }
#endif /* CRYPTO_num_locks */

    /* Configure the library. */
    if (init_openssl) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
        OPENSSL_init_ssl(OPENSSL_INIT_LOAD_CONFIG, NULL);
#else
        OPENSSL_config(NULL);
        SSL_library_init();
        SSL_load_error_strings();
#endif /* OPENSSL_VERSION_NUMBER */
    }

    return 0;
}

/*
 * Initialize SSL library.
 *
 * PARAMETERS
 * init_libssl     - set to non-zero to initialize libssl
 * init_libcrypto  - set to non-zero to initialize libcrypto
 *
 * RETURN VALUES
 * 0 upon success
 */
int cdb2_init_ssl(int init_libssl, int init_libcrypto)
{
    static int called_ssl_init = 0;
    static pthread_mutex_t ssl_init_lock = PTHREAD_MUTEX_INITIALIZER;
    int rc = 0;
    if (called_ssl_init == 0 &&
        (rc = pthread_mutex_lock(&ssl_init_lock)) == 0) {
        if (called_ssl_init == 0) {
            rc = ssl_init(init_libssl, init_libcrypto);
            called_ssl_init = 1;
        }
        if (rc == 0)
            rc = pthread_mutex_unlock(&ssl_init_lock);
        else
            pthread_mutex_unlock(&ssl_init_lock);
    }
    return rc;
}

int cdb2_is_ssl_encrypted(cdb2_hndl_tp *hndl)
{
    return hndl->sb == NULL ? 0 : sslio_has_ssl(hndl->sb);
}

void cdb2_setIdentityBlob(cdb2_hndl_tp *hndl, void *id)
{
    hndl->id_blob = (CDB2SQLQUERY__IdentityBlob *)id;
}

static cdb2_ssl_sess *cdb2_get_ssl_sessions(cdb2_hndl_tp *hndl)
{
    cdb2_ssl_sess *pos;
    int rc = pthread_mutex_lock(&cdb2_ssl_sess_lock);
    if (rc != 0)
        return NULL;

    for (pos = cdb2_ssl_sess_cache.next; pos != NULL; pos = pos->next) {
        if (strcasecmp(hndl->dbname, pos->dbname) == 0 &&
            strcasecmp(hndl->cluster, pos->cluster) == 0) {
            if (!pos->ref) {
                pos->ref = 1;
                break;
            }
        }
    }

    pthread_mutex_unlock(&cdb2_ssl_sess_lock);
    return pos;
}

static int cdb2_set_ssl_sessions(cdb2_hndl_tp *hndl, cdb2_ssl_sess *arg)
{
    if (arg == NULL)
        return EINVAL;

    if (hndl->sess != NULL) /* Already have a session cache. */
        return EPERM;

    hndl->sess = arg;
    return 0;
}

static int cdb2_add_ssl_session(cdb2_hndl_tp *hndl)
{
    int rc;
    SSL_SESSION *sess;
    cdb2_ssl_sess *store, *p;

    if (!hndl->cache_ssl_sess || !hndl->newsess)
        return 0;
    hndl->newsess = 1;
    if (hndl->sess == NULL) {
        hndl->sess = malloc(sizeof(cdb2_ssl_sess));
        if (hndl->sess == NULL)
            return ENOMEM;
        strncpy(hndl->sess->dbname, hndl->dbname, sizeof(hndl->dbname) - 1);
        hndl->sess->dbname[sizeof(hndl->dbname) - 1] = '\0';
        strncpy(hndl->sess->cluster, hndl->cluster, sizeof(hndl->cluster) - 1);
        hndl->sess->cluster[sizeof(hndl->cluster) - 1] = '\0';
        hndl->sess->ref = 1;
        hndl->sess->sessobj = NULL;

        /* Append it to our internal linkedlist. */
        rc = pthread_mutex_lock(&cdb2_ssl_sess_lock);
        if (rc != 0) {
            /* If we fail to lock (which is quite rare), don't error out.
               we lose the caching ability, and that's it. */
            free(hndl->sess);
            hndl->sess = NULL;
            hndl->cache_ssl_sess = 0;
            return 0;
        }

        /* move store to the last element. */
        for (store = &cdb2_ssl_sess_cache; store->next != NULL;
             store = store->next) {
            /* right, blank. */
        };
        hndl->sess->next = NULL;
        store->next = hndl->sess;
        pthread_mutex_unlock(&cdb2_ssl_sess_lock);
    }

    /* Refresh in case of renegotiation. */
    p = hndl->sess;
    sess = p->sessobj;
    /* In the prefer mode, we may end up here without an SSL connection,
       if the SSL negotiation on a reconnect attempt fails and we reconnect
       again using plaintext. Check for this. */
    p->sessobj = sslio_has_ssl(hndl->sb) ? SSL_get1_session(sslio_get_ssl(hndl->sb)) : NULL;
    if (sess != NULL)
        SSL_SESSION_free(sess);
    return 0;
}

int comdb2_cheapstack_char_array(char *str, int maxln);

int cdb2_open(cdb2_hndl_tp **handle, const char *dbname, const char *type,
              int flags)
{
    cdb2_hndl_tp *hndl;
    int rc = 0;
    void *callbackrc;
    cdb2_event *e = NULL;

    pthread_mutex_lock(&cdb2_cfg_lock);
    pthread_once(&init_once, do_init_once);
    pthread_mutex_unlock(&cdb2_cfg_lock);

    *handle = hndl = calloc(1, sizeof(cdb2_hndl_tp));
    hndl->events = calloc(1, sizeof(*hndl->events));
    hndl->gbl_event_version = calloc(1, sizeof(*hndl->gbl_event_version));
    strncpy(hndl->dbname, dbname, sizeof(hndl->dbname) - 1);
    strncpy(hndl->cluster, type, sizeof(hndl->cluster) - 1);
    strncpy(hndl->type, type, sizeof(hndl->type) - 1);
    hndl->flags = flags;
    hndl->dbnum = 1;
    hndl->connected_host = -1;
    hndl->send_stack = 0;
    hndl->read_intrans_results = 1;

    /* We don't do dbinfo if DIRECT_CPU. So we'd default peer SSL mode to
       ALLOW. We will find it out later when we send SSL negotitaion packet
       to the server. */
    hndl->s_sslmode = PEER_SSL_ALLOW;

    hndl->max_retries = MAX_RETRIES;
    hndl->min_retries = MIN_RETRIES;

    hndl->env_tz = getenv("COMDB2TZ");
    hndl->is_admin = (flags & CDB2_ADMIN);

    if (hndl->env_tz == NULL)
        hndl->env_tz = getenv("TZ");

    if (hndl->env_tz == NULL)
        hndl->env_tz = DB_TZNAME_DEFAULT;


    cdb2_init_context_msgs(hndl);

    if (getenv("CDB2_DEBUG")) {
        hndl->debug_trace = 1;
        debugprint("debug trace enabled\n");
    }

    if (hndl->flags & CDB2_RANDOM) {
        strcpy(hndl->policy, "random");
    } else if (hndl->flags & CDB2_RANDOMROOM) {
        strcpy(hndl->policy, "random_room");
    } else if (hndl->flags & CDB2_ROOM) {
        strcpy(hndl->policy, "room");
    } else if (hndl->flags & CDB2_MASTER) {
        strcpy(hndl->policy, "dc");
    } else {
        hndl->flags |= CDB2_RANDOMROOM;
        /* DIRECTCPU mode behaves like RANDOMROOM. But let's pick a shorter policy name
           for it so we can fit longer hostnames in the sockpool type string which is
           merely 48 chars. */
        strcpy(hndl->policy, (hndl->flags & CDB2_DIRECT_CPU) ? "dc" : "random_room");
    }

    if (hndl->flags & CDB2_DIRECT_CPU) {
        hndl->num_hosts = 1;
        /* Get defaults from comdb2db.cfg */
        only_read_config(hndl, 0, 0);
        strncpy(hndl->hosts[0], type, sizeof(hndl->hosts[0]) - 1);
        char *p = strchr(hndl->hosts[0], ':');
        if (p) {
            *p = '\0';
            hndl->ports[0] = atoi(p + 1);
        } else if (cdb2_allow_pmux_route) {
            hndl->ports[0] = CDB2_PORTMUXPORT;
        }
        debugprint("host %s port %d\n", hndl->hosts[0], hndl->ports[0]);
    } else if (is_machine_list(type)) {
        rc = configure_from_literal(hndl, type);
        if (rc) {
            debugprint("configure_from_literal %s returns %d\n", type, rc);
        }
    } else {
        rc = cdb2_get_dbhosts(hndl);
        if (rc)
            debugprint("cdb2_get_dbhosts returns %d\n", rc);
    }

    if (rc == 0) {
        rc = set_up_ssl_params(hndl);
        if (rc)
            debugprint("set_up_ssl_params returns %d\n", rc);
        /*
         * Check and set user and password if they have been specified using
         * the environment variables.
         */
        char cmd[60];
        int length;

        if (getenv("COMDB2_USER")) {
            length = snprintf(cmd, sizeof(cmd), "set user %s",
                              getenv("COMDB2_USER"));
            if (length > sizeof(cmd)) {
                fprintf(stderr, "COMDB2_USER too long\n");
                rc = -1;
            } else if (length < 0) {
                fprintf(stderr, "Failed to set user using COMDB2_USER "
                                "environment variable\n");
                rc = -1;
            } else {
                debugprint(
                    "Setting user via COMDB2_USER environment variable\n");
                rc = process_set_command(hndl, cmd);
            }
        }

        if (getenv("COMDB2_PASSWORD")) {
            length = snprintf(cmd, sizeof(cmd), "set password %s",
                              getenv("COMDB2_PASSWORD"));
            if (length > sizeof(cmd)) {
                fprintf(stderr, "COMDB2_PASSWORD too long\n");
                rc = -1;
            } else if (length < 0) {
                fprintf(stderr, "Failed to set password using COMDB2_PASSWORD "
                                "environment variable\n");
                rc = -1;
            } else {
                debugprint("Setting password via COMDB2_PASSWORD environment "
                           "variable\n");
                rc = process_set_command(hndl, cmd);
            }
        }
    }

    if (hndl->send_stack)
        comdb2_cheapstack_char_array(hndl->stack, MAX_STACK);

    if (rc == 0) {
        rc = refresh_gbl_events_on_hndl(hndl);
        if (rc != 0)
            goto out;

        while ((e = cdb2_next_callback(hndl, CDB2_AT_OPEN, e)) != NULL) {
            callbackrc = cdb2_invoke_callback(hndl, e, 1, CDB2_RETURN_VALUE,
                                              (intptr_t)rc);
            PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
        }
    }

    if (!hndl->protobuf_size)
        hndl->protobuf_size = CDB2_PROTOBUF_SIZE;
    hndl->protobuf_data = malloc(hndl->protobuf_size);
    hndl->allocator.alloc = &cdb2_protobuf_alloc;
    hndl->allocator.free = &cdb2_protobuf_free;
    hndl->allocator.allocator_data = hndl;

    hndl->request_fp = CDB2_REQUEST_FP;

out:
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
        if (!hndl->is_child_hndl) // parent handle will free
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

cdb2_event *cdb2_register_event(cdb2_hndl_tp *hndl, cdb2_event_type types,
                                cdb2_event_ctrl ctrls, cdb2_event_callback cb,
                                void *user_arg, int nargs, ...)
{
    cdb2_event *ret;
    cdb2_event *curr;
    va_list ap;
    int i;

    /* Allocate an event object. */
    ret = malloc(sizeof(cdb2_event) + nargs * sizeof(cdb2_event_arg));
    if (ret == NULL)
        return NULL;

    ret->types = types;
    ret->ctrls = ctrls;
    ret->cb = cb;
    ret->user_arg = user_arg;
    ret->next = NULL;
    ret->argc = nargs;

    /* Copy over argument types. */
    va_start(ap, nargs);
    for (i = 0; i != nargs; ++i)
        ret->argv[i] = va_arg(ap, cdb2_event_arg);
    va_end(ap);

    if (hndl == NULL) {
        /* handle is NULL. We want to register to the global events. */
        ret->global = 1;
        pthread_mutex_lock(&cdb2_event_mutex);
        for (curr = &cdb2_gbl_events; curr->next != NULL; curr = curr->next)
            ;
        curr->next = ret;
        /* Increment global version so handles are aware of the new event. */
        ++cdb2_gbl_event_version;
        pthread_mutex_unlock(&cdb2_event_mutex);
    } else {
        if (!hndl->events) {
            assert(!hndl->gbl_event_version);
            hndl->events = calloc(1, sizeof(*hndl->events));
            hndl->gbl_event_version = calloc(1, sizeof(*hndl->gbl_event_version));
        }
        ret->global = 0;
        for (curr = hndl->events; curr->next != NULL; curr = curr->next)
            ;
        curr->next = ret;
    }
    return ret;
}

int cdb2_unregister_event(cdb2_hndl_tp *hndl, cdb2_event *event)
{
    cdb2_event *curr, *prev;

    if (event == NULL)
        return 0;

    if (hndl == NULL) {
        pthread_mutex_lock(&cdb2_event_mutex);
        for (prev = &cdb2_gbl_events, curr = prev->next;
             curr != NULL && curr != event; prev = curr, curr = curr->next)
            ;
        if (curr != event) {
            pthread_mutex_lock(&cdb2_event_mutex);
            return EINVAL;
        }
        prev->next = curr->next;
        ++cdb2_gbl_event_version;
        pthread_mutex_unlock(&cdb2_event_mutex);
    } else if (hndl->events) {
        for (prev = hndl->events, curr = prev->next;
             curr != NULL && curr != event; prev = curr, curr = curr->next)
            ;
        if (curr != event)
            return EINVAL;
        prev->next = curr->next;
    } else
        return EINVAL;
    free(event);
    return 0;
}

static cdb2_event *cdb2_next_callback(cdb2_hndl_tp *hndl, cdb2_event_type type,
                                      cdb2_event *e)
{
    if (e != NULL)
        e = e->next;
    else {
        /* Refresh once on new iteration. */
        if (refresh_gbl_events_on_hndl(hndl) != 0)
            return NULL;
        e = hndl->events->next;
    }
    for (; e != NULL && !(e->types & type); e = e->next)
        ;
    return e;
}

static char *stringify_fingerprint(char *dst, uint8_t *val, int len)
{
    int i;
    char *ret = dst;
    char hex[] = "0123456789abcdef";
    for (i = 0; i != len; ++i) {
        *(dst++) = hex[(val[i] & 0xf0) >> 4];
        *(dst++) = hex[val[i] & 0x0f];
    }
    *dst = 0;
    return ret;
}

static void *cdb2_invoke_callback(cdb2_hndl_tp *hndl, cdb2_event *e, int argc,
                                  ...)
{
    int i;
    va_list ap;
    void **argv;

    const char *hostname;
    int port;
    const char *sql = NULL;
    void *rc;
    int state;
    char *fp;

    /* Fast return if no arguments need to be passed to the callback. */
    if (e->argc == 0)
        return e->cb(hndl, e->user_arg ? e->user_arg : hndl->user_arg, 0, NULL);

    /* Default arguments from the handle. */
    if (hndl == NULL) {
        hostname = NULL;
        port = -1;
    } else if (hndl->connected_host < 0) {
        if (hndl->cached_host[0] == '\0') {
            hostname = NULL;
            port = -1;
        } else {
            hostname = hndl->cached_host;
            port = hndl->cached_port;
        }
    } else {
        hostname = hndl->hosts[hndl->connected_host];
        port = hndl->ports[hndl->connected_host];
    }
    rc = 0;
    state = 0;

    if (hndl->firstresponse == NULL || !hndl->firstresponse->has_fp)
        fp = NULL;
    else {
        fp = alloca(hndl->firstresponse->fp.len * 2 + 1);
        stringify_fingerprint(fp, hndl->firstresponse->fp.data, hndl->firstresponse->fp.len);
    }

    /* If the event has specified its own arguments, use them. */
    va_start(ap, argc);
    for (i = 0; i < argc; ++i) {
        switch (va_arg(ap, cdb2_event_arg)) {
        case CDB2_HOSTNAME:
            hostname = va_arg(ap, char *);
            break;
        case CDB2_PORT:
            port = va_arg(ap, int);
            break;
        case CDB2_SQL:
            sql = va_arg(ap, char *);
            break;
        case CDB2_RETURN_VALUE:
            rc = va_arg(ap, void *);
            break;
        case CDB2_QUERY_STATE:
            state = va_arg(ap, int);
            break;
        case CDB2_FINGERPRINT:
            fp = va_arg(ap, char *);
            break;
        default:
            (void)va_arg(ap, void *);
            break;
        }
    }
    va_end(ap);

    argv = alloca(sizeof(void *) * e->argc);
    for (i = 0; i != e->argc; ++i) {
        switch (e->argv[i]) {
        case CDB2_HOSTNAME:
            argv[i] = (void *)hostname;
            break;
        case CDB2_PORT:
            argv[i] = (void *)(intptr_t)port;
            break;
        case CDB2_SQL:
            argv[i] = (void *)sql;
            break;
        case CDB2_RETURN_VALUE:
            argv[i] = (void *)(intptr_t)rc;
            break;
        case CDB2_QUERY_STATE:
            argv[i] = (void *)(intptr_t)state;
            break;
        case CDB2_FINGERPRINT:
            argv[i] = (void *)fp;
        default:
            break;
        }
    }

    return e->cb(hndl, e->user_arg ? e->user_arg : hndl->user_arg, e->argc,
                 argv);
}

static int refresh_gbl_events_on_hndl(cdb2_hndl_tp *hndl)
{
    cdb2_event *gbl, *lcl, *tmp, *knot;
    size_t elen;

    if (!hndl->events) {
        assert(!hndl->gbl_event_version);
        hndl->events = calloc(1, sizeof(*hndl->events));
        hndl->gbl_event_version = calloc(1, sizeof(*hndl->gbl_event_version));
    }

    /* Fast return if the version has not changed. */
    if (*hndl->gbl_event_version == cdb2_gbl_event_version)
        return 0;

    /* Otherwise we must recopy the global events to the handle. */
    pthread_mutex_lock(&cdb2_event_mutex);

    /* Clear cached global events. */
    gbl = hndl->events->next;
    while (gbl != NULL && gbl->global) {
        tmp = gbl;
        gbl = gbl->next;
        free(tmp);
    }

    /* `knot' is where local events begin. */
    knot = gbl;

    /* Clone and append global events to the handle. */
    for (gbl = cdb2_gbl_events.next, lcl = hndl->events; gbl != NULL;
         gbl = gbl->next) {
        elen = sizeof(cdb2_event) + gbl->argc * sizeof(cdb2_event_arg);
        tmp = malloc(elen);
        if (tmp == NULL) {
            pthread_mutex_unlock(&cdb2_event_mutex);
            return ENOMEM;
        }
        memcpy(tmp, gbl, elen);
        tmp->next = NULL;
        lcl->next = tmp;
        lcl = tmp;
    }

    /* Tie global and local events together. */
    lcl->next = knot;

    /* Latch the global version. */
    *hndl->gbl_event_version = cdb2_gbl_event_version;

    pthread_mutex_unlock(&cdb2_event_mutex);
    return 0;
}

char *cdb2_string_escape(cdb2_hndl_tp *hndl, const char *src)
{
    const char *escapestr = "'";
    size_t count = 2; // set initial value for wrapping characters

    char *curr = strchr(src, *escapestr);
    while (curr != NULL) {
        ++count;
        curr = strchr(curr + 1, *escapestr);
    }

    size_t len = count + strlen(src) + 1;
    char *out = malloc(len);
    char *dest = out;
    strcpy(dest, escapestr);
    ++dest;

    while (*src) {
        curr = strchr(src, *escapestr);
        if (curr == NULL) {
            strcpy(dest, src);
            dest += strlen(src);
            break;
        }

        size_t toklen = curr - src + 1;
        strncpy(dest, src, toklen);

        src = curr + 1;
        dest += toklen;
        strcpy(dest, escapestr);
        ++dest;
    }

    strcpy(dest, escapestr);
    return out;
}
