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
#include <comdb2buf.h>
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
#include <arpa/nameser.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>
#include <fcntl.h>
#include <resolv.h>
#include <math.h> // ceil
#include <limits.h> // int_max

#include "cdb2api.h"
#include "cdb2api_hndl.h"
#include "cdb2api_int.h"

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

#ifndef API_DRIVER_NAME
#define API_DRIVER_NAME open_cdb2api
#endif
static char api_driver_name[] = QUOTE(API_DRIVER_NAME);

#ifndef API_DRIVER_VERSION
#define API_DRIVER_VERSION latest
#endif
static char api_driver_version[] = QUOTE(API_DRIVER_VERSION);

#define SOCKPOOL_SOCKET_NAME "/tmp/sockpool.socket"
#define COMDB2DB "comdb2db"
#define COMDB2DB_NUM 32432
#define COMDB2DB_DEV "comdb3db"
#define COMDB2DB_DEV_NUM 192779

#define MAX_BUFSIZE_ONSTACK 8192
static char COMDB2DB_OVERRIDE[32] = {0};
static int COMDB2DB_NUM_OVERRIDE = 0;

static char *SOCKPOOL_OTHER_NAME = NULL;
static char CDB2DBCONFIG_NOBBENV[512] = "/opt/bb/etc/cdb2/config/comdb2db.cfg";
/* Per-database additional config path */
static char CDB2DBCONFIG_ADDL_PATH[512] = "";

/* The real path is COMDB2_ROOT + CDB2DBCONFIG_NOBBENV_PATH  */
static char CDB2DBCONFIG_NOBBENV_PATH[] = "/etc/cdb2/config.d/"; /* READ-ONLY */
static char CDB2DBCONFIG_PKG_PATH[64] = "/bb/etc/cdb2/config.d/";

static char CDB2DBCONFIG_TEMP_BB_BIN[512] = "/bb/bin/comdb2db.cfg";

static char *CDB2DBCONFIG_BUF = NULL;

static int have_read_env = 0;

static char cdb2_default_cluster[64] = "";
static int cdb2_default_cluster_set_from_env = 0;

static char cdb2_comdb2dbname[32] = "";
static int cdb2_comdb2dbname_set_from_env = 0;

#ifndef CDB2_DNS_SUFFIX
#define CDB2_DNS_SUFFIX
#endif
static char cdb2_dnssuffix[255] = QUOTE(CDB2_DNS_SUFFIX);
static int cdb2_dnssuffix_set_from_env = 0;

#ifndef CDB2_BMS_SUFFIX
#define CDB2_BMS_SUFFIX
#endif
static char cdb2_bmssuffix[128] = QUOTE(CDB2_BMS_SUFFIX);
static int cdb2_bmssuffix_set_from_env = 0;

static char cdb2_machine_room[16] = "";
static int cdb2_machine_room_set_from_env = 0;

static int CDB2_PORTMUXPORT = 5105;
static int cdb2_portmuxport_set_from_env = 0;

static int MAX_RETRIES = 20; /* We are looping each node twice. */

static int MIN_RETRIES = 16;

static int CDB2_CONNECT_TIMEOUT = 100;
static int cdb2_connect_timeout_set_from_env = 0;

static int CDB2_MAX_AUTO_CONSUME_ROWS = 10;

static int COMDB2DB_TIMEOUT = 2000;
static int cdb2_comdb2db_timeout_set_from_env = 0;

static int CDB2_API_CALL_TIMEOUT = 120000; /* defaults to 2 minute */
static int cdb2_api_call_timeout_set_from_env = 0;

static int CDB2_ENFORCE_API_CALL_TIMEOUT = 0;
static int cdb2_enforce_api_call_timeout_set_from_env = 0;

static int CDB2_SOCKET_TIMEOUT = 5000;
static int cdb2_socket_timeout_set_from_env = 0;

static int CDB2_SOCKPOOL_SEND_TIMEOUTMS = 0;
static int cdb2_sockpool_send_timeoutms_set_from_env = 0;

static int CDB2_SOCKPOOL_RECV_TIMEOUTMS = 0;
static int cdb2_sockpool_recv_timeoutms_set_from_env = 0;

static int CDB2_POLL_TIMEOUT = 250;

static int cdb2_tcpbufsz = 0;
static int cdb2_tcpbufsz_set_from_env = 0;

static int CDB2_PROTOBUF_SIZE = 0;
static int cdb2_protobuf_size_set_from_env = 0;
static int cdb2_use_bmsd = 1;
static int cdb2_use_bmsd_set_from_env = 0;
static int cdb2_comdb2db_fallback = 1;
static int cdb2_comdb2db_fallback_set_from_env = 0;
static int cdb2_use_optional_identity = 1;
static int cdb2_use_optional_identity_set_from_env = 0;
static int cdb2_discard_unread_socket_data = 0;
static int cdb2_discard_unread_socket_data_set_from_env = 0;
static int cdb2_alarm_unread_socket_data = 0;
static int cdb2_alarm_unread_socket_data_set_from_env = 0;
static int cdb2_max_discard_records = 1;
static int cdb2_max_discard_records_set_from_env = 0;
/* flattens column values in protobuf structure */
#ifdef CDB2_LEGACY_DEFAULTS
static int cdb2_flat_col_vals = 0;
#else
static int cdb2_flat_col_vals = 1;
#endif
static int cdb2_flat_col_vals_set_from_env = 0;
/* estimates how much memory protobuf will need, and pre-allocates that much */
static int CDB2_PROTOBUF_HEURISTIC_INIT_SIZE = 1024;
#ifdef CDB2_LEGACY_DEFAULTS
static int cdb2_protobuf_heuristic = 0;
#else
static int cdb2_protobuf_heuristic = 1;
#endif
static int cdb2_protobuf_heuristic_set_from_env = 0;

static int CDB2_REQUEST_FP = 0;

static int log_calls = 0;
#define LOG_CALL(fmt, ...)                                                                                             \
    do {                                                                                                               \
        if (log_calls) {                                                                                               \
            fprintf(stderr, "%d [pid %d tid %p]> " fmt, (int)time(NULL), getpid(), (void *)pthread_self(),             \
                    ##__VA_ARGS__);                                                                                    \
        }                                                                                                              \
    } while (0)

static void *cdb2_protobuf_alloc(void *allocator_data, size_t size)
{
    struct cdb2_hndl *hndl = allocator_data;
    void *p = NULL;
    if (hndl->protobuf_data && (size <= hndl->protobuf_size - hndl->protobuf_offset)) {
        p = hndl->protobuf_data + hndl->protobuf_offset;
        hndl->protobuf_offset += ((size + 7) & ~7);
        if (hndl->protobuf_offset > hndl->protobuf_size)
            hndl->protobuf_offset = hndl->protobuf_size;
        LOG_CALL("%s: got %zu bytes from pool max %d current %d\n", __func__, size, hndl->protobuf_size,
                 hndl->protobuf_offset);
    } else {
        LOG_CALL("%s: malloc(%zu) max %d current %d\n", __func__, size, hndl->protobuf_size, hndl->protobuf_offset);
        p = malloc(size);
    }
    return p;
}

void cdb2_protobuf_free(void *allocator_data, void *ptr)
{
    struct cdb2_hndl *hndl = allocator_data;
    char *start = hndl->protobuf_data;
    char *end = start + hndl->protobuf_size;
    char *p = ptr;
    if (hndl->protobuf_data == NULL || p < start || p >= end) {
        LOG_CALL("%s: calling system free\n", __func__);
        free(p);
    }
}

#include <openssl/conf.h>
#include <openssl/crypto.h>

struct local_cached_connection {
    union {
        int fd;
        COMDB2BUF *sb;
    };
    char typestr[TYPESTR_LEN];
    time_t when;
    TAILQ_ENTRY(local_cached_connection) local_cache_lnk;
};

static pthread_mutex_t local_connection_cache_lock = PTHREAD_MUTEX_INITIALIZER;
static int max_local_connection_cache_entries = 0;
static int max_local_connection_cache_entries_envvar = 0;
static int max_local_connection_cache_age = 10;
static int max_local_connection_cache_age_envvar = 0;
static int local_connection_cache_use_sbuf = 0;
static int local_connection_cache_use_sbuf_envvar = 0;
static int local_connection_cache_check_pid = 0;
static int local_connection_cache_check_pid_envvar = 0;

static int connection_cache_entries = 0;

int cdb2_use_ftruncate = 0;
static int cdb2_use_ftruncate_set_from_env = 0;
static int cdb2_use_env_vars = 1;
static int cdb2_install_set_from_env = 0;

TAILQ_HEAD(local_connection_cache_list, local_cached_connection);

/* owner of the in-proc connection cache */
static pid_t local_connection_cache_owner_pid;
struct local_connection_cache_list local_connection_cache;
struct local_connection_cache_list free_local_connection_cache;
typedef struct local_connection_cache_list local_connection_cache_list;

static int cdb2cfg_override = 0;
static int default_type_override_env = 0;

/* Each feature needs to be enabled by default */
static int connect_host_on_reject = 1;
static int cdb2_connect_host_on_reject_set_from_env = 0;
static int iam_identity = 1; /* on by default */
static int cdb2_iam_identity_set_from_env = 0;

static int donate_unused_connections = 1; /* on by default */
static int cdb2_donate_unused_connections_set_from_env = 0;
/* Fix last set repeat, disable if this breaks anything */
static int disable_fix_last_set = 0;
static int cdb2_disable_fix_last_set_set_from_env = 0;

/* Skip dbinfo query if sockpool provides connection */
static int get_dbinfo = 0;
static int cdb2_get_dbinfo_set_from_env = 0;

/* Request host name of a connection obtained from sockpool */
static int get_hostname_from_sockpool_fd = 0;
static int cdb2_get_hostname_from_sockpool_fd_set_from_env = 0;

#define CDB2_ALLOW_PMUX_ROUTE_DEFAULT 0
static int cdb2_allow_pmux_route = CDB2_ALLOW_PMUX_ROUTE_DEFAULT;
static int cdb2_allow_pmux_route_set_from_env = 0;

static int retry_dbinfo_on_cached_connection_failure = 1;
static int cdb2_retry_dbinfo_on_cached_connection_failure_set_from_env = 0;

/* ssl client mode */
static ssl_mode cdb2_c_ssl_mode = SSL_ALLOW;
/* path to ssl certificate directory. searches 'client.crt', 'client.key', 'root.crt'
   and 'root.crl' for certificate, key, CA certificate and revocation list, respectively */
static char cdb2_sslcertpath[PATH_MAX];
/* path to ssl certificate. overrides cdb2_sslcertpath if specified */
static char cdb2_sslcert[PATH_MAX];
/* path to ssl key. overrides cdb2_sslcertpath if specified */
static char cdb2_sslkey[PATH_MAX];
/* path to CA. overrides cdb2_sslcertpath if specified */
static char cdb2_sslca[PATH_MAX];
#if HAVE_CRL
/* path to CRL. overrides cdb2_sslcertpath if specified */
static char cdb2_sslcrl[PATH_MAX];
#endif
/* specify which NID in server certificate represents the database name */
#ifdef NID_host /* available as of RFC 4524 */
#define CDB2_NID_DBNAME_DEFAULT NID_host
#else
#define CDB2_NID_DBNAME_DEFAULT NID_commonName
#endif
int cdb2_nid_dbname = CDB2_NID_DBNAME_DEFAULT;

static int cdb2_cache_ssl_sess = 0;

static double cdb2_min_tls_ver = 0;

static int _PID;        /* ONE-TIME */
static int _MACHINE_ID; /* ONE-TIME */
static char *_ARGV0;    /* ONE-TIME */
#define DB_TZNAME_DEFAULT "America/New_York"

static pthread_mutex_t cdb2_event_mutex = PTHREAD_MUTEX_INITIALIZER;
static cdb2_event cdb2_gbl_events;
static int cdb2_gbl_event_version;
static cdb2_event *cdb2_next_callback(cdb2_hndl_tp *, cdb2_event_type, cdb2_event *);
static void *cdb2_invoke_callback(cdb2_hndl_tp *, cdb2_event *, int, ...);
static int refresh_gbl_events_on_hndl(cdb2_hndl_tp *);
static int cdb2_get_dbhosts(cdb2_hndl_tp *);
static void hndl_set_comdb2buf(cdb2_hndl_tp *, COMDB2BUF *);
static int send_reset(COMDB2BUF *sb, int localcache);

static int check_hb_on_blocked_write = 0; // temporary switch - this will be default behavior
static int check_hb_on_blocked_write_set_from_env = 0;

static pthread_mutex_t cdb2_ssl_sess_lock = PTHREAD_MUTEX_INITIALIZER;

static cdb2_ssl_sess *cdb2_get_ssl_sessions(cdb2_hndl_tp *hndl);
static int cdb2_set_ssl_sessions(cdb2_hndl_tp *hndl,
                                 cdb2_ssl_sess *sessions);
static int cdb2_add_ssl_session(cdb2_hndl_tp *hndl);

static pthread_mutex_t cdb2_cfg_lock = PTHREAD_MUTEX_INITIALIZER;

#ifdef CDB2API_TEST
#include "cdb2api_test.h"
#define MAKE_CDB2API_TEST_SWITCH(name)                                                                                 \
    static int name;                                                                                                   \
    void set_##name(int num)                                                                                           \
    {                                                                                                                  \
        name = num;                                                                                                    \
    }
MAKE_CDB2API_TEST_SWITCH(fail_dbhosts_invalid_response)
MAKE_CDB2API_TEST_SWITCH(fail_dbhosts_bad_response)
MAKE_CDB2API_TEST_SWITCH(fail_dbhosts_cant_read_response)
MAKE_CDB2API_TEST_SWITCH(fail_dbinfo_invalid_header)
MAKE_CDB2API_TEST_SWITCH(fail_dbinfo_invalid_response)
MAKE_CDB2API_TEST_SWITCH(fail_dbinfo_no_response)
MAKE_CDB2API_TEST_SWITCH(fail_next)
MAKE_CDB2API_TEST_SWITCH(fail_read)
MAKE_CDB2API_TEST_SWITCH(fail_reject)
MAKE_CDB2API_TEST_SWITCH(fail_sb)
MAKE_CDB2API_TEST_SWITCH(fail_send)
MAKE_CDB2API_TEST_SWITCH(fail_sockpool)
MAKE_CDB2API_TEST_SWITCH(fail_tcp)
MAKE_CDB2API_TEST_SWITCH(fail_timeout_sockpool_recv)
MAKE_CDB2API_TEST_SWITCH(fail_timeout_sockpool_send)
MAKE_CDB2API_TEST_SWITCH(fail_ssl_negotiation_once)
MAKE_CDB2API_TEST_SWITCH(fail_sslio_close_in_local_cache)

#define MAKE_CDB2API_TEST_COUNTER(name)                                                                                \
    static int name;                                                                                                   \
    int get_##name(void)                                                                                               \
    {                                                                                                                  \
        return name;                                                                                                   \
    }
MAKE_CDB2API_TEST_COUNTER(num_get_dbhosts)
MAKE_CDB2API_TEST_COUNTER(num_skip_dbinfo)
MAKE_CDB2API_TEST_COUNTER(num_sockpool_fd)
MAKE_CDB2API_TEST_COUNTER(num_sql_connects)
MAKE_CDB2API_TEST_COUNTER(num_tcp_connects)
MAKE_CDB2API_TEST_COUNTER(num_cache_lru_evicts)
MAKE_CDB2API_TEST_COUNTER(num_cache_hits)
MAKE_CDB2API_TEST_COUNTER(num_cache_misses)
MAKE_CDB2API_TEST_COUNTER(num_sockpool_recv)
MAKE_CDB2API_TEST_COUNTER(num_sockpool_send)
MAKE_CDB2API_TEST_COUNTER(num_sockpool_recv_timeouts)
MAKE_CDB2API_TEST_COUNTER(num_sockpool_send_timeouts)

// The tunable value needs to be a string literal or have the lifetime
// managed by the caller - I could strdup locally, but don't want to be
// flagged by valgrind.
#define MAKE_CDB2API_TEST_TUNABLE(name)                                                                                \
    static const char *cdb2api_test_##name = "";                                                                       \
    void set_cdb2api_test_##name(const char *value)                                                                    \
    {                                                                                                                  \
        cdb2api_test_##name = value;                                                                                   \
    }
MAKE_CDB2API_TEST_TUNABLE(comdb2db_cfg)
MAKE_CDB2API_TEST_TUNABLE(dbname_cfg)

int get_dbinfo_state(void)
{
    return get_dbinfo;
}
void set_dbinfo_state(int value)
{
    get_dbinfo = value;
}

void set_allow_pmux_route(int allow)
{
    cdb2_allow_pmux_route = allow;
}

void set_local_connections_limit(int lim)
{
    max_local_connection_cache_entries = lim;
}

static int sent_valid_identity = 0;
static char sent_identity_principal[500] = {0};

int sent_iam_identity(void)
{
    return sent_valid_identity;
}
char *sent_id_principal()
{
    return sent_identity_principal;
}

const char *get_default_cluster(void)
{
    return cdb2_default_cluster;
}

const char *get_default_cluster_hndl(cdb2_hndl_tp *hndl)
{
    return hndl->type;
}

void set_protobuf_size(int size)
{
    CDB2_PROTOBUF_SIZE = size;
}

int get_gbl_event_version(void)
{
    return cdb2_gbl_event_version;
}
#endif /* CDB2API_TEST */

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
extern void CDB2_INSTALL_LIBS(const char *);
#endif
void (*cdb2_install)(const char *) = CDB2_INSTALL_LIBS;

#ifndef CDB2_UNINSTALL_LIBS
#define CDB2_UNINSTALL_LIBS NULL
#else
extern void CDB2_UNINSTALL_LIBS(const char *);
#endif
void (*cdb2_uninstall)(const char *) = CDB2_UNINSTALL_LIBS;

#ifndef CDB2_IDENTITY_CALLBACKS
    struct cdb2_identity *identity_cb = NULL;
#else
    extern struct cdb2_identity CDB2_IDENTITY_CALLBACKS;
    struct cdb2_identity *identity_cb = &CDB2_IDENTITY_CALLBACKS;
#endif

#ifndef CDB2_PUBLISH_EVENT_CALLBACKS
    struct cdb2_publish_event *publish_event_cb = NULL;
#else
    extern struct cdb2_publish_event CDB2_PUBLISH_EVENT_CALLBACKS;
    struct cdb2_publish_event *publish_event_cb = &CDB2_PUBLISH_EVENT_CALLBACKS;
#endif

pthread_mutex_t cdb2_sockpool_mutex = PTHREAD_MUTEX_INITIALIZER;
#define MAX_SOCKPOOL_FDS 8

#include <netdb.h>

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

#define debugprint(fmt, args...)                                               \
    do {                                                                       \
        if (hndl && hndl->debug_trace)                                         \
            fprintf(stderr, "td 0x%p %s:%d " fmt, (void *)pthread_self(),      \
                    __func__, __LINE__, ##args);                               \
    } while (0);

COMDB2BUF *cdb2_cdb2buf_openread(const char *filename)
{
    int fd;
    COMDB2BUF *s;

    if ((fd = open(filename, O_RDONLY, 0)) < 0 ||
        (s = cdb2buf_open(fd, 0)) == NULL) {
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
    COMDB2BUF *s = cdb2_cdb2buf_openread(procname);
    if (s == NULL) {
        fprintf(stderr, "%s cannot open %s, %s\n", __func__, procname,
                strerror(errno));
        return NULL;
    }

    if ((cdb2buf_gets(argv0, PATH_MAX, s)) < 0) {
        fprintf(stderr, "%s error reading from %s, %s\n", __func__, procname,
                strerror(errno));
        cdb2buf_close(s);
        return NULL;
    }

    cdb2buf_close(s);
    return argv0;
}
#endif

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

static void sockpool_close_all(void);
#ifndef CDB2API_TEST
static
#endif
    void
    local_connection_cache_clear(int);

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

static void atfork_child(void) {
    local_connection_cache_clear(0);
    sockpool_close_all();
    local_connection_cache_owner_pid = _PID = getpid();
    if (identity_cb)
        identity_cb->resetIdentity_end(0);
    pthread_mutex_unlock(&cdb2_sockpool_mutex);
}

// Prints a warning if the env var's value is invalid.
//
// Invalid values are those that aren't integers ("abc", "abc123", "123abc", ...)
static int process_env_var_int(const char *var, int *value, int *indicator)
{
    char *s = getenv(var);

    if (s) {
        char *endp = NULL;
        int val = strtol(s, &endp, 10);
        if (endp && *endp == 0) {
            *value = val;

            if (endp == s) {
                fprintf(stderr, "WARNING: %s: Value of %s is not valid. Using value: %d.\n", __func__, var, *value);
            }
            if (indicator)
                *indicator = 1;
            return 0;
        } else {
            fprintf(stderr, "WARNING: %s: Value of %s is not valid. Using value: %d.\n", __func__, var, *value);
        }
    }
    return 1;
}

// Prints a warning if the env var's value is invalid.
//
// An env var's value is invalid if it is equal to the empty string or is longer than `len`.
static int process_env_var_str(const char *var, char *value, int len, int *indicator)
{
    char *s = getenv(var);

    if (s) {
        if (((strlen(s) + 1) <= len)) {
            strncpy(value, s, len);

            if (strcmp(value, "") == 0) {
                fprintf(stderr, "WARNING: %s: Value of %s is not valid. Using value '%s'.\n", __func__, var, value);
            }

            if (indicator)
                *indicator = 1;
            return 0;
        } else {
            fprintf(stderr, "WARNING: %s: Value of %s is not valid. Using value '%s'.\n", __func__, var, value);
        }
    }
    return 1;
}

static int value_on_off(const char *value, int *err);

// Prints a warning if the env var's value is invalid.
//
// See `value_on_off` for invalid values.
static int process_env_var_str_on_off(const char *var, int *value, int *indicator)
{
    char *s = getenv(var);
    int err;

    if (s) {
        *value = value_on_off(s, &err);

        if (err == -1) {
            fprintf(stderr, "WARNING: %s: Value of %s is not valid. Using value '%s'.\n", __func__, var,
                    *value == 0 ? "OFF" : "ON");
        }

        if (indicator)
            *indicator = 1;
        return 0;
    }
    return 1;
}

#ifdef CDB2API_TEST
char cdb2dbconfig_singleconfig[511];
int cdb2cfg_override_all_config_paths = 0;
void set_cdb2api_test_single_cfg(const char *cfg_file)
{
    cdb2cfg_override_all_config_paths = 1;
    strncpy(cdb2dbconfig_singleconfig, cfg_file, 511);
}
#endif

static inline int get_char(COMDB2BUF *s, const char *buf, int *chrno)
{
    int ch;
    if (s) {
        ch = cdb2buf_getc(s);
    } else {
        ch = buf[*chrno];
        *chrno += 1;
    }
    return ch;
}

int cdb2_read_line(char *line, int maxlen, COMDB2BUF *s, const char *buf, int *chrno)
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

static void process_env_vars(void)
{
    process_env_var_int("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_ENTRIES", &max_local_connection_cache_entries,
                        &max_local_connection_cache_entries_envvar);
    process_env_var_int("COMDB2_CONFIG_MAX_LOCAL_CONNECTION_CACHE_AGE", &max_local_connection_cache_age,
                        &max_local_connection_cache_age_envvar);
    process_env_var_int("COMDB2_CONFIG_LOCAL_CONNECTION_CACHE_USE_SBUF", &local_connection_cache_use_sbuf,
                        &local_connection_cache_use_sbuf_envvar);
    process_env_var_int("COMDB2_CONFIG_LOCAL_CONNECTION_CACHE_CHECK_PID", &local_connection_cache_check_pid,
                        &local_connection_cache_check_pid_envvar);
    process_env_var_str_on_off("COMDB2_CONFIG_USE_ENV_VARS", &cdb2_use_env_vars, 0);

    if (cdb2_use_env_vars) {
        char *default_type = getenv("COMDB2_CONFIG_DEFAULT_TYPE");
        if (default_type) {
            default_type_override_env = 1;
        }
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
    char *min_retries = getenv("COMDB2_CONFIG_MIN_RETRIES");
    if (min_retries) {
        MIN_RETRIES = atoi(min_retries);
    }
}

#ifdef CDB2API_TEST
void test_process_env_vars(void)
{
    process_env_vars();
}

int get_max_local_connection_cache_entries(void)
{
    return max_local_connection_cache_entries;
}
#endif

static int init_once_has_run = 0;
#ifdef CDB2API_TEST
void reset_once(void)
{
    init_once_has_run = 0;
}
#endif

static void do_init_once(void)
{
    if (!init_once_has_run) {
        srandom(time(0));
        local_connection_cache_owner_pid = _PID = getpid();
        _MACHINE_ID = gethostid();
        _ARGV0 = cdb2_getargv0();
        pthread_atfork(atfork_prepare, atfork_me, atfork_child);
        TAILQ_INIT(&local_connection_cache);
        TAILQ_INIT(&free_local_connection_cache);
        process_env_vars();
        init_once_has_run = 1;
    }
}

#define cdb2_skipws(str)                                                                                               \
    do {                                                                                                               \
        while (*(str) && isspace(*(str)))                                                                              \
            ++(str);                                                                                                   \
    } while (0);

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
    cdb2_skipws(sqlstr);
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

static int recv_fd_int(int sockfd, void *data, size_t nbytes, int *fd_recvd, int timeoutms)
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
        if (timeoutms > 0) {
            struct pollfd pol;
            int pollrc;
            pol.fd = sockfd;
            pol.events = POLLIN;
            pollrc = poll(&pol, 1, timeoutms);
#ifdef CDB2API_TEST
            if (fail_timeout_sockpool_recv) {
                fail_timeout_sockpool_recv--;
                num_sockpool_recv_timeouts++;
                return PASSFD_TIMEOUT;
            }
#endif

            if (pollrc == 0) {
#ifdef CDB2API_TEST
                num_sockpool_recv_timeouts++;
#endif
                return PASSFD_TIMEOUT;
            } else if (pollrc == -1) {
                /* error will be in errno */
                return PASSFD_POLL;
            }
        }

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

#ifdef CDB2API_TEST
    ++num_sockpool_recv;
#endif

    return PASSFD_SUCCESS;
}

static int is_api_call_timedout(cdb2_hndl_tp *hndl)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    long long current_time = tv.tv_sec * 1000 + tv.tv_usec / 1000;
    if (hndl && hndl->max_call_time && (hndl->max_call_time < current_time)) {
        return 1;
    }
    return 0;
}

static long long get_call_timeout(const cdb2_hndl_tp *hndl, long long timeout)
{
    if (!hndl)
        return timeout;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    long long current_time = tv.tv_sec * 1000 + tv.tv_usec / 1000;
    long long time_left = hndl->max_call_time - current_time;
    if (hndl->max_call_time && time_left <= 0)
        time_left = 1;
    if (time_left > 0 && (time_left < timeout))
        return time_left;
    return timeout;
}

static void set_max_call_time(cdb2_hndl_tp *hndl)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    if (hndl)
        hndl->max_call_time = tv.tv_sec * 1000 + tv.tv_usec / 1000 + hndl->api_call_timeout;
}

/* This wrapper ensures that on error we close any file descriptor that we
 * may have received before the error occured.  Alse we make sure that we
 * preserve the value of errno which may be needed if the error was
 * PASSFD_RECVMSG. */
static int recv_fd(const cdb2_hndl_tp *hndl, int sockfd, void *data, size_t nbytes, int *fd_recvd)
{
    int rc, timeoutms = hndl ? hndl->sockpool_recv_timeoutms : CDB2_SOCKPOOL_RECV_TIMEOUTMS;
    timeoutms = get_call_timeout(hndl, timeoutms);
    rc = recv_fd_int(sockfd, data, nbytes, fd_recvd, timeoutms);
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
#ifdef CDB2API_TEST
            if (fail_timeout_sockpool_send) {
                fail_timeout_sockpool_send--;
                num_sockpool_send_timeouts++;
                return PASSFD_TIMEOUT;
            }
#endif
            if (pollrc == 0) {
#ifdef CDB2API_TEST
                num_sockpool_send_timeouts++;
#endif
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

            bzero(msg.msg_control, msg.msg_controllen);

            cmsgptr = CMSG_FIRSTHDR(&msg);
            cmsgptr->cmsg_len = CMSG_LEN(sizeof(int));
            cmsgptr->cmsg_level = SOL_SOCKET;
            cmsgptr->cmsg_type = SCM_RIGHTS;

            memcpy(CMSG_DATA(cmsgptr), &fd_to_send, sizeof(fd_to_send));
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
#ifdef CDB2API_TEST
    ++num_sockpool_send;
#endif

    return PASSFD_SUCCESS;
}

static int send_fd(const cdb2_hndl_tp *hndl, int sockfd, const void *data, size_t nbytes, int fd_to_send)
{
    int timeoutms = hndl ? hndl->sockpool_send_timeoutms : CDB2_SOCKPOOL_SEND_TIMEOUTMS;
    timeoutms = get_call_timeout(hndl, timeoutms);
    return send_fd_to(sockfd, data, nbytes, fd_to_send, timeoutms);
}

static int cdb2_tcpresolve(const char *host, struct in_addr *in, int *port)
{
    /*RESOLVE AN ADDRESS*/
    in_addr_t inaddr;
    int len;
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
        struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM, .ai_protocol = 0, .ai_flags = 0};
        struct addrinfo *result = NULL;
        int gai_rc = getaddrinfo(tok, /*service=*/NULL, &hints, &result);
        if (gai_rc != 0) {
            fprintf(stderr, "%s:getaddrinfo(%s): %d %s\n", __func__, tok, gai_rc, gai_strerror(gai_rc));
            return -1;
        }

        /* if multiple results are returned, just use the first one. */
        if (!result) {
            fprintf(stderr, "%s:getaddrinfo(%s): no results returned\n", __func__, tok);
            return -1;
        }

        if (result->ai_family != AF_INET) {
            fprintf(stderr, "%s:getaddrinfo(%s): expected AF_INET\n", __func__, tok);
            freeaddrinfo(result);
            return -1;
        }

        memcpy(&in->s_addr, &((const struct sockaddr_in *)result->ai_addr)->sin_addr, sizeof(in->s_addr));

        freeaddrinfo(result);
    }
    return 0;
}

static int lclconn(cdb2_hndl_tp *hndl, int s, const struct sockaddr *name, int namelen,
                   int timeoutms)
{
#ifdef CDB2API_TEST
    if (fail_tcp) {
        --fail_tcp;
        return -1;
    }
#endif
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
        debugprint("connect rc:%d err:%s\n", rc, strerror(errno));
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

static int cdb2_do_tcpconnect(cdb2_hndl_tp *hndl, struct in_addr in, int port, int myport,
                              int timeoutms)
{
#ifdef CDB2API_TEST
    ++num_tcp_connects;
#endif
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
    rc = lclconn(hndl, sockfd, (struct sockaddr *)&tcp_srv_addr, sizeof(tcp_srv_addr),
                 timeoutms);

    if (rc < 0) {
        close(sockfd);
        return rc;
    }
    return (sockfd); /* all OK */
}

static cdb2_ssl_sess cdb2_ssl_sess_cache;

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

    rc = cdb2_do_tcpconnect(hndl, in, port, myport, timeoutms);

after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_TCP_CONNECT, e)) != NULL) {
        callbackrc =
            cdb2_invoke_callback(hndl, e, 3, CDB2_HOSTNAME, host, CDB2_PORT,
                                 port, CDB2_RETURN_VALUE, (intptr_t)rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }
    return rc;
}

/* Make it equal to FSQL header. */
struct newsqlheader {
    int type;
    int compression;
    int state; /* query state */
    int length;
};

#ifdef CDB2API_TEST
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
#endif

#ifdef CDB2API_SERVER
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
#endif

void cdb2_set_comdb2db_config(char *cfg_file)
{
    pthread_mutex_lock(&cdb2_cfg_lock);
    do_init_once();
    LOG_CALL("%s(\"%s\")\n", __func__, cfg_file);
    memset(CDB2DBCONFIG_NOBBENV, 0, sizeof(CDB2DBCONFIG_NOBBENV) /* 512 */);
    if (cfg_file != NULL) {
        cdb2cfg_override = 1;
        strncpy(CDB2DBCONFIG_NOBBENV, cfg_file, 511);
    }
    pthread_mutex_unlock(&cdb2_cfg_lock);
}

void cdb2_set_comdb2db_info(char *cfg_info)
{
    LOG_CALL("%s(\"%s\")\n", __func__, cfg_info);
    int len;
    pthread_mutex_lock(&cdb2_cfg_lock);
    do_init_once();
    if (CDB2DBCONFIG_BUF != NULL) {
        free(CDB2DBCONFIG_BUF);
        CDB2DBCONFIG_BUF = NULL;
    }
    if (cfg_info == NULL) {
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

static int is_valid_int(const char *str)
{
    while (*str) {
        if (!isdigit(*str))
            return 0;
        else
            ++str;
    }
    return 1;
}

// `err` is set to -1 if the `value` is invalid; otherwise, it is set to 0.
//
// `value` is invalid if it is not equal to one of the following: "on", "off", "yes", "no", "1", "0", "true", "false".
// Ideally these values would be rejected, but this wouldn't be backwards compatible (currently,
// all nonzero numbers are effectively "on" and all values that cannot be converted to integers other
// than "on", "off", "yes", and "no" are effectively "off")
//
// Returning a separate error allows us to print a warning message when this occurs while maintaining
// old behavior.
static int value_on_off(const char *value, int *err)
{
    *err = 0;

    if (strcasecmp("on", value) == 0) {
        return 1;
    } else if (strcasecmp("off", value) == 0) {
        return 0;
    } else if (strcasecmp("no", value) == 0) {
        return 0;
    } else if (strcasecmp("yes", value) == 0) {
        return 1;
    } else if (strcasecmp("0", value) == 0) {
        return 0;
    } else if (strcasecmp("1", value) == 0) {
        return 1;
    } else if (strcasecmp("true", value) == 0) {
        return 1;
    } else if (strcasecmp("false", value) == 0) {
        return 0;
    } else {
        *err = -1;

        return atoi(value);
    }
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
            cdb2_skipws(s);
            *nid_dbname = (*s != '\0') ? OBJ_txt2nid(s) : cdb2_nid_dbname;
        }
        return SSL_VERIFY_DBNAME;
    }
    return SSL_ALLOW;
}

static char *get_dbhosts_from_env(const char *dbname)
{
    char *DB_HOSTS_PREFIX = "COMDB2_CONFIG_HOSTS";
    char COMDB2_CONFIG_DB_HOSTS[sizeof(char) * (strlen(dbname) + strlen(DB_HOSTS_PREFIX) + 2)];
    snprintf(COMDB2_CONFIG_DB_HOSTS, sizeof(COMDB2_CONFIG_DB_HOSTS), "%s_%s", DB_HOSTS_PREFIX, dbname);
    return getenv(COMDB2_CONFIG_DB_HOSTS);
}

static void parse_dbhosts_from_env(int *dbnum, int *num_hosts, char hosts[][CDB2HOSTNAME_LEN], int *db_found,
                                   const char *dbname)
{
    char *db_info = get_dbhosts_from_env(dbname);

    if (db_info) {
        char *str = strdup(db_info);
        *num_hosts = 0; // overwrite hosts found in config files
        *db_found = 1;
        char *tok = NULL;
        char *last = NULL;
        tok = strtok_r(str, " :,", &last);
        if (tok == NULL) {
            return;
        }
        if (is_valid_int(tok)) {
            *dbnum = atoi(tok);
            tok = strtok_r(NULL, " :,", &last);
        }
        while (tok != NULL) {
            sprintf(hosts[*num_hosts], "%s", tok);
            (*num_hosts)++;
            tok = strtok_r(NULL, " :,", &last);
        }
        free(str);
    }
    return;
}

#ifdef CDB2API_TEST
void reprocess_env_vars(void)
{
    have_read_env = 0;
}
#endif

static void read_comdb2db_environment_cfg(cdb2_hndl_tp *hndl, const char *comdb2db_name,
                                          char comdb2db_hosts[][CDB2HOSTNAME_LEN], int *num_hosts, int *comdb2db_num,
                                          const char *dbname, char db_hosts[][CDB2HOSTNAME_LEN], int *num_db_hosts,
                                          int *dbnum, int *dbname_found, int *comdb2db_found)
{

    pthread_mutex_lock(&cdb2_sockpool_mutex);
    if (!have_read_env) {
        // Do these once.
        process_env_var_str_on_off("COMDB2_FEATURE_CONNECT_HOST_ON_REJECT", &connect_host_on_reject,
                                   &cdb2_connect_host_on_reject_set_from_env);
        process_env_var_str_on_off("COMDB2_FEATURE_IAM_IDENTITY", &iam_identity, &cdb2_iam_identity_set_from_env);
        process_env_var_str_on_off("COMDB2_FEATURE_DONATE_UNUSED_CONNECTIONS", &donate_unused_connections,
                                   &cdb2_donate_unused_connections_set_from_env);
        process_env_var_str_on_off("COMDB2_FEATURE_DISABLE_FIX_LAST_SET", &disable_fix_last_set,
                                   &cdb2_disable_fix_last_set_set_from_env);
        process_env_var_str_on_off("COMDB2_FEATURE_USE_FTRUNCATE", &cdb2_use_ftruncate,
                                   &cdb2_use_ftruncate_set_from_env);
        process_env_var_str("COMDB2_CONFIG_ROOM", (char *)&cdb2_machine_room, sizeof(cdb2_machine_room),
                            &cdb2_machine_room_set_from_env);
        process_env_var_int("COMDB2_CONFIG_PORTMUXPORT", &CDB2_PORTMUXPORT, &cdb2_portmuxport_set_from_env);
        process_env_var_str("COMDB2_CONFIG_COMDB2DBNAME", (char *)&cdb2_comdb2dbname, sizeof(cdb2_comdb2dbname),
                            &cdb2_comdb2dbname_set_from_env);
        process_env_var_int("COMDB2_CONFIG_TCPBUFSZ", &cdb2_tcpbufsz, &cdb2_tcpbufsz_set_from_env);
        process_env_var_str("COMDB2_CONFIG_DNSSUFFIX", (char *)&cdb2_dnssuffix, sizeof(cdb2_dnssuffix),
                            &cdb2_dnssuffix_set_from_env);
        process_env_var_str("COMDB2_CONFIG_BMSSUFFIX", (char *)&cdb2_bmssuffix, sizeof(cdb2_bmssuffix),
                            &cdb2_bmssuffix_set_from_env);
        process_env_var_str_on_off("COMDB2_CONFIG_ALLOW_PMUX_ROUTE", &cdb2_allow_pmux_route,
                                   &cdb2_allow_pmux_route_set_from_env);
        process_env_var_str_on_off("COMDB2_CONFIG_RETRY_DBINFO_ON_CACHED_CONNECTION_FAILURE",
                                   &retry_dbinfo_on_cached_connection_failure,
                                   &cdb2_retry_dbinfo_on_cached_connection_failure_set_from_env);
        process_env_var_str_on_off("COMDB2_CONFIG_GET_HOSTNAME_FROM_SOCKPOOL_FD", &get_hostname_from_sockpool_fd,
                                   &cdb2_get_hostname_from_sockpool_fd_set_from_env);
        process_env_var_str_on_off("COMDB2_CONFIG_GET_DBINFO", &get_dbinfo, &cdb2_get_dbinfo_set_from_env);
        process_env_var_str("COMDB2_CONFIG_DEFAULT_TYPE", (char *)&cdb2_default_cluster, sizeof(cdb2_default_cluster),
                            &cdb2_default_cluster_set_from_env);
        process_env_var_int("COMDB2_CONFIG_CONNECT_TIMEOUT", &CDB2_CONNECT_TIMEOUT, &cdb2_connect_timeout_set_from_env);
        process_env_var_int("COMDB2_CONFIG_SOCKPOOL_SEND_TIMEOUTMS", &CDB2_SOCKPOOL_SEND_TIMEOUTMS,
                            &cdb2_sockpool_send_timeoutms_set_from_env);
        process_env_var_int("COMDB2_CONFIG_SOCKPOOL_RECV_TIMEOUTMS", &CDB2_SOCKPOOL_RECV_TIMEOUTMS,
                            &cdb2_sockpool_recv_timeoutms_set_from_env);
        process_env_var_int("COMDB2_CONFIG_API_CALL_TIMEOUT", &CDB2_API_CALL_TIMEOUT,
                            &cdb2_api_call_timeout_set_from_env);
        process_env_var_int("COMDB2_CONFIG_ENFORCE_API_CALL_TIMEOUT", &CDB2_ENFORCE_API_CALL_TIMEOUT,
                            &cdb2_enforce_api_call_timeout_set_from_env);
        process_env_var_int("COMDB2_CONFIG_COMDB2DB_TIMEOUT", &COMDB2DB_TIMEOUT, &cdb2_comdb2db_timeout_set_from_env);
        process_env_var_int("COMDB2_CONFIG_SOCKET_TIMEOUT", &CDB2_SOCKET_TIMEOUT, &cdb2_socket_timeout_set_from_env);
        process_env_var_int("COMDB2_CONFIG_PROTOBUF_SIZE", &CDB2_PROTOBUF_SIZE, &cdb2_protobuf_size_set_from_env);
        process_env_var_int("COMDB2_FEATURE_PROTOBUF_HEURISTIC", &cdb2_protobuf_heuristic,
                            &cdb2_protobuf_heuristic_set_from_env);
        process_env_var_int("COMDB2_FEATURE_FLAT_COL_VALS", &cdb2_flat_col_vals, &cdb2_flat_col_vals_set_from_env);
        process_env_var_int("COMDB2_FEATURE_USE_BMSD", &cdb2_use_bmsd, &cdb2_use_bmsd_set_from_env);
        process_env_var_int("COMDB2_FEATURE_COMDB2DB_FALLBACK", &cdb2_comdb2db_fallback,
                            &cdb2_comdb2db_fallback_set_from_env);
        process_env_var_int("COMDB2_FEATURE_USE_OPTIONAL_IDENTITY", &cdb2_use_optional_identity,
                            &cdb2_use_optional_identity_set_from_env);
        process_env_var_int("COMDB2_FEATURE_DISCARD_UNREAD_SOCKET_DATA", &cdb2_discard_unread_socket_data,
                            &cdb2_discard_unread_socket_data_set_from_env);
        process_env_var_int("COMDB2_FEATURE_ALARM_UNREAD_SOCKET_DATA", &cdb2_alarm_unread_socket_data,
                            &cdb2_alarm_unread_socket_data_set_from_env);
        process_env_var_int("COMDB2_FEATURE_MAX_DISCARD_RECORDS", &cdb2_max_discard_records,
                            &cdb2_max_discard_records_set_from_env);
        process_env_var_str_on_off("COMDB2_CONFIG_CHECK_HB_ON_BLOCKED_WRITE", &check_hb_on_blocked_write,
                                   &check_hb_on_blocked_write_set_from_env);

        char *arg = getenv("COMDB2_CONFIG_INSTALL_STATIC_LIBS");
        if (cdb2_install != NULL && arg != NULL) {
            char *libs = strdup(arg), *lib = NULL, *lib_last = NULL;
            lib = strtok_r(libs, " ", &lib_last);
            while (lib != NULL) {
                (*cdb2_install)(lib);
                lib = strtok_r(NULL, " ", &lib_last);
            }
            free(libs);
            cdb2_install_set_from_env = 1;
        }

        arg = getenv("COMDB2_CONFIG_UNINSTALL_STATIC_LIBS");
        if (cdb2_uninstall != NULL && arg != NULL) {
            char *libs = strdup(arg), *lib = NULL, *lib_last = NULL;
            lib = strtok_r(libs, " ", &lib_last);
            while (lib != NULL) {
                (*cdb2_uninstall)(lib);
                lib = strtok_r(NULL, " ", &lib_last);
            }
            free(libs);
            cdb2_install_set_from_env = 1;
        }

        have_read_env = 1;
    }

    if (hndl) {
        if (hndl->resolv_def && cdb2_default_cluster_set_from_env) {
            /* Default tier envvar tier takes the 2nd highest priority.
               If we've resolved the actual tier from the envvar,
               we can stop resolving "default" further */
            strcpy(hndl->type, cdb2_default_cluster);
            hndl->resolv_def = (strcasecmp(hndl->type, "default") == 0);
        }
        if (cdb2_connect_timeout_set_from_env)
            hndl->connect_timeout = CDB2_CONNECT_TIMEOUT;
        if (cdb2_sockpool_send_timeoutms_set_from_env)
            hndl->sockpool_send_timeoutms = CDB2_SOCKPOOL_SEND_TIMEOUTMS;
        if (cdb2_sockpool_recv_timeoutms_set_from_env)
            hndl->sockpool_recv_timeoutms = CDB2_SOCKPOOL_RECV_TIMEOUTMS;
        if (cdb2_api_call_timeout_set_from_env)
            hndl->api_call_timeout = CDB2_API_CALL_TIMEOUT;
        if (cdb2_comdb2db_timeout_set_from_env)
            hndl->comdb2db_timeout = COMDB2DB_TIMEOUT;
        if (cdb2_socket_timeout_set_from_env)
            hndl->socket_timeout = CDB2_SOCKET_TIMEOUT;
    }

    if (comdb2db_name)
        parse_dbhosts_from_env(comdb2db_num, num_hosts, comdb2db_hosts, comdb2db_found, comdb2db_name);

    if (dbname)
        parse_dbhosts_from_env(dbnum, num_db_hosts, db_hosts, dbname_found, dbname);

    pthread_mutex_unlock(&cdb2_sockpool_mutex);
}

static void only_read_config(cdb2_hndl_tp *, int *); /* FORWARD */

static int cdb2_max_room_num = 0;
static int cdb2_has_room_distance = 0;
static int *cdb2_room_distance = NULL;

static void read_comdb2db_cfg(cdb2_hndl_tp *hndl, COMDB2BUF *s, const char *comdb2db_name, const char *buf,
                              char comdb2db_hosts[][CDB2HOSTNAME_LEN], int *num_hosts, int *comdb2db_num,
                              const char *dbname, char db_hosts[][CDB2HOSTNAME_LEN], int *num_db_hosts, int *dbnum,
                              int *dbname_found, int *comdb2db_found, int *stack_at_open, char shards[][DBNAME_LEN],
                              int *num_shards)
{
    char line[PATH_MAX > 2048 ? PATH_MAX : 2048] = {0};
    int line_no = 0;
    int err = 0;

    bzero(line, sizeof(line));
    debugprint("entering\n");
    while (cdb2_read_line((char *)&line, sizeof(line), s, buf, &line_no) != -1) {
        char *last = NULL;
        char *tok = NULL;
        tok = strtok_r(line, " :", &last);
        if (tok == NULL) {
            // Skip unparseable line
            continue;
        }
        if (comdb2db_name && strcasecmp(comdb2db_name, tok) == 0) {
            tok = strtok_r(NULL, " :,", &last);
            if (tok && is_valid_int(tok)) {
                *comdb2db_num = atoi(tok);
                tok = strtok_r(NULL, " :,", &last);
            }
            while (tok != NULL) {
                strncpy(comdb2db_hosts[*num_hosts], tok, CDB2HOSTNAME_LEN - 1);
                (*num_hosts)++;
                tok = strtok_r(NULL, " :,", &last);
                *comdb2db_found = 1;
            }
        } else if (dbname && (strcasecmp(dbname, tok) == 0)) {
            tok = strtok_r(NULL, " =:,", &last);
            if (tok != NULL && strcasecmp("additional_cfg", tok) == 0) {
                char *cfg_path = strtok_r(NULL, " =:,", &last);
                if (cfg_path != NULL) { /* copy out the path. it'll be parsed in the very end. */
                    cdb2_skipws(cfg_path);
                    strncpy(CDB2DBCONFIG_ADDL_PATH, cfg_path, sizeof(CDB2DBCONFIG_ADDL_PATH) - 1);
                    CDB2DBCONFIG_ADDL_PATH[sizeof(CDB2DBCONFIG_ADDL_PATH) - 1] = '\0';
                }
            } else { /* host list */
                if (tok && is_valid_int(tok)) {
                    *dbnum = atoi(tok);
                    tok = strtok_r(NULL, " :,", &last);
                }
                while (tok != NULL) {
                    strncpy(db_hosts[*num_db_hosts], tok, CDB2HOSTNAME_LEN - 1);
                    tok = strtok_r(NULL, " :,", &last);
                    (*num_db_hosts)++;
                    *dbname_found = 1;
                }
            }
        } else if (num_shards && strcasecmp("partition", tok) == 0) {
            tok = strtok_r(NULL, " :,", &last);
            if (!dbname || strcasecmp(dbname, tok) != 0)
                continue;
            tok = strtok_r(NULL, " :,", &last);
            while (tok != NULL) {
                if (strcasecmp(dbname, tok) == 0) {
                    fprintf(stderr, "%s: Can't use the partition name as one of the shards\n", __func__);
                } else {
                    strncpy(shards[*num_shards], tok, DBNAME_LEN - 1);
                    (*num_shards)++;
                }
                tok = strtok_r(NULL, " :,", &last);
            }
        } else if (strcasecmp("comdb2_feature", tok) == 0) {
            tok = strtok_r(NULL, " =:,", &last);
            if (!cdb2_connect_host_on_reject_set_from_env && (strcasecmp("connect_host_on_reject", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                connect_host_on_reject = value_on_off(tok, &err);
            } else if (!cdb2_iam_identity_set_from_env && (strcasecmp("iam_identity_v6", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok)
                    iam_identity = value_on_off(tok, &err);
            } else if (!cdb2_donate_unused_connections_set_from_env &&
                       (strcasecmp("donate_unused_connections", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok)
                    donate_unused_connections = value_on_off(tok, &err);
            } else if (!cdb2_disable_fix_last_set_set_from_env && (strcasecmp("disable_fix_last_set", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                disable_fix_last_set = value_on_off(tok, &err);
            } else if (!cdb2_use_ftruncate_set_from_env && (strcasecmp("use_ftruncate", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok)
                    cdb2_use_ftruncate = value_on_off(tok, &err);
            } else if (!cdb2_use_optional_identity_set_from_env && (strcasecmp("use_optional_identity", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok)
                    cdb2_use_optional_identity = value_on_off(tok, &err);
            } else if (!cdb2_comdb2db_fallback_set_from_env && (strcasecmp("comdb2db_fallback", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok)
                    cdb2_comdb2db_fallback = value_on_off(tok, &err);
            } else if (!cdb2_use_bmsd_set_from_env && (strcasecmp("use_bmsd", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok)
                    cdb2_use_bmsd = value_on_off(tok, &err);
                /* Add way to reset room_distance */
                if (cdb2_use_bmsd != 1 && cdb2_use_bmsd != 0) {
                    cdb2_use_bmsd = 0;
                    cdb2_has_room_distance = 0;
                    if (cdb2_room_distance) {
                        free(cdb2_room_distance);
                        cdb2_room_distance = NULL;
                    }
                }
            } else if (!cdb2_has_room_distance && (strcasecmp("room_distance", tok) == 0)) {
                int distance = INT_MAX;
                int current_size = 20;
                cdb2_room_distance = malloc((current_size + 1) * sizeof(int));
                tok = strtok_r(NULL, " =:,", &last);
                int ii;
                for (ii = 0; ii <= current_size; ii++)
                    cdb2_room_distance[ii] = INT_MAX;
                while (tok != NULL) {
                    if (!is_valid_int(tok))
                        break;
                    ii = atoi(tok);
                    if (ii == 0) {
                        break;
                    }
                    distance = INT_MAX;
                    if (ii > current_size) {
                        cdb2_room_distance = realloc(cdb2_room_distance, (ii + 1) * sizeof(int));
                        for (int j = current_size + 1; j <= ii; j++) {
                            cdb2_room_distance[j] = INT_MAX;
                        }
                        current_size = ii;
                    }
                    tok = strtok_r(NULL, " ,", &last);

                    if (tok && is_valid_int(tok))
                        distance = atoi(tok);
                    else
                        continue;

                    cdb2_room_distance[ii] = distance;
                    if (ii > cdb2_max_room_num)
                        cdb2_max_room_num = ii;
                    tok = strtok_r(NULL, " :", &last);
                }
                cdb2_has_room_distance = 1;
            } else if ((strcasecmp("use_env_vars", tok) == 0) && !hndl) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok)
                    cdb2_use_env_vars = value_on_off(tok, &err);
            } else if (!cdb2_discard_unread_socket_data_set_from_env &&
                       (strcasecmp("discard_unread_socket_data", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok)
                    cdb2_discard_unread_socket_data = value_on_off(tok, &err);
            } else if (!cdb2_alarm_unread_socket_data_set_from_env &&
                       (strcasecmp("alarm_unread_socket_data_v1", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok)
                    cdb2_alarm_unread_socket_data = value_on_off(tok, &err);
            } else if (!cdb2_max_discard_records_set_from_env && (strcasecmp("max_discard_records", tok) == 0)) {
                tok = strtok_r(NULL, " =:,", &last);
                if (tok && is_valid_int(tok))
                    cdb2_max_discard_records = atoi(tok);
            } else if (!cdb2_flat_col_vals_set_from_env && strcasecmp("flat_col_vals", tok) == 0) {
                if ((tok = strtok_r(NULL, " =:,", &last)) != NULL)
                    cdb2_flat_col_vals = value_on_off(tok, &err);
            } else if (!cdb2_protobuf_heuristic_set_from_env && strcasecmp("protobuf_heuristic", tok) == 0) {
                if ((tok = strtok_r(NULL, " =:,", &last)) != NULL)
                    cdb2_protobuf_heuristic = value_on_off(tok, &err);
            }
        } else if (strcasecmp("comdb2_config", tok) == 0) {
            tok = strtok_r(NULL, " =:,", &last);
            if (tok == NULL) continue;
            pthread_mutex_lock(&cdb2_sockpool_mutex);
            if (!cdb2_default_cluster_set_from_env && strcasecmp("default_type", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    if (hndl && hndl->resolv_def) {
                        strncpy(hndl->type, tok, sizeof(cdb2_default_cluster) - 1);
                    } else if (!hndl) {
                        strncpy(cdb2_default_cluster, tok, sizeof(cdb2_default_cluster) - 1);
                    }
                }
            } else if (!cdb2_machine_room_set_from_env && strcasecmp("room", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    strncpy(cdb2_machine_room, tok, sizeof(cdb2_machine_room) - 1);
            } else if (!cdb2_portmuxport_set_from_env &&
                       (strcasecmp("portmuxport", tok) == 0 || strcasecmp("pmuxport", tok) == 0)) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    CDB2_PORTMUXPORT = atoi(tok);
            } else if (!cdb2_connect_timeout_set_from_env && strcasecmp("connect_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl && tok)
                    hndl->connect_timeout = atoi(tok);
                else if (tok)
                    CDB2_CONNECT_TIMEOUT = atoi(tok);
            } else if (!cdb2_api_call_timeout_set_from_env && strcasecmp("api_call_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl && tok)
                    hndl->api_call_timeout = atoi(tok);
                else if (tok)
                    CDB2_API_CALL_TIMEOUT = atoi(tok);
            } else if (!cdb2_api_call_timeout_set_from_env && (strcasecmp("enforce_api_call_timeout", tok) == 0)) {
                tok = strtok_r(NULL, " :,", &last);
                CDB2_ENFORCE_API_CALL_TIMEOUT = value_on_off(tok, &err);
            } else if (strcasecmp("max_auto_consume_rows", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    CDB2_MAX_AUTO_CONSUME_ROWS = atoi(tok);
            } else if (!cdb2_comdb2db_timeout_set_from_env && strcasecmp("comdb2db_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl && tok)
                    hndl->comdb2db_timeout = atoi(tok);
                else if (tok)
                    COMDB2DB_TIMEOUT = atoi(tok);
            } else if (!cdb2_sockpool_recv_timeoutms_set_from_env &&
                       (strcasecmp("sockpool_recv_timeoutms", tok) == 0)) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl)
                    hndl->sockpool_recv_timeoutms = atoi(tok);
                else
                    CDB2_SOCKPOOL_RECV_TIMEOUTMS = atoi(tok);
            } else if (!cdb2_sockpool_send_timeoutms_set_from_env &&
                       (strcasecmp("sockpool_send_timeoutms", tok) == 0)) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl)
                    hndl->sockpool_send_timeoutms = atoi(tok);
                else
                    CDB2_SOCKPOOL_SEND_TIMEOUTMS = atoi(tok);
            } else if (!cdb2_socket_timeout_set_from_env && strcasecmp("socket_timeout", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (hndl && tok)
                    hndl->socket_timeout = atoi(tok);
                else if (tok)
                    CDB2_SOCKET_TIMEOUT = atoi(tok);
            } else if (!cdb2_comdb2dbname_set_from_env && strcasecmp("comdb2dbname", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    strncpy(cdb2_comdb2dbname, tok, sizeof(cdb2_comdb2dbname) - 1);
            } else if (!cdb2_protobuf_size_set_from_env && strcasecmp("protobuf_size_v2", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                int buf_size = atoi(tok);
                if (hndl && buf_size > 0)
                    hndl->protobuf_size = atoi(tok);
                else if (buf_size > 0)
                    CDB2_PROTOBUF_SIZE = atoi(tok);
            } else if (!cdb2_tcpbufsz_set_from_env && strcasecmp("tcpbufsz", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    cdb2_tcpbufsz = atoi(tok);
            } else if (!cdb2_dnssuffix_set_from_env &&
                       (strcasecmp("dnssufix", tok) == 0 || strcasecmp("dnssuffix", tok) == 0)) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    strncpy(cdb2_dnssuffix, tok, sizeof(cdb2_dnssuffix) - 1);
            } else if (!cdb2_bmssuffix_set_from_env && strcasecmp("bmssuffix", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    strncpy(cdb2_bmssuffix, tok, sizeof(cdb2_bmssuffix) - 1);
            } else if (!cdb2_allow_pmux_route_set_from_env && strcasecmp("allow_pmux_route", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    cdb2_allow_pmux_route = value_on_off(tok, &err);
                }
            } else if (!cdb2_install_set_from_env && (strcasecmp("uninstall_static_libs_v4", tok) == 0 ||
                                                      strcasecmp("disable_static_libs", tok) == 0)) {
                /* Provide a way to disable statically linked libraries. */
                tok = strtok_r(NULL, ":,", &last);
                if (cdb2_uninstall != NULL && tok == NULL) {
                    /* By convention, this should uninstall all static libs. */
                    cdb2_uninstall(NULL);
                } else if (cdb2_uninstall != NULL && tok != NULL) {
                    char *libs = strdup(tok), *lib = NULL, *lib_last = NULL;
                    lib = strtok_r(libs, " ", &lib_last);
                    while (lib != NULL) {
                        (*cdb2_uninstall)(lib);
                        lib = strtok_r(NULL, " ", &lib_last);
                    }
                    free(libs);
                }
            } else if (!cdb2_install_set_from_env &&
                       (strcasecmp("install_static_libs_v4", tok) == 0 || strcasecmp("enable_static_libs", tok) == 0)) {
                /* Provide a way to enable statically linked libraries. */
                tok = strtok_r(NULL, ":,", &last);
                if (cdb2_install != NULL && tok == NULL) {
                    /* By convention, this should install all static libs. */
                    cdb2_install(NULL);
                } else if (cdb2_install != NULL && tok != NULL) {
                    char *libs = strdup(tok), *lib = NULL, *lib_last = NULL;
                    lib = strtok_r(libs, " ", &lib_last);
                    while (lib != NULL) {
                        (*cdb2_install)(lib);
                        lib = strtok_r(NULL, " ", &lib_last);
                    }
                    free(libs);
                }
            } else if (strcasecmp("stack_at_open", tok) == 0 && stack_at_open) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    if (strncasecmp(tok, "true", 4) == 0) {
                        *stack_at_open = 1;
                    } else {
                        *stack_at_open = 0;
                    }
                }
            } else if (!cdb2_get_dbinfo_set_from_env && (strcasecmp("get_dbinfo_v3", tok) == 0)) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    get_dbinfo = value_on_off(tok, &err);
                }
            } else if ((strcasecmp("max_local_connection_cache_entries", tok) == 0)) {
                if (hndl == NULL && !max_local_connection_cache_entries_envvar) {
                    tok = strtok_r(NULL, " :,", &last);
                    if (tok) {
                        max_local_connection_cache_entries = atoi(tok);
                    }
                }
            } else if ((strcasecmp("max_local_connection_cache_age", tok) == 0)) {
                if (hndl == NULL && !max_local_connection_cache_age_envvar) {
                    tok = strtok_r(NULL, " :,", &last);
                    if (tok) {
                        max_local_connection_cache_age = atoi(tok);
                    }
                }
            } else if ((strcasecmp("local_connection_cache_use_sbuf", tok) == 0)) {
                if (hndl == NULL && !local_connection_cache_use_sbuf_envvar) {
                    tok = strtok_r(NULL, " :,", &last);
                    if (tok)
                        local_connection_cache_use_sbuf = !!atoi(tok);
                }
            } else if ((strcasecmp("local_connection_cache_check_pid", tok) == 0)) {
                if (hndl == NULL && !local_connection_cache_check_pid_envvar) {
                    tok = strtok_r(NULL, " :,", &last);
                    if (tok)
                        local_connection_cache_check_pid = !!atoi(tok);
                }
            } else if (!cdb2_retry_dbinfo_on_cached_connection_failure_set_from_env &&
                       (strcasecmp("retry_dbinfo_on_cached_connection_failure", tok) == 0)) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok) {
                    retry_dbinfo_on_cached_connection_failure = value_on_off(tok, &err);
                }
            } else if (!cdb2_get_hostname_from_sockpool_fd_set_from_env &&
                       (strcasecmp("get_hostname_from_sockpool_fd_v3", tok) == 0)) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    get_hostname_from_sockpool_fd = value_on_off(tok, &err);
            } else if (strcasecmp("ssl_mode", tok) == 0 || strcasecmp("ssl_mode_v9", tok) == 0) {
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
            } else if (strcasecmp("check_hb_on_blocked_write", tok) == 0) {
                if (!check_hb_on_blocked_write_set_from_env) {
                    tok = strtok_r(NULL, " :,", &last);
                    if (tok)
                        check_hb_on_blocked_write = !!atoi(tok);
                }
            } else if (strcasecmp("request_fingerprint", tok) == 0) {
                tok = strtok_r(NULL, " :,", &last);
                if (tok)
                    CDB2_REQUEST_FP = (strncasecmp(tok, "true", 4) == 0);
            }
            pthread_mutex_unlock(&cdb2_sockpool_mutex);
        }
        bzero(line, sizeof(line));
    }
}

static int cdb2_dbinfo_query(cdb2_hndl_tp *hndl, const char *type, const char *dbname, int dbnum, const char *host,
                             char valid_hosts[][CDB2HOSTNAME_LEN], int *valid_ports, int *master_node,
                             int *num_valid_hosts, int *num_valid_sameroom_hosts);
static int get_config_file(const char *dbname, char *f, size_t s, int use_pkg_path)
{
    char *root = getenv("COMDB2_ROOT");

    /* `dbname' is NULL if we're only reading defaults from comdb2db.cfg.
       Formatting a NULL pointer with %s specifier is undefined behavior,
       and whatever file path it produces (in glibc, it's "(null).cfg")
       does not make sense. Return an error. */
    if (dbname == NULL)
        return -1;

    size_t n;
    if (use_pkg_path) {
        n = snprintf(f, s, "%s%s.cfg", CDB2DBCONFIG_PKG_PATH, dbname);
        if (n >= s)
            return -1;
        return 0;
    }

    if (root == NULL)
        root = QUOTE(COMDB2_ROOT);
    n = snprintf(f, s, "%s%s%s.cfg", root, CDB2DBCONFIG_NOBBENV_PATH, dbname);
    if (n >= s)
        return -1;
    return 0;
}

static void set_cdb2_timeouts(cdb2_hndl_tp *hndl)
{
    if (!hndl)
        return;
    if (!hndl->connect_timeout)
        hndl->connect_timeout = CDB2_CONNECT_TIMEOUT;
    if (!hndl->comdb2db_timeout)
        hndl->comdb2db_timeout = COMDB2DB_TIMEOUT;
    if (!hndl->socket_timeout)
        hndl->socket_timeout = CDB2_SOCKET_TIMEOUT;
    if (!hndl->sockpool_recv_timeoutms)
        hndl->sockpool_recv_timeoutms = CDB2_SOCKPOOL_RECV_TIMEOUTMS;
    if (!hndl->sockpool_send_timeoutms)
        hndl->sockpool_send_timeoutms = CDB2_SOCKPOOL_SEND_TIMEOUTMS;
    if (!hndl->max_auto_consume_rows)
        hndl->max_auto_consume_rows = CDB2_MAX_AUTO_CONSUME_ROWS;
    if (!hndl->api_call_timeout)
        hndl->api_call_timeout = CDB2_API_CALL_TIMEOUT;

    if (hndl->connect_timeout > hndl->api_call_timeout)
        hndl->connect_timeout = hndl->api_call_timeout;
    if (hndl->comdb2db_timeout > hndl->api_call_timeout)
        hndl->comdb2db_timeout = hndl->api_call_timeout;
    if (hndl->socket_timeout > hndl->api_call_timeout)
        hndl->socket_timeout = hndl->api_call_timeout;

    if (!CDB2_ENFORCE_API_CALL_TIMEOUT || !hndl->max_call_time)
        set_max_call_time(hndl);
}

/* Read all available comdb2 configuration files.
   The function returns -1 if the config file path is longer than PATH_MAX;
   returns 0 otherwise. */
static int read_available_comdb2db_configs(cdb2_hndl_tp *hndl, char comdb2db_hosts[][CDB2HOSTNAME_LEN],
                                           const char *comdb2db_name, int *num_hosts, int *comdb2db_num,
                                           const char *dbname, char db_hosts[][CDB2HOSTNAME_LEN], int *num_db_hosts,
                                           int *dbnum, char shards[][DBNAME_LEN], int *num_shards)
{
    COMDB2BUF *s = NULL;
    char filename[PATH_MAX];
    int comdb2db_found = 0;
    int dbname_found = 0;
    int fallback_on_bb_bin = 1;

    if (hndl)
        debugprint("entering\n");

    pthread_mutex_lock(&cdb2_cfg_lock);

    if (num_hosts)
        *num_hosts = 0;
    if (num_db_hosts)
        *num_db_hosts = 0;
    if (num_shards)
        *num_shards = 0;
    int *send_stack = hndl ? (&hndl->send_stack) : NULL;

#ifdef CDB2API_TEST
    if (cdb2cfg_override_all_config_paths) {
        if ((s = cdb2_cdb2buf_openread(cdb2dbconfig_singleconfig)) != NULL) {
            read_comdb2db_cfg(NULL, s, comdb2db_name, NULL, comdb2db_hosts, num_hosts, comdb2db_num, dbname, db_hosts,
                              num_db_hosts, dbnum, &dbname_found, &comdb2db_found, send_stack, shards, num_shards);
            cdb2buf_close(s);
        }

        if ((s = cdb2_cdb2buf_openread(cdb2dbconfig_singleconfig)) != NULL) {
            read_comdb2db_cfg(hndl, s, comdb2db_name, NULL, comdb2db_hosts, num_hosts, comdb2db_num, dbname, db_hosts,
                              num_db_hosts, dbnum, &dbname_found, &comdb2db_found, send_stack, shards, num_shards);
            cdb2buf_close(s);
        }

        if (cdb2_use_env_vars) {
            read_comdb2db_environment_cfg(hndl, comdb2db_name, comdb2db_hosts, num_hosts, comdb2db_num, dbname,
                                          db_hosts, num_db_hosts, dbnum, &dbname_found, &comdb2db_found);
        }

    } else {
#endif

#ifdef CDB2API_TEST
        if ((s = cdb2_cdb2buf_openread(cdb2api_test_comdb2db_cfg)) != NULL) {
            read_comdb2db_cfg(NULL, s, comdb2db_name, NULL, comdb2db_hosts, num_hosts, comdb2db_num, dbname, db_hosts,
                              num_db_hosts, dbnum, &dbname_found, &comdb2db_found, send_stack, shards, num_shards);
            cdb2buf_close(s);
            fallback_on_bb_bin = 0;
        } else
#endif /* CDB2API_TEST */

            if (CDB2DBCONFIG_BUF != NULL) {
                read_comdb2db_cfg(NULL, NULL, comdb2db_name, CDB2DBCONFIG_BUF, comdb2db_hosts, num_hosts, comdb2db_num,
                                  dbname, db_hosts, num_db_hosts, dbnum, &dbname_found, &comdb2db_found, send_stack,
                                  shards, num_shards);
                fallback_on_bb_bin = 0;
            } else if (*CDB2DBCONFIG_NOBBENV != '\0' && (s = cdb2_cdb2buf_openread(CDB2DBCONFIG_NOBBENV)) != NULL) {
                read_comdb2db_cfg(NULL, s, comdb2db_name, NULL, comdb2db_hosts, num_hosts, comdb2db_num, dbname,
                                  db_hosts, num_db_hosts, dbnum, &dbname_found, &comdb2db_found, send_stack, shards,
                                  num_shards);
                cdb2buf_close(s);
                fallback_on_bb_bin = 0;
            }

        /* This is a temporary solution.  There's no clear plan for how comdb2db.cfg
         * will be deployed to non-dbini machines. In the meantime, we have
         * programmers who want to use the API on dbini/mini machines. So if we
         * can't find the file in any standard location, look at /bb/bin
         * Once deployment details for comdb2db.cfg solidify, this will go away. */
        if (fallback_on_bb_bin) {
            if ((s = cdb2_cdb2buf_openread(CDB2DBCONFIG_TEMP_BB_BIN)) != NULL) {
                read_comdb2db_cfg(NULL, s, comdb2db_name, NULL, comdb2db_hosts, num_hosts, comdb2db_num, dbname,
                                  db_hosts, num_db_hosts, dbnum, &dbname_found, &comdb2db_found, send_stack, shards,
                                  num_shards);
                cdb2buf_close(s);
            }
        }

#ifdef CDB2API_TEST
        if ((s = cdb2_cdb2buf_openread(cdb2api_test_dbname_cfg)) != NULL) {
            read_comdb2db_cfg(hndl, s, comdb2db_name, NULL, comdb2db_hosts, num_hosts, comdb2db_num, dbname, db_hosts,
                              num_db_hosts, dbnum, &dbname_found, &comdb2db_found, send_stack, shards, num_shards);
            cdb2buf_close(s);
        } else
#endif /* CDB2API_TEST */

            if (get_config_file(dbname, filename, sizeof(filename), 0) == 0 &&
                (s = cdb2_cdb2buf_openread(filename)) != NULL) {
                read_comdb2db_cfg(hndl, s, comdb2db_name, NULL, comdb2db_hosts, num_hosts, comdb2db_num, dbname,
                                  db_hosts, num_db_hosts, dbnum, &dbname_found, &comdb2db_found, send_stack, shards,
                                  num_shards);
                cdb2buf_close(s);
            } else if (get_config_file(dbname, filename, sizeof(filename), 1) == 0 &&
                       (s = cdb2_cdb2buf_openread(filename)) != NULL) {
                read_comdb2db_cfg(hndl, s, comdb2db_name, NULL, comdb2db_hosts, num_hosts, comdb2db_num, dbname,
                                  db_hosts, num_db_hosts, dbnum, &dbname_found, &comdb2db_found, send_stack, shards,
                                  num_shards);
                cdb2buf_close(s);
            }

        /* Process additional_cfg if specified */
        if (CDB2DBCONFIG_ADDL_PATH[0] != '\0') {
            if ((s = cdb2_cdb2buf_openread(CDB2DBCONFIG_ADDL_PATH)) != NULL) {
                read_comdb2db_cfg(hndl, s, comdb2db_name, NULL, comdb2db_hosts, num_hosts, comdb2db_num, dbname,
                                  db_hosts, num_db_hosts, dbnum, &dbname_found, &comdb2db_found, send_stack, shards,
                                  num_shards);
                cdb2buf_close(s);
            }
            CDB2DBCONFIG_ADDL_PATH[0] = '\0';
        }

        if (cdb2_use_env_vars) {
            read_comdb2db_environment_cfg(hndl, comdb2db_name, comdb2db_hosts, num_hosts, comdb2db_num, dbname,
                                          db_hosts, num_db_hosts, dbnum, &dbname_found, &comdb2db_found);
        }
#ifdef CDB2API_TEST
    }
#endif

    if (hndl && strcasecmp(hndl->type, "default") == 0 && cdb2_default_cluster[0] != '\0') {
        strncpy(hndl->type, cdb2_default_cluster, sizeof(hndl->type) - 1);
    }

    pthread_mutex_unlock(&cdb2_cfg_lock);
    return 0;
}

#ifdef CDB2API_SERVER
int cdb2_get_comdb2db(char **comdb2dbname, char **comdb2dbclass)
{
    if (!strlen(cdb2_comdb2dbname)) {
        read_available_comdb2db_configs(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    }
    (*comdb2dbname) = strdup(cdb2_comdb2dbname);
    (*comdb2dbclass) = strdup((strcmp(cdb2_comdb2dbname, "comdb3db") == 0) ? "dev" : "prod");
    return 0;
}
#endif

/* populate comdb2db_hosts based on hostname info of comdb2db_name
 * returns -1 if error or no hosts were found
 * returns 0 if hosts were found
 * this function has functionality similar to cdb2_tcpresolve()
 */
static int get_host_by_addr(const char *comdb2db_name, char comdb2db_hosts[][CDB2HOSTNAME_LEN], int *num_hosts)
{
    char dns_name[256];
    int size = 0;

    if (cdb2_default_cluster[0] == '\0') {
        size = snprintf(dns_name, sizeof(dns_name), "%s.%s", comdb2db_name, cdb2_dnssuffix);
    } else {
        size = snprintf(dns_name, sizeof(dns_name), "%s-%s.%s", cdb2_default_cluster, comdb2db_name, cdb2_dnssuffix);
    }

    if (size < 0 || size >= sizeof(dns_name)) {
        fprintf(stderr, "%s: DNS name too long\n", __func__);
        return -1;
    }

    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM, .ai_protocol = 0, .ai_flags = 0};
    struct addrinfo *result = NULL;
    int gai_rc = getaddrinfo(dns_name, /*service=*/NULL, &hints, &result);
    if (gai_rc != 0) {
        fprintf(stderr, "%s:getaddrinfo(%s): %d %s\n", __func__, dns_name, gai_rc, gai_strerror(gai_rc));
        return -1;
    }

    int count = 0;
    const struct addrinfo *rp;
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        if (count >= MAX_NODES) {
            fprintf(stderr, "WARNING: %s:getaddrinfo(%s) returned more than %d results\n", __func__, dns_name,
                    MAX_NODES);
            break;
        } else if (rp->ai_family != AF_INET) {
            fprintf(stderr, "WARNING: %s:getaddrinfo(%s) returned non-AF_INET results\n", __func__, dns_name);
        } else if (!inet_ntop(AF_INET, &((const struct sockaddr_in *)rp->ai_addr)->sin_addr, comdb2db_hosts[count],
                              sizeof(comdb2db_hosts[count]))) {
            fprintf(stderr, "%s:inet_ntop(): %d %s\n", __func__, errno, strerror(errno));
            /* Don't make this a fatal error; just don't increment num_hosts */
        } else {
            count++;
            (*num_hosts)++;
        }
    }

    freeaddrinfo(result);

    if (count > 0)
        return 0;

    return -1;
}

static int get_comdb2db_hosts(cdb2_hndl_tp *hndl, char comdb2db_hosts[][CDB2HOSTNAME_LEN], int *comdb2db_ports,
                              int *master, const char *comdb2db_name, int *num_hosts, int *comdb2db_num,
                              const char *dbname, char db_hosts[][CDB2HOSTNAME_LEN], int *num_db_hosts, int *dbnum,
                              int dbinfo_or_dns, char shards[][DBNAME_LEN], int *num_shards)
{
    int rc;

    if (hndl)
        debugprint("entering\n");

    rc = read_available_comdb2db_configs(hndl, comdb2db_hosts, comdb2db_name, num_hosts, comdb2db_num, dbname, db_hosts,
                                         num_db_hosts, dbnum, shards, num_shards);
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
        LOG_CALL("dbinfo_query %d rc %d\n", __LINE__, rc);
        /* DNS lookup comdb2db hosts. */
        if (rc != 0)
            rc = get_host_by_addr(comdb2db_name, comdb2db_hosts, num_hosts);
    }

    return rc;
}

/* SOCKPOOL CODE START */

static int sockpool_enabled = 1;
static time_t sockpool_fail_time = 0;

struct sockpool_fd_list {
    int sockpool_fd;
    int in_use;
};

static struct sockpool_fd_list *sockpool_fds = NULL;
static int sockpool_fd_count = 0;

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
    struct sockaddr_sun addr;
    int fd;
    struct sockpool_hello hello;
    const char *ptr;
    size_t bytesleft;

    pthread_mutex_lock(&cdb2_cfg_lock);
    do_init_once();
    pthread_mutex_unlock(&cdb2_cfg_lock);

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd == -1) {
        fprintf(stderr, "%s:socket: %d %s\n", __func__, errno, strerror(errno));
        return -1;
    }

    bzero(&addr, sizeof(addr));
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

/* Disable sockpool and close sockpool socket */
void cdb2_disable_sockpool()
{
    pthread_mutex_lock(&cdb2_sockpool_mutex);
    sockpool_enabled = -1;
    pthread_mutex_unlock(&cdb2_sockpool_mutex);
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

static void get_host_and_port_from_fd(int fd, char *buf, size_t n, int *port)
{
    int rc;
    struct sockaddr_in addr;
    socklen_t addr_size = sizeof(struct sockaddr_in);

    if (!get_hostname_from_sockpool_fd)
        return;

    if (fd == -1)
        return;

    rc = getpeername(fd, (struct sockaddr *)&addr, &addr_size);
    if (rc == 0) {
        if (port != NULL)
            *port = addr.sin_port;
        /* Request a short host name. Set buf to empty on error. */
        if (getnameinfo((struct sockaddr *)&addr, addr_size, buf, n, NULL, 0, NI_NOFQDN))
            buf[0] = '\0';
    }
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

#ifndef TAILQ_FIRST
#define TAILQ_FIRST(head) ((head)->tqh_first)
#endif

#ifndef TAILQ_NEXT
#define TAILQ_NEXT(v, field) ((v)->field.tqe_next)
#endif

#ifndef TAILQ_LAST
#define TAILQ_LAST(head, headname) (*(((struct headname *)((head)->tqh_last))->tqh_last))
#endif

#ifndef TAILQ_FOREACH
#define TAILQ_FOREACH(v, head, field) for ((v) = TAILQ_FIRST(head); (v); (v) = TAILQ_NEXT(v, field))
#endif

#ifndef TAILQ_FOREACH_SAFE
#define TAILQ_FOREACH_SAFE(v, head, field, tmp)                                                                        \
    for ((v) = TAILQ_FIRST(head); (v) && ((tmp) = TAILQ_NEXT((v), field), 1); (v) = (tmp))
#endif

#ifdef CDB2API_TEST

void dump_cached_connections(void)
{
    struct local_cached_connection *cc;
    printf("lru:\n");
    TAILQ_FOREACH(cc, &local_connection_cache, local_cache_lnk)
    {
        if (local_connection_cache_use_sbuf)
            printf("%p %s %d\n", cc, cc->typestr, cdb2buf_fileno(cc->sb));
        else
            printf("%p %s %d\n", cc, cc->typestr, cc->fd);
    }
    printf("used:\n");
    TAILQ_FOREACH(cc, &free_local_connection_cache, local_cache_lnk)
    {
        if (local_connection_cache_use_sbuf)
            printf("%p %d\n", cc, cdb2buf_fileno(cc->sb));
        else
            printf("%p %d\n", cc, cc->fd);
    }
    printf("hits %d missed %d evicts %d\n", num_cache_hits, num_cache_misses, num_cache_lru_evicts);
}

int get_num_cached_connections_for(const char *dbname)
{
    char typestr[20];
    sprintf(typestr, "/%s/", dbname);
    struct local_cached_connection *cc;
    int nconn = 0;
    TAILQ_FOREACH(cc, &local_connection_cache, local_cache_lnk)
    {
        if (strstr(cc->typestr, typestr))
            nconn++;
    }
    return nconn;
}

int get_cached_connection_index(const char *dbname)
{
    char typestr[20];
    sprintf(typestr, "/%s/", dbname);
    struct local_cached_connection *cc;
    int nconn = 0;
    TAILQ_FOREACH(cc, &local_connection_cache, local_cache_lnk)
    {
        if (strstr(cc->typestr, typestr))
            return nconn;
        nconn++;
    }
    return -1;
}

#endif

static int local_connection_cache_get_fd(const cdb2_hndl_tp *hndl, const char *typestr)
{
    int fd = -1;
    if (max_local_connection_cache_entries) {
        struct local_cached_connection *cc;

        pthread_mutex_lock(&local_connection_cache_lock);
        TAILQ_FOREACH(cc, &local_connection_cache, local_cache_lnk)
        {
            if (strcmp(cc->typestr, typestr) == 0) {
                fd = cc->fd;
                cc->fd = -1;
                cc->typestr[0] = 0;
                TAILQ_REMOVE(&local_connection_cache, cc, local_cache_lnk);
                TAILQ_INSERT_TAIL(&free_local_connection_cache, cc, local_cache_lnk);
                connection_cache_entries--;
#ifdef CDB2API_TEST
                ++num_cache_hits;
#endif
                break;
            }
        }
#ifdef CDB2API_TEST
        if (fd == -1)
            ++num_cache_misses;
#endif
        pthread_mutex_unlock(&local_connection_cache_lock);
    }
    return fd;
}

static COMDB2BUF *local_connection_cache_get_sbuf(const cdb2_hndl_tp *hndl, const char *typestr)
{
    COMDB2BUF *sb = NULL;
    struct local_cached_connection *ptr = NULL, *expired = NULL;
    int nexpired = 0;
    time_t now;
    if (max_local_connection_cache_entries) {
        if (local_connection_cache_check_pid && getpid() != local_connection_cache_owner_pid) {
            LOG_CALL("%s: pid mismatch my pid is %d. skipping inproc cache\n", __func__, (int)getpid());
            return NULL;
        }
        struct local_cached_connection *cc, *tmp;
        now = time(NULL);
        pthread_mutex_lock(&local_connection_cache_lock);
        expired = alloca(sizeof(struct local_cached_connection) * max_local_connection_cache_entries);
        nexpired = 0;
        TAILQ_FOREACH_SAFE(cc, &local_connection_cache, local_cache_lnk, tmp)
        {
            /* Found a connection in local cache, use it. */
            if (strcmp(cc->typestr, typestr) == 0) {
                sb = cc->sb;
                cc->sb = NULL;
                cc->typestr[0] = 0;
                TAILQ_REMOVE(&local_connection_cache, cc, local_cache_lnk);
                TAILQ_INSERT_TAIL(&free_local_connection_cache, cc, local_cache_lnk);
                connection_cache_entries--;
#ifdef CDB2API_TEST
                ++num_cache_hits;
#endif
                break;
            }

            /* Otherwise check if the connection should be spilled to sockpool */
            if (max_local_connection_cache_age > 0 && now - cc->when >= max_local_connection_cache_age) {
                ptr = &expired[nexpired];
                ptr->sb = cc->sb;
                strncpy(ptr->typestr, cc->typestr, TYPESTR_LEN);
                ++nexpired;

                cc->sb = NULL;
                cc->typestr[0] = 0;
                TAILQ_REMOVE(&local_connection_cache, cc, local_cache_lnk);
                TAILQ_INSERT_TAIL(&free_local_connection_cache, cc, local_cache_lnk);
                connection_cache_entries--;
            }
        }
#ifdef CDB2API_TEST
        if (sb == NULL)
            ++num_cache_misses;
#endif
        pthread_mutex_unlock(&local_connection_cache_lock);
    }
    while (nexpired-- > 0) {
        ptr = &expired[nexpired];
        int fd = cdb2buf_fileno(ptr->sb);
        if (cdb2buf_free(ptr->sb) == 0) /* the function closes fd on error */
            cdb2_socket_pool_donate_ext(hndl, ptr->typestr, fd, 10, 0);
    }
    return sb;
}

static COMDB2BUF *local_connection_cache_get(const cdb2_hndl_tp *hndl, const char *typestr)
{
    int fd;
    COMDB2BUF *rv = NULL;
    if (local_connection_cache_use_sbuf) {
        rv = local_connection_cache_get_sbuf(hndl, typestr);
    } else if ((fd = local_connection_cache_get_fd(hndl, typestr)) != -1) {
        rv = cdb2buf_open(fd, 0);
    }
    LOG_CALL("%s(%s): fd=%d\n", __func__, typestr, cdb2buf_fileno(rv));
    return rv;
}

static int local_connection_cache_put_fd(const cdb2_hndl_tp *hndl, const char *typestr, COMDB2BUF *sb)
{
    int donated = 0;
    int fd = sb ? cdb2buf_fileno(sb) : -1;

    if (max_local_connection_cache_entries && fd != -1) {
        struct local_cached_connection *cc;

        /* reset the connection first before we do anything else */
        int rc = send_reset(sb, 1);
        if (rc)
            return 0;
        rc = cdb2buf_flush(sb);
        if (rc == -1)
            return 0;

        /* Stripe out SSL before placing the fd in the cache. */
        rc = sslio_close(sb, 1);
#ifdef CDB2API_TEST
        if (fail_sslio_close_in_local_cache)
            rc = -1;
#endif
        /* The fd can't be shut down cleanly. Don't donate it.
           Also return a fake good rcode to prevent caller from donating it to sockpool */
        if (rc != 0) {
            cdb2buf_free(sb);
            return 1;
        }

        pthread_mutex_lock(&local_connection_cache_lock);
        if (connection_cache_entries >= max_local_connection_cache_entries) {
            /* lru an entry if we've reached the max number we want to cache */
            cc = TAILQ_LAST(&local_connection_cache, local_connection_cache_list);
#ifdef CDB2API_TEST
            ++num_cache_lru_evicts;
#endif
            TAILQ_REMOVE(&local_connection_cache, cc, local_cache_lnk);
            close(cc->fd);
            cc->fd = -1;
            cc->typestr[0] = 0;
        } else {
            /* see if we have any unused entries we can use */
            cc = TAILQ_FIRST(&free_local_connection_cache);
            if (cc == NULL) {
                cc = malloc(sizeof(struct local_cached_connection));
                if (cc)
                    cc->fd = -1;
            } else {
                TAILQ_REMOVE(&free_local_connection_cache, cc, local_cache_lnk);
            }
            connection_cache_entries++;
        }
        if (cc) {
            strcpy(cc->typestr, typestr);
            cc->fd = fd;
            TAILQ_INSERT_HEAD(&local_connection_cache, cc, local_cache_lnk);

#ifdef CDB2API_TEST
            struct local_cached_connection *cc;
            int found = 0;
            TAILQ_FOREACH(cc, &local_connection_cache, local_cache_lnk)
            {
                if (strcmp(cc->typestr, typestr) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found)
                abort();
#endif
            cdb2buf_free(sb);
            donated = 1;
        }
        pthread_mutex_unlock(&local_connection_cache_lock);
    }

    return donated;
}

static int local_connection_cache_put_sbuf(const cdb2_hndl_tp *hndl, const char *typestr, COMDB2BUF *sb)
{
    int donated = 0;
    int fd = sb ? cdb2buf_fileno(sb) : -1;

    if (max_local_connection_cache_entries && fd != -1) {
        if (local_connection_cache_check_pid && getpid() != local_connection_cache_owner_pid) {
            LOG_CALL("%s: pid mismatch my pid is %d. skipping inproc cache\n", __func__, (int)getpid());
            return 0;
        }
        struct local_cached_connection *cc;
        /* struct to hold sbuf to close without holding mutex */
        struct local_cached_connection evict_this = {{0}};

        /* reset the connection first before we do anything else */
        int rc = send_reset(sb, 1);
        if (rc)
            return 0;
        rc = cdb2buf_flush(sb);
        if (rc == -1)
            return 0;

        pthread_mutex_lock(&local_connection_cache_lock);
        LOG_CALL("%s placing fd %d to localcache current size %d max %d\n", __func__, cdb2buf_fileno(sb),
                 connection_cache_entries, max_local_connection_cache_entries);
        if (connection_cache_entries >= max_local_connection_cache_entries) {
            /* lru an entry if we've reached the max number we want to cache */
            cc = TAILQ_LAST(&local_connection_cache, local_connection_cache_list);
#ifdef CDB2API_TEST
            ++num_cache_lru_evicts;
#endif
            TAILQ_REMOVE(&local_connection_cache, cc, local_cache_lnk);
            evict_this = *cc;

            cc->sb = NULL;
            cc->typestr[0] = 0;
        } else {
            /* see if we have any unused entries we can use */
            cc = TAILQ_FIRST(&free_local_connection_cache);
            if (cc == NULL) {
                cc = calloc(1, sizeof(struct local_cached_connection));
            } else {
                TAILQ_REMOVE(&free_local_connection_cache, cc, local_cache_lnk);
            }
            connection_cache_entries++;
        }
        if (cc) {
            strcpy(cc->typestr, typestr);
            cc->sb = sb;
            cc->when = time(NULL);
            TAILQ_INSERT_HEAD(&local_connection_cache, cc, local_cache_lnk);

#ifdef CDB2API_TEST
            struct local_cached_connection *cc;
            int found = 0;
            TAILQ_FOREACH(cc, &local_connection_cache, local_cache_lnk)
            {
                if (strcmp(cc->typestr, typestr) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found)
                abort();
#endif
            donated = 1;
        }
        pthread_mutex_unlock(&local_connection_cache_lock);
        if (evict_this.sb != NULL) {
            fd = cdb2buf_fileno(evict_this.sb);
            if (cdb2buf_free(evict_this.sb) == 0) {
                /* the function closes fd on error */
                cdb2_socket_pool_donate_ext(hndl, evict_this.typestr, fd, 10, 0);
            } else {
                LOG_CALL("not donating %d because cdb2buf_free failed\n", fd);
            }
        }
    }

    return donated;
}

static int local_connection_cache_put(const cdb2_hndl_tp *hndl, const char *typestr, COMDB2BUF *sb)
{
    if (local_connection_cache_use_sbuf)
        return local_connection_cache_put_sbuf(hndl, typestr, sb);
    return local_connection_cache_put_fd(hndl, typestr, sb);
}

#ifndef CDB2API_TEST
static
#endif
    void
    local_connection_cache_clear(int do_close)
{
    struct local_cached_connection *cc;
    while ((cc = TAILQ_FIRST(&local_connection_cache)) != NULL) {
        TAILQ_REMOVE(&local_connection_cache, cc, local_cache_lnk);
        if (do_close) {
            if (local_connection_cache_use_sbuf && cc->sb != NULL)
                cdb2buf_close(cc->sb);
            else if (!local_connection_cache_use_sbuf && cc->fd != -1)
                close(cc->fd);
            free(cc);
        }
    }
    while ((cc = TAILQ_FIRST(&free_local_connection_cache)) != NULL) {
        TAILQ_REMOVE(&free_local_connection_cache, cc, local_cache_lnk);
        if (do_close) {
            free(cc);
        }
    }
    connection_cache_entries = 0;

#ifdef CDB2API_TEST
    num_cache_lru_evicts = 0;
    num_cache_hits = 0;
    num_cache_misses = 0;
#endif
}

// cdb2_socket_pool_get_ll: low-level
static int cdb2_socket_pool_get_ll(cdb2_hndl_tp *hndl, const char *typestr, int dbnum, int *port)
{
    int fd = -1;

#ifdef CDB2API_TEST
    if (fail_sockpool) {
        --fail_sockpool;
        return -1;
    }
#endif

    int enabled = 0;
    int sockpool_fd = -1;
    pthread_mutex_lock(&cdb2_sockpool_mutex);
    if (cdb2_use_env_vars) {
        if (hndl && (hndl->sockpool_enabled != -1) && (sockpool_enabled == 0)) {
            time_t current_time = time(NULL);
            /* Check every 10 seconds. */
            if ((current_time - sockpool_fail_time) > 10) {
                sockpool_enabled = 1;
            }
        }
    } else if (sockpool_enabled == 0) {
        time_t current_time = time(NULL);
        /* Check every 10 seconds. */
        if ((current_time - sockpool_fail_time) > 10) {
            sockpool_enabled = 1;
        }
    }
    enabled = sockpool_enabled;
    if (enabled == 1) {
        sockpool_fd = sockpool_get_from_pool();
    }
    pthread_mutex_unlock(&cdb2_sockpool_mutex);

    if (enabled == 1) {
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
        if (sockpool_fd != -1) {
            struct sockpool_msg_vers0 msg = {0};
            int rc;
            if (strlen(typestr) >= sizeof(msg.typestr)) {
                int do_close;
                pthread_mutex_lock(&cdb2_sockpool_mutex);
                do_close = sockpool_place_fd_in_pool(sockpool_fd);
                pthread_mutex_unlock(&cdb2_sockpool_mutex);
                if (do_close)
                    close(sockpool_fd);
                return -1;
            }
            /* Please may I have a file descriptor */
            bzero(&msg, sizeof(msg));
            msg.request = SOCKPOOL_REQUEST;
            msg.dbnum = dbnum;
            strncpy(msg.typestr, typestr, sizeof(msg.typestr) - 1);

            errno = 0;
            rc = send_fd(hndl, sockpool_fd, &msg, sizeof(msg), -1);
            if (rc != PASSFD_SUCCESS) {
                fprintf(stderr, "%s: send_fd rc %d errno %d %s\n", __func__, rc, errno, strerror(errno));
                pthread_mutex_lock(&cdb2_sockpool_mutex);
                sockpool_remove_fd(sockpool_fd);
                pthread_mutex_unlock(&cdb2_sockpool_mutex);
                close(sockpool_fd);
                sockpool_fd = -1;
                fd = -1;
            } else {
                /* Read reply from server.  It can legitimately not send
                 * us a file descriptor. */
                errno = 0;
                rc = recv_fd(hndl, sockpool_fd, &msg, sizeof(msg), &fd);
                if (rc != PASSFD_SUCCESS) {
                    fprintf(stderr, "%s: recv_fd rc %d errno %d %s\n", __func__, rc, errno, strerror(errno));
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
            }
        }
    }

    if (sockpool_fd != -1) {
        int do_close;
        pthread_mutex_lock(&cdb2_sockpool_mutex);
        do_close = sockpool_place_fd_in_pool(sockpool_fd);
        pthread_mutex_unlock(&cdb2_sockpool_mutex);
        if (do_close)
            close(sockpool_fd);
    }

#ifdef CDB2API_TEST
    if (fd > 0) {
        ++num_sockpool_fd;
    }
#endif
    return fd;
}

#ifdef CDB2API_SERVER
/* Get the file descriptor of a socket matching the given type string from
 * the pool.  Returns -1 if none is available or the file descriptor on
 * success. Don't support local cache since it may not return fd */
int cdb2_socket_pool_get_fd(cdb2_hndl_tp *hndl, const char *typestr, int dbnum, int *port)
{
    int fd = cdb2_socket_pool_get_ll(hndl, typestr, dbnum, port);
    LOG_CALL("%s(%s,%d,%p): fd=%d\n", __func__, typestr, dbnum, port, fd);
    return fd;
}
#endif

static int typestr_type_is_default(const char *typestr)
{
    const int TYPESTR_TIER_IDX = 2;
    const char *type_start = typestr;
    for (int i = 0; i < TYPESTR_TIER_IDX; i++) {
        type_start = strchr(type_start, '/');
        assert(type_start);
        type_start++;
    }
    const char *const type_end = strchr(type_start, '/');
    assert(type_end);
    return strncmp(type_start, "default", type_end - type_start) == 0;
}

/* Get the sbuf of a socket matching the given type string from
 * the pool.  Returns NULL if none is available or the sbuf on
 * success. */
static COMDB2BUF *cdb2_socket_pool_get(cdb2_hndl_tp *hndl, const char *typestr, int dbnum, int *port,
                                       int *was_from_local_cache)
{
    COMDB2BUF *sb = NULL;

#ifdef CDB2API_TEST
    if (fail_sockpool) {
        --fail_sockpool;
        LOG_CALL("%s(%s,%d,%p,%p[%d]): fd=%d\n", __func__, typestr, dbnum, port, was_from_local_cache,
                 (was_from_local_cache ? *was_from_local_cache : 0), cdb2buf_fileno(sb));
        return NULL;
    }
#endif

#ifdef CDB2API_TEST
    if (typestr_type_is_default(typestr)) {
        fprintf(stderr, "%s: ERR: Did not expect default type in typestring\n", __func__);
        abort();
    }
#endif

    if (was_from_local_cache) {
        *was_from_local_cache = 0;
        sb = local_connection_cache_get(hndl, typestr);
        if (sb != NULL) {
            *was_from_local_cache = 1;
            LOG_CALL("%s(%s,%d,%p,%p[%d]): fd=%d\n", __func__, typestr, dbnum, port, was_from_local_cache,
                     (was_from_local_cache ? *was_from_local_cache : 0), cdb2buf_fileno(sb));
            return sb;
        }
    }

    int fd = cdb2_socket_pool_get_ll(hndl, typestr, dbnum, port);
    LOG_CALL("%s(%s,%d,%p,%p[%d]): fd=%d\n", __func__, typestr, dbnum, port, was_from_local_cache,
             (was_from_local_cache ? *was_from_local_cache : 0), fd);
    return ((fd > 0) ? cdb2buf_open(fd, 0) : NULL);
}

void cdb2_socket_pool_donate_ext(const cdb2_hndl_tp *hndl, const char *typestr, int fd, int ttl, int dbnum)
{
    LOG_CALL("%s(%s,%d): fd=%d\n", __func__, typestr, dbnum, fd);
#ifdef CDB2API_TEST
    if (typestr_type_is_default(typestr)) {
        fprintf(stderr, "%s: ERR: Did not expect default type in typestring\n", __func__);
        abort();
    }
#endif

    int enabled = 0;
    int sockpool_fd = -1;

    pthread_mutex_lock(&cdb2_sockpool_mutex);
    if (cdb2_use_env_vars) {
        enabled = (hndl && hndl->sockpool_enabled == -1) ? -1 : sockpool_enabled;
    } else {
        enabled = sockpool_enabled;
    }
    if (enabled == 1)
        sockpool_fd = sockpool_get_from_pool();
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
            rc = send_fd(hndl, sockpool_fd, &msg, sizeof(msg), fd);
            if (rc != PASSFD_SUCCESS) {
                fprintf(stderr, "%s: send_fd rc %d errno %d %s\n", __func__, rc,
                        errno, strerror(errno));
                pthread_mutex_lock(&cdb2_sockpool_mutex);
                sockpool_remove_fd(sockpool_fd);
                pthread_mutex_unlock(&cdb2_sockpool_mutex);
                close(sockpool_fd);
                sockpool_fd = -1;
            }
        } else if (sockpool_fd != -1) {
            if (log_calls) {
                fprintf(stderr, "%s: typestr too long to donate to sockpool, length %ld max %ld\n", __func__,
                        strlen(typestr), sizeof(msg.typestr) - 1);
            }
        }

        if (sockpool_fd != -1) {
            int do_close;
            pthread_mutex_lock(&cdb2_sockpool_mutex);
            do_close = sockpool_place_fd_in_pool(sockpool_fd);
            pthread_mutex_unlock(&cdb2_sockpool_mutex);
            if (do_close)
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
    NEWSQL_STATE_NONE,
    /* Query is making progress. Applicable only when type is HEARTBEAT */
    NEWSQL_STATE_ADVANCING,
    /* RESET from in-process cache. Applicable only when type is RESET */
    NEWSQL_STATE_LOCALCACHE
} newsql_client_state;

typedef enum { NEWSQL_SERVER_STATE_NONE } newsql_server_state;

static int send_reset(COMDB2BUF *sb, int localcache)
{
    int rc = 0;
    int state = localcache ? NEWSQL_STATE_LOCALCACHE : NEWSQL_STATE_NONE;
    struct newsqlheader hdr = {.type = ntohl(CDB2_REQUEST_TYPE__RESET), .state = ntohl(state)};
    rc = cdb2buf_fwrite((char *)&hdr, sizeof(hdr), 1, sb);
    if (rc != 1) {
        return -1;
    }
    return 0;
}

static int try_ssl(cdb2_hndl_tp *hndl, COMDB2BUF *sb)
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
    rc = cdb2buf_fwrite((char *)&hdr, sizeof(hdr), 1, sb);
    if (rc != 1)
        return -1;
#ifdef CDB2API_TEST
    if ((rc = cdb2buf_flush(sb)) < 0 || (rc = cdb2buf_getc(sb)) < 0 || fail_ssl_negotiation_once) {
#else
    if ((rc = cdb2buf_flush(sb)) < 0 || (rc = cdb2buf_getc(sb)) < 0) {
#endif
        /* If SSL is optional (this includes ALLOW and PREFER), change
           my mode to ALLOW so that we can reconnect in plaintext. */
        if (SSL_IS_OPTIONAL(hndl->c_sslmode))
            hndl->c_sslmode = SSL_ALLOW;
#ifdef CDB2API_TEST
        if (rc >= 0) {
            --fail_ssl_negotiation_once;
            return -1;
        }
#endif
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
    LOG_CALL("sslio_connect %s\n", (rc == 1) ? "success" : "error");

    SSL_CTX_free(ctx);
    if (rc != 1) {
        hndl->sslerr = cdb2buf_lasterror(sb, hndl->errstr, sizeof(hndl->errstr));
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
    COMDB2BUF *ss = NULL;
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
    ss = cdb2buf_open(fd, 0);
    if (ss == 0) {
        close(fd);
        return -1;
    }
    const int timeout = hndl->api_call_timeout < hndl->connect_timeout ? hndl->api_call_timeout : hndl->connect_timeout;
    cdb2buf_settimeout(ss, timeout, timeout);
    cdb2buf_printf(ss, "rte %s\n", name);
    cdb2buf_flush(ss);
    res[0] = '\0';
    cdb2buf_gets(res, sizeof(res), ss);
    debugprint("rte '%s' returns res=%s", name, res);
    if (res[0] != '0') { // character '0' is indication of success
        cdb2buf_close(ss);
        return -1;
    }
    cdb2buf_free(ss);
    return fd;
}

#ifdef CDB2API_SERVER
static int newsql_connect_via_fd(cdb2_hndl_tp *hndl)
{
    int rc = 0;
    COMDB2BUF *sb = NULL;
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
        if ((sb = cdb2buf_open(fd, CDB2BUF_NO_CLOSE_FD)) == 0) {
            rc = -1;
            goto after_callback;
        }
        cdb2buf_printf(sb, hndl->is_admin ? "@newsql\n" : "newsql\n");
        cdb2buf_flush(sb);
    }

    cdb2buf_settimeout(sb, hndl->socket_timeout, hndl->socket_timeout);

    if (try_ssl(hndl, sb) != 0) {
        cdb2buf_close(sb);
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
#endif

static int cdb2portmux_get(cdb2_hndl_tp *hndl, const char *type, const char *remote_host, const char *app,
                           const char *service, const char *instance);

/* Tries to connect to specified node using sockpool.
 * If there is none, then makes a new socket connection.
 */
static int newsql_connect(cdb2_hndl_tp *hndl, int idx)
{
#ifdef CDB2API_TEST
    ++num_sql_connects;
#endif
    const char *host = hndl->hosts[idx];
    int port = hndl->ports[idx];
    debugprint("entering, host '%s:%d'\n", host, port);
    int fd = -1;
    int rc = 0;
    int nprinted;
    COMDB2BUF *sb = NULL;
    void *callbackrc;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;
    int use_local_cache;

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

retry_newsql_connect:
    if (!hndl->is_admin && !(hndl->flags & CDB2_MASTER)) {
        if (hndl->num_hosts && hndl->is_rejected && connect_host_on_reject) {
            char host_typestr[TYPESTR_LEN - TYPE_LEN + CDB2HOSTNAME_LEN];
            snprintf(host_typestr, sizeof(host_typestr), "comdb2/%s/%s/newsql/%s", hndl->dbname, host, hndl->policy);
            sb = cdb2_socket_pool_get(hndl, host_typestr, hndl->dbnum, NULL, &use_local_cache);
        } else {
            sb = cdb2_socket_pool_get(hndl, hndl->newsql_typestr, hndl->dbnum, NULL, &use_local_cache);
        }
        fd = cdb2buf_fileno(sb);
        get_host_and_port_from_fd(fd, hndl->cached_host, sizeof(hndl->cached_host), &hndl->cached_port);
    }

    if (fd < 0) {
        if (hndl->num_hosts == 0 && hndl->got_dbinfo == 0) {
            hndl->got_dbinfo = 1;
            cdb2_get_dbhosts(hndl);
            host = hndl->hosts[idx];
        }
        if (port <= 0) {
            if (!cdb2_allow_pmux_route)
                port = cdb2portmux_get(hndl, hndl->type, host, "comdb2", "replication", hndl->dbname);
            else {
                /* cdb2portmux_route() works without the assignment. We do it here to make it clear
                   that we will be connecting directly to portmux on this host. */
                port = CDB2_PORTMUXPORT;
            }
            hndl->ports[idx] = port;
        }

        if (!cdb2_allow_pmux_route)
            fd = cdb2_tcpconnecth_to(hndl, host, port, 0, hndl->connect_timeout);
        else
            fd = cdb2portmux_route(hndl, host, "comdb2", "replication", hndl->dbname);
        LOG_CALL("%s(%s@%s): fd=%d\n", __func__, hndl->dbname, host, fd);
        if (fd < 0) {
            rc = -1;
            goto after_callback;
        }
        sb = cdb2buf_open(fd, 0);
        if (sb == 0) {
            close(fd);
            rc = -1;
            goto after_callback;
        }
        if (hndl->is_admin)
            cdb2buf_printf(sb, "@");
        cdb2buf_printf(sb, "newsql\n");
        cdb2buf_flush(sb);
    } else {
        if (send_reset(sb, 0) != 0) {
            cdb2buf_close(sb);
            if (hndl->is_admin)
                fd = -1;
            sb = NULL;
            goto retry_newsql_connect;
        }
    }

    cdb2buf_settimeout(sb, hndl->socket_timeout, hndl->socket_timeout);

    if (try_ssl(hndl, sb) != 0) {
        cdb2buf_close(sb);
        rc = -1;
        goto after_callback;
    }

    hndl->sb = sb;
    hndl->num_set_commands_sent = 0;
    hndl->sent_client_info = 0;
    hndl->connected_host = idx;
    hndl->hosts_connected[hndl->connected_host] = 1;
    debugprint("connected_host=%s\n", hndl->hosts[hndl->connected_host]);

after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_NEWSQL_CONNECT, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 3, CDB2_HOSTNAME, host, CDB2_PORT, port, CDB2_RETURN_VALUE, rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }
    return rc;
}

static int cdb2_read_record_ignore_data(cdb2_hndl_tp *hndl, uint8_t **buf, int *count, int *type)
{
    COMDB2BUF *sb = hndl->sb;
    struct newsqlheader hdr = {0};
    int rc;
reread:
    rc = cdb2buf_fread((char *)&hdr, 1, sizeof(hdr), sb);
    if (rc != sizeof(hdr))
        return -1;
    hdr.type = ntohl(hdr.type);
    hdr.length = ntohl(hdr.length);
    hdr.state = ntohl(hdr.state);
    if (hdr.length == 0) /* ignore heartbeats */
        goto reread;
    if (hdr.type != RESPONSE_HEADER__SQL_RESPONSE)
        return -1;
    *buf = realloc(*buf, hdr.length);
    rc = cdb2buf_fread((char *)(*buf), 1, hdr.length, sb);
    if (rc != hdr.length)
        return -1;
    if (hdr.state == NEWSQL_SERVER_STATE_NONE) {
        CDB2SQLRESPONSEIGNOREDATA *response = cdb2__sqlresponse__ignoredata__unpack(NULL, hdr.length, *buf);
        if (!response)
            return -1;
        *type = response->response_type;
        cdb2__sqlresponse__ignoredata__free_unpacked(response, NULL);
    } else {
        // Running state
        *type = RESPONSE_TYPE__COLUMN_VALUES;
    }
    if (count)
        (*count)++;
    return 0;
}

static int cdb2_discard_unread_data(cdb2_hndl_tp *hndl)
{
    int rc = 0;
    int type = 0;
    int count = 0;
    if (!cdb2_discard_unread_socket_data)
        return 1;
    cdb2buf_setnowait(hndl->sb, 1); // Will be reset when we get it back from pool
    while ((rc = cdb2_read_record_ignore_data(hndl, &hndl->last_buf, &count, &type)) == 0 &&
           (type != RESPONSE_TYPE__LAST_ROW) && count < cdb2_max_discard_records)
        ;
    ;
    // Reset it in here only instead of relying on other functions
#ifdef CDB2API_TEST
    printf("Discarded socket %d Discarded %d Max %d type %d\n", (type != RESPONSE_TYPE__LAST_ROW), count,
           cdb2_max_discard_records, type);
#endif
    cdb2buf_setnowait(hndl->sb, 0);

    if (cdb2_alarm_unread_socket_data && publish_event_cb && publish_event_cb->publish_event &&
        type != RESPONSE_TYPE__LAST_ROW && count > 0)
        publish_event_cb->publish_event("Unread rows", _ARGV0, _PID, hndl->dbname, hndl->type, hndl->partial_sql,
                                        "Unread records for the task");

    return (type != RESPONSE_TYPE__LAST_ROW);
}

static int newsql_disconnect(cdb2_hndl_tp *hndl, COMDB2BUF *sb, int line)
{
    if (sb == NULL)
        return 0;

    debugprint("disconnecting from %s, line %d\n",
               hndl->hosts[hndl->connected_host], line);
    int fd = cdb2buf_fileno(sb);

#ifdef CDB2API_SERVER
    int fd_condition = ((hndl->flags & CDB2_TYPE_IS_FD) != 0);
#else
    int fd_condition = 0;
#endif
    int timeoutms = 10 * 1000;
    if (hndl->is_admin || hndl->is_rejected ||
        (!hndl->firstresponse && (hndl->sent_client_info || !donate_unused_connections)) || hndl->in_trans ||
        hndl->pid != _PID ||
        (hndl->firstresponse &&
         ((!hndl->lastresponse || (hndl->lastresponse->response_type != RESPONSE_TYPE__LAST_ROW)) &&
          cdb2_discard_unread_data(hndl))) ||
        fd_condition) {
        LOG_CALL("newsql_disconnect: not donating fd %d because is_admin %d, is_rejected %d, firstresponse %p,"
                 "lastresponse->type %d, sent_client_info %d, donate_unused_connections %d, in_trans %d, pid %d\n",
                 fd, hndl->is_admin, hndl->is_rejected, hndl->firstresponse,
                 (hndl->lastresponse ? hndl->lastresponse->response_type : -1), hndl->sent_client_info,
                 donate_unused_connections, hndl->in_trans, hndl->pid);
        cdb2buf_close(sb);
    } else {
        int donated = local_connection_cache_put(hndl, hndl->newsql_typestr, sb);
        if (!donated && (cdb2buf_free(sb) == 0)) {
            cdb2_socket_pool_donate_ext(hndl, hndl->newsql_typestr, fd, timeoutms / 1000, hndl->dbnum);
        }
    }
    hndl->sb = NULL;
    return 0;
}

/* returns port number, or -1 for error*/
static int cdb2portmux_get(cdb2_hndl_tp *hndl, const char *type,
                           const char *remote_host, const char *app,
                           const char *service, const char *instance)
{
    char name[128]; /* app/service/dbname */
    char res[32];
    COMDB2BUF *ss = NULL;
    int rc, fd, port = -1;
    bzero(name, sizeof(name));
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

    int connect_timeout = hndl->connect_timeout;

    if (CDB2_ENFORCE_API_CALL_TIMEOUT) {
#ifdef CDB2API_TEST
        printf("RETRY with timeout %lld\n", get_call_timeout(hndl, connect_timeout));
#endif
        if (is_api_call_timedout(hndl)) {
            snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d Timed out connecting to db\n", __func__, __LINE__);
            port = -1;
            goto after_callback;
        }
        connect_timeout = get_call_timeout(hndl, connect_timeout);
    }

    fd = cdb2_tcpconnecth_to(hndl, remote_host, CDB2_PORTMUXPORT, 0, connect_timeout);
    if (fd < 0) {
        debugprint("cdb2_tcpconnecth_to returns fd=%d'\n", fd);
        if (errno == EINPROGRESS) {
            snprintf(hndl->errstr, sizeof(hndl->errstr),
                     "%s:%d Can't connect to portmux dbname: %s tier: %s host: %s port %d. "
                     "Err(%d): %s. Firewall or connect timeout issue.",
                     __func__, __LINE__, instance, type, remote_host, CDB2_PORTMUXPORT, errno, strerror(errno));
        } else {
            snprintf(hndl->errstr, sizeof(hndl->errstr),
                     "%s:%d Can't connect to portmux dbname: %s tier: %s host: %s port %d. "
                     "Err(%d): %s. Invalid remote machine or portmux down on remote machine.",
                     __func__, __LINE__, instance, type, remote_host, CDB2_PORTMUXPORT, errno, strerror(errno));
        }
        port = -1;
        goto after_callback;
    }
    ss = cdb2buf_open(fd, 0);
    if (ss == 0) {
        snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d out of memory\n",
                 __func__, __LINE__);
        close(fd);
        debugprint("cdb2buf_open returned 0\n");
        port = -1;
        goto after_callback;
    }
    if (CDB2_ENFORCE_API_CALL_TIMEOUT) {
#ifdef CDB2API_TEST
        printf("RETRY with timeout %lld\n", get_call_timeout(hndl, connect_timeout));
#endif
        if (is_api_call_timedout(hndl)) {
            snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d Timed out connecting to db\n", __func__, __LINE__);
            port = -1;
            goto after_callback;
        }
        connect_timeout = get_call_timeout(hndl, connect_timeout);
    }
    cdb2buf_settimeout(ss, connect_timeout, connect_timeout);
    cdb2buf_printf(ss, "get %s\n", name);
    cdb2buf_flush(ss);
    res[0] = 0;
    cdb2buf_gets(res, sizeof(res), ss);
    debugprint("get '%s' returns res='%s'\n", name, res);
    if (res[0] == '\0') {
        cdb2buf_close(ss);
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
    cdb2buf_close(ss);

after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_PMUX, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(
            hndl, e, 3, CDB2_HOSTNAME, remote_host, CDB2_PORT, CDB2_PORTMUXPORT,
            CDB2_RETURN_VALUE, (intptr_t)port);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, port, callbackrc);
    }

    return port;
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
#ifdef CDB2API_SERVER
    if (hndl->flags & CDB2_TYPE_IS_FD) {
        return newsql_connect_via_fd(hndl);
    }
#endif

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

    cdb2buf_write((void *)&hdr, sizeof(hdr), hndl->sb);
    cdb2buf_flush(hndl->sb);
}

static int cdb2_read_record(cdb2_hndl_tp *hndl, uint8_t **buf, int *len, int *type)
{
    /* Got response */
    COMDB2BUF *sb = hndl->sb;
    struct newsqlheader hdr = {0};
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
    if (CDB2_ENFORCE_API_CALL_TIMEOUT) {
        int socket_timeout = get_call_timeout(hndl, hndl->socket_timeout);
#ifdef CDB2API_TEST
        printf("GOT HEARTBEAT || Set timeout to %d\n", socket_timeout);
#endif
        if (is_api_call_timedout(hndl)) {
            snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d Timed out reading response from the db\n", __func__,
                     __LINE__);
            rc = -1;
        }
        cdb2buf_settimeout(sb, socket_timeout, socket_timeout);
    }
    rc = cdb2buf_fread((char *)&hdr, 1, sizeof(hdr), sb);
    debugprint("READ HDR rc=%d, sizeof(hdr)=(%zu):\n", rc, sizeof(hdr));

#ifdef CDB2API_TEST
    if (fail_read > 0) {
        --fail_read;
        rc = -1;
    }
#endif
    if (rc != sizeof(hdr)) {
        debugprint("bad read or numbytes, rc=%d, sizeof(hdr)=(%zu):\n", rc, sizeof(hdr));
        rc = -1;
        /* In TLS 1.3, client authentication happens after handshake (RFC 8446).
           An invalid client (e.g., a revoked cert) may see a successful
           handshake but encounter an error when reading data from the server.
           Catch the error here. */
        if ((hndl->sslerr = cdb2buf_lasterror(sb, NULL, 0)))
            cdb2buf_lasterror(sb, hndl->errstr, sizeof(hndl->errstr));
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

    /* Validate hdr.type */
    switch (hdr.type) {
    case RESPONSE_HEADER__SQL_RESPONSE:
    case RESPONSE_HEADER__SQL_RESPONSE_HEARTBEAT:
    case RESPONSE_HEADER__SQL_RESPONSE_PING:
    case RESPONSE_HEADER__SQL_RESPONSE_SSL:
    case RESPONSE_HEADER__SQL_RESPONSE_TRACE:
    case RESPONSE_HEADER__SQL_EFFECTS:
    case RESPONSE_HEADER__DISTTXN_RESPONSE:
    case RESPONSE_HEADER__SQL_RESPONSE_RAW:
    case RESPONSE_HEADER__DBINFO_RESPONSE:
        rc = 0;
        break;
    default:
        fprintf(stderr, "%s: invalid response type %d\n", __func__, hdr.type);
        rc = -1;
    }

    if (rc == -1) {
        goto after_callback;
    }

    if (hdr.length == 0) {
        if (hdr.type != RESPONSE_HEADER__SQL_RESPONSE_HEARTBEAT) {
            fprintf(stderr, "%s: invalid response type for 0-length %d\n", __func__, hdr.type);
            rc = -1;
            goto after_callback;
        }
        debugprint("hdr length (0) from mach %s - going to retry\n", hndl->hosts[hndl->connected_host]);

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
        if (hndl->retry_clbk) {
            const char *errstring = (*hndl->retry_clbk)(hndl);
            if (errstring) {
                debugprint("retry callback %s:%d returned error %s\n", __func__, __LINE__, errstring);
                rc = -1;
                goto after_callback;
            }
        }
        goto retry;
    }

    if (type)
        *type = hdr.type;

    *buf = realloc(*buf, hdr.length);
    if ((*buf) == NULL) {
        fprintf(stderr, "%s: out of memory len:%d\n", __func__, hdr.length);
        rc = -1;
        goto after_callback;
    }

    rc = cdb2buf_fread((char *)(*buf), 1, hdr.length, sb);
    debugprint("READ MSG rc=%d hdr.length(%d) type(%d)\n", rc, hdr.length, hdr.type);

    *len = hdr.length;
    if (rc != *len) {
        debugprint("bad read or numbytes, rc=%d != *len(%d) type(%d)\n", rc, *len, *type);
        rc = -1;
        goto after_callback;
    }

    if (hdr.type == RESPONSE_HEADER__SQL_RESPONSE_TRACE) {
        CDB2SQLRESPONSE *response =
            cdb2__sqlresponse__unpack(NULL, hdr.length, *buf);
        if (response->response_type == RESPONSE_TYPE__SP_TRACE) {
            fprintf(stdout, "%s\n", response->info_string);
            cdb2__sqlresponse__free_unpacked(response, NULL);
        } else {
            fprintf(stdout, "%s", response->info_string);
            cdb2__sqlresponse__free_unpacked(response, NULL);
            char cmd[250];
            if (fgets(cmd, 250, stdin) == NULL ||
                strncasecmp(cmd, "quit", 4) == 0) {
                /* don't exit program, just quit sp in the middle
                   finish should be used to quit debugger and run sp till end */
                rc = -1;
                goto after_callback;
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

            cdb2buf_write((char *)&hdr, sizeof(hdr), hndl->sb);
            cdb2buf_write((char *)locbuf, loc_len, hndl->sb);

            int rc = cdb2buf_flush(hndl->sb);
            free(locbuf);
            if (rc < 0) {
                rc = -1;
                goto after_callback;
            }
        }
        debugprint("- going to retry\n");
        if (hndl->retry_clbk) {
            const char *errstring = (*hndl->retry_clbk)(hndl);
            if (errstring) {
                debugprint("retry callback %s:%d returned error %s\n", __func__, __LINE__, errstring);
                rc = -1;
                goto after_callback;
            }
        }
        goto retry;
    }

    rc = 0;
after_callback:
    // reset here
    if (CDB2_ENFORCE_API_CALL_TIMEOUT) {
        cdb2buf_settimeout(sb, hndl->socket_timeout, hndl->socket_timeout);
    }
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
    case CDB2__ERROR_CODE__INCOMPLETE:
        return CDB2ERR_INCOMPLETE;
    default:
        return rc;
    }
}

static void clear_responses(cdb2_hndl_tp *hndl)
{
    if (hndl->lastresponse) {
        cdb2__sqlresponse__free_unpacked(hndl->lastresponse, hndl->allocator);
        if (hndl->protobuf_size)
            hndl->protobuf_offset = 0;
        hndl->lastresponse = NULL;
        free((void *)hndl->last_buf);
        hndl->last_buf = NULL;
    }

    if (hndl->firstresponse) {
        cdb2__sqlresponse__free_unpacked(hndl->firstresponse, NULL);
        hndl->firstresponse = NULL;
        free((void *)hndl->first_buf);
        hndl->first_buf = NULL;
    }
}

#ifdef CDB2API_SERVER
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

    cdb2buf_write((char *)&hdr, sizeof(hdr), hndl->sb);
    cdb2buf_write((char *)buf, len, hndl->sb);

    int rc = cdb2buf_flush(hndl->sb);
    free(buf);

    if (rc < 0) {
        sprintf(hndl->errstr, "%s: Error writing to other master", __func__);
        cdb2buf_close(hndl->sb);
        hndl->sb = NULL;
        return -1;
    }

    if (async) {
        return 0;
    }

    rc = cdb2buf_fread((char *)&hdr, 1, sizeof(hdr), hndl->sb);
    if (rc != sizeof(hdr)) {
        cdb2buf_close(hndl->sb);
        hndl->sb = NULL;
        return -1;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.length = ntohl(hdr.length);
    char *p = malloc(hdr.length);
    rc = cdb2buf_fread(p, 1, hdr.length, hndl->sb);

    if (rc != hdr.length) {
        cdb2buf_close(hndl->sb);
        hndl->sb = NULL;
        free(p);
        return -1;
    }
    CDB2DISTTXNRESPONSE *disttxn_response = cdb2__disttxnresponse__unpack(NULL, hdr.length, (const unsigned char *)p);

    if (disttxn_response == NULL) {
        cdb2buf_close(hndl->sb);
        hndl->sb = NULL;
        free(p);
        return -1;
    }

    int response_rcode = disttxn_response->rcode;

    cdb2__disttxnresponse__free_unpacked(disttxn_response, NULL);

    return response_rcode;
}
#endif

enum { CHUNK_NO = 0, CHUNK_IN_PROGRESS = 1, CHUNK_COMPLETE = 2 };

static int cdb2_effects_request(cdb2_hndl_tp *hndl)
{
    if (hndl && hndl->is_set)
        return -1;
    // We should still be able to grab effects if chunk transaction
    if (hndl && !hndl->in_trans && hndl->is_chunk == CHUNK_NO) {
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

    cdb2buf_write((char *)&hdr, sizeof(hdr), hndl->sb);
    cdb2buf_write((char *)buf, len, hndl->sb);

    int rc = cdb2buf_flush(hndl->sb);
    free(buf);
    if (rc < 0)
        return -1;

    int type;

retry_read:
    rc = cdb2_read_record(hndl, &hndl->first_buf, &len, &type);
    if (rc) {
        free((void *)hndl->first_buf);
        hndl->first_buf = NULL;
        cdb2buf_close(hndl->sb);
        hndl->sb = NULL;
        return -1;
    }
    if (type ==
        RESPONSE_HEADER__SQL_RESPONSE) { /* This might be the error that
                                            happened within transaction. */
        hndl->firstresponse =
            cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
        hndl->error_in_trans = cdb2_convert_error_code(hndl->firstresponse->error_code);
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

    if (hndl->first_buf != NULL) {
        hndl->firstresponse = cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
    } else {
        fprintf(stderr, "td 0x%p %s: Can't read response from the db\n",
                (void *)pthread_self(), __func__);
        return -1;
    }
    return 0;
}

static int wait_for_write(COMDB2BUF *sb, cdb2_hndl_tp *hndl, cdb2_hndl_tp *event_hndl)
{
    int fd = cdb2buf_fileno(sb);
    if (fd < 0)
        return -1;
    if (read(fd, NULL, 0) != 0)
        return -1;

    while (1) {
        if (!cdb2buf_rd_pending(sb)) {
            struct pollfd p = {.fd = fd, .events = POLLIN | POLLOUT};
            int rc = poll(&p, 1, hndl->socket_timeout);
            if (rc != 1) {
                fprintf(stderr, "%s: Unexpected poll fd:%d rc:%d errno:%d %s\n", __func__, fd, rc, errno,
                        strerror(errno));
                break;
            }
            if (p.revents & POLLOUT) {
                return cdb2buf_flush(sb);
            }
            if ((p.revents & POLLIN) == 0) {
                fprintf(stderr, "%s: Unexpected poll event\n", __func__);
                break;
            }
        }
        struct newsqlheader hdr = {0};
        int rc = cdb2buf_fread((char *)&hdr, 1, sizeof(hdr), sb);
        if (rc != sizeof(hdr)) {
            fprintf(stderr, "%s: Failed to read heartbeat rc:%d\n", __func__, rc);
            break;
        }
        if (hdr.length != 0) {
            fprintf(stderr, "%s: Unexpected read (wanted heartbeat) len:%d type:%d\n", __func__, ntohl(hdr.length),
                    ntohl(hdr.type));
            break;
        }
        cdb2_event *e = NULL;
        void *callbackrc;
        while ((e = cdb2_next_callback(event_hndl, CDB2_AT_RECEIVE_HEARTBEAT, e)) != NULL) {
            int unused;
            (void)unused;
            callbackrc = cdb2_invoke_callback(event_hndl, e, 1, CDB2_QUERY_STATE, ntohl(hdr.state));
            PROCESS_EVENT_CTRL_AFTER(event_hndl, e, unused, callbackrc);
        }
    }
    return -1;
}

static int requesting_sql_rows(const cdb2_hndl_tp *hndl)
{
#ifdef CDB2API_SERVER
    return hndl->flags & CDB2_SQL_ROWS;
#else
    return 0;
#endif
}

static int cdb2_send_query(cdb2_hndl_tp *hndl, cdb2_hndl_tp *event_hndl, COMDB2BUF *sb, const char *dbname, const char *sql,
                           int n_set_commands, int n_set_commands_sent, char **set_commands, int n_bindvars,
                           CDB2SQLQUERY__Bindvalue **bindvars, int ntypes, const int *types, int is_begin,
                           int skip_nrows, int retries_done, int do_append, int fromline)
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

    LOG_CALL("%s line %d\n", __func__, __LINE__);

    int n_features = 0;
    int features[20]; // Max 20 client features??
    CDB2QUERY query = CDB2__QUERY__INIT;
    CDB2SQLQUERY sqlquery = CDB2__SQLQUERY__INIT;

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
    cdb2_skipws(sql);
    sqlquery.sql_query = (char *)sql;
    sqlquery.little_endian = 0;
#if _LINUX_SOURCE
    sqlquery.little_endian = 1;
#endif

    sqlquery.n_bindvars = n_bindvars;
    sqlquery.bindvars = bindvars;
    sqlquery.n_types = ntypes;
    sqlquery.types = (int *)types; // TODO: Is this ok? bb is able to assign without casting
    sqlquery.tzname = (hndl) ? hndl->env_tz : DB_TZNAME_DEFAULT;
    sqlquery.mach_class = cdb2_default_cluster;

    if (hndl && hndl->id_blob) {
        sqlquery.identity = hndl->id_blob;
    } else if (iam_identity && identity_cb && (hndl && requesting_sql_rows(hndl) == 0)) {
        id_blob = identity_cb->getIdentity(hndl, cdb2_use_optional_identity);
        if (id_blob->data.data) {
            sqlquery.identity = id_blob;
        }
    }

    query.sqlquery = &sqlquery;

    sqlquery.n_set_flags = n_set_commands - n_set_commands_sent;
    if (sqlquery.n_set_flags)
        sqlquery.set_flags = &set_commands[n_set_commands_sent];

    /* hndl is NULL when we query comdb2db in comdb2db_get_dbhosts(). */
    if (hndl && hndl->is_retry) {
        sqlquery.has_retry = 1;
        sqlquery.retry = hndl->is_retry;
    }

    if (hndl && !(hndl->flags & CDB2_READ_INTRANS_RESULTS) && is_begin) {
        features[n_features++] = CDB2_CLIENT_FEATURES__SKIP_INTRANS_RESULTS;
    }

    if (hndl) {
        features[n_features++] = CDB2_CLIENT_FEATURES__SSL;
        features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO;
        if (hndl->request_fp) /* Request server to send back query fingerprint */
            features[n_features++] = CDB2_CLIENT_FEATURES__REQUEST_FP;
        /* Request server to send back row data flat, instead of storing it in
           a nested data structure. This helps reduce server's memory footprint. */
        if (cdb2_flat_col_vals)
            features[n_features++] = CDB2_CLIENT_FEATURES__FLAT_COL_VALS;

        if ((hndl->flags & (CDB2_DIRECT_CPU | CDB2_MASTER)) ||
            (retries_done >= (hndl->num_hosts * 2 - 1) && hndl->master == hndl->connected_host)) {
            features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC;
        }
        if (retries_done > hndl->num_hosts + 1) {
            features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_QUEUING;
        }

        if ((hndl->flags & CDB2_REQUIRE_FASTSQL) != 0) {
            features[n_features++] = CDB2_CLIENT_FEATURES__REQUIRE_FASTSQL;
        }
        if ((hndl->flags & CDB2_ALLOW_INCOHERENT) != 0) {
            features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_INCOHERENT;
        }
        features[n_features++] = CDB2_CLIENT_FEATURES__CAN_REDIRECT_FDB;

        debugprint("sending to %s '%s' from-line %d retries is"
                   " %d do_append is %d\n",
                   hndl->connected_host >= 0 ? hndl->hosts[hndl->connected_host]
                                             : "NOT-CONNECTED",
                   sql, fromline, retries_done, do_append);
    } else if (retries_done) {
        features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO;
        features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_QUEUING;
        features[n_features++] = CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC;
    }

    if (hndl && hndl->cnonce.len > 0) { /* Have a query id associated with each transaction/query */
        sqlquery.has_cnonce = 1;
        sqlquery.cnonce.data = (uint8_t *)hndl->cnonce.str;
        sqlquery.cnonce.len = hndl->cnonce.len;
    }

    CDB2SQLQUERY__Snapshotinfo snapshotinfo;
    if (hndl && hndl->snapshot_file && !hndl->in_trans) { /* This is a retry transaction. */
        cdb2__sqlquery__snapshotinfo__init(&snapshotinfo);
        snapshotinfo.file = hndl->snapshot_file;
        snapshotinfo.offset = hndl->snapshot_offset;
        sqlquery.snapshot_info = &snapshotinfo;
    }

    if (hndl && requesting_sql_rows(hndl)) {
        features[n_features++] = CDB2_CLIENT_FEATURES__SQLITE_ROW_FORMAT;
    }

    if (n_features) {
        sqlquery.n_features = n_features;
        sqlquery.features = features;
    }

    if (skip_nrows) {
        sqlquery.has_skip_rows = 1;
        sqlquery.skip_rows = skip_nrows;
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
    rc = cdb2buf_write((char *)&hdr, sizeof(hdr), sb);
    if (rc != sizeof(hdr))
        debugprint("cdb2buf_write rc = %d\n", rc);

    rc = cdb2buf_write((char *)buf, len, sb);
    if (rc != len)
        debugprint("cdb2buf_write rc = %d (len = %d)\n", rc, len);

    struct timeval start;
    if (check_hb_on_blocked_write)
        gettimeofday(&start, NULL);

    rc = cdb2buf_flush(sb);

    if (check_hb_on_blocked_write && rc < 0) {
        // Failed writing to server. Don't know if cdb2buf_flush failed due to
        // a timeout. Make a best effort guess:
        struct timeval end;
        gettimeofday(&end, NULL);
        int64_t end_us = end.tv_sec * 1000000 + end.tv_usec;
        int64_t start_us = start.tv_sec * 1000000 + start.tv_usec;
        int64_t elapsed_us = end_us - start_us;
        if (!is_begin && !hndl->is_read && hndl->in_trans && hndl->socket_timeout &&
            elapsed_us >= (hndl->socket_timeout * 1000)) {
            rc = wait_for_write(sb, hndl, event_hndl);
        }
    }

    if (rc < 0) {
        debugprint("cdb2buf_flush rc = %d\n", rc);
        if (on_heap)
            free(buf);
        rc = -1;
        goto after_callback;
    }

#ifdef CDB2API_TEST
    if (sqlquery.identity && sqlquery.identity->principal && sqlquery.identity->data.data) {
        sent_valid_identity = 1;
        strncpy(sent_identity_principal, sqlquery.identity->principal, sizeof(sent_identity_principal));
    } else {
        sent_valid_identity = 0;
    }
#endif

    if (trans_append && hndl->snapshot_file > 0) {
        /* Retry number of transaction is different from that of query.*/
        struct cdb2_query *item = malloc(sizeof(struct cdb2_query));
        item->buf = buf;
        item->len = len;
        item->is_read = hndl->is_read;
        item->sql = strdup(sql);
        TAILQ_INSERT_TAIL(&hndl->queries, item, entry);
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
    case CDB2ERR_NOTDURABLE:
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

    if (hndl->firstresponse && hndl->firstresponse->error_code)
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
#ifdef CDB2API_TEST
    if (fail_next) {
        --fail_next;
        rc = -1;
    }
#endif
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
        cdb2__sqlresponse__free_unpacked(hndl->lastresponse, hndl->allocator);
        if (hndl->protobuf_size)
            hndl->protobuf_offset = 0;
    }

    hndl->lastresponse = cdb2__sqlresponse__unpack(hndl->allocator, len, hndl->last_buf);
    debugprint("hndl->lastresponse->response_type=%d\n",
               hndl->lastresponse->response_type);

    if (requesting_sql_rows(hndl) && hndl->lastresponse->response_type != RESPONSE_TYPE__LAST_ROW &&
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

    LOG_CALL("cdb2_next_record(%p) = %d\n", hndl, rc);

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
            hndl->firstresponse = NULL;
            free((void *)hndl->first_buf);
            hndl->first_buf = NULL;
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
        if (rc != 0) {
            LOG_CALL("cdb_get_effects(%p) = %d\n", hndl, rc);
        } else {
            LOG_CALL("cdb2_get_effects(%p) = %d => affected %d, selected %d, updated %d, deleted %d, inserted %d\n",
                     hndl, rc, effects->num_affected, effects->num_selected, effects->num_updated, effects->num_deleted,
                     effects->num_inserted);
        }
    }

    return rc;
}

/*
  Clear ack flag so cdb2_close will not consume event
*/
int cdb2_clear_ack(cdb2_hndl_tp *hndl)
{
    if (hndl && hndl->ack) {
        hndl->ack = 0;
    }
    return 0;
}

static void free_query_list(struct query_list *head)
{
    struct cdb2_query *q, *tmp;
    TAILQ_FOREACH_SAFE(q, head, entry, tmp)
    {
        TAILQ_REMOVE(head, q, entry);
        free(q->sql);
        free(q->buf);
        free(q);
    }
}

/* Free query list and reset the pointer. */
static inline void free_query_list_on_handle(cdb2_hndl_tp *hndl)
{
    free_query_list(&hndl->queries);
}

static void free_events(cdb2_hndl_tp *hndl)
{
    cdb2_event *curre, *preve;
    curre = hndl->events.next;
    while (curre != NULL) {
        preve = curre;
        curre = curre->next;
        free(preve);
    }
}

int cdb2_close(cdb2_hndl_tp *hndl)
{
    cdb2_event *curre;
    void *callbackrc;
    int rc = 0;

    LOG_CALL("cdb2_close(%p)\n", hndl);

    if (!hndl)
        return 0;

    if (hndl->stmt_types) {
        free(hndl->stmt_types);
        hndl->stmt_types = NULL;
    }

    if (hndl->fdb_hndl) {
        cdb2_close(hndl->fdb_hndl);
        hndl->fdb_hndl = NULL;
    }

    if (hndl->ack)
        ack(hndl);

    if (hndl->sb)
        newsql_disconnect(hndl, hndl->sb, __LINE__);

    if (hndl->firstresponse) {
        cdb2__sqlresponse__free_unpacked(hndl->firstresponse, NULL);
        hndl->firstresponse = NULL;
        free((void *)hndl->first_buf);
        hndl->first_buf = NULL;
    }

    if (hndl->lastresponse) {
        cdb2__sqlresponse__free_unpacked(hndl->lastresponse, hndl->allocator);
        if (hndl->protobuf_size)
            hndl->protobuf_offset = 0;
        hndl->lastresponse = NULL;
        free((void *)hndl->last_buf);
        hndl->last_buf = NULL;
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

    free(hndl->sql);

    cdb2_clearbindings(hndl);
    free_query_list_on_handle(hndl);
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
        c->len = out - c->str;
    }
    return rc;
}

struct cdb2_stmt_types {
    int n;
    int types[0];
};

int cdb2_run_statement(cdb2_hndl_tp *hndl, const char *sql)
{
    return cdb2_run_statement_typed(hndl, sql, 0, NULL);
}

static void parse_dbresponse(CDB2DBINFORESPONSE *dbinfo_response, char valid_hosts[][CDB2HOSTNAME_LEN],
                             int *valid_ports, int *master_node, int *num_valid_hosts, int *num_valid_sameroom_hosts,
                             int debug_trace, peer_ssl_mode *s_mode);

static int retry_queries(cdb2_hndl_tp *hndl, int num_retry, int run_last)
{
    debugprint("retry_all %d, intran %d\n", hndl->retry_all, hndl->in_trans);

    if (!hndl->retry_all || !hndl->in_trans)
        return 0;

    int rc = 0;
    if (!hndl->snapshot_file) {
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
        cdb2buf_close(hndl->sb);
        hndl->sb = NULL;
        debugprint("send_query host=%s rc=%d; returning 1\n", host, rc);
        return 1;
    }
    int len = 0;
    int type = 0;
    rc = cdb2_read_record(hndl, &hndl->first_buf, &len, &type);
    if (rc) {
        cdb2buf_close(hndl->sb);
        hndl->sb = NULL;
        free(hndl->first_buf);
        hndl->first_buf = NULL;
        debugprint("cdb2_read_record from %s rc=%d, returning 1\n", host, rc);
        return 1;
    }

    if (type == RESPONSE_HEADER__DBINFO_RESPONSE) {
        if (hndl->flags & CDB2_DIRECT_CPU) {
            debugprint("directcpu will ignore dbinfo\n");
            return 1;
        }
        /* The master sent info about nodes that might be coherent. */
        cdb2buf_close(hndl->sb);
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
        snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d Can't read response from the db\n", __func__, __LINE__);
        cdb2buf_close(hndl->sb);
        hndl->sb = NULL;
        return 1;
    }
    while ((rc = cdb2_next_record_int(hndl, 0)) == CDB2_OK)
        ;

    if (hndl->sb == NULL) {
        debugprint("sb is NULL, next_record returns %d, returning 1\n", rc);
        return 1;
    }

    struct cdb2_query *item;
    TAILQ_FOREACH(item, &hndl->queries, entry)
    {
        /* This is the case when we got disconnected while reading the query.
           In that case retry all the queries and skip their results,
           except the last one. */
        if (!run_last && item == TAILQ_LAST(&hndl->queries, query_list)) {
            break;
        }

        struct newsqlheader hdr = {.type = ntohl(CDB2_REQUEST_TYPE__CDB2QUERY),
                                   .compression = ntohl(0),
                                   .length = ntohl(item->len)};
        debugprint("resending '%s' to %s\n", item->sql, host);

        cdb2buf_write((char *)&hdr, sizeof(hdr), hndl->sb);
        cdb2buf_write((char *)item->buf, item->len, hndl->sb);
        cdb2buf_flush(hndl->sb);

        clear_responses(hndl);

        int len = 0;

        if (!hndl->read_intrans_results && !item->is_read) {
            continue;
        }
        /* This is for select queries, we send just the last row. */
        rc = cdb2_read_record(hndl, &hndl->first_buf, &len, NULL);
        if (rc) {
            snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d Can't read response from the db\n", __func__, __LINE__);
            free(hndl->first_buf);
            hndl->first_buf = NULL;
            cdb2buf_close(hndl->sb);
            hndl->sb = NULL;
            return 1;
        }
        if (hndl->first_buf != NULL) {
            hndl->firstresponse =
                cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
        } else {
            snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d Can't read response from the db\n", __func__, __LINE__);
            cdb2buf_close(hndl->sb);
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
        rc = retry_queries(hndl, num_retry, 0);
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

static void parse_dbresponse(CDB2DBINFORESPONSE *dbinfo_response, char valid_hosts[][CDB2HOSTNAME_LEN],
                             int *valid_ports, int *master_node, int *num_valid_hosts, int *num_valid_sameroom_hosts,
                             int debug_trace, peer_ssl_mode *s_mode)
{
    LOG_CALL("%s line %d\n", __func__, __LINE__);
    int num_hosts = dbinfo_response->n_nodes;
    *num_valid_hosts = 0;
    if (num_valid_sameroom_hosts)
        *num_valid_sameroom_hosts = 0;
    int myroom = 0;
    int i = 0;

    if (debug_trace) {
        /* Print a list of nodes received via dbinforesponse. */
        fprintf(stderr, "dbinforesponse:\n%s (master)\n", dbinfo_response->master->name);
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
            fprintf(stderr, "td %" PRIxPTR "%s:%d, %d) host=%s(%d)%s\n", (intptr_t)pthread_self(), __func__, __LINE__,
                    *num_valid_hosts, valid_hosts[*num_valid_hosts], valid_ports[*num_valid_hosts],
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
        strncpy(valid_hosts[*num_valid_hosts], currnode->name, sizeof(valid_hosts[*num_valid_hosts]) - 1);
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

static void process_set_local(cdb2_hndl_tp *hndl, const char *set_command)
{
    const char *p = &set_command[sizeof("SET") - 1];

    cdb2_skipws(p);

    if (strncasecmp(p, "hasql", 5) == 0) {
        p += sizeof("HASQL");
        cdb2_skipws(p);
        if (strncasecmp(p, "on", 2) == 0)
            hndl->is_hasql = 1;
        else
            hndl->is_hasql = 0;
        return;
    }

    if (strncasecmp(p, "admin", 5) == 0) {
        p += sizeof("ADMIN");
        cdb2_skipws(p);
        if (strncasecmp(p, "on", 2) == 0)
            hndl->is_admin = 1;
        else
            hndl->is_admin = 0;
        return;
    }

    if (strncasecmp(p, "transaction", 11) == 0) {
        p += sizeof("TRANSACTION");
        cdb2_skipws(p);
        // only set is chunk to true, don't set to false here
        if (strncasecmp(p, "chunk", 5) == 0) {
            p += sizeof("CHUNK");
            cdb2_skipws(p);
            if (strncasecmp(p, "throttle", 8) != 0)
                hndl->is_chunk = CHUNK_IN_PROGRESS;
        }
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
    cdb2_skipws(p);

    if (strncasecmp(p, "SSL_MODE", sizeof("SSL_MODE") - 1) == 0) {
        p += sizeof("SSL_MODE");
        cdb2_skipws(p);
        hndl->c_sslmode = ssl_string_to_mode(p, &hndl->nid_dbname);
    } else if (strncasecmp(p, SSL_CERT_PATH_OPT,
                           sizeof(SSL_CERT_PATH_OPT) - 1) == 0) {
        p += sizeof(SSL_CERT_PATH_OPT);
        cdb2_skipws(p);
        free(hndl->sslpath);
        hndl->sslpath = strdup(p);
        if (hndl->sslpath == NULL)
            rc = ENOMEM;
    } else if (strncasecmp(p, SSL_CERT_OPT, sizeof(SSL_CERT_OPT) - 1) == 0) {
        p += sizeof(SSL_CERT_OPT);
        cdb2_skipws(p);
        free(hndl->cert);
        hndl->cert = strdup(p);
        if (hndl->cert == NULL)
            rc = ENOMEM;
    } else if (strncasecmp(p, SSL_KEY_OPT, sizeof(SSL_KEY_OPT) - 1) == 0) {
        p += sizeof(SSL_KEY_OPT);
        cdb2_skipws(p);
        free(hndl->key);
        hndl->key = strdup(p);
        if (hndl->key == NULL)
            rc = ENOMEM;
    } else if (strncasecmp(p, SSL_CA_OPT, sizeof(SSL_CA_OPT) - 1) == 0) {
        p += sizeof(SSL_CA_OPT);
        cdb2_skipws(p);
        free(hndl->ca);
        hndl->ca = strdup(p);
        if (hndl->ca == NULL)
            rc = ENOMEM;
#if HAVE_CRL
    } else if (strncasecmp(p, SSL_CRL_OPT, sizeof(SSL_CRL_OPT) - 1) == 0) {
        p += sizeof(SSL_CRL_OPT);
        cdb2_skipws(p);
        free(hndl->crl);
        hndl->crl = strdup(p);
        if (hndl->crl == NULL)
            rc = ENOMEM;
#endif /* HAVE_CRL */
    } else if (strncasecmp(p, "SSL_SESSION_CACHE",
                           sizeof("SSL_SESSION_CACHE") - 1) == 0) {
        p += sizeof("SSL_SESSION_CACHE");
        cdb2_skipws(p);
        hndl->cache_ssl_sess = (strncasecmp(p, "ON", 2) == 0);
        if (hndl->cache_ssl_sess)
            cdb2_set_ssl_sessions(hndl, cdb2_get_ssl_sessions(hndl));
    } else if (strncasecmp(p, SSL_MIN_TLS_VER_OPT,
                           sizeof(SSL_MIN_TLS_VER_OPT) - 1) == 0) {
        p += sizeof(SSL_MIN_TLS_VER_OPT);
        cdb2_skipws(p);
        hndl->min_tls_ver = atof(p);
    } else {
        rc = -1;
    }

    if (rc == 0) {
        /* clear local connection cache and disable this feature,
         * for it does not support multiple certificates */
        local_connection_cache_clear(1);
        max_local_connection_cache_entries = 0;

        /* Reset ssl error flag. */
        hndl->sslerr = 0;
        /* Refresh connection if SSL config has changed. */
        if (hndl->sb != NULL) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
        }

        /* XXX-GET-DBINFO when client asks for SSL, learn server's SSL capability
           from dbinfo if we haven't done so. We need dbinfo for 1) handling old
           server versions which do not have the ssl bit in dbinfo; 2) figuring
           out the host name for an SSL session in order for the session to be reused
           correctly (i.e., if cluster nodes are configured with different certificates,
           we would want to match a session to its corresponding host, and we get that
           host information from dbinfo). There's a similar logic in set_up_ssl_params(). */
        if (hndl->c_sslmode >= SSL_REQUIRE && (hndl->num_hosts == 0 && hndl->got_dbinfo == 0)) {
            hndl->got_dbinfo = 1;
            rc = cdb2_get_dbhosts(hndl);
        }
    }

    return rc;
}

static inline void cleanup_query_list(cdb2_hndl_tp *hndl, struct query_list *commit_query_list, int line)
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

static const struct {
    const char *name;
    size_t name_sz;
    cdb2_coltype type;
} all_types[] = {{"INTEGER", sizeof("INTEGER") - 1, CDB2_INTEGER},
                 {"CSTRING", sizeof("CSTRING") - 1, CDB2_CSTRING},
                 {"REAL", sizeof("REAL") - 1, CDB2_REAL},
                 {"BLOB", sizeof("BLOB") - 1, CDB2_BLOB},
                 {"DATETIME", sizeof("DATETIME") - 1, CDB2_DATETIME},
                 {"DATETIMEUS", sizeof("DATETIMEUS") - 1, CDB2_DATETIMEUS},
                 {"INTERVALDS", sizeof("INTERVALDS") - 1, CDB2_INTERVALDS},
                 {"INTERVALDSUS", sizeof("INTERVALDSUS") - 1, CDB2_INTERVALDSUS},
                 {"INTERVALYM", sizeof("INTERVALYM") - 1, CDB2_INTERVALYM}};

static const int total_types = sizeof(all_types) / sizeof(all_types[0]);

#define get_toklen(tok)                                                                                                \
    ({                                                                                                                 \
        cdb2_skipws(tok);                                                                                              \
        int len = 0;                                                                                                   \
        while (tok[len] && !isspace(tok[len]))                                                                         \
            ++len;                                                                                                     \
        len;                                                                                                           \
    })

static int process_set_stmt_return_types(cdb2_hndl_tp *hndl, const char *sql)
{
    int toklen;
    const char *tok = sql + 3; /* if we're here, first token is "set" */

    toklen = get_toklen(tok);
    if (toklen != 9 || strncasecmp(tok, "statement", 9) != 0)
        return -1;
    tok += toklen;

    toklen = get_toklen(tok);
    if (toklen != 6 || strncasecmp(tok, "return", 6) != 0)
        return -1;
    tok += toklen;

    toklen = get_toklen(tok);
    if (toklen != 5 || strncasecmp(tok, "types", 5) != 0)
        return -1;
    tok += toklen;

    if (hndl->stmt_types) {
        sprintf(hndl->errstr, "%s: already have %d parameter(s)", __func__, hndl->stmt_types->n);
        return 1;
    }

    enum {max_args = 1024};
    uint8_t types[max_args];
    int count = 0;

    while (1) {
        toklen = get_toklen(tok);
        if (toklen == 0)
            break;
        if (count == max_args) {
            sprintf(hndl->errstr, "%s: max number of columns:%d", __func__, max_args);
            return 1;
        }
        int i;
        for (i = 0; i < total_types; ++i) {
            if (toklen == all_types[i].name_sz && strncasecmp(tok, all_types[i].name, toklen) == 0) {
                tok += toklen;
                types[count++] = all_types[i].type;
                break;
            }
        }
        if (i >= total_types) {
            snprintf(hndl->errstr, sizeof(hndl->errstr), "%s: column:%d has bad type:'%.*s'", __func__, count, toklen,
                     tok);
            return 1;
        }
    }
    if (count == 0) {
        sprintf(hndl->errstr, "%s: bad number of columns:%d", __func__, count);
        return 1;
    }
    hndl->stmt_types = malloc(sizeof(struct cdb2_stmt_types) + sizeof(int) * count);
    hndl->stmt_types->n = count;
    for (int i = 0; i < count; ++i) {
        hndl->stmt_types->types[i] = types[i];
    }
    return 0;
}

static int process_set_command(cdb2_hndl_tp *hndl, const char *sql)
{
    hndl->is_set = 1;
    if (hndl->in_trans) {
        sprintf(hndl->errstr, "Can't run set query inside transaction.");
        hndl->error_in_trans = CDB2ERR_BADREQ;
        hndl->client_side_error = 1;
        return CDB2ERR_BADREQ;
    }

    int rc;
    if ((rc = process_ssl_set_command(hndl, sql)) >= 0)
        return rc;
    if ((rc = process_set_stmt_return_types(hndl, sql)) >= 0)
        return rc;

    int i, j, k;
    i = hndl->num_set_commands;
    if (i > 0) {
        for (j = 0; j < i; j++) {
            /* If this matches any of the previous commands. */
            if ((strcmp(hndl->commands[j], sql) == 0)) {
                if (j == (i - 1)) {
                    /* Do Nothing; But send the set again!. */
                    if (!disable_fix_last_set) {
                        if (hndl->num_set_commands_sent)
                            hndl->num_set_commands_sent--;
                    }
                    return 0;
                }
                char *cmd = hndl->commands[j];
                /* Move all the commands down the array. */
                for (k = j; k < i - 1; k++) {
                    hndl->commands[k] = hndl->commands[k + 1];
                }
                hndl->commands[i - 1] = cmd;
                if (hndl->num_set_commands_sent)
                    hndl->num_set_commands_sent--;
                return 0;
            }
        }
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
    int hardbound = 0;
    while (cdb2_next_record_int(hndl, 0) == CDB2_OK) {
        hardbound++;
        if (hardbound > hndl->max_auto_consume_rows) {
            newsql_disconnect(hndl, hndl->sb, __LINE__);
            break;
        }
    }

    clear_responses(hndl);
    hndl->rows_read = 0;
}

#define GOTO_RETRY_QUERIES()                                                                                           \
    do {                                                                                                               \
        debugprint("goto retry_queries\n");                                                                            \
        goto retry_queries;                                                                                            \
    } while (0);

/*
Make newly opened child handle have same settings (references) as parent
NOTE: ONLY WORKS CURRENTLY FOR PERIOD OF TIME
      WHERE NUM BIND VARS AND SET COMMANDS
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

    free_events(child);
    child->events = parent->events;
    child->gbl_event_version = parent->gbl_event_version;

    /* XXX-GET-DBINFO when client asks for SSL, learn server's SSL capability
    from dbinfo if we haven't done so. We need dbinfo for 1) handling old
    server versions which do not have the ssl bit in dbinfo; 2) figuring
    out the host name for an SSL session in order for the session to be reused
    correctly (i.e., if cluster nodes are configured with different certificates,
    we would want to match a session to its corresponding host, and we get that
    host information from dbinfo). There's a similar logic in set_up_ssl_params(). */
    if (child->c_sslmode >= SSL_REQUIRE && (child->num_hosts == 0 && child->got_dbinfo == 0)) {
        child->got_dbinfo = 1;
        cdb2_get_dbhosts(child);
    }
}

static void pb_alloc_heuristic(cdb2_hndl_tp *hndl)
{
    if (!cdb2_protobuf_heuristic)
        return;
    if (hndl->first_record_read)
        return;
    if (hndl->firstresponse == NULL || hndl->firstresponse->n_value <= 0)
        return;

    /*
     * Protobuf-c stores scanned repeated fields in its memory slabs. Each memory slab is
     * twice the size of the previous slab. There's a 32-byte overhead to scan each repeated field.
     * So we calculate how many slabs protobuf-c will likely need, multiply it by 32,
     * and allocate twice as much to also accommodate real unpacked data.
     */
    int nrepeated = hndl->firstresponse->n_value;
    if (cdb2_flat_col_vals) {
        /* both value and isnull are repeated */
        nrepeated <<= 1;
    }

    int nextpow2 = 1;
    while (nextpow2 < nrepeated)
        nextpow2 <<= 1;
    int newsize = 32 * nextpow2 * 2;
    LOG_CALL("%s: ncolumns %ld, current alloc size %d, new est alloc size %d\n", __func__, hndl->firstresponse->n_value,
             hndl->protobuf_size, newsize);
    if (newsize > hndl->protobuf_size) {
        hndl->protobuf_data = realloc(hndl->protobuf_data, newsize);
        hndl->protobuf_size = newsize;
    }
}

static int cdb2_run_statement_typed_int(cdb2_hndl_tp *hndl, const char *sql, int ntypes, const int *types, int line,
                                        int *set_stmt)
{
    int return_value;
    int rc = 0;
    int is_begin = 0;
    int is_commit = 0;
    int is_hasql_commit = 0;
    int commit_file = 0;
    int commit_offset = 0;
    int commit_is_retry = 0;
    struct query_list commit_query_list;
    TAILQ_INIT(&commit_query_list);
    int is_rollback = 0;
    int retries_done = 0;

    debugprint("running '%s' from line %d\n", sql, line);

    if (hndl->is_invalid) {
        sprintf(hndl->errstr, "Running query on an invalid sql handle\n");
        PRINT_AND_RETURN(CDB2ERR_BADSTATE);
    }

    if (hndl->pid != _PID) {
        pid_t oldpid = hndl->pid;
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        hndl->pid = _PID;

        if (hndl->in_trans) {
            sprintf(hndl->errstr, "Can't continue transaction started in another process (pid %d)", (int)oldpid);
            PRINT_AND_RETURN(CDB2ERR_BADSTATE);
        }
    } else if (hndl->ack) {
        hndl->ack = 0;
        if (hndl->sb) {
            int fd = cdb2buf_fileno(hndl->sb);
            shutdown(fd, SHUT_RDWR);
        }
    } else {
        consume_previous_query(hndl);
    }

    clear_responses(hndl);

    hndl->rows_read = 0;

    if (!sql)
        return 0;

    /* sniff out 'set hasql on' here */
    if (strncasecmp(sql, "set", 3) == 0) {
        *set_stmt = 1;
        return process_set_command(hndl, sql);
    }

    if (hndl->stmt_types) {
        if (ntypes || types) {
            sprintf(hndl->errstr, "%s: provided %d type(s), but already have %d", __func__, ntypes,
                    hndl->stmt_types->n);
            return -1;
        }
        ntypes = hndl->stmt_types->n;
        types = hndl->stmt_types->types;
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

    if (!hndl->in_trans) { /* only one cnonce for a transaction. */
        clear_snapshot_info(hndl, __LINE__);
        if ((rc = next_cnonce(hndl)) != 0)
            PRINT_AND_RETURN(rc);
    }
    hndl->retry_all = 1;
    int run_last = 1;

    set_max_call_time(hndl);

retry_queries:
    debugprint(
        "retry_queries: hndl->host=%d (%s)\n", hndl->connected_host,
        (hndl->connected_host >= 0 ? hndl->hosts[hndl->connected_host] : ""));

    hndl->first_record_read = 0;

    retries_done++;

    if (hndl->retry_clbk) {
        const char *errstring = (*hndl->retry_clbk)(hndl);
        if (errstring) {
            debugprint("retry callback %s:%d returned error %s\n", __func__, __LINE__, errstring);
            PRINT_AND_RETURN(CDB2ERR_STOPPED);
        }
    }

    if (hndl->num_hosts == 0 && hndl->got_dbinfo == 0) {
        if (retries_done == 1) {
            goto after_delay;
        }
        /* discount first attempt without dbinfo */
        --retries_done;
        hndl->got_dbinfo = 1;
        cdb2_get_dbhosts(hndl);
    }

    if (hndl->sslerr != 0)
        PRINT_AND_RETURN(CDB2ERR_CONNECT_ERROR);

    if (hndl->max_retries < hndl->num_hosts * 3) {
        hndl->max_retries = hndl->num_hosts * 3;
    }

    if ((retries_done > 1) && ((retries_done > hndl->max_retries) || (is_api_call_timedout(hndl)))) {
        sprintf(hndl->errstr, "%s: Maximum number of retries done num_retries:%d.", __func__, retries_done);
        if (is_hasql_commit) {
            cleanup_query_list(hndl, &commit_query_list, __LINE__);
        }
        if (is_begin) {
            hndl->in_trans = 0;
        }
        PRINT_AND_RETURN(CDB2ERR_TRAN_IO_ERROR);
    }

    if (!hndl->sb) {
        if (is_rollback) {
            cleanup_query_list(hndl, &commit_query_list, __LINE__);
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
            // Add wait if we have already tried on all the nodes.
            poll(NULL, 0, tmsec);
        }
    }
after_delay:
#ifdef CDB2API_TEST
    if (fail_sb) {
        --fail_sb;
        if (hndl->sb)
            cdb2buf_close(hndl->sb);
        hndl->sb = NULL;
    }
#endif

    if (!hndl->sb) {
        cdb2_connect_sqlhost(hndl);
        if (hndl->sb == NULL) {
            debugprint("rc=%d goto retry_queries on connect failure\n", rc);
            goto retry_queries;
        }
        if (!is_begin) {
            hndl->retry_all = 1;
            rc = retry_queries(hndl, (retries_done - 1), run_last);
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

    snprintf(hndl->partial_sql, sizeof(hndl->partial_sql), "%s", sql);
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
#ifdef CDB2API_TEST
    if (fail_send) {
        --fail_send;
        rc = -1;
    }
#endif
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
            commit_query_list = hndl->queries;
            TAILQ_INIT(&hndl->queries);
            is_hasql_commit = 1;
        }
        hndl->read_intrans_results = 1;
        clear_snapshot_info(hndl, __LINE__);
        hndl->error_in_trans = 0;
        debugprint("setting in_trans to 0\n");

        hndl->in_trans = 0;
        if (hndl->is_chunk == CHUNK_IN_PROGRESS) {
            // set this on commit/rollback so next time we run statement we know to clear it
            hndl->is_chunk = CHUNK_COMPLETE;
        }

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

            /* first_buf is a protobuf payload and firstresponse is the unpacked
                C structure of first_buf. Hence either both first_buf and
                firstresponse are NULL, or neither of them should be NULL.

                Here firstresponse is NULL. So first_buf must be manually
                freed and set to NULL. */
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
                    hndl->queries = commit_query_list;
                    TAILQ_INIT(&commit_query_list);
                    commit_file = 0;
                }
                debugprint("goto retry_queries err_val=%d\n", err_val);
                goto retry_queries;
            } else {
                if (is_commit) {
                    cleanup_query_list(hndl, &commit_query_list, __LINE__);
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
                hndl->queries = commit_query_list;
                TAILQ_INIT(&commit_query_list);
                commit_file = 0;
            }
            hndl->retry_all = 1;
            debugprint("goto retry_queries rc=%d, err_val=%d\n", rc, err_val);
            goto retry_queries;
        }

        if (is_hasql_commit) {
            cleanup_query_list(hndl, &commit_query_list, __LINE__);
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
                    hndl->queries = commit_query_list;
                    TAILQ_INIT(&commit_query_list);
                    commit_file = 0;
                }
                debugprint("goto retry_queries err_val=%d\n", err_val);
                goto retry_queries;
            } else {
                if (is_hasql_commit) {
                    cleanup_query_list(hndl, &commit_query_list, __LINE__);
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
            cleanup_query_list(hndl, &commit_query_list, __LINE__);
        }
        PRINT_AND_RETURN(-1);
    }

    // we have (hndl->first_buf != NULL)
    hndl->firstresponse = cdb2__sqlresponse__unpack(NULL, len, hndl->first_buf);
    if (!hndl->firstresponse) {
        err_val = CDB2ERR_CORRUPT_RESPONSE;
    }
    if (err_val) {
        /* we've read the 1st response of commit/rollback.
           that is all we need so simply return here.
           I dont think we should get here normally */
        debugprint("err_val is %d\n", err_val);
        if (is_rollback) {
            PRINT_AND_RETURN(0);
        } else {
            if (is_hasql_commit) {
                cleanup_query_list(hndl, &commit_query_list, __LINE__);
            }
            PRINT_AND_RETURN(err_val);
        }
    }

    debugprint("Received message %d\n", hndl->firstresponse->response_type);

    if (hndl->firstresponse->error_code == CDB2__ERROR_CODE__WRONG_DB && !hndl->in_trans) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        hndl->retry_all = 1;
        for (int i = 0; i < hndl->num_hosts; i++) {
            hndl->ports[i] = -1;
        }
        GOTO_RETRY_QUERIES();
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
            hndl->queries = commit_query_list;
            TAILQ_INIT(&commit_query_list);
            commit_file = 0;
        }
        debugprint("goto retry_queries error_code=%d\n",
                   hndl->firstresponse->error_code);
        goto retry_queries;
    }

    if (hndl->firstresponse->error_code == CDB2__ERROR_CODE__APPSOCK_LIMIT) {
        hndl->is_rejected = 1;
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        hndl->sb = NULL;
        // retry all shouldn't matter here. Can only happen at beginning of transaction on begin?
        goto retry_queries;
    }

    if (is_begin) {
        debugprint("setting in_trans to 1\n");
        hndl->in_trans = 1;
    } else if (!is_hasql_commit && (is_rollback || is_commit)) {
        cleanup_query_list(hndl, &commit_query_list, __LINE__);
    }

    hndl->node_seq = 0;
    bzero(hndl->hosts_connected, sizeof(hndl->hosts_connected));

    if (is_commit) {
        clear_snapshot_info(hndl, __LINE__);
    }

    if (hndl->firstresponse->foreign_db) {
        int foreign_flags = hndl->firstresponse->foreign_policy_flag;
        foreign_flags |=
            (hndl->flags & ~(CDB2_DIRECT_CPU | CDB2_RANDOM | CDB2_RANDOMROOM | CDB2_ROOM)); // don't look at room flags
        if (cdb2_open(&hndl->fdb_hndl, hndl->firstresponse->foreign_db, hndl->firstresponse->foreign_class, foreign_flags)) {
            cdb2_close(hndl->fdb_hndl);
            hndl->fdb_hndl = NULL;
            if (is_hasql_commit)
                cleanup_query_list(hndl, &commit_query_list, __LINE__);

            sprintf(hndl->errstr, "%s: Can't open fdb %s:%s", __func__, hndl->firstresponse->foreign_db, hndl->firstresponse->foreign_class);
            debugprint("Can't open fdb %s:%s\n", hndl->firstresponse->foreign_db, hndl->firstresponse->foreign_class);
            clear_responses(hndl);  // signal to cdb2_next_record_int that we are done
            PRINT_AND_RETURN(-1);
        }
        attach_to_handle(hndl->fdb_hndl, hndl);

        return_value = cdb2_run_statement_typed(hndl->fdb_hndl, sql, ntypes, types);
        if (is_hasql_commit)
            cleanup_query_list(hndl, &commit_query_list, __LINE__);

        clear_responses(hndl);  // signal to cdb2_next_record_int that we are done
        PRINT_AND_RETURN(return_value);
    }

    if (hndl->firstresponse->response_type == RESPONSE_TYPE__COLUMN_NAMES) {
#ifdef CDB2API_TEST
        if (fail_reject) {
            --fail_reject;
            hndl->firstresponse->error_code = CDB2__ERROR_CODE__REJECTED;
        }
#endif
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
                hndl->queries = commit_query_list;
                TAILQ_INIT(&commit_query_list);
                commit_file = 0;
            }
            debugprint("goto retry_queries error_code=%d\n",
                       hndl->firstresponse->error_code);
            goto retry_queries;
        }

        hndl->is_rejected = 0;
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
                cleanup_query_list(hndl, &commit_query_list, __LINE__);
            PRINT_AND_RETURN(return_value);
        }

        pb_alloc_heuristic(hndl);
        int rc = cdb2_next_record_int(hndl, 1);
        if (rc == CDB2_OK_DONE || rc == CDB2_OK) {
            return_value = cdb2_convert_error_code(hndl->firstresponse->error_code);
            if (is_hasql_commit)
                cleanup_query_list(hndl, &commit_query_list, __LINE__);
            PRINT_AND_RETURN(return_value);
        }

        if (hndl->is_hasql &&
            (((is_retryable(rc) && hndl->snapshot_file) || is_begin) ||
             (!hndl->sb &&
              ((hndl->in_trans && hndl->snapshot_file) || commit_file)))) {

            if (hndl->sb)
                cdb2buf_close(hndl->sb);

            hndl->sb = NULL;

            if (commit_file) {
                debugprint("setting in_trans to 1\n");
                hndl->in_trans = 1;
                hndl->snapshot_file = commit_file;
                hndl->snapshot_offset = commit_offset;
                hndl->is_retry = commit_is_retry;
                hndl->queries = commit_query_list;
                TAILQ_INIT(&commit_query_list);
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
            cleanup_query_list(hndl, &commit_query_list, __LINE__);

        PRINT_AND_RETURN(return_value);
    }

    sprintf(hndl->errstr, "%s: Unknown response type %d", __func__,
            hndl->firstresponse->response_type);
    if (is_hasql_commit)
        cleanup_query_list(hndl, &commit_query_list, __LINE__);
    PRINT_AND_RETURN(-1);
}

int cdb2_run_statement_typed(cdb2_hndl_tp *hndl, const char *sql, int ntypes, const int *types)
{
    int rc = 0;

    int set_stmt = 0;
    int overwrite_rc = 0;
    cdb2_event *e = NULL;

    if (hndl->fdb_hndl) {
        cdb2_close(hndl->fdb_hndl);
        hndl->fdb_hndl = NULL;
    }

    if (hndl->is_chunk == CHUNK_COMPLETE) {
        // this isn't a chunk statement anymore
        hndl->is_chunk = CHUNK_NO;
    }

    hndl->is_set = 0;

    while ((e = cdb2_next_callback(hndl, CDB2_AT_ENTER_RUN_STATEMENT, e)) !=
           NULL) {
        void *callbackrc = cdb2_invoke_callback(hndl, e, 1, CDB2_SQL, sql);
        PROCESS_EVENT_CTRL_BEFORE(hndl, e, rc, callbackrc, overwrite_rc);
    }

    if (overwrite_rc) {
        const char *first = sql;
        int len = get_toklen(first);
        if (len == 3 && strncasecmp(first, "set", 3) == 0) {
            set_stmt = 1;
        }
        goto after_callback;
    }

    if (hndl->temp_trans && hndl->in_trans) {
        cdb2_run_statement_typed_int(hndl, "rollback", 0, NULL, __LINE__, &set_stmt);
    }

    hndl->temp_trans = 0;

    if (hndl->is_hasql && !hndl->in_trans &&
        (strncasecmp(sql, "set", 3) != 0 && strncasecmp(sql, "begin", 5) != 0 &&
         strncasecmp(sql, "commit", 6) != 0 &&
         strncasecmp(sql, "rollback", 8) != 0)) {
        rc = cdb2_run_statement_typed_int(hndl, "begin", 0, NULL, __LINE__, &set_stmt);
        if (rc) {
            debugprint("cdb2_run_statement_typed_int rc = %d\n", rc);
            goto after_callback;
        }
        hndl->temp_trans = 1;
    }

    cdb2_skipws(sql);
    rc = cdb2_run_statement_typed_int(hndl, sql, ntypes, types, __LINE__, &set_stmt);
    if (rc)
        debugprint("rc = %d\n", rc);

    // XXX This code does not work correctly for WITH statements
    // (they can be either read or write)
    if (hndl->temp_trans && !is_sql_read(sql)) {
        if (rc == 0) {
            int commit_rc = cdb2_run_statement_typed_int(hndl, "commit", 0, NULL, __LINE__, &set_stmt);
            debugprint("rc = %d\n", commit_rc);
            rc = commit_rc;
        } else {
            cdb2_run_statement_typed_int(hndl, "rollback", 0, NULL, __LINE__, &set_stmt);
        }
        hndl->temp_trans = 0;
    }

    if (log_calls) {
        if (set_stmt || (ntypes == 0 && hndl->stmt_types == NULL))
            LOG_CALL("cdb2_run_statement(%p, \"%s\") = %d\n", hndl, sql, rc);
        else if (ntypes) {
            LOG_CALL("cdb2_run_statement_typed(%p, \"%s\", [", hndl, sql);
            for (int i = 0; i < ntypes; i++) {
                fprintf(stderr, "%s%s", cdb2_type_str(types[i]),
                        i == ntypes - 1 ? "" : ", ");
            }
            fprintf(stderr, "] = %d\n", rc);
        } else {
            int n = hndl->stmt_types->n;
            int *t = hndl->stmt_types->types;
            LOG_CALL("cdb2_run_statement_typed(%p, \"%s\", [", hndl, sql);
            for (int i = 0; i < n; ++i) {
                fprintf(stderr, "%s%s", cdb2_type_str(t[i]), i == n - 1 ? "" : ", ");
            }
            fprintf(stderr, "] = %d\n", rc);
        }
    }

after_callback:
    while ((e = cdb2_next_callback(hndl, CDB2_AT_EXIT_RUN_STATEMENT, e)) !=
           NULL) {
        void *callbackrc = cdb2_invoke_callback(hndl, e, 2, CDB2_SQL, sql, CDB2_RETURN_VALUE, (intptr_t)rc);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }

    if (hndl->stmt_types && !set_stmt) {
        free(hndl->stmt_types);
        hndl->stmt_types = NULL;
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
    LOG_CALL("cdb2_numcolumns(%p) = %d\n", hndl, rc);
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
    LOG_CALL("cdb2_column_name(%p, %d) = \"%s\"\n", hndl, col, ret == NULL ? "NULL" : ret);
    return ret;
}

#ifdef CDB2API_TEST
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
#endif

#ifdef CDB2API_SERVER
void cdb2_set_debug_trace(cdb2_hndl_tp *hndl)
{
    hndl->debug_trace = 1;
}
#endif

#ifdef CDB2API_TEST
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
#endif

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
    LOG_CALL("cdb2_errstr(%p) = \"%s\"\n", hndl, ret ? ret : "NULL");
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
    LOG_CALL("cdb2_column_type(%p, %d) = %s\n", hndl, col, cdb2_type_str(ret));
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
    if (hndl->lastresponse == NULL)
        return -1;
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
    if (hndl->lastresponse == NULL)
        return NULL;

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

static void cdb2_bind_param_helper(cdb2_hndl_tp *hndl, int type, const void *varaddr, int length)
{
    hndl->n_bindvars++;
    hndl->bindvars = realloc(hndl->bindvars, sizeof(CDB2SQLQUERY__Bindvalue *) *
                                                 hndl->n_bindvars);
    CDB2SQLQUERY__Bindvalue *bindval = malloc(sizeof(CDB2SQLQUERY__Bindvalue));
    cdb2__sqlquery__bindvalue__init(bindval);
    bindval->type = type;
    bindval->value.data = (void *)varaddr;
    if (varaddr == NULL) {
        /* protobuf-c discards `data' if `len' is 0. A NULL value and a 0-length
           value would look the same from the server's perspective. Hence we
           need an addtional isnull flag to tell them apart. */
        bindval->value.len = 0;
        bindval->has_isnull = 1;
        bindval->isnull = 1;
    } else if (type == CDB2_CSTRING && length == 0) {
        /* R6 and old R7 ignore isnull for cstring and treat a 0-length string
           as NULL. So we send 1 dummy byte here to be backward compatible with
           an old backend. */
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
}

int cdb2_bind_param(cdb2_hndl_tp *hndl, const char *varname, int type,
                    const void *varaddr, int length)
{
    cdb2_bind_param_helper(hndl, type, varaddr, length);
    CDB2SQLQUERY__Bindvalue *bindval = hndl->bindvars[hndl->n_bindvars - 1];
    bindval->varname = (char *)varname;
    LOG_CALL("cdb2_bind_param(%p, \"%s\", %s, %p, %d) = 0\n", hndl, varname, cdb2_type_str(type), varaddr, length);
    return 0;
}

int cdb2_bind_index(cdb2_hndl_tp *hndl, int index, int type,
                    const void *varaddr, int length)
{
    LOG_CALL("cdb2_bind_index(%p, %d, %s, %p, %d)\n", hndl, index, cdb2_type_str(type), varaddr, length);
    if (index <= 0) {
        sprintf(hndl->errstr, "%s: bind index starts at value 1", __func__);
        return -1;
    }
    cdb2_bind_param_helper(hndl, type, varaddr, length);
    CDB2SQLQUERY__Bindvalue *bindval = hndl->bindvars[hndl->n_bindvars - 1];
    bindval->varname = NULL;
    bindval->has_index = 1;
    bindval->index = index;
    return 0;
}


static int cdb2_bind_array_helper(cdb2_hndl_tp *hndl, cdb2_coltype type, const void *varaddr, size_t count, size_t typelen, const char *func)
{
    if (count <= 0 || count > CDB2_MAX_BIND_ARRAY) {
        sprintf(hndl->errstr, "%s: bad array length:%zd (max:%d)", func, count, CDB2_MAX_BIND_ARRAY);
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
    bindval->carray = carray;

    hndl->n_bindvars++;
    hndl->bindvars = realloc(hndl->bindvars, sizeof(CDB2SQLQUERY__Bindvalue *) * hndl->n_bindvars);
    hndl->bindvars[hndl->n_bindvars - 1] = bindval;

    return 0;

notsupported:
    free(carray);
    sprintf(hndl->errstr, "%s: bind array type not supported", __func__);
    return -1;
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
    int rc = 0;
    rc = cdb2_bind_array_helper(hndl, type, varaddr, count, typelen, __func__);
    if (rc)
        return rc;

    CDB2SQLQUERY__Bindvalue *bindval = hndl->bindvars[hndl->n_bindvars - 1];
    bindval->varname = (char *)name;
    LOG_CALL("%s(%p, \"%s\", %zu, %s, %p, %zu) = 0\n", __func__, hndl, name, count, cdb2_type_str(type), varaddr,
             typelen);
    return rc;    
}

int cdb2_bind_array_index(cdb2_hndl_tp *hndl, int index, cdb2_coltype type, const void *varaddr, size_t count, size_t typelen)
{
    int rc = 0;
    if (index <= 0) {
        sprintf(hndl->errstr, "%s: bind array index starts at value 1", __func__);
        return -1;
    }
    rc = cdb2_bind_array_helper(hndl, type, varaddr, count, typelen, __func__);
    if (rc)
        return rc;

    CDB2SQLQUERY__Bindvalue *bindval = hndl->bindvars[hndl->n_bindvars - 1];
    bindval->varname = NULL;
    bindval->has_index = 1;
    bindval->index = index;
    LOG_CALL("%s(%p, %d, %zu, %s, %p, %zu) = 0\n", __func__, hndl, index, count, cdb2_type_str(type), varaddr, typelen);
    return rc;
}

int cdb2_clearbindings(cdb2_hndl_tp *hndl)
{
    if (hndl->is_child_hndl) {
        // don't free memory for this, parent handle will free
        hndl->bindvars = NULL;
        hndl->n_bindvars = 0;
        return 0;
    }
    LOG_CALL("cdb2_clearbindings(%p)\n", hndl);
    if (hndl->bindvars == NULL)
        return 0;
    if (hndl->fdb_hndl)
        cdb2_clearbindings(hndl->fdb_hndl);
    int i = 0;
    for (i = 0; i < hndl->n_bindvars; i++) {
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

typedef struct SRVRecord {
    unsigned short priority;
    unsigned short weight;
    unsigned short port;
    char dname[NS_MAXCDNAME];
} SRVRecord;

static int bms_srv_lookup(char hosts[][CDB2HOSTNAME_LEN], const char *dbname, const char *tier, int *num_hosts,
                          int *num_same_room)
{
    int rc;
    char dns_name[256] = {0};

    *num_hosts = 0;

    rc = snprintf(dns_name, sizeof(dns_name), "%s.comdb2.%s.%s", dbname, tier, cdb2_bmssuffix);

    if (rc < 0 || rc >= sizeof(dns_name))
        return -1;

    struct __res_state res;
    memset(&res, 0, sizeof(res));
    if (res_ninit(&res) != 0)
        return -1;

    unsigned char answer[16384];
    int len = res_nsearch(&res, dns_name, ns_c_in, ns_t_srv, answer, sizeof(answer));

    if (len < 0) {
        rc = -1;
        goto done;
    }

    ns_msg handle;
    ns_rr rr;

    rc = -1;

    ns_initparse(answer, len, &handle);

    SRVRecord resolved;

    int min_distance = INT_MAX;
    int min_distance_nodes = 0;
    int near_nodes = 0;
    int far_nodes = 0;

    char db_hosts[MAX_NODES][64];
    int host_distance[MAX_NODES];
    for (int i = 0; i < ns_msg_count(handle, ns_s_an); i++) {
        if (ns_parserr(&handle, ns_s_an, i, &rr) < 0 || ns_rr_type(rr) != ns_t_srv) {
            perror("ns_parserr");
            continue;
        }
        resolved.port = ns_get16(ns_rr_rdata(rr) + 2 * NS_INT16SZ);
        // decompress domain name
        if (dn_expand(ns_msg_base(handle), ns_msg_end(handle), ns_rr_rdata(rr) + 3 * NS_INT16SZ, resolved.dname,
                      sizeof(resolved.dname)) < 0)
            continue;

        strcpy(db_hosts[*num_hosts], resolved.dname);
        if (resolved.port > cdb2_max_room_num)
            resolved.port = 0;
        int distance = host_distance[*num_hosts] = cdb2_room_distance[resolved.port];
        if (distance < min_distance) {
            min_distance = distance;
            min_distance_nodes = 1;
        } else if (distance == min_distance) {
            min_distance_nodes++;
        }
        (*num_hosts)++;
        rc = 0;
    }
    for (int i = 0; i < *num_hosts; i++) {
        if (host_distance[i] == min_distance) {
            strcpy(hosts[near_nodes], db_hosts[i]);
            near_nodes++;
        } else {
            strcpy(hosts[min_distance_nodes + far_nodes], db_hosts[i]);
            far_nodes++;
        }
    }
#ifdef CDB2API_TEST
    for (int i = 0; i < *num_hosts; i++) {
        fprintf(stderr, "FINAL NODE no:%d host:%s near nodes:%d\n", i, hosts[i], near_nodes);
    }
#endif
    if (num_same_room)
        *num_same_room = near_nodes;

done:
#if defined(_SUN_SOURCE)
    res_ndestroy(&res);
#else
    res_nclose(&res);
#endif
    return rc;
}

static int bms_ip_lookup(char hosts[][CDB2HOSTNAME_LEN], const char *dbname, const char *room, const char *tier,
                         int *num_hosts, int *num_same_room, int start_count)
{
    int rc;
    char dns_name[256] = {0};
    if (start_count == 0) {
        if (num_hosts)
            *num_hosts = 0;
        if (num_same_room)
            *num_same_room = 0;
    }
    if (room && room[0]) {
        rc = snprintf(dns_name, sizeof(dns_name), "%s.%s.comdb2.%s.%s", room, dbname, tier, cdb2_bmssuffix);
    } else {
        rc = snprintf(dns_name, sizeof(dns_name), "%s.comdb2.%s.%s", dbname, tier, cdb2_bmssuffix);
    }

    if (rc < 0 || rc >= sizeof(dns_name))
        return -1;

    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM, .ai_protocol = 0, .ai_flags = 0};
    struct addrinfo *result = NULL;
    int gai_rc = getaddrinfo(dns_name, /*service=*/NULL, &hints, &result);
    if (gai_rc != 0) {
        if (!room)
            return -1;
        else
            goto no_roomresult;
    }

    int count = start_count;
    const struct addrinfo *rp;
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        char host[64];
        if (count >= MAX_NODES) {
            fprintf(stderr, "WARNING: %s:getaddrinfo(%s) returned more than %d results\n", __func__, dns_name,
                    MAX_NODES);
            break;
        } else if (rp->ai_family != AF_INET) {
            fprintf(stderr, "WARNING: %s:getaddrinfo(%s) returned non-AF_INET results\n", __func__, dns_name);
        } else if (!inet_ntop(AF_INET, &((const struct sockaddr_in *)rp->ai_addr)->sin_addr, host, sizeof(host))) {
            fprintf(stderr, "%s:inet_ntop(): %d %s\n", __func__, errno, strerror(errno));
            /* Don't make this a fatal error; just don't increment num_hosts */
        } else {
            count++;
            int add_host = 1;
            if (start_count > 0) {
                for (int i = 0; i < start_count; i++) {
                    if (strncmp(host, hosts[i], sizeof(host)) == 0) {
                        add_host = 0;
                        continue;
                    }
                }
            }
            if (add_host) {
                strcpy(hosts[*num_hosts], host);
                (*num_hosts)++;
            }
        }
    }
no_roomresult:
    if (result)
        freeaddrinfo(result);
    if (room) {
        if (num_same_room)
            *num_same_room = *num_hosts;
        return bms_ip_lookup(hosts, dbname, NULL, tier, num_hosts, NULL, *num_hosts);
    }
#ifdef CDB2API_TEST
    for (int i = 0; i < *num_hosts; i++) {
        fprintf(stderr, "FINAL NODE no:%d host:%s near nodes:%d\n", i, hosts[i], start_count);
    }
#endif
    return 0;
}

static int comdb2db_get_dbhosts(cdb2_hndl_tp *hndl, const char *comdb2db_name, int comdb2db_num, const char *host,
                                int port, char hosts[][CDB2HOSTNAME_LEN], int *num_hosts, const char *dbname,
                                char *cluster, int *dbnum, int *num_same_room, int num_retries, int use_bmsd,
                                char shards[][DBNAME_LEN], int *num_shards, int *num_shards_same_room)
{
    int find_shards =
        !!*num_shards; // boolean on whether we should be finding shards of a partition instead of nodes for a single db
    int bmsd_rc = -1;

    if (use_bmsd && cdb2_has_room_distance)
        bmsd_rc = bms_srv_lookup(hosts, dbname, cluster, num_hosts, num_same_room);
    else if (use_bmsd)
        bmsd_rc = bms_ip_lookup(hosts, dbname, cdb2_machine_room, cluster, num_hosts, num_same_room, 0);

    if (bmsd_rc == 0) {
        return 0;
    } else if (use_bmsd) {
        return -1;
    }

#ifdef CDB2API_TEST
    if (cdb2_use_bmsd)
        printf("Going ahead with comdb2db query\n");
#endif
    if (!find_shards)
        *dbnum = 0;
    int n_bindvars = 3;
    const char *sql_query = "select M.name, D.dbnum, M.room from machines M "
                            "join databases D where M.cluster IN (select "
                            "cluster_machs from clusters where name=@dbname "
                            "and cluster_name=@cluster) and D.name=@dbname "
                            "order by (room = @room) desc";
    if (find_shards) {
        // only care about if each shard has a node in the same room for now
        // when we connect to a specific shard, then we will run above query
        sql_query = "select C.name, max(M.room=@room) as has_same_room from clusters C "
                    "join machines M ON C.cluster_machs = M.cluster and C.name in carray(@dbnames) "
                    "and C.cluster_name=@cluster group by C.name order by has_same_room desc";
    }

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

    if (!find_shards) {
        bind_dbname->type = CDB2_CSTRING;
        bind_dbname->varname = "dbname";
        bind_dbname->value.data = (unsigned char *)dbname;
        bind_dbname->value.len = strlen(dbname);
    } else { // bind carray of dbnames
        bind_dbname->type = CDB2_CSTRING;
        bind_dbname->varname = "dbnames";
        CDB2SQLQUERY__Bindvalue__Array *carray = malloc(sizeof(CDB2SQLQUERY__Bindvalue__Array));
        cdb2__sqlquery__bindvalue__array__init(carray);
        CDB2SQLQUERY__Bindvalue__TxtArray *txt = malloc(sizeof(CDB2SQLQUERY__Bindvalue__TxtArray));
        cdb2__sqlquery__bindvalue__txt_array__init(txt);
        // txt->elements = (char **)hndl->shards;
        txt->n_elements = hndl->num_shards;
        txt->elements = malloc(txt->n_elements * sizeof(char *));
        for (int i = 0; i < txt->n_elements; i++) {
            txt->elements[i] = strdup(hndl->shards[i]);
        }

        carray->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_TXT;
        carray->txt = txt;
        bind_dbname->carray = carray;
    }

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
    char newsql_typestr[TYPESTR_LEN];
    int is_sockfd = 1;
    int i = 0;

    if (!find_shards && num_same_room)
        *num_same_room = 0;

    int rc = snprintf(newsql_typestr, sizeof(newsql_typestr),
                      "comdb2/%s/%s/newsql/%s", comdb2db_name, cluster,
                      hndl->policy);
    if (rc < 1 || rc >= sizeof(newsql_typestr)) {
        debugprint(
            "ERROR: can not fit entire string 'comdb2/%s/%s/newsql/%s'\n",
            comdb2db_name, cluster, hndl->policy);
    }

    COMDB2BUF *ss = cdb2_socket_pool_get(hndl, newsql_typestr, comdb2db_num, NULL, NULL);
    int fd = cdb2buf_fileno(ss);
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

        if (fd < 0) {
            if (find_shards) {
                for (i = 0; i < bindvars[0]->carray->txt->n_elements; i++) {
                    free(bindvars[0]->carray->txt->elements[i]);
                }
                free(bindvars[0]->carray->txt->elements);
                free(bindvars[0]->carray->txt);
                free(bindvars[0]->carray);
            }
            for (i = 0; i < 3; i++) {
                free(bindvars[i]);
            }
            free(bindvars);
            snprintf(hndl->errstr, sizeof(hndl->errstr), "%s: Can't connect to host %s port %d", __func__, host, port);
            return -1;
        }
        ss = cdb2buf_open(fd, 0);
        if (ss == 0) {
            close(fd);
            if (find_shards) {
                for (i = 0; i < bindvars[0]->carray->txt->n_elements; i++) {
                    free(bindvars[0]->carray->txt->elements[i]);
                }
                free(bindvars[0]->carray->txt->elements);
                free(bindvars[0]->carray->txt);
                free(bindvars[0]->carray);
            }
            for (i = 0; i < n_bindvars; i++) {
                free(bindvars[i]);
            }
            free(bindvars);
            snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d out of memory\n", __func__, __LINE__);

            return -1;
        }
    }
    cdb2buf_settimeout(ss, hndl->socket_timeout, hndl->socket_timeout);
    if (is_sockfd == 0) {
        if (hndl->is_admin)
            cdb2buf_printf(ss, "@");
        cdb2buf_printf(ss, "newsql\n");
        cdb2buf_flush(ss);
    } else {
        rc = send_reset(ss, 0);
        if (rc != 0) {
            goto free_vars;
        }
    }
    rc = cdb2_send_query(NULL, hndl, ss, comdb2db_name, sql_query, 0, 0, NULL,
                         3, bindvars, 0, NULL, 0, 0, num_retries, 0, __LINE__);
    if (rc)
        debugprint("cdb2_send_query rc = %d\n", rc);

free_vars:
    if (find_shards) {
        for (i = 0; i < bindvars[0]->carray->txt->n_elements; i++) {
            free(bindvars[0]->carray->txt->elements[i]);
        }
        free(bindvars[0]->carray->txt->elements);
        free(bindvars[0]->carray->txt);
        free(bindvars[0]->carray);
    }
    for (i = 0; i < 3; i++) {
        free(bindvars[i]);
    }
    free(bindvars);

    if (rc != 0) {
        sprintf(hndl->errstr, "%s: Can't send query to comdb2db", __func__);
        cdb2buf_close(ss);
        return -1;
    }
    uint8_t *p = NULL;
    int len;
    CDB2SQLRESPONSE *sqlresponse = NULL;
    cdb2_hndl_tp tmp = {.sb = ss};
    rc = cdb2_read_record(&tmp, &p, &len, NULL);
#ifdef CDB2API_TEST
    if (fail_dbhosts_invalid_response) {
        --fail_dbhosts_invalid_response;
        rc = -1;
    }
#endif
    if (rc) {
        debugprint("cdb2_read_record rc = %d\n", rc);
        cdb2buf_close(ss);
        snprintf(hndl->errstr, sizeof(hndl->errstr),
                 "%s:%d  Invalid sql response from db %s \n", __func__,
                 __LINE__, comdb2db_name);
        free_events(&tmp);
#ifdef CDB2API_TEST_DEBUG
        printf("*** %s", hndl->errstr);
#endif
        return -1;
    }
    if ((p != NULL) && (len != 0)) {
        sqlresponse = cdb2__sqlresponse__unpack(NULL, len, p);
    }
#ifdef CDB2API_TEST
    if (fail_dbhosts_bad_response) {
        --fail_dbhosts_bad_response;
        sqlresponse = NULL;
    }
#endif
    if ((len == 0) || (sqlresponse == NULL) || (sqlresponse->error_code != 0) ||
        (sqlresponse->response_type != RESPONSE_TYPE__COLUMN_NAMES &&
         sqlresponse->n_value != 1 && sqlresponse->value[0]->has_type != 1 &&
         sqlresponse->value[0]->type != 3)) {
        snprintf(hndl->errstr, sizeof(hndl->errstr), "%s: Got bad response for %s query. Reply len: %d\n", __func__,
                 comdb2db_name, len);
#ifdef CDB2API_TEST_DEBUG
        printf("*** %s", hndl->errstr);
#endif
        cdb2buf_close(ss);
        free_events(&tmp);
        return -1;
    }

    if (!find_shards)
        *num_hosts = 0;
    int prev_num_shards = 0;
    int prev_num_shards_same_room = 0;
    if (find_shards) { // keep track of previous data in case we get 0 rows back
        prev_num_shards = *num_shards;
        prev_num_shards_same_room = *num_shards_same_room;
        *num_shards = 0;
        *num_shards_same_room = 0;
    }
    while (sqlresponse->response_type <= RESPONSE_TYPE__COLUMN_VALUES) {
        cdb2__sqlresponse__free_unpacked(sqlresponse, NULL);
        rc = cdb2_read_record(&tmp, &p, &len, NULL);
#ifdef CDB2API_TEST
        if (fail_dbhosts_cant_read_response) {
            --fail_dbhosts_cant_read_response;
            rc = -1;
        }
#endif
        if (rc) {
            snprintf(hndl->errstr, sizeof(hndl->errstr),
                     "%s: Can't read dbinfo response from %s \n", __func__,
                     comdb2db_name);
            cdb2buf_close(ss);
#ifdef CDB2API_TEST_DEBUG
            printf("*** %s", hndl->errstr);
#endif
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
            if (find_shards) {
                strcpy(shards[*num_shards], (const char *)sqlresponse->value[0]->value.data);
                // TODO: maybe try not to get NULL values? Using max so that's why getting NULL. Adding types to
                // send_query not working.
                if (sqlresponse->value[1]->value.data && *((long long *)sqlresponse->value[1]->value.data) == 1)
                    (*num_shards_same_room)++;

                (*num_shards)++;
                continue;
            }
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
    if (find_shards && *num_shards == 0 && prev_num_shards > 0) { // revert to prev data if we didn't get any rows back
        *num_shards = prev_num_shards;
        *num_shards_same_room = prev_num_shards_same_room;
    }
    cdb2__sqlresponse__free_unpacked(sqlresponse, NULL);
    free(p);
    int timeoutms = 10 * 1000;
    if (cdb2buf_free(ss) == 0)
        cdb2_socket_pool_donate_ext(hndl, newsql_typestr, fd, timeoutms / 1000, comdb2db_num);
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
    char newsql_typestr[TYPESTR_LEN];
    COMDB2BUF *sb = NULL;
    int fd = -1;
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
    int connection_was_cached;
again:
    connection_was_cached = 0;
    sb = cdb2_socket_pool_get(hndl, newsql_typestr, dbnum, NULL, &connection_was_cached);
    fd = cdb2buf_fileno(sb);
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
            if (fd < 0) {
                snprintf(hndl->errstr, sizeof(hndl->errstr), "%s: Can't route connection to host %s", __func__, host);
                rc = -1;
                goto after_callback;
            }
        }
        if (fd < 0) {
            snprintf(hndl->errstr, sizeof(hndl->errstr),
                     "%s: Can't connect to host %s port %d", __func__, host,
                     port);
            rc = -1;
            goto after_callback;
        }
        sb = cdb2buf_open(fd, 0);
        if (sb == 0) {
            snprintf(hndl->errstr, sizeof(hndl->errstr),
                     "%s:%d out of memory\n", __func__, __LINE__);
            close(fd);
            rc = -1;
            goto after_callback;
        }
        if (hndl->is_admin)
            cdb2buf_printf(sb, "@");
        cdb2buf_printf(sb, "newsql\n");
        cdb2buf_flush(sb);
    }

    cdb2buf_settimeout(sb, hndl->comdb2db_timeout, hndl->comdb2db_timeout);

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

    cdb2buf_write((char *)&hdr, sizeof(hdr), sb);
    cdb2buf_write((char *)buf, len, sb);

    cdb2buf_flush(sb);
    free(buf);

    rc = cdb2buf_fread((char *)&hdr, 1, sizeof(hdr), sb);
#ifdef CDB2API_TEST
    if (fail_dbinfo_invalid_header) {
        --fail_dbinfo_invalid_header;
        rc = -1;
    }
#endif
    if (rc != sizeof(hdr)) {
        LOG_CALL("%s: failed reading hdr fd %d rc %d\n", __func__, cdb2buf_fileno(sb), rc);
        snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d  Invalid header from db %s \n", __func__, __LINE__, dbname);
#ifdef CDB2API_TEST_DEBUG
        printf("*** %s", hndl->errstr);
#endif
        cdb2buf_close(sb);
        rc = -1;
        if (connection_was_cached && retry_dbinfo_on_cached_connection_failure)
            goto again;
        goto after_callback;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.length = ntohl(hdr.length);

    CDB2DBINFORESPONSE *dbinfo_response = NULL;
    char *p = NULL;
    p = malloc(hdr.length);
    if (!p) {
        snprintf(hndl->errstr, sizeof(hndl->errstr), "%s:%d out of memory", __func__, __LINE__);
        cdb2buf_close(sb);
        free(p);
        rc = -1;
        goto after_callback;
    }

    rc = cdb2buf_fread(p, 1, hdr.length, sb);
#ifdef CDB2API_TEST
    if (fail_dbinfo_invalid_response) {
        --fail_dbinfo_invalid_response;
        rc = -1;
    }
#endif
    if (rc != hdr.length) {
        snprintf(hndl->errstr, sizeof(hndl->errstr),
                 "%s:%d  Invalid dbinfo response from db %s \n", __func__,
                 __LINE__, dbname);
#ifdef CDB2API_TEST_DEBUG
        printf("*** %s", hndl->errstr);
#endif
        cdb2buf_close(sb);
        free(p);
        rc = -1;
        goto after_callback;
    }
    dbinfo_response = cdb2__dbinforesponse__unpack(NULL, hdr.length, (const unsigned char *)p);
#ifdef CDB2API_TEST
    if (fail_dbinfo_no_response) {
        --fail_dbinfo_no_response;
        dbinfo_response = NULL;
    }
#endif

    if (dbinfo_response == NULL) {
        sprintf(hndl->errstr, "%s: Got no dbinfo response from comdb2 database",
                __func__);
#ifdef CDB2API_TEST_DEBUG
        printf("*** %s", hndl->errstr);
#endif
        cdb2buf_close(sb);
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

    int donated = local_connection_cache_put(hndl, newsql_typestr, sb);
    if (!donated && (cdb2buf_free(sb) == 0)) {
        cdb2_socket_pool_donate_ext(hndl, newsql_typestr, fd, timeoutms / 1000, dbnum);
    }

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

static inline void only_read_config(cdb2_hndl_tp *hndl, int *default_err)
{
    read_available_comdb2db_configs(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    if (default_err && strcasecmp(hndl->type, "default") == 0) {
        if (cdb2_default_cluster[0] != '\0') {
            strncpy(hndl->type, cdb2_default_cluster, sizeof(hndl->type) - 1);
        } else {
            snprintf(hndl->errstr, sizeof(hndl->errstr), "No default_type entry in comdb2db config.\n");
            *default_err = -1;
        }
    }
    set_cdb2_timeouts(hndl);
}

static int cdb2_get_dbhosts(cdb2_hndl_tp *hndl)
{
#ifdef CDB2API_TEST
    ++num_get_dbhosts;
#endif

    int use_bmsd = 0;
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

    rc = get_comdb2db_hosts(hndl, comdb2db_hosts, comdb2db_ports, &master, comdb2db_name, &num_comdb2db_hosts,
                            &comdb2db_num, hndl->dbname, hndl->hosts, &(hndl->num_hosts), &hndl->dbnum, 0, hndl->shards,
                            &(hndl->num_shards));

    if (strcasecmp(hndl->type, "default") == 0) {
        sprintf(hndl->errstr, "No default_type entry in comdb2db config.\n");
        return -1;
    }

    /* Before database destination discovery */
    cdb2_event *e = NULL;
    void *callbackrc;
    while ((e = cdb2_next_callback(hndl, CDB2_BEFORE_DISCOVERY, e)) != NULL) {
        int unused;
        (void)unused;
        /* return unresolved tier name (eg "default") on CDB2_BEFORE_DISCOVERY */
        callbackrc = cdb2_invoke_callback(hndl, e, 1, CDB2_DBTYPE, (hndl->resolv_def ? "default" : hndl->type));
        PROCESS_EVENT_CTRL_AFTER(hndl, e, unused, callbackrc);
    }

    if (rc != 0)
        goto after_callback;

    if (cdb2_use_env_vars) {
        if ((hndl->db_default_type_override_env || (!cdb2cfg_override && !default_type_override_env))) {
            /* Try dbinfo query without any host info. */
            rc = cdb2_dbinfo_query(hndl, hndl->type, hndl->dbname, hndl->dbnum, NULL, hndl->hosts, hndl->ports,
                                   &hndl->master, &hndl->num_hosts, &hndl->num_hosts_sameroom);
            LOG_CALL("dbinfo_query %d rc %d\n", __LINE__, rc);
            if (rc == 0) {
                goto after_callback;
            } else {
                rc = 0;
            }
        }
    } else if (!cdb2cfg_override) {
        /* Try dbinfo query without any host info. */
        rc = cdb2_dbinfo_query(hndl, hndl->type, hndl->dbname, hndl->dbnum, NULL, hndl->hosts, hndl->ports,
                               &hndl->master, &hndl->num_hosts, &hndl->num_hosts_sameroom);
        LOG_CALL("dbinfo_query %d rc %d\n", __LINE__, rc);
        if (rc == 0) {
            goto after_callback;
        } else {
            rc = 0;
        }
    }

    if ((cdb2_default_cluster[0] != '\0') && (cdb2_comdb2dbname[0] != '\0')) {
        strcpy(comdb2db_name, cdb2_comdb2dbname);
    }

    if (strcasecmp(hndl->type, "local") == 0) {
        hndl->num_hosts = 1;
        strcpy(hndl->hosts[0], "localhost");
        hndl->flags |= CDB2_DIRECT_CPU;

        /* Skip dbinfo to avoid pulling other hosts in the cluster. */
        rc = 0;
        goto after_callback;
    } else {
        rc = get_comdb2db_hosts(hndl, comdb2db_hosts, comdb2db_ports, &master, comdb2db_name, &num_comdb2db_hosts,
                                &comdb2db_num, hndl->dbname, hndl->hosts, &(hndl->num_hosts), &hndl->dbnum, 1,
                                hndl->shards, &(hndl->num_shards));
        if (rc != 0 || (num_comdb2db_hosts == 0 && hndl->num_hosts == 0)) {
            if (hndl->num_shards > 0) { // can't query comdb2db for room info but we found shards. Let's continue
                rc = 0;
                goto after_callback;
            }
            sprintf(hndl->errstr, "cdb2_get_dbhosts: no %s hosts found.",
                    comdb2db_name);
            rc = -1;
            goto after_callback;
        }
    }

    if (!hndl->max_call_time)
        set_max_call_time(hndl);

    use_bmsd = cdb2_use_bmsd && (*cdb2_bmssuffix != '\0') && !hndl->num_shards; // cannot find shards via bmsd yet
retry:
    if (rc) {
        if (num_retry >= MAX_RETRIES || is_api_call_timedout(hndl))
            goto after_callback;

        num_retry++;
        poll(NULL, 0, CDB2_POLL_TIMEOUT); // Sleep for 250ms everytime and total
                                          // of 5 seconds
        rc = 0;
    }
    debugprint("num_retry=%d hndl->num_hosts=%d num_comdb2db_hosts=%d\n",
               num_retry, hndl->num_hosts, num_comdb2db_hosts);

    if (hndl->num_hosts == 0) {
        if (!use_bmsd && master == -1) {
            for (int i = 0; i < num_comdb2db_hosts; i++) {
                rc = cdb2_dbinfo_query(
                    hndl, cdb2_default_cluster, comdb2db_name, comdb2db_num,
                    comdb2db_hosts[i], comdb2db_hosts, comdb2db_ports, &master,
                    &num_comdb2db_hosts, NULL);
                LOG_CALL("dbinfo_query %d rc %d\n", __LINE__, rc);
                if (rc == 0 || is_api_call_timedout(hndl)) {
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
            rc = comdb2db_get_dbhosts(hndl, comdb2db_name, comdb2db_num, comdb2db_hosts[i], comdb2db_ports[i],
                                      hndl->hosts, &hndl->num_hosts, hndl->dbname, hndl->type, &hndl->dbnum,
                                      &hndl->num_hosts_sameroom, num_retry, use_bmsd, hndl->shards, &hndl->num_shards,
                                      &hndl->num_shards_sameroom);
            if (rc == 0 || is_api_call_timedout(hndl)) {
                break;
            } else if (use_bmsd) {
                if (cdb2_comdb2db_fallback)
                    use_bmsd = 0;
                goto retry;
            }
        }
        if (rc == -1 && !is_api_call_timedout(hndl)) {
            rc = comdb2db_get_dbhosts(hndl, comdb2db_name, comdb2db_num, comdb2db_hosts[master], comdb2db_ports[master],
                                      hndl->hosts, &hndl->num_hosts, hndl->dbname, hndl->type, &hndl->dbnum,
                                      &hndl->num_hosts_sameroom, num_retry, use_bmsd, hndl->shards, &hndl->num_shards,
                                      &hndl->num_shards_sameroom);
        }

        if (rc != 0) {
            goto retry;
        }
    }

    if (hndl->num_shards > 0) {
        rc = 0;
        goto after_callback;
    }
    if (hndl->num_hosts == 0) {
        sprintf(hndl->errstr,
                "cdb2_get_dbhosts: comdb2db has no entry of "
                "db %s of cluster type %s.",
                hndl->dbname, hndl->type);
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
            // comment out for now. Extra output fails ssl_dbname and ssl_set_cmd test.
            // #ifdef CDB2API_TEST
            //             if (cdb2_use_bmsd)
            //                 fprintf(stderr, "Try node %d name %s master %d\n", try_node, hndl->hosts[try_node],
            //                         hndl->master);
            // #endif
            rc = cdb2_dbinfo_query(hndl, hndl->type, hndl->dbname, hndl->dbnum,
                                   hndl->hosts[try_node], hndl->hosts,
                                   hndl->ports, &hndl->master, &hndl->num_hosts,
                                   &hndl->num_hosts_sameroom);
            LOG_CALL("dbinfo_query %d rc %d\n", __LINE__, rc);
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
        LOG_CALL("dbinfo_query %d rc %d\n", __LINE__, rc);
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

struct db_info {
    char name[DBNAME_LEN];
    char hosts[MAX_NODES][CDB2HOSTNAME_LEN];
    int ports[MAX_NODES];
    int n_hosts;
    int found;
    int num;
};

static void hndl_set_comdb2buf(cdb2_hndl_tp *hndl, COMDB2BUF *sb)
{
    cdb2buf_settimeout(sb, hndl->socket_timeout, hndl->socket_timeout);
    hndl->sb = sb;
    hndl->num_set_commands_sent = 0;
    hndl->sent_client_info = 0;
}

static int init_connection(cdb2_hndl_tp *hndl, COMDB2BUF *buf)
{
    LOG_CALL("%s: init fd %d\n", __func__, cdb2buf_fileno(buf));
    if (send_reset(buf, 0) != 0) {
        cdb2buf_close(buf);
        return -1;
    }
    set_cdb2_timeouts(hndl);
    hndl_set_comdb2buf(hndl, buf);
#ifdef CDB2API_TEST
    ++num_skip_dbinfo;
#endif
    return 0;
}

static COMDB2BUF *sockpool_get(cdb2_hndl_tp *hndl)
{
    COMDB2BUF *sb = NULL;
    int use_local_cache;
    set_cdb2_timeouts(hndl);
    snprintf(hndl->newsql_typestr, sizeof(hndl->newsql_typestr), "comdb2/%s/%s/newsql/%s", hndl->dbname, hndl->type,
             hndl->policy);
    sb = cdb2_socket_pool_get(hndl, hndl->newsql_typestr, hndl->dbnum, NULL, &use_local_cache);
    get_host_and_port_from_fd(cdb2buf_fileno(sb), hndl->cached_host, sizeof(hndl->cached_host), &hndl->cached_port);
    return sb;
}

static void before_discovery(cdb2_hndl_tp *hndl)
{
    void *callbackrc;
    cdb2_event *e = NULL;
    while ((e = cdb2_next_callback(hndl, CDB2_BEFORE_DISCOVERY, e)) != NULL) {
        int unused;
        (void)unused;
        /* return unresolved tier name (eg "default") on CDB2_BEFORE_DISCOVERY */
        callbackrc = cdb2_invoke_callback(hndl, e, 1, CDB2_DBTYPE, (hndl->resolv_def ? "default" : hndl->type));
        PROCESS_EVENT_CTRL_AFTER(hndl, e, unused, callbackrc);
    }
}

static void after_discovery(cdb2_hndl_tp *hndl)
{
    void *callbackrc;
    cdb2_event *e = NULL;
    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_DISCOVERY, e)) != NULL) {
        int unused;
        (void)unused;
        callbackrc = cdb2_invoke_callback(hndl, e, 0);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, unused, callbackrc);
    }
}

static int get_connection_int(cdb2_hndl_tp *hndl, int *err)
{
    only_read_config(hndl, err);
    if (get_dbinfo || *err)
        return -1;
    before_discovery(hndl);
    COMDB2BUF *sb = sockpool_get(hndl);
    after_discovery(hndl);
    if (sb == NULL)
        return -1;
    return init_connection(hndl, sb);
}

static int get_connection(cdb2_hndl_tp *hndl, int *err)
{
    LOG_CALL("%s: cdb2_use_env_vars %d "
             "get_dbinfo %d "
             "sockpool_enabled %d "
             "hndl->sockpook_enabled %d "
             "hndl->db_default_type_override_env %d "
             "cdb2cfg_override %d "
             "default_type_override_env %d\n",
             __func__, cdb2_use_env_vars, get_dbinfo, sockpool_enabled, hndl->sockpool_enabled,
             hndl->db_default_type_override_env, cdb2cfg_override, default_type_override_env);
    if (hndl->is_admin || (hndl->flags & CDB2_MASTER)) // don't grab from sockpool
        return -1;
    if (cdb2_use_env_vars) {
        // If we read a value for the environment variable `COMDB2_CONFIG_DEFAULT_TYPE_<dbname>`, then we would have
        // overwritten type=default with this override value before this point since it has the highest priority of all
        // of the possible default type overrides (*db-specific override set in env* > db-specific override set in file
        // > general override set in env > general override set in file).
        //
        // If we did not get a value for that environment variable, then we need to delay sockpool communication until
        // we have read all given default type overrides, at which point we can resolve `default` with the highest
        // priority of these values and then reach out to sockpool.
        if (get_dbinfo || sockpool_enabled == -1 || hndl->sockpool_enabled == -1 ||
            (!hndl->db_default_type_override_env && (cdb2cfg_override || default_type_override_env))) {
            return -1;
        }
    } else if (get_dbinfo || sockpool_enabled == -1 || cdb2cfg_override) {
        return -1;
    }
    LOG_CALL("%s: calling get_connection_int\n", __func__);
    int rc = get_connection_int(hndl, err);
    LOG_CALL("%s: get_connection_int returned rc %d\n", __func__, rc);
    return rc;
}

const char *cdb2_host(cdb2_hndl_tp *hndl)
{
    if (hndl && hndl->connected_host >= 0) {
        return hndl->hosts[hndl->connected_host];
    }
    return NULL;
}

static inline int is_machine_list(const char *type)
{
    cdb2_skipws(type);
    return *type == '@';
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
    cdb2_skipws(type);
    char *type_copy = strdup(type);
    char *eomachine = NULL;
    char *eooptions = NULL;
    int rc = 0;
    int port;
    char *dc;
    struct machine m[MAX_NODES];
    int num_hosts = 0;

    assert(type_copy[0] == '@');
    char *s = type_copy + 1; // advance past the '@'

    only_read_config(hndl, NULL); // don't care about default here

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

    /* XXX-GET-DBINFO when client asks for SSL, learn server's SSL capability
       from dbinfo if we haven't done so. We need dbinfo for 1) handling old
       server versions which do not have the ssl bit in dbinfo; 2) figuring
       out the host name for an SSL session in order for the session to be reused
       correctly (i.e., if cluster nodes are configured with different certificates,
       we would want to match a session to its corresponding host, and we get that
       host information from dbinfo). There's a similar logic in process_ssl_set_command(). */
    if (hndl->c_sslmode >= SSL_REQUIRE && (hndl->num_hosts == 0 && hndl->got_dbinfo == 0)) {
        newsql_disconnect(hndl, hndl->sb, __LINE__);
        hndl->got_dbinfo = 1;
        cdb2_get_dbhosts(hndl);
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
    cdb2_cache_ssl_sess = 0;
    cdb2_min_tls_ver = 0;
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

#ifdef CDB2API_SERVER
void cdb2_setIdentityBlob(cdb2_hndl_tp *hndl, void *id)
{
    hndl->id_blob = (CDB2SQLQUERY__IdentityBlob *)id;
}
#endif

static cdb2_ssl_sess *cdb2_get_ssl_sessions(cdb2_hndl_tp *hndl)
{
    cdb2_ssl_sess *pos;
    int rc = pthread_mutex_lock(&cdb2_ssl_sess_lock);
    if (rc != 0)
        return NULL;

    for (pos = cdb2_ssl_sess_cache.next; pos != NULL; pos = pos->next) {
        if (strcasecmp(hndl->dbname, pos->dbname) == 0 && strcasecmp(hndl->type, pos->cluster) == 0) {
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
        strncpy(hndl->sess->cluster, hndl->type, sizeof(hndl->type) - 1);
        hndl->sess->cluster[sizeof(hndl->type) - 1] = '\0';
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
    void *callbackrc;
    cdb2_event *e = NULL;

    pthread_mutex_lock(&cdb2_cfg_lock);
    do_init_once();
    pthread_mutex_unlock(&cdb2_cfg_lock);

    *handle = hndl = calloc(1, sizeof(cdb2_hndl_tp));
    TAILQ_INIT(&hndl->queries);
    strncpy(hndl->dbname, dbname, sizeof(hndl->dbname) - 1);
    strncpy(hndl->type, type, sizeof(hndl->type) - 1);
    hndl->resolv_def = (strcasecmp(type, "default") == 0);
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

    if (cdb2_use_env_vars) {
        hndl->db_default_type_override_env = 0;
        hndl->sockpool_enabled = 0;
        char *DEFAULT_TYPE_PREFIX = "COMDB2_CONFIG_DEFAULT_TYPE";
        char COMDB2_CONFIG_DB_DEFAULT_TYPE[sizeof(char) * (strlen(dbname) + strlen(DEFAULT_TYPE_PREFIX) + 2)];
        snprintf(COMDB2_CONFIG_DB_DEFAULT_TYPE, sizeof(COMDB2_CONFIG_DB_DEFAULT_TYPE), "%s_%s", DEFAULT_TYPE_PREFIX,
                 dbname);
        char *db_default_type = getenv(COMDB2_CONFIG_DB_DEFAULT_TYPE);
        if (db_default_type && hndl->resolv_def) {
            strncpy(hndl->type, db_default_type, sizeof(hndl->type) - 1);
            hndl->db_default_type_override_env = 1;
            /* Per-database envvar tier takes the highest priority.
               If we've resolved the actual tier from the envvar,
               we can stop resolving "default" further */
            hndl->resolv_def = (strcasecmp(hndl->type, "default") == 0);
        }
    }

    hndl->pid = _PID;

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

    /* Install the distributed trace plugin by default. Configs are loaded later,
     * so users still have a way to disable it, if so desired. */
    if (cdb2_install != NULL) {
        pthread_mutex_lock(&cdb2_sockpool_mutex);
        (*cdb2_install)("dt");
        pthread_mutex_unlock(&cdb2_sockpool_mutex);
    }

    int rc = 0;

    while ((e = cdb2_next_callback(hndl, CDB2_AFTER_HNDL_ALLOC, e)) != NULL) {
        callbackrc = cdb2_invoke_callback(hndl, e, 0);
        PROCESS_EVENT_CTRL_AFTER(hndl, e, rc, callbackrc);
    }
    if (rc != 0)
        goto out;

    if ((hndl->flags & CDB2_DIRECT_CPU) || (hndl->flags & CDB2_ADMIN)) {
        /* Get defaults from comdb2db.cfg */
        only_read_config(hndl, &rc);
        if (rc)
            goto out;
        hndl->got_dbinfo = 1;
        hndl->num_hosts = 1;
        set_cdb2_timeouts(hndl);
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
        if (cdb2_use_env_vars) {
            // If db hosts have been provided in the client's
            // environment, disable sockpool so that cdb2api
            // just connects to these hosts (since sockpool
            // would potentially connect to other machines if the same hosts
            // aren't included in the database's global config file).
            char *db_host_info = get_dbhosts_from_env(dbname);
            hndl->sockpool_enabled = db_host_info ? -1 : 0;
        }

        const int should_get_dbinfo = get_connection(hndl, &rc);
        if (rc)
            goto out;
        if (should_get_dbinfo) {
            hndl->got_dbinfo = 1;
            rc = cdb2_get_dbhosts(hndl);
        }
        if (rc)
            debugprint("cdb2_get_dbhosts returns %d\n", rc);
        if (rc == 0 && hndl->num_shards > 0) {
            char db_shard[DBNAME_LEN];
            int shard_num = (hndl->num_shards_sameroom > 0 && (hndl->flags & CDB2_RANDOMROOM))
                                ? rand() % hndl->num_shards_sameroom
                                : rand() % hndl->num_shards;
            strcpy(db_shard, hndl->shards[shard_num]);
            cdb2_close(hndl);
            return cdb2_open(handle, db_shard, type, flags);
        }
    }

    if (rc == 0) {
        rc = set_up_ssl_params(hndl);
        if (rc)
            debugprint("set_up_ssl_params returns %d\n", rc);

        if (hndl->num_hosts == 0 && hndl->got_dbinfo == 0 && /* skip-dbinfo */
            SSL_IS_PREFERRED(hndl->c_sslmode) && SSL_IS_OPTIONAL(hndl->c_sslmode) /* Prefer but not require */) {
            /* XXX-GET-DBINFO Try to establish an SSL connection. If server supports SSL, we're all set.
               If server understands our SSL negotiation protocol but does not have certificates, it'll send back
               an ssl-unable packet. If server doesn't understand the SSL negotiation protocol (eg R6), it'll
               close the connection, and we'll reconnect in plaintext. */
            (void)try_ssl(hndl, hndl->sb);
        }

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

    if (cdb2_protobuf_heuristic)
        hndl->protobuf_size = CDB2_PROTOBUF_HEURISTIC_INIT_SIZE;
    else if (!hndl->protobuf_size)
        hndl->protobuf_size = CDB2_PROTOBUF_SIZE;

    if (hndl->protobuf_size > 0) {
        hndl->protobuf_data = malloc(hndl->protobuf_size);
        hndl->protobuf_offset = 0;
        hndl->s_allocator.alloc = &cdb2_protobuf_alloc;
        hndl->s_allocator.free = &cdb2_protobuf_free;
        hndl->s_allocator.allocator_data = hndl;
        hndl->allocator = &hndl->s_allocator;
    } else {
        hndl->protobuf_data = NULL;
        hndl->allocator = NULL;
    }

    hndl->request_fp = CDB2_REQUEST_FP;

out:
    if (rc != 0 && hndl)
        hndl->is_invalid = 1;
    LOG_CALL("cdb2_open(dbname: \"%s\", type: \"%s\", flags: %x) = %d => %p\n", dbname, type, hndl->flags, rc, *handle);
    if (rc == 0 && hndl->sb != NULL) {
        LOG_CALL("cdb2_open success %p fd %d\n", *handle, cdb2buf_fileno(hndl->sb));
    }
    return rc;
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
    ret = calloc(1, sizeof(cdb2_event) + nargs * sizeof(cdb2_event_arg));
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
        ret->global = 0;
        for (curr = &hndl->events; curr->next != NULL; curr = curr->next)
            ;
        curr->next = ret;
    }
    return ret;
}

int cdb2_unregister_event(cdb2_hndl_tp *hndl, cdb2_event *event)
{
    cdb2_event *curr, *prev;

    /* no-op on null. */
    if (event == NULL)
        return 0;

    if (hndl == NULL) {
        pthread_mutex_lock(&cdb2_event_mutex);
        for (prev = &cdb2_gbl_events, curr = prev->next;
             curr != NULL && curr != event; prev = curr, curr = curr->next)
            ;
        if (curr != event) {
            pthread_mutex_unlock(&cdb2_event_mutex);
            return EINVAL;
        }
        prev->next = curr->next;
        ++cdb2_gbl_event_version;
        pthread_mutex_unlock(&cdb2_event_mutex);
    } else {
        for (prev = &hndl->events, curr = prev->next; curr != NULL && curr != event; prev = curr, curr = curr->next)
            ;
        if (curr != event)
            return EINVAL;
        prev->next = curr->next;
    }
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
        e = hndl->events.next;
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
    const char *dbtype;

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

    if (hndl == NULL || strcasecmp(hndl->type, "default") == 0)
        dbtype = cdb2_default_cluster;
    else
        dbtype = hndl->type;

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
        case CDB2_DBTYPE:
            dbtype = va_arg(ap, char *);
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
            break;
        case CDB2_DBTYPE:
            argv[i] = (void *)dbtype;
            break;
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

    /* Fast return if the version has not changed. */
    if (hndl->gbl_event_version == cdb2_gbl_event_version)
        return 0;

    /* Otherwise we must recopy the global events to the handle. */
    pthread_mutex_lock(&cdb2_event_mutex);

    /* Clear cached global events. */
    gbl = hndl->events.next;
    while (gbl != NULL && gbl->global) {
        tmp = gbl;
        gbl = gbl->next;
        free(tmp);
    }

    /* `knot' is where local events begin. */
    knot = gbl;

    /* Clone and append global events to the handle. */
    for (gbl = cdb2_gbl_events.next, lcl = &hndl->events; gbl != NULL; gbl = gbl->next) {
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
    hndl->gbl_event_version = cdb2_gbl_event_version;

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

int cdb2_get_property(cdb2_hndl_tp *hndl, const char *key, char **value) {
    if (hndl == NULL) {
        return CDB2ERR_NOSTATEMENT;
    } else if (strcmp(key, "sql:tail") != 0) {
        return CDB2ERR_UNKNOWN_PROPERTY;
    } else if (hndl->firstresponse == NULL) {
        return CDB2ERR_NOSTATEMENT;
    } else if (!hndl->firstresponse->has_sql_tail_offset) {
        return CDB2ERR_OLD_SERVER;
    } else {
        *value = malloc(ceil(log10(INT_MAX))+1);
        if (!value) { return ENOMEM; }
        sprintf(*value, "%d", hndl->firstresponse->sql_tail_offset);
        return 0;
    }
}

int cdb2_register_retry_callback(cdb2_hndl_tp *hndl, RETRY_CALLBACK f)
{
    hndl->retry_clbk = f;
    return 0;
}

void cdb2_set_identity(cdb2_hndl_tp *hndl, const void *identity)
{
    if (identity_cb)
        identity_cb->set_identity(hndl, identity);
}

void cdb2_identity_create()
{
    if (identity_cb)
        identity_cb->identity_create();
}

void cdb2_identity_destroy(int is_task_exit)
{
    if (identity_cb)
        identity_cb->identity_destroy(is_task_exit);
}

int cdb2_identity_valid()
{
    if (identity_cb)
        return identity_cb->identity_valid();
    return 0;
}

void cdb2_use_hint(cdb2_hndl_tp *hndl)
{
}
