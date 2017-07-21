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

/* code needed to support various comdb2 interfaces to the sql engine */

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <poll.h>

#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <pthread.h>
#include <sys/types.h>
#include <util.h>
#include <netinet/in.h>
#include <inttypes.h>
#include <fcntl.h>
#include <limits.h>
#include <time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include <epochlib.h>

#include <plhash.h>
#include <segstr.h>
#include <lockmacro.h>

#include <list.h>

#include <sbuf2.h>
#include <bdb_api.h>

#include "pb_alloc.h"
#include "comdb2.h"
#include "types.h"
#include "tag.h"
#include "thdpool.h"
#include "ssl_bend.h"

#include <dynschematypes.h>
#include <dynschemaload.h>
#include <cdb2api.h>

#include <sys/time.h>
#include <plbitlib.h>
#include <strbuf.h>

#include <sqlite3.h>
#include <sqliteInt.h>
#include <vdbeInt.h>

#include "sql.h"
#include "sqlinterfaces.h"

#include "locks.h"
#include "sqloffload.h"
#include "osqlcomm.h"
#include "osqlcheckboard.h"
#include "osqlsqlthr.h"
#include "osqlshadtbl.h"

#include <sqlresponse.pb-c.h>

#include <alloca.h>
#include <fsnap.h>

#include "flibc.h"

#include "sp.h"
#include "lrucache.h"

#include <ctrace.h>
#include <bb_oscompat.h>
#include <netdb.h>

#include "fdb_bend_sql.h"
#include "fdb_access.h"
#include "sqllog.h"
#include <stdbool.h>
#include <quantize.h>
#include <intern_strings.h>

#include "debug_switches.h"

#include "views.h"
#include "mem.h"
#include "comdb2_atomic.h"
#include "logmsg.h"

/* delete this after comdb2_api.h changes makes it through */
#define SQLHERR_MASTER_QUEUE_FULL -108
#define SQLHERR_MASTER_TIMEOUT -109
#define SQLHERR_APPSOCK_LIMIT -110
#define SQLHERR_WRONG_DB -111

extern unsigned long long gbl_sql_deadlock_failures;
extern unsigned int gbl_new_row_data;
extern int gbl_use_appsock_as_sqlthread;
extern int g_osql_max_trans;
extern int gbl_fdb_track;
extern int gbl_return_long_column_names;

/* Once and for all:

   struct sqlthdstate:
      This is created per thread executing SQL.  Has per-thread resources
      like an SQLite handle, logger, etc.

   struct sqlclntstate:
      Per connection.  If a connection is handed off to another handle on the
      client side (via sockpool), client request a reset of this structure.

   struct sql_thread:
      Linked from sqlthdstate.  Has per query stats like accumulated cost, etc
      as well as the connection lock (which is really a per-session resource
      that should be in sqlclntstate).  Also has to Btree* which probably
      also belongs in sqlthdstate, or sqlclntstate, or lord only knows where
   else.

   struct Btree:
      This is per instance of sqlite, which may be shared when idle among
   multiple
      connections.
*/

/* An alternate interface. */
extern pthread_mutex_t appsock_mutex;
extern pthread_attr_t appsock_attr;
extern int gbl_dtastripe;
extern int gbl_notimeouts;
extern int gbl_dump_sql_dispatched; /* dump all sql strings dispatched */
int gbl_dump_fsql_response = 0;
extern int gbl_time_osql; /* dump timestamps for osql steps */
extern int gbl_time_fdb;  /* dump timestamps for remote sql */
extern int gbl_print_syntax_err;
extern int gbl_max_sqlcache;
extern int gbl_track_sqlengine_states;
extern int gbl_disable_sql_dlmalloc;

extern int active_appsock_conns;
int gbl_check_access_controls;

/* Count this- we'd eventually like to remove extended tm support. */
extern int gbl_extended_tm_from_sql;

struct thdpool *gbl_sqlengine_thdpool = NULL;

static void sql_reset_sqlthread(sqlite3 *db, struct sql_thread *thd);
int blockproc2sql_error(int rc, const char *func, int line);
static int test_no_btcursors(struct sqlthdstate *thd);
static void sql_thread_describe(void *obj, FILE *out);
static int watcher_warning_function(void *arg, int timeout, int gap);
static char *get_query_cost_as_string(struct sql_thread *thd,
                                      struct sqlclntstate *clnt);

static void handle_sql_intrans_unrecoverable_error(struct sqlclntstate *clnt);

void comdb2_set_sqlite_vdbe_tzname(Vdbe *p);
void comdb2_set_sqlite_vdbe_dtprec(Vdbe *p);
static int execute_sql_query_offload(struct sqlclntstate *clnt,
                                     struct sqlthdstate *poolthd);
static int _push_row_new(struct sqlclntstate *clnt, int type,
                         CDB2SQLRESPONSE *sql_response,
                         CDB2SQLRESPONSE__Column **columns, int ncols,
                         void *(*alloc)(size_t size), int flush);
struct sql_state;
static int send_ret_column_info(struct sqlthdstate *thd,
                                struct sqlclntstate *clnt,
                                struct sql_state *rec, int ncols,
                                CDB2SQLRESPONSE__Column **columns);
static int send_row(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                    int new_row_data_type, int ncols, int row_id, int rc,
                    CDB2SQLRESPONSE__Column **columns);
void send_prepare_error(struct sqlclntstate *clnt, const char *errstr,
                        int clnt_retry);
static int send_err_but_msg(struct sqlclntstate *clnt, const char *errstr,
                            int irc);
static int flush_row(struct sqlclntstate *clnt);
static int send_dummy(struct sqlclntstate *clnt);
static void send_last_row(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                          const char *func, int line);

uint8_t *fsqlreq_put(const struct fsqlreq *p_fsqlreq, uint8_t *p_buf,
                     const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSQLREQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_fsqlreq->request), sizeof(p_fsqlreq->request), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_fsqlreq->flags), sizeof(p_fsqlreq->flags), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&(p_fsqlreq->parm), sizeof(p_fsqlreq->parm), p_buf, p_buf_end);
    p_buf = buf_put(&(p_fsqlreq->followlen), sizeof(p_fsqlreq->followlen),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *fsqlreq_get(struct fsqlreq *p_fsqlreq, const uint8_t *p_buf,
                           const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSQLREQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_fsqlreq->request), sizeof(p_fsqlreq->request), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_fsqlreq->flags), sizeof(p_fsqlreq->flags), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&(p_fsqlreq->parm), sizeof(p_fsqlreq->parm), p_buf, p_buf_end);
    p_buf = buf_get(&(p_fsqlreq->followlen), sizeof(p_fsqlreq->followlen),
                    p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *conninfo_put(const struct conninfo *p_conninfo, uint8_t *p_buf,
                             const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CONNINFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_conninfo->pindex), sizeof(p_conninfo->pindex), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_conninfo->node), sizeof(p_conninfo->node), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&(p_conninfo->pid), sizeof(p_conninfo->pid), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_conninfo->pename), sizeof(p_conninfo->pename),
                           p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *conninfo_get(struct conninfo *p_conninfo,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CONNINFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_conninfo->pindex), sizeof(p_conninfo->pindex), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_conninfo->node), sizeof(p_conninfo->node), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&(p_conninfo->pid), sizeof(p_conninfo->pid), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_conninfo->pename), sizeof(p_conninfo->pename),
                           p_buf, p_buf_end);

    return p_buf;
}

uint8_t *fsqlresp_put(const struct fsqlresp *p_fsqlresp, uint8_t *p_buf,
                      const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSQLRESP_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_fsqlresp->response), sizeof(p_fsqlresp->response),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_fsqlresp->flags), sizeof(p_fsqlresp->flags), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_fsqlresp->rcode), sizeof(p_fsqlresp->rcode), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_fsqlresp->parm), sizeof(p_fsqlresp->parm), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_fsqlresp->followlen), sizeof(p_fsqlresp->followlen),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *fsqlresp_get(struct fsqlresp *p_fsqlresp, const uint8_t *p_buf,
                            const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSQLRESP_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_fsqlresp->response), sizeof(p_fsqlresp->response),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_fsqlresp->flags), sizeof(p_fsqlresp->flags), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_fsqlresp->rcode), sizeof(p_fsqlresp->rcode), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_fsqlresp->parm), sizeof(p_fsqlresp->parm), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_fsqlresp->followlen), sizeof(p_fsqlresp->followlen),
                    p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *column_info_put(const struct column_info *p_column_info,
                                uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || COLUMN_INFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_column_info->type), sizeof(p_column_info->type), p_buf,
                    p_buf_end);
    p_buf =
        buf_no_net_put(&(p_column_info->column_name),
                       sizeof(p_column_info->column_name), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *column_info_get(struct column_info *p_column_info,
                                      const uint8_t *p_buf,
                                      const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || COLUMN_INFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_column_info->type), sizeof(p_column_info->type), p_buf,
                    p_buf_end);
    p_buf =
        buf_no_net_get(&(p_column_info->column_name),
                       sizeof(p_column_info->column_name), p_buf, p_buf_end);

    return p_buf;
}

static uint8_t *sqlfield_put(const struct sqlfield *p_sqlfield, uint8_t *p_buf,
                             const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SQLFIELD_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_sqlfield->offset), sizeof(p_sqlfield->offset), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&(p_sqlfield->len), sizeof(p_sqlfield->len), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *sqlfield_get(struct sqlfield *p_sqlfield,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SQLFIELD_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_sqlfield->offset), sizeof(p_sqlfield->offset), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&(p_sqlfield->len), sizeof(p_sqlfield->len), p_buf, p_buf_end);

    return p_buf;
}

static inline void comdb2_set_sqlite_vdbe_tzname_int(Vdbe *p,
                                                     struct sqlclntstate *clnt)
{
    memcpy(p->tzname, clnt->tzname, TZNAME_MAX);
}

static inline void comdb2_set_sqlite_vdbe_dtprec_int(Vdbe *p,
                                                     struct sqlclntstate *clnt)
{
    p->dtprec = clnt->dtprec;
}

static inline int disable_server_sql_timeouts(void)
{
    extern int gbl_sql_release_locks_on_slow_reader;
    extern int gbl_sql_no_timeouts_on_release_locks;

    return (gbl_sql_release_locks_on_slow_reader &&
            gbl_sql_no_timeouts_on_release_locks);
}

static int send_heartbeat(struct sqlclntstate *clnt);

static void send_fsql_error(struct sqlclntstate *clnt, int rc, char *errstr)
{
    struct fsqlresp resp = {0};
    resp.response = FSQL_ERROR;
    resp.rcode = rc;
    fsql_write_response(clnt, &resp, (void *)errstr, strlen(errstr) + 1, 1,
                        __func__, __LINE__);
}

static int handle_failed_dispatch(struct sqlclntstate *clnt, char *errstr)
{
    struct fsqlresp resp;

    bzero(&resp, sizeof(resp));
    resp.response = FSQL_ERROR;
    resp.rcode = CDB2ERR_REJECTED;

    pthread_mutex_lock(&clnt->wait_mutex);
    fsql_write_response(clnt, &resp, (void *)errstr, strlen(errstr) + 1, 1,
                        __func__, __LINE__);
    pthread_mutex_unlock(&clnt->wait_mutex);

    return 0;
}

static int handle_fastsql_requests_io_read(struct sqlthdstate *thd,
                                           struct sqlclntstate *clnt,
                                           size_t bytes)
{
    int rc;
    int pre_enabled = 0;

    if (thd->maxbuflen < bytes) {
        char *p;
        if (bytes <= gbl_blob_sz_thresh_bytes) {
            /* it is a small buffer. simply realloc. */
            p = realloc(thd->buf, bytes);
        } else if (thd->maxbuflen > gbl_blob_sz_thresh_bytes) {
            /* maxbuflen > threshold, meaning the buffer is already in blobmem.
               timedrealloc so we can wake up to send the client heartbeats. */
            while (1) {
                p = comdb2_timedrealloc(blobmem, thd->buf, bytes, 1000);

                if (p != NULL || errno != ETIMEDOUT)
                    break;

                pthread_mutex_lock(&clnt->wait_mutex);
                if (clnt->heartbeat == 0)
                    clnt->heartbeat = 1;
                if (clnt->ready_for_heartbeats == 0) {
                    pre_enabled = 1;
                    clnt->ready_for_heartbeats = 1;
                }
                send_heartbeat(clnt);
                fdb_heartbeats(clnt);
                pthread_mutex_unlock(&clnt->wait_mutex);
            }

            if (pre_enabled) {
                pthread_mutex_lock(&clnt->wait_mutex);
                clnt->ready_for_heartbeats = 0;
                pthread_mutex_unlock(&clnt->wait_mutex);
            }
        } else {
            /* it was a small buffer. now it grows large enough.
               relocate it (malloc + memset and free the original chunk)
               to blobmem */
            while (1) {
                p = comdb2_timedmalloc(blobmem, bytes, 1000);

                if (p != NULL || errno != ETIMEDOUT)
                    break;

                pthread_mutex_lock(&clnt->wait_mutex);
                if (clnt->heartbeat == 0)
                    clnt->heartbeat = 1;
                if (clnt->ready_for_heartbeats == 0) {
                    pre_enabled = 1;
                    clnt->ready_for_heartbeats = 1;
                }
                send_heartbeat(clnt);
                fdb_heartbeats(clnt);
                pthread_mutex_unlock(&clnt->wait_mutex);
            }

            if (pre_enabled) {
                pthread_mutex_lock(&clnt->wait_mutex);
                clnt->ready_for_heartbeats = 0;
                pthread_mutex_unlock(&clnt->wait_mutex);
            }

            if (p != NULL) {
                memcpy(p, thd->buf, thd->maxbuflen);
                free(thd->buf);
            }
        }

        if (!p) {
            logmsg(LOGMSG_ERROR, "%s: out of memory realloc %d\n", __func__, bytes);
            return -1;
        }

        thd->maxbuflen = bytes;
        thd->buf = p;
    }
    if (bytes) {
        int reset_timeouts = 0;
        int tmwrite = 0, tmread = 0;
        int was_timeout = 0;

    back_to_waiting:

        /* if we are in a distributed transaction, we need heartbeats */
        if (reset_timeouts || clnt->dbtran.dtran) {
            if (!reset_timeouts) {
                /* only reset that first time */
                sbuf2gettimeout(clnt->sb, &tmread, &tmwrite);

                sbuf2settimeout(clnt->sb, 5000,
                                5000); /* TODO: make this settable */
            }

            rc = sbuf2fread_timeout(thd->buf, bytes, 1, clnt->sb, &was_timeout);
        } else {
            rc = sbuf2fread_timeout(thd->buf, bytes, 1, clnt->sb, &was_timeout);
            if (rc != 1) {
                if (was_timeout) {
                    handle_failed_dispatch(clnt, "Socket read timeout.");
                }
            }
            was_timeout = 0; /* we don't care now */
        }

        if (rc != 1 && was_timeout) {
            fdb_heartbeats(clnt);
            goto back_to_waiting;
        }

        if (reset_timeouts) {
            sbuf2settimeout(clnt->sb, tmread, tmwrite);
        }

        if (rc != 1) {
            return -1;
        }
    }

    return 0;
}

static char *fsql_reqcode_str(int req)
{
    switch (req) {
    case FSQL_EXECUTE_INLINE_PARAMS:
        return "FSQL_EXECUTE_INLINE_PARAMS";
    case FSQL_EXECUTE_STOP:
        return "FSQL_EXECUTE_STOP";
    case FSQL_SET_ISOLATION_LEVEL:
        return "FSQL_SET_ISOLATION_LEVEL";
    case FSQL_SET_TIMEOUT:
        return "FSQL_SET_TIMEOUT";
    case FSQL_SET_INFO:
        return "FSQL_SET_INFO";
    case FSQL_EXECUTE_INLINE_PARAMS_TZ:
        return "FSQL_EXECUTE_INLINE_PARAMS_TZ";
    case FSQL_SET_HEARTBEAT:
        return "FSQL_SET_HEARTBEAT";
    case FSQL_PRAGMA:
        return "FSQL_PRAGMA";
    case FSQL_RESET:
        return "FSQL_RESET";
    case FSQL_EXECUTE_REPLACEABLE_PARAMS:
        return "FSQL_EXECUTE_REPLACEABLE_PARAMS";
    case FSQL_SET_SQL_DEBUG:
        return "FSQL_SET_SQL_DEBUG";
    case FSQL_GRAB_DBGLOG:
        return "FSQL_GRAB_DBGLOG";
    case FSQL_SET_USER:
        return "FSQL_SET_USER";
    case FSQL_SET_PASSWORD:
        return "FSQL_SET_PASSWORD";
    case FSQL_SET_ENDIAN:
        return "FSQL_SET_ENDIAN";
    case FSQL_EXECUTE_REPLACEABLE_PARAMS_TZ:
        return "FSQL_EXECUTE_REPLACEABLE_PARAMS_TZ";
    case FSQL_SET_DATETIME_PRECISION:
        return "FSQL_SET_DATETIME_PRECISION";
    default:
        return "???";
    };
}

static char *fsql_respcode_str(int rsp)
{
    switch (rsp) {
    case FSQL_COLUMN_DATA:
        return "FSQL_COLUMN_DATA";
    case FSQL_ROW_DATA:
        return "FSQL_ROW_DATA";
    case FSQL_NO_MORE_DATA:
        return "FSQL_NO_MORE_DATA";
    case FSQL_ERROR:
        return "FSQL_ERROR";
    case FSQL_QUERY_STATS:
        return "FSQL_QUERY_STATS";
    case FSQL_HEARTBEAT:
        return "FSQL_HEARTBEAT";
    case FSQL_SOSQL_TRAN_RESPONSE:
        return "FSQL_SOSQL_TRAN_RESPONSE";
    case FSQL_DONE:
        return "FSQL_DONE";
    default: {
        return "???";
    }
    }
}

char *tranlevel_tostr(int lvl)
{
    switch (lvl) {
    case TRANLEVEL_SOSQL:
        return "TRANLEVEL_SOSQL";
    case TRANLEVEL_RECOM:
        return "TRANLEVEL_RECOM";
    case TRANLEVEL_SERIAL:
        return "TRANLEVEL_SERIAL";
    default:
        return "???";
    };
}

extern int gbl_catch_response_on_retry;
int fsql_write_response(struct sqlclntstate *clnt, struct fsqlresp *resp,
                        void *dta, int len, int flush, const char *func,
                        uint32_t line)
{
    int rc;
    SBUF2 *sb;

    sb = clnt->sb;

    if (gbl_dump_fsql_response && resp)
        logmsg(LOGMSG_USER, "Sending response=%d dta length %d to node %s for sql "
                        "%s newsql-flag %d\n",
                resp->response, len, clnt->origin, clnt->sql, clnt->is_newsql);

    if (gbl_catch_response_on_retry && clnt->osql.replay == OSQL_RETRY_DO &&
        resp && resp->response != FSQL_HEARTBEAT) {
        logmsg(LOGMSG_ERROR, "Error: writing a response on a retry\n");
        if (resp) {
            logmsg(LOGMSG_ERROR, "<- %s (%d) rcode %d\n",
                    fsql_respcode_str(resp->response), resp->response,
                    resp->rcode);
        }
        if (flush)
            logmsg(LOGMSG_USER, "<- flush\n");
        cheap_stack_trace();
    }
    rc = pthread_mutex_lock(&clnt->write_lock);

    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "couldnt get clnt->write_lock\n");
        exit(1);
    }

    if (resp) {
        if (clnt->is_newsql) {
            CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;
            if (gbl_dump_fsql_response) {
                logmsg(LOGMSG_USER, "td=%u %s line %d sending newsql response "
                                "sent_column_data=%d\n",
                        (uint32_t)pthread_self(), __func__, __LINE__,
                        clnt->osql.sent_column_data);
            }
            if (clnt->osql.sent_column_data) {
                sql_response.response_type = RESPONSE_TYPE__COLUMN_VALUES;
            } else {
                sql_response.response_type = RESPONSE_TYPE__COLUMN_NAMES;
            }

            sql_response.n_value = 0;
            sql_response.value = NULL;
            sql_response.error_code = resp->rcode;
            if (resp->rcode) {
                sql_response.error_string = (char *)dta;
            } else {
                sql_response.error_string = NULL;
            }
            sql_response.effects = NULL;
            int len = cdb2__sqlresponse__get_packed_size(&sql_response);
            void *buf = malloc(len + 1);
            struct newsqlheader hdr;
            hdr.type = ntohl(RESPONSE_HEADER__SQL_RESPONSE);
            hdr.compression = 0;
            hdr.dummy = 0;
            hdr.length = ntohl(len);
            rc = sbuf2write((char *)&hdr, sizeof(hdr), sb);

            if (rc != sizeof(hdr)) {

                if (gbl_dump_fsql_response) {
                    logmsg(LOGMSG_USER, 
                           "td %u response for %s error writing header, rc=%d\n",
                           (uint32_t)pthread_self(), clnt->sql, rc);
                }

                rc = pthread_mutex_unlock(&clnt->write_lock);
                if (rc != 0) {
                    logmsg(LOGMSG_FATAL, "couldnt put clnt->write_lock\n");
                    exit(1);
                }
                free(buf);
                return -1;
            }

            if (sql_response.error_code == 1) {
                logmsg(LOGMSG_ERROR, "%s line %d returning DUP from %s line %d\n",
                        __func__, __LINE__, func, line);
            }

            if (gbl_dump_fsql_response) {
                logmsg(LOGMSG_USER, 
                        "td %u Response for %s: response_type=%d error_code=%d "
                        "error_string=%s\n",
                        (uint32_t)pthread_self(), clnt->sql,
                        sql_response.response_type, sql_response.error_code,
                        sql_response.error_string ? sql_response.error_string
                                                  : "(NULL)");
            }

            if (gbl_dump_fsql_response) {
                logmsg(LOGMSG_USER, "td %u %s line %d Sql %s sending response "
                                "resp->response=%d\n",
                        (uint32_t)pthread_self(), __func__, __LINE__, clnt->sql,
                        resp->response);
            }
            cdb2__sqlresponse__pack(&sql_response, buf);

            rc = sbuf2write(buf, len, sb);
            free(buf);
            buf = NULL;

            if (rc != len) {
                if (gbl_dump_fsql_response) {
                    logmsg(LOGMSG_USER, "td %u %s line %d Sql %s: error writing, "
                                    "rc=%d len=%d\n",
                            (uint32_t)pthread_self(), __func__, __LINE__,
                            clnt->sql, rc, len);
                }
                rc = pthread_mutex_unlock(&clnt->write_lock);
                if (rc != 0) {
                    logmsg(LOGMSG_FATAL, "couldnt put clnt->write_lock\n");
                    exit(1);
                }
                return -1;
            }

            sbuf2flush(sb);
            rc = pthread_mutex_unlock(&clnt->write_lock);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "couldnt get clnt->write_lock\n");
                exit(1);
            }
            return 0;

        } else {

            if (flush &&
                active_appsock_conns >=
                    bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXSOCKCACHED))
                resp->flags |= FRESP_FLAG_CLOSE;

            uint8_t buf_resp[FSQLRESP_LEN];

            resp->followlen = len;

            /* pack the response */
            if (!fsqlresp_put(resp, buf_resp, buf_resp + sizeof(buf_resp))) {
                rc = pthread_mutex_unlock(&clnt->write_lock);
                if (rc != 0) {
                    logmsg(LOGMSG_FATAL, "couldnt put clnt->write_lock\n");
                    exit(1);
                }

                return -1;
            }

            rc = sbuf2write((char *)buf_resp, sizeof(buf_resp), sb);
            if (rc != sizeof(buf_resp)) {
                rc = pthread_mutex_unlock(&clnt->write_lock);
                if (rc != 0) {
                    logmsg(LOGMSG_FATAL, "couldnt get clnt->write_lock\n");
                    exit(1);
                }

                return -1;
            }
        }
    }

    if (dta) {
        rc = sbuf2write(dta, len, sb);
        if (rc != len) {
            rc = pthread_mutex_unlock(&clnt->write_lock);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "couldnt get clnt->write_lock\n");
                exit(1);
            }

            return -1;
        }
    }

    if (flush) {
        sbuf2flush(sb);
    }

    rc = pthread_mutex_unlock(&clnt->write_lock);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "couldnt get clnt->write_lock\n");
        exit(1);
    }

    return 0;
}

int newsql_write_response(struct sqlclntstate *clnt, int type,
                          CDB2SQLRESPONSE *sql_response, int flush,
                          void *(*alloc)(size_t size), const char *func,
                          int line)
{
    struct newsqlheader hdr;
    int rc;
    SBUF2 *sb;
    int len;
    void *dta;

    sb = clnt->sb;

    if (gbl_dump_fsql_response) {
        int file = -1, offset = -1, response_type = -1;
        if (sql_response && sql_response->snapshot_info) {
            file = sql_response->snapshot_info->file;
            offset = sql_response->snapshot_info->offset;
            response_type = sql_response->response_type;
        }
        logmsg(LOGMSG_USER, 
                "td=%u %s line %d Sending response=%d sqlresponse_type=%d "
                "lsn[%d][%d] dta length %d to %s for sql %s from %s line %d\n",
                (uint32_t)pthread_self(), __func__, __LINE__, type, file,
                offset, response_type, len, clnt->origin, clnt->sql, func,
                line);
    }

    if (clnt->in_client_trans && clnt->sql_query &&
        clnt->sql_query->skip_rows == -1 && (clnt->isselect != 0)) {
        // Client doesn't expect any response at this point.
        logmsg(LOGMSG_DEBUG, "sending nothing back to client \n");
        return 0;
    }

    /* payload */
    if (sql_response) {
        len = cdb2__sqlresponse__get_packed_size(sql_response);
        dta = (*alloc)(len + 1);
        cdb2__sqlresponse__pack(sql_response, dta);
    } else {
        len = 0;
        dta = NULL;
    }

    /* header */
    hdr.type = ntohl(type);
    hdr.compression = 0;
    hdr.dummy = 0;
    hdr.length = ntohl(len);

    rc = pthread_mutex_lock(&clnt->write_lock);

    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "couldnt get clnt->write_lock\n");
        exit(1);
    }

    rc = sbuf2write((char *)&hdr, sizeof(struct newsqlheader), sb);
    if (rc != sizeof(struct newsqlheader)) {
        rc = pthread_mutex_unlock(&clnt->write_lock);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "couldnt put clnt->write_lock\n");
            exit(1);
        }
        free(dta);
        return -1;
    }

    if (dta) {
        rc = sbuf2write(dta, len, sb);
        if (rc != len) {

            if (gbl_dump_fsql_response) {
                logmsg(LOGMSG_USER, "sbuf2write error for %s rc=%d\n", clnt->sql,
                        rc);
            }

            rc = pthread_mutex_unlock(&clnt->write_lock);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "couldnt get clnt->write_lock\n");
                exit(1);
            }

            free(dta);
            return -1;
        }
    }

    if (flush) {
        sbuf2flush(sb);
    }

    rc = pthread_mutex_unlock(&clnt->write_lock);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "couldnt get clnt->write_lock\n");
        exit(1);
    }

    if (dta)
        free(dta);

    return 0;
}

int request_durable_lsn_from_master(bdb_state_type *bdb_state, uint32_t *file,
                                    uint32_t *offset, uint32_t *durable_gen);

static int fill_snapinfo(struct sqlclntstate *clnt, int *file, int *offset)
{
    char cnonce[256];
    int rcode = 0;
    if (gbl_extended_sql_debug_trace && clnt->sql_query) {
        snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
    }
    if (clnt->sql_query && clnt->sql_query->snapshot_info &&
        clnt->sql_query->snapshot_info->file > 0) {
        *file = clnt->sql_query->snapshot_info->file;
        *offset = clnt->sql_query->snapshot_info->offset;

        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, 
                    "%s line %d cnonce '%s' sql_query->snapinfo is [%d][%d], "
                    "clnt->snapinfo is [%d][%d]: use client snapinfo!\n",
                    __func__, __LINE__, cnonce,
                    clnt->sql_query->snapshot_info->file,
                    clnt->sql_query->snapshot_info->offset, clnt->snapshot_file,
                    clnt->snapshot_offset);
        }
        return 0;
    }

    if (*file == 0 && clnt->sql_query &&
        (clnt->in_client_trans || clnt->is_hasql_retry) &&
        clnt->snapshot_file) {
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, 
                    "%s line %d cnonce '%s' sql_query->snapinfo is [%d][%d], "
                    "clnt->snapinfo is [%d][%d]\n",
                    __func__, __LINE__, cnonce,
                    (clnt->sql_query && clnt->sql_query->snapshot_info)
                        ? clnt->sql_query->snapshot_info->file
                        : -1,
                    (clnt->sql_query && clnt->sql_query->snapshot_info)
                        ? clnt->sql_query->snapshot_info->offset
                        : -1,
                    clnt->snapshot_file, clnt->snapshot_offset);
        }

        *file = clnt->snapshot_file;
        *offset = clnt->snapshot_offset;
        logmsg(LOGMSG_USER, 
                "%s line %d setting newsql snapinfo retry info is [%d][%d]\n",
                __func__, __LINE__, *file, *offset);
        return 0;
    }

    if (*file == 0 && clnt->is_newsql && clnt->sql_query &&
        clnt->ctrl_sqlengine == SQLENG_STRT_STATE) {

        if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DURABLE_LSNS)) {
            if (bdb_attr_get(thedb->bdb_attr,
                             BDB_ATTR_RETRIEVE_DURABLE_LSN_AT_BEGIN)) {
                uint32_t durable_file, durable_offset, durable_gen;

                int rc = request_durable_lsn_from_master(
                    thedb->bdb_env, &durable_file, &durable_offset,
                    &durable_gen);

                if (rc == 0) {
                    *file = durable_file;
                    *offset = durable_offset;

                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER, "%s line %d cnonce='%s' master "
                                        "returned durable-lsn "
                                        "[%d][%d], clnt->is_hasql_retry=%d\n",
                                __func__, __LINE__, cnonce, *file, *offset,
                                clnt->is_hasql_retry);
                    }
                } else {
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER, 
                               "%s line %d cnonce='%s' durable-lsn request "
                               "returns %d "
                               "clnt->snapshot_file=%d clnt->snapshot_offset=%d "
                               "clnt->is_hasql_retry=%d\n",
                               __func__, __LINE__, cnonce, rc, clnt->snapshot_file,
                               clnt->snapshot_offset, clnt->is_hasql_retry);
                    }
                    rcode = -1;
                }
            }
            // Defer returning lsn until adding to trn_repo
            else {
                *file = *offset = 0;
                if (gbl_extended_sql_debug_trace) {
                    logmsg(LOGMSG_USER, "%s line %d cnonce='%s' durable-lsns set, "
                                    "returning 0\n",
                            __func__, __LINE__, cnonce);
                }
            }
            return rcode;
        }
    }

    if (*file == 0) {
        bdb_tran_get_start_file_offset(thedb->bdb_env, clnt->dbtran.shadow_tran,
                                       file, offset);
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, "%s line %d start_file_offset snapinfo is "
                            "[%d][%d], sqlengine-state is %d\n",
                    __func__, __LINE__, file, offset, clnt->ctrl_sqlengine);
        }
    }
    return rcode;
}

#define _has_effects(clnt, sql_response)                                       \
    CDB2EFFECTS effects = CDB2__EFFECTS__INIT;                                 \
                                                                               \
    clnt->effects.num_affected = clnt->effects.num_updated +                   \
                                 clnt->effects.num_deleted +                   \
                                 clnt->effects.num_inserted;                   \
    effects.num_affected = clnt->effects.num_affected;                         \
    effects.num_selected = clnt->effects.num_selected;                         \
    effects.num_updated = clnt->effects.num_updated;                           \
    effects.num_deleted = clnt->effects.num_deleted;                           \
    effects.num_inserted = clnt->effects.num_inserted;                         \
                                                                               \
    sql_response.effects = &effects;

#define _has_features(clnt, sql_response)                                      \
    CDB2ServerFeatures features[10];                                           \
    int n_features = 0;                                                        \
                                                                               \
    if (clnt->skip_feature) {                                                  \
        features[n_features] = CDB2_SERVER_FEATURES__SKIP_ROWS;                \
        n_features++;                                                          \
    }                                                                          \
                                                                               \
    if (n_features) {                                                          \
        sql_response.n_features = n_features;                                  \
        sql_response.features = features;                                      \
    }

#define _has_snapshot(clnt, sql_response)                                      \
    CDB2SQLRESPONSE__Snapshotinfo snapshotinfo =                               \
        CDB2__SQLRESPONSE__SNAPSHOTINFO__INIT;                                 \
                                                                               \
    if (clnt->high_availability) {                                             \
        int file = 0, offset = 0, rc;                                          \
        if (fill_snapinfo(clnt, &file, &offset)) {                             \
            sql_response.error_code = CDB2ERR_CHANGENODE;                      \
        }                                                                      \
        if (file) {                                                            \
            snapshotinfo.file = file;                                          \
            snapshotinfo.offset = offset;                                      \
            sql_response.snapshot_info = &snapshotinfo;                        \
        }                                                                      \
    }

int newsql_send_last_row(struct sqlclntstate *clnt, int is_begin,
                         const char *func, int line)
{
    CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;
    int rc;

    _has_effects(clnt, sql_response);
    _has_snapshot(clnt, sql_response);
    _has_features(clnt, sql_response);

    if (gbl_extended_sql_debug_trace) {
        char cnonce[256] = {0};
        snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
        logmsg(LOGMSG_USER,
               "%u: %s line %d cnonce='%s' [%d][%d] sending last_row, "
               "selected=%u updated=%u deleted=%u inserted=%u\n",
               pthread_self(), func, line, cnonce, clnt->snapshot_file,
               clnt->snapshot_offset, sql_response.effects->num_selected,
               sql_response.effects->num_updated,
               sql_response.effects->num_deleted,
               sql_response.effects->num_inserted);
    }

    return _push_row_new(clnt, RESPONSE_TYPE__LAST_ROW, &sql_response, NULL, 0,
                         malloc, 1);
}

CDB2SQLRESPONSE__Column **newsql_alloc_row(int ncols)
{
    CDB2SQLRESPONSE__Column **columns;
    int col;

    columns = (CDB2SQLRESPONSE__Column **)calloc(
        ncols, sizeof(CDB2SQLRESPONSE__Column **));
    if (columns) {
        for (int i = 0; i < ncols; i++) {
            columns[i] = malloc(sizeof(CDB2SQLRESPONSE__Column));
            if (!columns[i]) {
                for (i--; i >= 0; i--)
                    free(columns[i]);
                free(columns);
                columns = NULL;
                break;
            }
        }
    }
    return columns;
}

void newsql_dealloc_row(CDB2SQLRESPONSE__Column **columns, int ncols)
{
    for (int i = 0; i < ncols; i++) {
        free(columns[i]);
    }
    free(columns);
}

void newsql_send_strbuf_response(struct sqlclntstate *clnt, const char *str,
                                 int slen)
{
    CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;
    CDB2SQLRESPONSE__Column **columns = NULL;
    int ncols = 1;

    columns = newsql_alloc_row(ncols);

    for (int i = 0; i < ncols; i++) {
        cdb2__sqlresponse__column__init(columns[i]);
        columns[i]->has_type = 0;
        columns[i]->value.len = slen;
        columns[i]->value.data = (char *)str;
    }

    _push_row_new(clnt, RESPONSE_TYPE__COLUMN_VALUES, &sql_response, columns,
                  ncols, malloc, 0);

    newsql_dealloc_row(columns, ncols);
}

int newsql_send_dummy_resp(struct sqlclntstate *clnt, const char *func,
                           int line)
{
    CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;

    return _push_row_new(clnt, RESPONSE_TYPE__COLUMN_NAMES, &sql_response, NULL,
                         0, malloc, 1);
}

static int toggle_case_sensitive_like(sqlite3 *db, int enable)
{
    char sql[80];
    int rc;
    char *err;

    snprintf(sql, sizeof(sql), "PRAGMA case_sensitive_like = %d;",
             enable ? 0 : 1);
    rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    if (rc)
        logmsg(LOGMSG_ERROR, "Failed to set case_insensitive_like rc %d err \"%s\"\n", rc,
                err ? err : "");
    if (err)
        sqlite3_free(err);
    return rc;
}

#ifdef DEBUG_SQLITE_MEMORY

#include <execinfo.h>

#define MAX_DEBUG_FRAMES 50

struct blk {
    int nframes;
    void *frames[MAX_DEBUG_FRAMES];
    int in_init;
    size_t sz;
    void *p;
};

static __thread hash_t *sql_blocks;

static int dump_block(void *obj, void *arg)
{
    struct blk *b = (struct blk *)obj;
    int i;
    int *had_blocks = (int *)arg;

    if (!b->in_init) {
        if (!*had_blocks) {
            logmsg(LOGMSG_USER, "outstanding blocks:\n");
            *had_blocks = 1;
        }
        logmsg(LOGMSG_USER, "%lld %x ", b->sz, b->p);
        for (int i = 0; i < b->nframes; i++)
            logmsg(LOGMSG_USER, "%x ", b->frames[i]);
        logmsg(LOGMSG_USER, "\n");
    }

    return 0;
}

static __thread int in_init = 0;

void sqlite_init_start(void) { in_init = 1; }

void sqlite_init_end(void) { in_init = 0; }

#endif // DEBUG_SQLITE_MEMORY

static __thread comdb2ma sql_mspace = NULL;
int sql_mem_init(void *arg)
{
    if (unlikely(sql_mspace)) {
        return 0;
    }

    /* We used to start with 1MB - this isn't quite necessary
       as comdb2_malloc pre-allocation is much smarter now.
       We also name it "SQLITE" (uppercase) to differentiate it
       from those implicitly created per-thread allocators
       whose names are "sqlite" (lowercase). Those allocators
       are used by other types of threads, e.g., appsock threads. */
    sql_mspace = comdb2ma_create(0, 0, "SQLITE", COMDB2MA_MT_UNSAFE);
    if (sql_mspace == NULL) {
        logmsg(LOGMSG_FATAL, "%s: comdb2a_create failed\n", __func__);
        exit(1);
    }

#ifdef DEBUG_SQLITE_MEMORY
    sql_blocks = hash_init_o(offsetof(struct blk, p), sizeof(void));
#endif

    return 0;
}

void sql_mem_shutdown(void *arg)
{
    if (sql_mspace) {
        comdb2ma_destroy(sql_mspace);
        sql_mspace = NULL;
    }
}

static void *sql_mem_malloc(int size)
{
    if (unlikely(sql_mspace == NULL))
        sql_mem_init(NULL);

    void *out = comdb2_malloc(sql_mspace, size);

#ifdef DEBUG_SQLITE_MEMORY
    struct blk *b = malloc(sizeof(struct blk));
    b->p = out;
    b->sz = size;
    b->nframes = backtrace(b->frames, MAX_DEBUG_FRAMES);
    b->in_init = in_init;
    if (!in_init) {
        fprintf(stderr, "allocated %d bytes in non-init\n", size);
    }
    if (b->nframes <= 0)
        free(b);
    else {
        hash_add(sql_blocks, b);
    }
#endif

    return out;
}

static void sql_mem_free(void *mem)
{
#ifdef DEBUG_SQLITE_MEMORY
    struct blk *b;
    b = hash_find(sql_blocks, &mem);
    if (!b) {
        fprintf(stderr, "no block associated with %p\n", mem);
        abort();
    }
    hash_del(sql_blocks, b);
    free(b);
#endif
    comdb2_free(mem);
}

static void *sql_mem_realloc(void *mem, int size)
{
    if (unlikely(sql_mspace == NULL))
        sql_mem_init(NULL);

    void *out = comdb2_realloc(sql_mspace, mem, size);

#ifdef DEBUG_SQLITE_MEMORY
    struct blk *b;
    b = hash_find(sql_blocks, &mem);
    if (!b) {
        fprintf(stderr, "no block associated with %p\n", mem);
        abort();
    }
    hash_del(sql_blocks, b);
    b->nframes = backtrace(b->frames, MAX_DEBUG_FRAMES);
    b->p = out;
    b->sz = size;
    b->in_init = in_init;
    if (b->nframes <= 0)
        free(b);
    else {
        hash_add(sql_blocks, b);
    }
#endif

    return out;
}

static int sql_mem_size(void *mem) { return comdb2_malloc_usable_size(mem); }

static int sql_mem_roundup(int i) { return i; }

void sql_dlmalloc_init(void)
{
    sqlite3_mem_methods m;
    if (gbl_disable_sql_dlmalloc) {
        return;
    }
    m.xMalloc = sql_mem_malloc;
    m.xFree = sql_mem_free;
    m.xRealloc = sql_mem_realloc;
    m.xSize = sql_mem_size;
    m.xRoundup = sql_mem_roundup;
    m.xInit = sql_mem_init;
    m.xShutdown = sql_mem_shutdown;
    m.pAppData = NULL;
    sqlite3_config(SQLITE_CONFIG_MALLOC, &m);
}

static pthread_mutex_t open_serial_lock = PTHREAD_MUTEX_INITIALIZER;
int sqlite3_open_serial(const char *filename, sqlite3 **ppDb,
                        struct sqlthdstate *thd)
{
    int serial = gbl_serialise_sqlite3_open;
    if (serial)
        pthread_mutex_lock(&open_serial_lock);
    int rc = sqlite3_open(filename, ppDb, thd);
    if (serial)
        pthread_mutex_unlock(&open_serial_lock);
    return rc;
}

/* We'll probably play around with this formula quite a bit. The
   idea is that reads/writes to/from temp tables are cheap, since
   they are in memory, writes to real tables are really expensive
   since they need to replicate, finds are more expensive then
   nexts. The last assertion is less true if we are in index mode
   since a next is effectively a find, but we'll overlook that here
   since we're moving towards cursors these days. Temp table
   reads/writes should also be considered more expensive if the
   temp table spills to disk, etc. */
double query_cost(struct sql_thread *thd)
{
#if 0
    return (double) thd->nwrite * 100 + (double) thd->nfind * 10 + 
        (double) thd->nmove * 1 + (double) thd->ntmpwrite * 0.2 + 
        (double) thd->ntmpread * 0.1;
#endif
    return thd->cost;
}

void sql_dump_hist_statements(void)
{
    struct sql_hist *h;
    struct tm tm;
    char rqid[50];

    pthread_mutex_lock(&gbl_sql_lock);
    LISTC_FOR_EACH(&thedb->sqlhist, h, lnk)
    {
        time_t t;
        if (h->txnid)
            snprintf(rqid, sizeof(rqid), "txn %016llx ",
                     (unsigned long long)h->txnid);
        else
            rqid[0] = 0;

        t = h->when;
        localtime_r((time_t *)&t, &tm);
        if (h->conn.pename[0]) {
            logmsg(LOGMSG_USER, "%02d/%02d/%02d %02d:%02d:%02d %spindex %d task %.8s pid %d "
                   "mach %d time %dms cost %f sql: %s\n",
                   tm.tm_mon + 1, tm.tm_mday, 1900 + tm.tm_year, tm.tm_hour,
                   tm.tm_min, tm.tm_sec, rqid, h->conn.pindex,
                   (char *)h->conn.pename, h->conn.pid, h->conn.node, h->time,
                   h->cost, h->sql);
        } else {
            logmsg(LOGMSG_USER, 
                   "%02d/%02d/%02d %02d:%02d:%02d %stime %dms cost %f sql: %s\n",
                   tm.tm_mon + 1, tm.tm_mday, 1900 + tm.tm_year, tm.tm_hour,
                   tm.tm_min, tm.tm_sec, rqid, h->time, h->cost, h->sql);
        }
    }
    pthread_mutex_unlock(&gbl_sql_lock);
}

/* Save copy of sql statement and performance data.  If any other code
   should run after a sql statement is completed it should end up here. */
static void sql_statement_done(struct sql_thread *thd, struct reqlogger *logger,
                               unsigned long long rqid, int stmt_rc)
{
    struct sql_hist *h;
    LISTC_T(struct sql_hist) lst;
    struct query_path_component *qc;
    struct sqlclntstate *clnt;
    int rc;
    int cost;
    int timems;

    if (thd == NULL)
        return;

    clnt = thd->sqlclntstate;
    if (clnt) {
        int fd;
        int rc;
        struct sockaddr_in peeraddr;
        int len = sizeof(struct sockaddr_in);
        char addr[64];

        fd = sbuf2fileno(clnt->sb);
        rc = getpeername(fd, (struct sockaddr *)&peeraddr, &len);
        if (rc)
            snprintf(addr, sizeof(addr), "<unknown>");
        else {
            if (inet_ntop(peeraddr.sin_family, &peeraddr.sin_addr, addr,
                          sizeof(addr)) == NULL)
                snprintf(addr, sizeof(addr), "<unknown>");
        }

        if (clnt->limits.maxcost_warn &&
            (thd->cost > clnt->limits.maxcost_warn)) {
            logmsg(LOGMSG_USER, 
                   "[%s] warning: query exceeded cost threshold (%f >= %f): %s\n",
                   addr, thd->cost, clnt->limits.maxcost_warn, clnt->sql);
        }
        if (clnt->limits.tablescans_warn && thd->had_tablescans) {
            logmsg(LOGMSG_USER, 
                   "[%s] warning: query had a table scan: %s\n", addr,
                   clnt->sql);
        }
        if (clnt->limits.temptables_warn && thd->had_temptables) {
            logmsg(LOGMSG_USER, 
                   "[%s] warning: query created a temporary table: %s\n", addr,
                   clnt->sql);
        }
        if (clnt->osql.rqid != 0 && clnt->osql.rqid != OSQL_RQID_USE_UUID)
            reqlog_set_rqid(logger, &clnt->osql.rqid, sizeof(clnt->osql.rqid));
        else {
            /* have an "id_set" instead? */
            if (!comdb2uuid_is_zero(clnt->osql.uuid))
                reqlog_set_rqid(logger, clnt->osql.uuid, sizeof(uuid_t));
        }
    }

    listc_init(&lst, offsetof(struct sql_hist, lnk));

    h = calloc(1, sizeof(struct sql_hist));
    if (thd->sqlclntstate && thd->sqlclntstate->sql)
        h->sql = strdup(thd->sqlclntstate->sql);
    else
        h->sql = strdup("unknown");
    cost = h->cost = query_cost(thd);
    timems = h->time = time_epochms() - thd->startms;
    h->when = thd->stime;
    h->txnid = rqid;

    /* request logging framework takes care of logging long sql requests */
    reqlog_set_cost(logger, h->cost);
    if (rqid) {
        reqlog_logf(logger, REQL_INFO, "rqid=%llx", rqid);
    }

    if (clnt->query_stats == NULL) {
        record_query_cost(thd, clnt);
        reqlog_set_path(logger, clnt->query_stats);
    }
    reqlog_set_vreplays(logger, clnt->verify_retries);

    reqlog_end_request(logger, stmt_rc, __func__, __LINE__);

    thd->nmove = thd->nfind = thd->nwrite = thd->ntmpread = thd->ntmpwrite = 0;

    if (thd->sqlclntstate->conninfo.pename[0]) {
        h->conn = thd->sqlclntstate->conninfo;
    }

    reqlog_set_cost(logger, cost);
    reqlog_set_rows(logger, clnt->nrows);

    pthread_mutex_lock(&gbl_sql_lock);
    {
        quantize(q_sql_min, h->time);
        quantize(q_sql_hour, h->time);
        quantize(q_sql_all, h->time);
        quantize(q_sql_steps_min, h->cost);
        quantize(q_sql_steps_hour, h->cost);
        quantize(q_sql_steps_all, h->cost);

        if (clnt->is_newsql) {
            gbl_nnewsql_steps += h->cost;
        } else {
            gbl_nsql_steps += h->cost;
        }
        listc_abl(&thedb->sqlhist, h);
        while (listc_size(&thedb->sqlhist) > gbl_sqlhistsz) {
            h = listc_rtl(&thedb->sqlhist);
            listc_abl(&lst, h);
        }
    }
    pthread_mutex_unlock(&gbl_sql_lock);
    for (h = listc_rtl(&lst); h; h = listc_rtl(&lst)) {
        free(h->sql);
        free(h);
    }

    qc = listc_rtl(&thd->query_stats);
    while (qc) {
        free(qc);
        qc = listc_rtl(&thd->query_stats);
    }
    hash_clear(thd->query_hash);
    thd->cost = 0;
    thd->had_tablescans = 0;
    thd->had_temptables = 0;
}

void sql_set_sqlengine_state(struct sqlclntstate *clnt, char *file, int line,
                             int newstate)
{
    if (gbl_track_sqlengine_states)
        logmsg(LOGMSG_USER, "%d: %p %s:%d %d->%d\n", pthread_self(), clnt, file, line,
               clnt->ctrl_sqlengine, newstate);

    if (newstate == SQLENG_WRONG_STATE) {
        logmsg(LOGMSG_ERROR, "sqlengine entering wrong state from %s line %d.\n",
                file, line);
    }

    clnt->ctrl_sqlengine = newstate;
}

/* skip spaces and tabs, requires at least one space */
static char *skipws(char *str)
{
    if (str && *str && isspace(*str)) {
        while (*str && isspace(*str))
            str++;
    }
    return str;
}

/* skip spaces and tabs if present. */
static char *cdb2_skipws(char *str)
{
    if (*str && isspace(*str)) {
        while (*str && isspace(*str))
            str++;
    }
    return str;
}

static int retrieve_snapshot_info(char *sql, char *tzname)
{
    char *str = sql;

    if (str && *str) {
        if (isspace(*str))
            str = skipws(str);

        if (str && *str) {
            /* skip "transaction" if any */
            if (!strncasecmp(str, "transaction", 11)) {
                str += 11;
                str = skipws(str);
            }

            if (str && *str) {
                /* Alex wants "as of" */
                if (!strncasecmp(str, "as", 2)) {
                    str += 2;
                    str = skipws(str);
                    if (str && *str) {
                        if (!strncasecmp(str, "of", 2)) {
                            str += 2;
                            str = skipws(str);
                        }
                    } else {
                        logmsg(LOGMSG_ERROR, 
                               "Incorrect syntax, use begin ... as of ...\n");
                        return -1;
                    }
                } else {
                    logmsg(LOGMSG_USER, 
                            "Incorrect syntax, use begin ... as of ...\n");
                    return -1;
                }
            } else
                return 0;

            if (str && *str) {
                if (!strncasecmp(str, "datetime", 8)) {
                    str += 8;
                    str = skipws(str);

                    if (str && *str) {
                        /* convert this to a decimal and pass it along */
                        server_datetime_t sdt;
                        struct field_conv_opts_tz convopts = {0};
                        int outdtsz;
                        long long ret = 0;
                        int isnull = 0;

                        memcpy(convopts.tzname, tzname,
                               sizeof(convopts.tzname));
                        convopts.flags = FLD_CONV_TZONE;

                        if (CLIENT_CSTR_to_SERVER_DATETIME(
                                str, strlen(str) + 1, 0,
                                (struct field_conv_opts *)&convopts, NULL, &sdt,
                                sizeof(sdt), &outdtsz, NULL, NULL)) {
                            logmsg(LOGMSG_ERROR, 
                                   "Failed to parse snapshot datetime value\n");
                            return -1;
                        }

                        if (SERVER_DATETIME_to_CLIENT_INT(
                                &sdt, sizeof(sdt), NULL, NULL, &ret,
                                sizeof(ret), &isnull, &outdtsz, NULL, NULL)) {
                            logmsg(LOGMSG_ERROR, "Failed to convert snapshot "
                                                 "datetime value to epoch\n");
                            return -1;
                        } else {
                            long long lcl_ret = flibc_ntohll(ret);
                            if (gbl_new_snapisol_asof &&
                                bdb_is_timestamp_recoverable(thedb->bdb_env,
                                                             lcl_ret) <= 0) {
                                logmsg(LOGMSG_ERROR, "No log file to maintain "
                                                     "snapshot epoch %lld\n",
                                        lcl_ret);
                                return -1;
                            } else {
                                logmsg(LOGMSG_DEBUG, "Detected snapshot epoch %lld\n",
                                        lcl_ret);
                                return lcl_ret;
                            }
                        }
                    } else {
                        logmsg(LOGMSG_ERROR, "Missing datetime info for snapshot\n");
                        return -1;
                    }
                } else {
                    logmsg(LOGMSG_ERROR, "Missing snapshot information or garbage "
                                    "after \"begin\"\n");
                    return -1;
                }
                /*
                   else if (!strncasecmp(str, "genid"))
                   {

                   }
                 */
            } else {
                logmsg(LOGMSG_ERROR, "Missing snapshot info after \"as of\"\n");
                return -1;
            }
        }
    }

    return 0;
}

extern int gbl_disable_skip_rows;

void clear_snapshot_info(struct sqlclntstate *clnt, const char *func, int line)
{
    if (gbl_extended_sql_debug_trace) {
        char cnonce[256] = {0};
        if (clnt->sql_query) {
            int sz = clnt->sql_query->cnonce.len + 1;
            snprintf(cnonce, sz, "%s", clnt->sql_query->cnonce.data);
        }
        logmsg(LOGMSG_USER, "%s line %d clearing snapshot info cnonce='%s' current "
                        "info is [%d][%d]\n",
                func, line, cnonce, clnt->snapshot_file, clnt->snapshot_offset);

        comdb2_linux_cheap_stack_trace();
    }
    clnt->snapshot_file = 0;
    clnt->snapshot_offset = 0;
    clnt->is_hasql_retry = 0;
}

static void update_snapshot_info(struct sqlclntstate *clnt)
{
    int epoch = 0;

    if (gbl_extended_sql_debug_trace) {
        char cnonce[256] = {0};
        if (clnt->sql_query) {
            snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
        }
        logmsg(LOGMSG_USER, "%s line %d cnonce '%s'\n", __func__, __LINE__, cnonce);
    }

    /* We need to restore skip_feature, want_query_effects and
       send_one_row on clnt even if the snapshot info has been populated. */
    if (clnt->is_newsql && clnt->sql_query &&
        (clnt->sql_query->n_features > 0) && (gbl_disable_skip_rows == 0)) {
        for (int ii = 0; ii < clnt->sql_query->n_features; ii++) {
            if (CDB2_CLIENT_FEATURES__SKIP_ROWS ==
                clnt->sql_query->features[ii]) {
                clnt->skip_feature = 1;
                clnt->want_query_effects = 1;
                if ((clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
                     clnt->dbtran.mode == TRANLEVEL_SERIAL) &&
                    clnt->high_availability) {
                    clnt->send_one_row = 1;
                    clnt->skip_feature = 0;
                }
            }
        }
    }

    if (clnt->is_hasql_retry) {
        char cnonce[256] = {0};
        snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
        assert(clnt->sql_query->snapshot_info->file);
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, 
                   "%s line %d cnonce '%s': returning because hasql_retry is 1\n",
                   __func__, __LINE__, cnonce);
        }
        return;
    }

    // If this is a retry, we should already have the snapshot file and offset

    clear_snapshot_info(clnt, __func__, __LINE__);

    if (strlen(clnt->sql) > 6)
        epoch = retrieve_snapshot_info(&clnt->sql[6], clnt->tzname);

    if (clnt->is_newsql && clnt->sql_query && clnt->sql_query->snapshot_info) {
        clnt->snapshot_file = clnt->sql_query->snapshot_info->file;
        clnt->snapshot_offset = clnt->sql_query->snapshot_info->offset;
        if (gbl_extended_sql_debug_trace) {
            int sz = clnt->sql_query->cnonce.len;
            char cnonce[256];
            snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
            logmsg(LOGMSG_USER, "%s starting newsql client at [%d][%d] cnonce='%s' "
                            "retry=%d sql_query=%p\n",
                    __func__, clnt->snapshot_file, clnt->snapshot_offset,
                    cnonce, clnt->sql_query->retry, clnt->sql_query);
        }
    } else if (epoch < 0) {
        /* overload this for now */
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_WRONG_STATE);
    } else {
        clnt->snapshot = epoch;
    }
}

/**
 * Cluster here all pre-sqlite parsing, detecting requests that
 * are not handled by sqlite (transaction commands, pragma, stored proc,
 * blocked sql, and so on)
 */
static void sql_update_usertran_state(struct sqlclntstate *clnt)
{
    const char *sql = clnt->sql;

    if (!sql)
        return;

    while (isspace(*sql))
        ++sql;

    /* begin, commit, rollback should arrive over the socket only
       for socksql, recom, snapisol and serial */
    if (!strncasecmp(clnt->sql, "begin", 5)) {
        clnt->snapshot = 0;

        /*fprintf(stderr, "got begin\n");*/
        if (clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS) {
            /* already in a transaction */
            if (clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
                logmsg(LOGMSG_ERROR, "%s CLNT %p I AM ALREADY IN TRANS\n", __func__,
                        clnt);
            } else {
                logmsg(LOGMSG_ERROR, "%s I AM IN TRANS-STATE %d\n", __func__,
                        clnt->ctrl_sqlengine);
            }
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_WRONG_STATE);
        } else {
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_PRE_STRT_STATE);
            clnt->in_client_trans = 1;
            update_snapshot_info(clnt);
        }
    } else if (!strncasecmp(clnt->sql, "commit", 6)) {
        clnt->snapshot = 0;

        if (clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE &&
            clnt->ctrl_sqlengine != SQLENG_STRT_STATE) {
            /* this is for empty transactions */

            /* not in a transaction */
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_WRONG_STATE);
        } else {
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_FNSH_STATE);
            clnt->in_client_trans = 0;
        }
    } else if (!strncasecmp(clnt->sql, "rollback", 8)) {
        clnt->snapshot = 0;

        if (clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE &&
            clnt->ctrl_sqlengine != SQLENG_STRT_STATE)
        /* this is for empty transactions */
        {
            /* not in a transaction */
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_WRONG_STATE);
        } else {
            sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                    SQLENG_FNSH_RBK_STATE);
            clnt->in_client_trans = 0;
        }
    }
}

static void log_queue_time(struct reqlogger *logger, struct sqlclntstate *clnt)
{
    if (!gbl_track_queue_time)
        return;
    if (clnt->deque_timeus - clnt->enque_timeus > 0)
        reqlog_logf(logger, REQL_INFO, "queuetime took %dms",
                    U2M(clnt->deque_timeus - clnt->enque_timeus));
    reqlog_set_queue_time(logger, clnt->deque_timeus - clnt->enque_timeus);
}

static void log_cost(struct reqlogger *logger, int64_t cost, int64_t rows) {
    reqlog_set_cost(logger, cost);
    reqlog_set_rows(logger, rows);
}

/*
  Check and print the client context messages.
*/
static void log_client_context(struct reqlogger *logger,
                               struct sqlclntstate *clnt)
{
    if (clnt->sql_query == NULL) return;

    if (clnt->sql_query->n_context > 0) {
        int i = 0;
        while (i < clnt->sql_query->n_context) {
            reqlog_logf(logger, REQL_INFO, "(%d) %s", ++i,
                        clnt->sql_query->context[i]);
        }
    }

    /* If request context is set, the client is changing the context. */
    if (clnt->sql_query->context) {
        /* Latch the context - client only re-sends context if
           it changes.  TODO: this seems needlessly expensive. */
        clnt->ncontext = clnt->sql_query->n_context;
        if (clnt->context) free(clnt->context);
        clnt->context = malloc(sizeof(char *) * clnt->sql_query->n_context);
        for (int i = 0; i < clnt->sql_query->n_context; i++)
            clnt->context[i] = strdup(clnt->sql_query->context[i]);
    }
    /* Whether latched from previous run, or just set, pass this to logger. */
    reqlog_set_context(logger, clnt->ncontext, clnt->context);
}

/* begin; send return code */
int handle_sql_begin(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                     int sendresponse)
{
    struct fsqlresp resp;
    struct column_info cinfo;
    uint8_t coldata[COLUMN_INFO_LEN];
    uint8_t *p_buf_colinfo = coldata;
    uint8_t *p_buf_colinfo_end = (p_buf_colinfo + COLUMN_INFO_LEN);

    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->ready_for_heartbeats = 0;

    reqlog_new_sql_request(thd->logger, clnt->sql);
    log_queue_time(thd->logger, clnt);

    /* this is a good "begin", just say "ok" */
    sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_STRT_STATE);

    /* clients don't expect column data if it's a converted request */
    reqlog_logf(thd->logger, REQL_QUERY, "\"%s\" new transaction\n",
                (clnt->sql) ? clnt->sql : "(???.)");

    /* clients expect COLUMN DATA, grrr */
    bzero(&cinfo, sizeof(cinfo));
    cinfo.type = SQLITE_INTEGER;
    strcpy(cinfo.column_name, "dummy");

    if (clnt->osql.replay)
        goto done;

    bzero(&resp, sizeof(resp));
    resp.response = FSQL_COLUMN_DATA;
    resp.parm = 1;

    if (!(column_info_put(&cinfo, p_buf_colinfo, p_buf_colinfo_end))) {
        logmsg(LOGMSG_ERROR, "%s line %d: column_info_put failed??\n", __func__,
                __LINE__);
        return SQLITE_INTERNAL;
    }
    if (sendresponse && !clnt->is_newsql)
        fsql_write_response(clnt, &resp, coldata, sizeof(cinfo), 0, __func__,
                            __LINE__);

    bzero(&resp, sizeof(resp));
    resp.response = FSQL_DONE;

    if (sendresponse && !clnt->is_newsql)
        fsql_write_response(clnt, &resp, NULL, 0, 1, __func__, __LINE__);

    if (sendresponse && clnt->is_newsql) {
        clnt->num_retry = clnt->sql_query->retry;
        newsql_send_dummy_resp(clnt, __func__, __LINE__);
        newsql_send_last_row(clnt, 1, __func__, __LINE__);
    }
#if 0
   fprintf( stderr, "%p (U) begin transaction %d %d\n", clnt, pthread_self(), clnt->dbtran.mode);
#endif

/*clnt->ready_for_heartbeats = 1;*/
done:
    pthread_mutex_unlock(&clnt->wait_mutex);

    if (srs_tran_add_query(clnt))
        logmsg(LOGMSG_ERROR, "Fail to create a transaction replay session\n");

    reqlog_end_request(thd->logger, -1, __func__, __LINE__);

    return SQLITE_OK;
}

static int handle_sql_wrongstate(struct sqlthdstate *thd,
                                 struct sqlclntstate *clnt)
{

    struct fsqlresp resp;
    char *errstr = "sqlinterfaces: wrong sql handle state\n";

    sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_NORMAL_PROCESS);

    reqlog_new_sql_request(thd->logger, clnt->sql);
    log_queue_time(thd->logger, clnt);

    reqlog_logf(thd->logger, REQL_QUERY,
                "\"%s\" wrong transaction command receive\n",
                (clnt->sql) ? clnt->sql : "(???.)");

    /* send error */
    bzero(&resp, sizeof(resp));
    resp.response = FSQL_ERROR;
    resp.rcode = CDB2ERR_BADSTATE;
    fsql_write_response(clnt, &resp, (void *)errstr, strlen(errstr) + 1, 1,
                        __func__, __LINE__);

    if (srs_tran_destroy(clnt))
        logmsg(LOGMSG_ERROR, "Fail to destroy transaction replay session\n");

    clnt->intrans = 0;
    reset_clnt_flags(clnt);

    reqlog_end_request(thd->logger, -1, __func__, __LINE__);

    return SQLITE_INTERNAL;
}

void reset_query_effects(struct sqlclntstate *clnt)
{
    bzero(&clnt->effects, sizeof(clnt->effects));
    bzero(&clnt->log_effects, sizeof(clnt->effects));
}

static int send_query_effects(struct sqlclntstate *clnt)
{
    /* Send this only after commit, or when non-transactional query is complete.
     */
    struct fsqlresp effects_resp;
    effects_resp.response = FSQL_QUERY_EFFECTS;
    effects_resp.flags = 0;
    effects_resp.rcode = 0;
    effects_resp.followlen = sizeof(struct query_effects);

    struct query_effects q_effects;

    clnt->effects.num_affected = clnt->effects.num_updated +
                                 clnt->effects.num_deleted +
                                 clnt->effects.num_inserted;
    clnt->log_effects.num_affected = clnt->log_effects.num_updated +
                                     clnt->log_effects.num_deleted +
                                     clnt->log_effects.num_inserted;

    q_effects.num_affected = ntohl(clnt->effects.num_affected);
    q_effects.num_selected = ntohl(clnt->effects.num_selected);
    q_effects.num_updated = ntohl(clnt->effects.num_updated);
    q_effects.num_deleted = ntohl(clnt->effects.num_deleted);
    q_effects.num_inserted = ntohl(clnt->effects.num_inserted);

    return fsql_write_response(clnt, &effects_resp, &q_effects,
                               sizeof(q_effects), 1, __func__, __LINE__);
}

static char *sqlenginestate_tostr(int state)
{
    switch (state) {
    case SQLENG_NORMAL_PROCESS:
        return "SQLENG_NORMAL_PROCESS";
        break;
    case SQLENG_PRE_STRT_STATE:
        return "SQLENG_PRE_STRT_STATE";
        break;
    case SQLENG_STRT_STATE:
        return "SQLENG_STRT_STATE";
        break;
    case SQLENG_INTRANS_STATE:
        return "SQLENG_INTRANS_STATE";
        break;
    case SQLENG_FNSH_STATE:
        return "SQLENG_FNSH_STATE";
        break;
    case SQLENG_FNSH_RBK_STATE:
        return "SQLENG_FNSH_RBK_STATE";
        break;
    case SQLENG_WRONG_STATE:
        return "SQLENG_WRONG_STATE";
        break;
    default:
        return "???";
    }
}

int handle_sql_commitrollback(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt, int sendresponse)
{
    int rcline = 0;
    struct fsqlresp resp;
    struct column_info cinfo;
    int bdberr = 0;
    int rc = 0;
    rcline = __LINE__;
    int irc = 0;
    uint8_t coldata[COLUMN_INFO_LEN];
    uint8_t *p_buf_colinfo = coldata;
    uint8_t *p_buf_colinfo_end = (p_buf_colinfo + COLUMN_INFO_LEN);
    int outrc = 0;

    reqlog_new_sql_request(thd->logger, clnt->sql);
    log_queue_time(thd->logger, clnt);

    int64_t rows = clnt->log_effects.num_updated +
               clnt->log_effects.num_deleted +
               clnt->log_effects.num_inserted;

    reqlog_set_cost(thd->logger, 0);
    reqlog_set_rows(thd->logger, rows);


    if (!clnt->intrans) {
        reqlog_logf(thd->logger, REQL_QUERY, "\"%s\" ignore (no transaction)\n",
                    (clnt->sql) ? clnt->sql : "(???.)");

        rc = SQLITE_OK;
        rcline = __LINE__;
    } else {
        bzero(clnt->dirty, sizeof(clnt->dirty));

        switch (clnt->dbtran.mode) {
        case TRANLEVEL_RECOM: {
            /* here we handle the communication with bp */
            if (clnt->ctrl_sqlengine == SQLENG_FNSH_STATE) {
                rc = recom_commit(clnt, thd->sqlthd, clnt->tzname, 0);
                rcline = __LINE__;
                /* if a transaction exists
                   (it doesn't for empty begin/commit */
                if (clnt->dbtran.shadow_tran) {
                    if (rc == SQLITE_OK) {
                        irc = trans_commit_shadow(clnt->dbtran.shadow_tran,
                                                  &bdberr);

                        reqlog_logf(thd->logger, REQL_QUERY,
                                    "\"%s\" RECOM commit irc=%d rc=%d\n",
                                    (clnt->sql) ? clnt->sql : "(???.)", irc,
                                    rc);
                    } else {
                        if (rc == SQLITE_TOOBIG) {
                            strncpy(clnt->osql.xerr.errstr,
                                    "transaction too big",
                                    sizeof(clnt->osql.xerr.errstr));
                            rc = 202;
                            rcline = __LINE__;
                        } else if (rc == SQLITE_ABORT) {
                            /* convert this to user code */
                            rc = blockproc2sql_error(clnt->osql.xerr.errval,
                                                     __func__, __LINE__);
                            rcline = __LINE__;
                        }
                        irc = trans_abort_shadow(
                            (void **)&clnt->dbtran.shadow_tran, &bdberr);

                        reqlog_logf(thd->logger, REQL_QUERY,
                                    "\"%s\" RECOM abort irc=%d rc=%d\n",
                                    (clnt->sql) ? clnt->sql : "(???.)", irc,
                                    rc);
                    }
                    if (irc) {
                        logmsg(LOGMSG_ERROR, "%s: failed %s rc=%d bdberr=%d\n",
                                __func__,
                                (rc == SQLITE_OK) ? "commit" : "abort", irc,
                                bdberr);
                    }
                }
            } else {
                reset_query_effects(clnt);
                rc = recom_abort(clnt);
                rcline = __LINE__;
                if (rc)
                    logmsg(LOGMSG_ERROR, "%s: recom abort failed %d??\n", __func__,
                            rc);
                reqlog_logf(thd->logger, REQL_QUERY,
                            "\"%s\" RECOM abort(2) irc=%d rc=%d\n",
                            (clnt->sql) ? clnt->sql : "(???.)", irc, rc);
            }

            break;
        }

        case TRANLEVEL_SNAPISOL:
        case TRANLEVEL_SERIAL: {

            /* here we handle the communication with bp */
            if (clnt->ctrl_sqlengine == SQLENG_FNSH_STATE) {
                if (clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                    rc = serial_commit(clnt, thd->sqlthd, clnt->tzname);
                    rcline = __LINE__;
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER, 
                                "td=%u serial-txn %s line %d returns %d\n",
                                pthread_self(), __func__, __LINE__, rc);
                    }
                } else {
                    rc = snapisol_commit(clnt, thd->sqlthd, clnt->tzname);
                    rcline = __LINE__;
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_ERROR, 
                                "td=%u snapshot-txn %s line %d returns %d\n",
                                pthread_self(), __func__, __LINE__, rc);
                    }
                }
                /* if a transaction exists
                   (it doesn't for empty begin/commit */
                if (clnt->dbtran.shadow_tran) {
                    if (rc == SQLITE_OK) {
                        irc = trans_commit_shadow(clnt->dbtran.shadow_tran,
                                                  &bdberr);

                        reqlog_logf(thd->logger, REQL_QUERY,
                                    "\"%s\" %s commit irc=%d rc=%d\n",
                                    (clnt->sql) ? clnt->sql : "(???.)",
                                    (clnt->dbtran.mode == TRANLEVEL_SERIAL)
                                        ? "SERIAL"
                                        : "SNAPISOL",
                                    irc, rc);
                    } else {
                        if (rc == SQLITE_TOOBIG) {
                            strncpy(clnt->osql.xerr.errstr,
                                    "transaction too big",
                                    sizeof(clnt->osql.xerr.errstr));
                            rc = 202;
                            rcline = __LINE__;
                        } else if (rc == SQLITE_ABORT) {
                            /* convert this to user code */
                            rc = blockproc2sql_error(clnt->osql.xerr.errval,
                                                     __func__, __LINE__);
                            rcline = __LINE__;
                            if (gbl_extended_sql_debug_trace) {
                                logmsg(LOGMSG_USER, "td=%u %s line %d returning "
                                                "converted-rc %d\n",
                                        pthread_self(), __func__, __LINE__, rc);
                            }
                        } else if (rc == SQLITE_CLIENT_CHANGENODE) {
                            rc = clnt->high_availability
                                     ? CDB2ERR_CHANGENODE
                                     : SQLHERR_MASTER_TIMEOUT;
                        }
                        irc = trans_abort_shadow(
                            (void **)&clnt->dbtran.shadow_tran, &bdberr);

                        reqlog_logf(thd->logger, REQL_QUERY,
                                    "\"%s\" %s abort irc=%d rc=%d\n",
                                    (clnt->sql) ? clnt->sql : "(???.)",
                                    (clnt->dbtran.mode == TRANLEVEL_SERIAL)
                                        ? "SERIAL"
                                        : "SNAPISOL",
                                    irc, rc);
                    }
                    if (irc) {
                        logmsg(LOGMSG_ERROR, "%s: failed %s rc=%d bdberr=%d\n",
                                __func__,
                                (rc == SQLITE_OK) ? "commit" : "abort", irc,
                                bdberr);
                    }
                } else {
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER, 
                                "td=%u no-shadow-tran %s line %d, returning %d\n",
                                pthread_self(), __func__, __LINE__, rc);
                    }
                    if (rc == SQLITE_ABORT) {
                        rc = blockproc2sql_error(clnt->osql.xerr.errval,
                                                 __func__, __LINE__);
                        logmsg(LOGMSG_ERROR, "td=%u no-shadow-tran %s line %d, returning %d\n",
                            pthread_self(), __func__, __LINE__, rc);
                    } else if (rc == SQLITE_CLIENT_CHANGENODE) {
                        rc = clnt->high_availability ? CDB2ERR_CHANGENODE
                                                     : SQLHERR_MASTER_TIMEOUT;
                        logmsg(LOGMSG_ERROR, 
                                "td=%u no-shadow-tran %s line %d, returning %d\n",
                                pthread_self(), __func__, __LINE__, rc);
                    }
                }
            } else {
                reset_query_effects(clnt);
                if (clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                    rc = serial_abort(clnt);
                    rcline = __LINE__;
                } else {
                    rc = snapisol_abort(clnt);
                    rcline = __LINE__;
                }
                if (rc)
                    logmsg(LOGMSG_ERROR, "%s: serial abort failed %d??\n", __func__,
                            rc);

                reqlog_logf(thd->logger, REQL_QUERY,
                            "\"%s\" %s abort(2) irc=%d rc=%d\n",
                            (clnt->sql) ? clnt->sql : "(???.)",
                            (clnt->dbtran.mode == TRANLEVEL_SERIAL)
                                ? "SERIAL"
                                : "SNAPISOL",
                            irc, rc);
            }

            if (clnt->arr) {
                currangearr_free(clnt->arr);
                clnt->arr = NULL;
            }
            if (clnt->selectv_arr) {
                currangearr_free(clnt->selectv_arr);
                clnt->selectv_arr = NULL;
            }

            break;
        }

        case TRANLEVEL_SOSQL:

            if (clnt->ctrl_sqlengine == SQLENG_FNSH_RBK_STATE) {
                /* user cancelled the transaction */
                clnt->osql.xerr.errval = SQLITE_INTERNAL;
                /* this will cancel the bp tran */

                reqlog_logf(
                    thd->logger, REQL_QUERY, "\"%s\" SOCKSL abort replay=%d\n",
                    (clnt->sql) ? clnt->sql : "(???.)", clnt->osql.replay);
            }
            if (clnt->ctrl_sqlengine == SQLENG_FNSH_STATE) {
                if (gbl_selectv_rangechk) {
                    rc = selectv_range_commit(clnt);
                    rcline = __LINE__;
                }
                if (rc || clnt->early_retry) {
                    int irc = 0;
                    irc = osql_sock_abort(clnt, OSQL_SOCK_REQ);
                    if (irc) {
                        logmsg(LOGMSG_ERROR, 
                                "%s: failed to abort sorese transactin irc=%d\n",
                                __func__, irc);
                    }
                    if (clnt->early_retry) {
                        clnt->osql.xerr.errval = ERR_BLOCK_FAILED + ERR_VERIFY;
                        clnt->early_retry = 0;
                        rc = SQLITE_ABORT;
                    }
                } else {
                    rc = osql_sock_commit(clnt, OSQL_SOCK_REQ);
                    rcline = __LINE__;
                }
                if (rc == SQLITE_ABORT) {
                    /* convert this to user code */
                    rc = blockproc2sql_error(clnt->osql.xerr.errval, __func__,
                                             __LINE__);
                    rcline = __LINE__;
                    if (clnt->osql.xerr.errval == ERR_UNCOMMITABLE_TXN) {
                        osql_set_replay(__FILE__, __LINE__, clnt,
                                        OSQL_RETRY_LAST);
                    }

                    reqlog_logf(thd->logger, REQL_QUERY,
                                "\"%s\" SOCKSL failed commit rc=%d replay=%d\n",
                                (clnt->sql) ? clnt->sql : "(???.)", rc,
                                clnt->osql.replay);
                } else if (rc == 0) {
                    reqlog_logf(thd->logger, REQL_QUERY,
                                "\"%s\" SOCKSL commit rc=%d replay=%d\n",
                                (clnt->sql) ? clnt->sql : "(???.)", rc,
                                clnt->osql.replay);
                }

                if (rc) {
                    clnt->had_errors = 1;
                    clnt->saved_rc = rc;
                    if (clnt->saved_errstr)
                        free(clnt->saved_errstr);
                    clnt->saved_errstr = strdup(clnt->osql.xerr.errstr);
                }
            } else {
                reset_query_effects(clnt);
                rc = osql_sock_abort(clnt, OSQL_SOCK_REQ);
                rcline = __LINE__;

                reqlog_logf(thd->logger, REQL_QUERY,
                            "\"%s\" SOCKSL abort(2) rc=%d replay=%d\n",
                            (clnt->sql) ? clnt->sql : "(???.)", rc,
                            clnt->osql.replay);
            }

            if (clnt->selectv_arr) {
                currangearr_free(clnt->selectv_arr);
                clnt->selectv_arr = NULL;
            }

            break;
        }
    }

    /* reset the state after send_done; we use ctrl_sqlengine to know
       if this is a user rollback or an sqlite engine error */
    sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_NORMAL_PROCESS);

/* we are out of transaction, mark this here */
#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        logmsg(LOGMSG_USER, "%p (U) commits transaction %d %d intran=%d\n", clnt,
                pthread_self(), clnt->dbtran.mode, clnt->intrans);
    }
#endif

    clnt->intrans = 0;
    clnt->dbtran.shadow_tran = NULL;

    if (rc == SQLITE_OK) {
        /* send return code */

        if (!clnt->is_newsql && clnt->want_query_effects) {
            rc = send_query_effects(clnt);
            rcline = __LINE__;
            reset_query_effects(clnt);
        }

        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;

        bzero(&cinfo, sizeof(cinfo));
        cinfo.type = SQLITE_INTEGER;
        strcpy(cinfo.column_name, "dummy");

        bzero(&resp, sizeof(resp));
        resp.response = FSQL_COLUMN_DATA;
        resp.parm = 1;

        if (!(column_info_put(&cinfo, p_buf_colinfo, p_buf_colinfo_end))) {
            logmsg(LOGMSG_ERROR, "%s line %d: column_info_put failed???\n", __func__,
                    __LINE__);
        }

        if (sendresponse && !clnt->is_newsql) {
            int retryflags;
            /* This is a a commit, so we'll have something to send
             * here even on a retry.  Don't trigger code in
             * fsql_write_response
             * that's there to catch bugs when we send back responses
             * on a retry. */
            retryflags = clnt->osql.replay;
            clnt->osql.replay = OSQL_RETRY_NONE;
            fsql_write_response(clnt, &resp, &coldata, sizeof(cinfo), 0,
                                __func__, __LINE__);
            clnt->osql.replay = retryflags;
        } else if (sendresponse) {
            clnt->want_query_effects = 0;
            clnt->num_retry = 0;
            newsql_send_dummy_resp(clnt, __func__, __LINE__);
        }

        bzero(&resp, sizeof(resp));
        resp.response = FSQL_DONE;

        if (sendresponse && !clnt->is_newsql) {
            int retryflags;
            retryflags = clnt->osql.replay;
            clnt->osql.replay = OSQL_RETRY_NONE;
            fsql_write_response(clnt, &resp, NULL, 0, 1, __func__, __LINE__);
            clnt->osql.replay = retryflags;
        } else if (sendresponse) {
            newsql_send_last_row(clnt, 0, __func__, __LINE__);
        }

        outrc = SQLITE_OK; /* the happy case */

        pthread_mutex_unlock(&clnt->wait_mutex);

        if (clnt->osql.replay != OSQL_RETRY_NONE) {
            /* successful retry */
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);

            reqlog_logf(thd->logger, REQL_QUERY,
                        "\"%s\" SOCKSL retried done sendresp=%d\n",
                        (clnt->sql) ? clnt->sql : "(???.)", sendresponse);
        }
    } else {
        /* error */

        /* if this is a verify error and we are not running in
           snapshot/serializable mode, repeat this request ad nauseam
           (Alex and Sam made me do it) */
        if (rc == CDB2ERR_VERIFY_ERROR &&
            clnt->dbtran.mode != TRANLEVEL_SNAPISOL &&
            clnt->dbtran.mode != TRANLEVEL_SERIAL &&
            clnt->osql.replay != OSQL_RETRY_LAST && !clnt->has_recording &&
            (clnt->verifyretry_off == 0)) {
            if (srs_tran_add_query(clnt))
                logmsg(LOGMSG_USER, 
                        "Fail to add commit to transaction replay session\n");

            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_DO);

            reqlog_logf(thd->logger, REQL_QUERY, "\"%s\" SOCKSL retrying\n",
                        (clnt->sql) ? clnt->sql : "(???.)");

            outrc = SQLITE_OK; /* logical error */
            goto done;
        }

        /* last retry */
        if (rc == CDB2ERR_VERIFY_ERROR &&
            (clnt->osql.replay == OSQL_RETRY_LAST || clnt->verifyretry_off)) {
            reqlog_logf(thd->logger, REQL_QUERY,
                        "\"%s\" SOCKSL retried done (hit last) sendresp=%d\n",
                        (clnt->sql) ? clnt->sql : "(???.)", sendresponse);
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
        }
        /* if this is still an error, but not verify, pass it back to client */
        else if (rc != DB_ERR_TRN_VERIFY) {
            reqlog_logf(thd->logger, REQL_QUERY, "\"%s\" SOCKSL retried done "
                                                 "(non verify error rc=%d) "
                                                 "sendresp=%d\n",
                        (clnt->sql) ? clnt->sql : "(???.)", rc, sendresponse);
            osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
        }

        bzero(&resp, sizeof(resp));

        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        pthread_mutex_unlock(&clnt->wait_mutex);

        resp.response = FSQL_ERROR;
        resp.rcode = rc;

        outrc = rc;

        if (sendresponse) {
            int retryflags = clnt->osql.replay;
            clnt->osql.replay = OSQL_RETRY_NONE;
            rc = fsql_write_response(clnt, &resp, clnt->osql.xerr.errstr,
                                     sizeof(clnt->osql.xerr.errstr), 1,
                                     __func__, __LINE__);
            clnt->osql.replay = retryflags;
        }
    }

    /* if this is a retry, let the upper layer free the structure */
    if (clnt->osql.replay == OSQL_RETRY_NONE) {
        if (srs_tran_destroy(clnt))
            logmsg(LOGMSG_ERROR, "Fail to destroy transaction replay session\n");
    }

done:

    reset_clnt_flags(clnt);

    reqlog_end_request(thd->logger, -1, __func__, __LINE__);
    return outrc;
}

static int strcmpfunc_stmt(char *a, char *b, int len) { return strcmp(a, b); }

static u_int strhashfunc_stmt(u_char *keyp, int len)
{
    unsigned hash;
    int jj;
    u_char *key = keyp;
    for (hash = 0; *key; key++)
        hash = ((hash % 8388013) << 8) + ((*key));
    return hash;
}

static int finalize_stmt_hash(void *stmt_entry, void *args)
{
    stmt_hash_entry_type *entry = (stmt_hash_entry_type *)stmt_entry;
    sqlite3_finalize(entry->stmt);
    if (entry->params_to_bind) {
        free_tag_schema(entry->params_to_bind);
    }
    if (entry->query && gbl_debug_temptables) {
        free(entry->query);
        entry->query = NULL;
    }
    sqlite3_free(entry);
    return 0;
}

static void delete_stmt_table(hash_t *stmt_table)
{
    /* parse through hash table and finalize all the statements */
    hash_for(stmt_table, finalize_stmt_hash, NULL);
    hash_clear(stmt_table);
    hash_free(stmt_table);
}

static void init_stmt_table(hash_t **stmt_table)
{
    *stmt_table =
        hash_init_user((hashfunc_t *)strhashfunc_stmt,
                       (cmpfunc_t *)strcmpfunc_stmt, 0, MAX_HASH_SQL_LENGTH);
}

void touch_stmt_entry(struct sqlthdstate *thd, stmt_hash_entry_type *entry)
{
    stmt_hash_entry_type **tail = NULL;
    stmt_hash_entry_type **head = NULL;

    if (entry->params_to_bind) {
        tail = &thd->param_stmt_tail;
        head = &thd->param_stmt_head;
    } else {
        tail = &thd->noparam_stmt_tail;
        head = &thd->noparam_stmt_head;
    }

    stmt_hash_entry_type *prev_entry = entry->prev;
    stmt_hash_entry_type *next_entry = entry->next;

    if (prev_entry != NULL) {
        prev_entry->next = next_entry;
    } else {
        /* This is already the first entry. */
        return;
    }

    if (next_entry != NULL) {
        next_entry->prev = prev_entry;
    } else {
        /* This means this is the end of list.
            set the new tail. */
        (*tail) = prev_entry;
    }
    entry->next = (*head);
    entry->prev = NULL;
    (*head)->prev = entry;
    (*head) = entry;
}

static void delete_last_stmt_entry(struct sqlthdstate *thd,
                                   struct schema *params_to_bind)
{
    stmt_hash_entry_type **tail = NULL;
    if (params_to_bind) {
        tail = &thd->param_stmt_tail;
    } else {
        tail = &thd->noparam_stmt_tail;
    }
    int has_params = 0;
    hash_t *stmt_table = thd->stmt_table;
    stmt_hash_entry_type *entry = (*tail)->prev;
    if ((*tail)->query && gbl_debug_temptables) {
        free((*tail)->query);
        (*tail)->query = NULL;
    }
    sqlite3_finalize((*tail)->stmt);
    if ((*tail)->params_to_bind) {
        has_params = 1;
        free_tag_schema((*tail)->params_to_bind);
    }
    hash_del(stmt_table, (*tail)->sql);
    entry->next = NULL;
    sqlite3_free(*tail);
    *tail = entry;
    if (has_params) {
        thd->param_cache_entries--;
    } else {
        thd->noparam_cache_entries--;
    }
}

int add_stmt_table(struct sqlthdstate *thd, const char *sql, char *actual_sql,
                   sqlite3_stmt *stmt, struct schema *params_to_bind)
{
    int ret = -1;
    int len = strlen(sql);

    stmt_hash_entry_type **tail = NULL;
    stmt_hash_entry_type **head = NULL;
    if (params_to_bind) {
        if (thd->param_cache_entries > gbl_max_sqlcache)
            delete_last_stmt_entry(thd, params_to_bind);
        tail = &thd->param_stmt_tail;
        head = &thd->param_stmt_head;
    } else {
        if (thd->noparam_cache_entries > gbl_max_sqlcache)
            delete_last_stmt_entry(thd, params_to_bind);
        tail = &thd->noparam_stmt_tail;
        head = &thd->noparam_stmt_head;
    }

    if (len < MAX_HASH_SQL_LENGTH) {
        stmt_hash_entry_type *entry =
            sqlite3_malloc(sizeof(stmt_hash_entry_type));
        strcpy(entry->sql, sql);
        entry->stmt = stmt;
        entry->params_to_bind = params_to_bind;
        if (actual_sql && gbl_debug_temptables)
            entry->query = strdup(actual_sql);
        else
            entry->query = NULL;
        ret = hash_add(thd->stmt_table, entry);
        if (ret == 0) {
            if ((*head) == NULL) {
                entry->prev = NULL;
                entry->next = NULL;
                (*head) = entry;
                (*tail) = entry;
            } else {
                (*head)->prev = entry;
                entry->next = (*head);
                entry->prev = NULL;
                (*head) = entry;
            }
            if (params_to_bind) {
                thd->param_cache_entries++;
            } else {
                thd->noparam_cache_entries++;
            }
        }
    } else {
        sqlite3_finalize(stmt);
    }
    return ret;
}

int find_stmt_table(hash_t *stmt_table, const char *sql,
                    stmt_hash_entry_type **entry)
{
    if (strlen(sql) < MAX_HASH_SQL_LENGTH) {
        *entry = hash_find(stmt_table, sql);
        if (*entry)
            return 0;
    }
    return -1;
}

static const char *osqlretrystr(int i)
{
    switch (i) {
    case OSQL_RETRY_NONE:
        return "OSQL_RETRY_NONE";
    case OSQL_RETRY_DO:
        return "OSQL_RETRY_DO";
    case OSQL_RETRY_LAST:
        return "OSQL_RETRY_LAST";
    default:
        return "???";
    }
}

/** Table which stores sql strings and sql hints
  * We will hit this table if the thread running the queries from
  * certain sql control changes.
  **/

lrucache *sql_hints = NULL;

/* sql_hint/sql_str/tag all point to mem (tag can also be NULL) */
typedef struct {
    char *sql_hint;
    char *sql_str;
    char *tag;
    lrucache_link lnk;
    char mem[1];
} sql_hint_hash_entry_type;

void delete_sql_hint_table() { lrucache_destroy(sql_hints); }

static unsigned int sqlhint_hash(const void *p, int len)
{
    unsigned char *s;
    unsigned h = 0;

    memcpy(&s, p, sizeof(char *));

    while (*s) {
        h = ((h % 8388013) << 8) + (*s);
        s++;
    }
    return h;
}

int sqlhint_cmp(const void *key1, const void *key2, int len)
{
    char *s1, *s2;
    memcpy(&s1, key1, sizeof(char *));
    memcpy(&s2, key2, sizeof(char *));
    return strcmp((char *)s1, (char *)s2);
}

void init_sql_hint_table()
{
    sql_hints = lrucache_init(sqlhint_hash, sqlhint_cmp, free,
                              offsetof(sql_hint_hash_entry_type, lnk),
                              offsetof(sql_hint_hash_entry_type, sql_hint),
                              sizeof(char *), gbl_max_sql_hint_cache);
}

void reinit_sql_hint_table()
{
    pthread_mutex_lock(&gbl_sql_lock);
    {
        delete_sql_hint_table();
        init_sql_hint_table();
    }
    pthread_mutex_unlock(&gbl_sql_lock);
}

static int add_sql_hint_table(char *sql_hint, char *sql_str, char *tag)
{
    int ret = -1;
    int len;
    int sql_hint_len, sql_len, tag_len = 0;
    sql_hint_hash_entry_type *entry;

    sql_hint_len = strlen(sql_hint) + 1;
    sql_len = strlen(sql_str) + 1;
    if (tag)
        tag_len = strlen(tag) + 1;
    len = sql_hint_len + sql_len + tag_len;

    entry = malloc(offsetof(sql_hint_hash_entry_type, mem) + len);
    memcpy(entry->mem, sql_hint, sql_hint_len);
    entry->sql_hint = entry->mem;
    memcpy(entry->mem + sql_hint_len, sql_str, sql_len);
    entry->sql_str = entry->mem + sql_hint_len;
    if (tag) {
        memcpy(entry->mem + sql_hint_len + sql_len, tag, tag_len);
        entry->tag = entry->mem + sql_hint_len + sql_len;
    } else {
        entry->tag = NULL;
    }

    pthread_mutex_lock(&gbl_sql_lock);
    {
        if (lrucache_hasentry(sql_hints, &sql_hint) == 0) {
            lrucache_add(sql_hints, entry);
        } else {
            free(entry);
            logmsg(LOGMSG_ERROR, "Client BUG: Two threads using same SQL tag.\n");
        }
    }
    pthread_mutex_unlock(&gbl_sql_lock);

    return ret;
}

static int find_sql_hint_table(char *sql_hint, char **sql_str, char **tag)
{
    sql_hint_hash_entry_type *entry;
    pthread_mutex_lock(&gbl_sql_lock);
    {
        entry = lrucache_find(sql_hints, &sql_hint);
    }
    pthread_mutex_unlock(&gbl_sql_lock);
    if (entry) {
        *sql_str = entry->sql_str;
        *tag = entry->tag;
        return 0;
    }
    return -1;
}

static int has_sql_hint_table(char *sql_hint)
{
    int ret;
    pthread_mutex_lock(&gbl_sql_lock);
    {
        ret = lrucache_hasentry(sql_hints, &sql_hint);
    }
    pthread_mutex_unlock(&gbl_sql_lock);
    return ret;
}

#define SQLCACHEHINT "/*+ RUNCOMDB2SQL"

int has_sqlcache_hint(const char *sql, const char **pstart, const char **pend)
{
    char *start, *end;
    start = strstr(sql, SQLCACHEHINT);
    if (pstart)
        *pstart = start;
    if (start) {
        end = strstr(start, " */");
        if (pend)
            *pend = end;
        if (end) {
            end += 3;
            if (pend)
                *pend = end;
            return 1;
        }
    }
    return 0;
}

int extract_sqlcache_hint(const char *sql, char *hint, int *hintlen)
{
    const char *start = NULL;
    const char *end = NULL;
    int length;
    int ret;

    ret = has_sqlcache_hint(sql, &start, &end);

    if (ret) {
        length = end - start;
        if (length >= *hintlen) {
            logmsg(LOGMSG_WARN, "Query has very long hint! \"%s\"\n", sql);
            length = *hintlen - 1;
        }
        strncpy(hint, start, length);
        hint[length] = '\0';
    }
    return ret;
}

/* Execute the query.  Caller should flush the sbuf when this returns.
 * Returns -1 if we should abort the client connection, 0 otherwise.
 * We handle the verify comming during commit phase from the master
 */

pthread_key_t current_sql_query_key;
pthread_once_t current_sql_query_once = PTHREAD_ONCE_INIT;

void free_sql(void *p) { free(p); }

void init_current_current_sql_key(void)
{
    int rc;
    rc = pthread_key_create(&current_sql_query_key, free_sql);
    if (rc) {
        logmsg(LOGMSG_FATAL, "pthread_key_create current_sql_query_key rc %d\n", rc);
        exit(1);
    }
}

extern int gbl_debug_temptables;

static const char *type_to_typestr(int type)
{
    switch (type) {
    case SQLITE_NULL:
        return "null";
    case SQLITE_INTEGER:
        return "integer";
    case SQLITE_FLOAT:
        return "real";
    case SQLITE_TEXT:
        return "text";
    case SQLITE_BLOB:
        return "blob";
    case SQLITE_DATETIME:
        return "datetime";
    case SQLITE_DATETIMEUS:
        return "datetimeus";
    case SQLITE_INTERVAL_YM:
        return "year";
    case SQLITE_INTERVAL_DSUS:
    case SQLITE_INTERVAL_DS:
        return "day";
    default:
        return "???";
    }
}

static int typestr_to_type(const char *ctype)
{
    if (ctype == NULL)
        return SQLITE_TEXT;
    if ((strcmp("smallint", ctype) == 0) || (strcmp("int", ctype) == 0) ||
        (strcmp("largeint", ctype) == 0))
        return SQLITE_INTEGER;
    else if ((strcmp("smallfloat", ctype) == 0) ||
             (strcmp("float", ctype) == 0))
        return SQLITE_FLOAT;
    else if (strncmp("char", ctype, 4) == 0)
        return SQLITE_TEXT;
    else if (strncmp("blob", ctype, 4) == 0)
        return SQLITE_BLOB;
    else if (strncmp("datetimeus", ctype, 9) == 0)
        return SQLITE_DATETIMEUS;
    else if (strncmp("datetime", ctype, 8) == 0)
        return SQLITE_DATETIME;
    else if (strstr(ctype, "year") || strstr(ctype, "month"))
        return SQLITE_INTERVAL_YM;
    else if (strstr(ctype, "day") || strstr(ctype, "sec"))
        return SQLITE_INTERVAL_DS;
    else {
        return SQLITE_TEXT;
    }
}

static int is_with_statement(char *sql)
{
    if (!sql)
        return 0;
    sql = skipws(sql);
    if (strncasecmp(sql, "with", 4) == 0)
        return 1;
    return 0;
}

static void compare_estimate_cost(sqlite3_stmt *stmt)
{
    int showScanStat =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_PLANNER_SHOW_SCANSTATS);
#define MAX_DISC_SHOW 30
    int i, k, n, mx;
    if (showScanStat)
        logmsg(LOGMSG_USER, "-------- scanstats --------\n");
    mx = 0;
    for (k = 0; k <= mx; k++) {
        double rEstLoop = 1.0;
        struct {
            double rEst;
            double rActual;
            double delta;
            int isSignificant;
        } discrepancies[MAX_DISC_SHOW] = {0};
        int hasDiscrepancy = 0;

        for (i = n = 0; 1; i++) {
            sqlite3_int64 nLoop, nVisit;
            double rEst;
            int iSid;
            const char *zExplain;
            if (sqlite3_stmt_scanstatus(stmt, i, SQLITE_SCANSTAT_NLOOP,
                                        (void *)&nLoop)) {
                break;
            }
            sqlite3_stmt_scanstatus(stmt, i, SQLITE_SCANSTAT_SELECTID,
                                    (void *)&iSid);
            if (iSid > mx)
                mx = iSid;
            if (iSid != k)
                continue;
            if (n == 0) {
                rEstLoop = (double)nLoop;
                if (k > 0)
                    if (showScanStat)
                        logmsg(LOGMSG_USER, "-------- subquery %d -------\n", k);
            }
            sqlite3_stmt_scanstatus(stmt, i, SQLITE_SCANSTAT_NVISIT,
                                    (void *)&nVisit);
            sqlite3_stmt_scanstatus(stmt, i, SQLITE_SCANSTAT_EST,
                                    (void *)&rEst);
            sqlite3_stmt_scanstatus(stmt, i, SQLITE_SCANSTAT_EXPLAIN,
                                    (void *)&zExplain);

            rEstLoop *= rEst;
            double rActual = nLoop > 0 ? (double)nVisit / nLoop
                                       : 0.0; // actual rows per loop
            double delta = abs(rActual - rEst);

            if (n < MAX_DISC_SHOW) {
                discrepancies[n].rActual = rActual;
                discrepancies[n].rEst = rEst;
                discrepancies[n].delta = delta;
            }
            if ((rActual < 5000 && delta > 10 * rActual) ||
                (rActual >= 5000 && rActual < 10000 && delta > rActual) ||
                (rActual >= 10000 && rActual < 1000000 &&
                 delta > 0.5 * rActual) ||
                (rActual >= 1000000 && delta > 0.1 * rActual)) {
                discrepancies[n].isSignificant = 1;
                hasDiscrepancy++;
            }

            n++;
            if (showScanStat) {
                logmsg(LOGMSG_USER, "Loop %2d: %s\n", n, zExplain);
                logmsg(LOGMSG_USER, "         nLoop=%-8lld nVisit=%-8lld estRowAcc=%-8lld "
                       "rEst=%-8g loopEst=%-8g rAct=%-8g D=%-8g\n",
                       nLoop, nVisit, (sqlite3_int64)(rEstLoop + 0.5), rEst,
                       rEst * nLoop, rActual, delta);
            }
        }

        if (hasDiscrepancy > 0 &&
            bdb_attr_get(thedb->bdb_attr,
                         BDB_ATTR_PLANNER_WARN_ON_DISCREPANCY)) {
            // printf("Problem on:\nLoop    nVisit <> loopEst :: delta\n");
            for (int i = 0; i < n; i++) {
                if (discrepancies[i].isSignificant)
                    logmsg(LOGMSG_USER, "Problem Loop: %-8d rActual: %-8g rEst:%-8g "
                           "Delta:%-8g\n",
                           i, discrepancies[i].rActual, discrepancies[i].rEst,
                           discrepancies[i].delta);
            }
        }
    }
    if (showScanStat)
        logmsg(LOGMSG_USER, "---------------------------\n");
}

void send_prepare_error(struct sqlclntstate *clnt, const char *errstr,
                        int clnt_retry)
{
    struct fsqlresp resp;

    resp.response = FSQL_COLUMN_DATA;
    if (clnt_retry)
        resp.flags = FRESP_FLAG_RETRY; /* Makes client resend the query. */
    else
        resp.flags = 0;
    resp.rcode = FSQL_PREPARE; /* this specific case is handled by
                                  the client, ugh */
    resp.parm = 0;
    /* no packing needed, sending a string */
    fsql_write_response(clnt, &resp, (void *)errstr, strlen(errstr) + 1,
                        1 /*flush*/, __func__, __LINE__);
}

static int reload_analyze(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    // if analyze is running, don't reload
    extern volatile int analyze_running_flag;
    if (analyze_running_flag)
        return 0;
    int rc, got_curtran;
    rc = got_curtran = 0;
    if (!clnt->dbtran.cursor_tran) {
        if ((rc = get_curtran(thedb->bdb_env, clnt)) != 0) {
            logmsg(LOGMSG_ERROR, "%s get_curtran rc:%d\n", __func__, rc);
            return SQLITE_INTERNAL;
        }
        got_curtran = 1;
    }
    if ((rc = sqlite3AnalysisLoad(thd->sqldb, 0)) == SQLITE_OK) {
        thd->analyze_gen = gbl_analyze_gen;
    } else {
        logmsg(LOGMSG_ERROR, "%s sqlite3AnalysisLoad rc:%d\n", __func__, rc);
    }
    if (got_curtran && put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s failed to put_curtran\n", __func__);
    }
    return rc;
}

static void delete_prepared_stmts(struct sqlthdstate *thd)
{
    if (thd->stmt_table) {
        delete_stmt_table(thd->stmt_table);
        thd->stmt_table = NULL;
        thd->param_stmt_head = NULL;
        thd->param_stmt_tail = NULL;
        thd->noparam_stmt_head = NULL;
        thd->noparam_stmt_tail = NULL;
        thd->param_cache_entries = 0;
        thd->noparam_cache_entries = 0;
    }
}

// Call with schema_lk held and no_transaction == 1
int check_thd_gen(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    if (gbl_fdb_track)
        logmsg(LOGMSG_USER, "XXX: thd dbopen=%d vs %d thd analyze %d vs %d\n",
                thd->dbopen_gen, gbl_dbopen_gen, thd->analyze_gen,
                gbl_analyze_gen);

    if (thd->dbopen_gen != gbl_dbopen_gen) {
        return SQLITE_SCHEMA;
    }
    if (thd->analyze_gen != gbl_analyze_gen) {
        int ret;
        delete_prepared_stmts(thd);
        clnt->no_transaction = 1;
        ret = reload_analyze(thd, clnt);
        clnt->no_transaction = 0;
        return ret;
    }

    if (thd->views_gen != gbl_views_gen) {
        return SQLITE_SCHEMA_REMOTE;
    }

    return SQLITE_OK;
}

static int is_snap_uid_retry(struct sqlclntstate *clnt)
{
    if (gbl_extended_sql_debug_trace) {
        char cnonce[256] = {0};
        snprintf(cnonce, clnt->sql_query->cnonce.len + 1, "%s",
                 clnt->sql_query->cnonce.data);
        logmsg(LOGMSG_USER, "Entering %s, cnonce='%s'\n", __func__, cnonce);
    }

    // Retries happen with a 'begin'.  This can't be a retry if we are already
    // in a transaction
    if (clnt->ctrl_sqlengine == SQLENG_STRT_STATE ||
        clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE ||
        clnt->ctrl_sqlengine == SQLENG_PRE_STRT_STATE ||
        clnt->ctrl_sqlengine == SQLENG_FNSH_STATE ||
        clnt->ctrl_sqlengine == SQLENG_FNSH_RBK_STATE) {

        if (gbl_extended_sql_debug_trace) {
            char cnonce[256] = {0};
            snprintf(cnonce, clnt->sql_query->cnonce.len + 1, "%s",
                     clnt->sql_query->cnonce.data);
            logmsg(LOGMSG_USER, "%s line %d cnonce='%s' returning 0 because "
                            "ctrl_sqlengine is %d, "
                            "in_client_trans=%d clnt->snapshot_lsn=[%d][%d]\n",
                    __func__, __LINE__, cnonce, clnt->ctrl_sqlengine,
                    clnt->in_client_trans, clnt->snapshot_file,
                    clnt->snapshot_offset);
        }

        return 0;
    } else if (gbl_extended_sql_debug_trace) {
        char cnonce[256] = {0};
        snprintf(cnonce, clnt->sql_query->cnonce.len + 1, "%s",
                 clnt->sql_query->cnonce.data);
        logmsg(LOGMSG_USER, "%s line %d  cnonce='%s' clearing snapinfo because "
                        "ctrl_sqlengine is %d "
                        "in_client_trans=%d and snapshot_file is %d\n",
                __func__, __LINE__, cnonce, clnt->ctrl_sqlengine,
                clnt->in_client_trans, clnt->snapshot_file);
    }

    // Need to clear snapshot info here: we are not in a transaction.  This code
    // makes sure that snapshot_file is cleared.
    clear_snapshot_info(clnt, __func__, __LINE__);

    // Returning 0 because the retry flag is not lit
    if (!clnt->is_newsql || (clnt->sql_query && clnt->sql_query->retry == 0)) {
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, 
                   "%s line %d returning 0, is_newsql=%d sql_query=%p retry=%d\n",
                   __func__, __LINE__, clnt->is_newsql, clnt->sql_query,
                   clnt->sql_query ? clnt->sql_query->retry : -1);
        }
        return 0;
    } else if (clnt->high_availability == 0) {
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, "%s line %d returning -1, high_availability=0\n");
        }
        return -1;
    }

    /**
     * If this is a retry, then:
     *
     * 1) this should be a BEGIN
     * 2) the retry flag should be set (we only set retry flag on a begin)
     * 3) we *could* have a valid snapshot_file and snapshot_offset
     *
     * 3 is the most difficult, as it looks like we don't actually know the
     * durable lsn until the first statement sent after the begin.  This is
     * okay, but to make this work we just need to be extremely careful and
     * only send back the snapshot_file and snapshot_offset at the correct
     * time
     **/

    /* Retry case has flag lit on "begin" */
    if (clnt->high_availability && clnt->is_newsql && clnt->sql_query &&
        clnt->sql_query->retry && clnt->sql_query->snapshot_info &&
        (strncasecmp(clnt->sql, "begin", 5) == 0) &&
        clnt->sql_query->snapshot_info->file) {

        clnt->snapshot_file = clnt->sql_query->snapshot_info->file;
        clnt->snapshot_offset = clnt->sql_query->snapshot_info->offset;
        clnt->is_hasql_retry = 1;

        if (gbl_extended_sql_debug_trace) {
            char cnonce[256] = {0};
            snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
            logmsg(LOGMSG_USER, "%s line %d retry setting lsn to [%d][%d]\n",
                    __func__, __LINE__, clnt->snapshot_file,
                    clnt->snapshot_offset);
        }
    } else {
        if (gbl_extended_sql_debug_trace) {
            char cnonce[256] = {0};
            snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
            logmsg(LOGMSG_USER, 
                    "%s line %d cnonce '%s' not setting snapshot info: ha=%d, "
                    "is_newsql=%d sql_query=%p retry=%d snapshot_info=[%d][%d] "
                    "sql='%s'\n",
                    __func__, __LINE__, cnonce, clnt->high_availability,
                    clnt->is_newsql, clnt->sql_query,
                    (clnt->sql_query) ? clnt->sql_query->retry : -1,
                    (clnt->sql_query && clnt->sql_query->snapshot_info)
                        ? clnt->sql_query->snapshot_info->file
                        : -1,
                    (clnt->sql_query && clnt->sql_query->snapshot_info)
                        ? clnt->sql_query->snapshot_info->offset
                        : -1,
                    clnt->sql);
        }
    }
    // XXX short circuiting the last-row optimization until i have a chance
    // to verify it.
    return 0;
}

    
static int _push_row_new(struct sqlclntstate *clnt, int type,
        CDB2SQLRESPONSE *sql_response,
        CDB2SQLRESPONSE__Column **columns, int ncols,
        void *(*alloc)(size_t size), int flush)
{

    sql_response->response_type = type;
    sql_response->n_value = ncols;
    sql_response->value = columns;
    // Don't overwrite this: it could be CHANGENODE if we fail to get a 
    // durable_lsn at the beginning of a transaction
    //sql_response->error_code = 0;
    if (gbl_extended_sql_debug_trace) {
        char cnonce[256] = {0};
        snprintf(cnonce, clnt->sql_query->cnonce.len + 1, "%s", 
                clnt->sql_query->cnonce.data);
        logmsg(LOGMSG_USER, "%s line %d cnonce '%s' push-row type=%d error_code=%d\n",
                __func__, __LINE__, cnonce, type, sql_response->error_code);
    }
    
    return newsql_write_response(clnt, RESPONSE_HEADER__SQL_RESPONSE,
                                 sql_response, flush, alloc, 
                                  __func__, __LINE__);
}

/* write back column information */
int newsql_send_column_info(struct sqlclntstate *clnt,
                            struct column_info *cinfo, int ncols,
                            sqlite3_stmt *stmt,
                            CDB2SQLRESPONSE__Column **columns)
{
    CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;
    int rc = 0;
    const char *colname;

    for (int i = 0; i < ncols; i++) {
        cdb2__sqlresponse__column__init(columns[i]);
        columns[i]->has_type = 1;
        columns[i]->type = cinfo[i].type;
        /* DH: here we could use column_info only, but that would limit
        names to 31 chars, so we need to go through sqlite for now */
        if (stmt)
            colname = sqlite3_column_name(stmt, i);
        else
            colname = cinfo[i].column_name;
        columns[i]->value.len = strlen(colname) + 1;
        columns[i]->value.data = (char *)colname;
        if (!gbl_return_long_column_names && columns[i]->value.len > 31) {
            columns[i]->value.data[31] = '\0';
            columns[i]->value.len = 32;
        }
    }

    rc = _push_row_new(clnt, RESPONSE_TYPE__COLUMN_NAMES, 
                       &sql_response, columns, ncols, malloc, 0);

    clnt->osql.sent_column_data = 1;

    return rc;
}

int release_locks(const char *trace)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd ? thd->sqlclntstate : NULL;
    int rc = 0;

    if (clnt && clnt->dbtran.cursor_tran) {
        extern int gbl_sql_release_locks_trace;
        if (gbl_sql_release_locks_trace)
            logmsg(LOGMSG_USER, "Releasing locks for lockid %d, %s\n",
                   bdb_get_lid_from_cursortran(clnt->dbtran.cursor_tran),
                   trace);
        rc = recover_deadlock_silent(thedb->bdb_env, thd, NULL, -1);
    }
    return rc;
}

int release_locks_on_emit_row(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt)
{
    extern int gbl_sql_release_locks_on_emit_row;
    extern int gbl_locks_check_waiters;
    if (gbl_locks_check_waiters && gbl_sql_release_locks_on_emit_row) {
        extern int gbl_sql_random_release_interval;
        if (bdb_curtran_has_waiters(thedb->bdb_env, clnt->dbtran.cursor_tran)) {
            release_locks("lockwait at emit-row");
        } else if (gbl_sql_random_release_interval &&
                   !(rand() % gbl_sql_random_release_interval)) {
            release_locks("random release emit-row");
        }
    }
    return 0;
}

static int check_sql(struct sqlclntstate *clnt, int *sp)
{
    char buf[256];
    struct fsqlresp resp;
    char *sql = clnt->sql;
    while (isspace(*sql))
        ++sql;
    size_t len = 4; // strlen "exec"
    if (strncasecmp(sql, "exec", len) == 0) {
        sql += len;
        if (isspace(*sql)) {
            *sp = 1;
            return 0;
        }
    }
    len = 6; // strlen "pragma"
    if (strncasecmp(sql, "pragma", len) == 0) {
        sql += len;
        if (!isspace(*sql)) {
            return 0;
        }
    error: /* pretend that a real prepare error occured */
        strcpy(buf, "near \"");
        strncat(buf + len, sql, len);
        strcat(buf, "\": syntax error");
        send_prepare_error(clnt, buf, 0);
        return SQLITE_ERROR;
    }
    len = 6; // strlen "create"
    if (strncasecmp(sql, "create", len) == 0) {
        char *trigger = sql;
        trigger += len;
        if (!isspace(*trigger)) {
            return 0;
        }
        while (isspace(*trigger))
            ++trigger;
        if (strncasecmp(trigger, "trigger", 7) != 0) {
            return 0;
        }
        trigger += 7;
        if (isspace(*trigger)) {
            goto error;
        }
    }
    return 0;
}

/* if userpassword does not match this function
 * will write error response and return a non 0 rc
 */
inline static int check_user_password(struct sqlclntstate *clnt,
                                      struct fsqlresp *resp)
{
    int password_rc = 0;
    char *err = "access denied";
    int valid_user;

    if (!gbl_uses_password)
        return 0;

    if (gbl_uses_password) {
        if (!clnt->have_user) {
            clnt->have_user = 1;
            strcpy(clnt->user, DEFAULT_USER);
        }
        if (!clnt->have_password) {
            clnt->have_password = 1;
            strcpy(clnt->password, DEFAULT_PASSWORD);
        }
    }
    if (clnt->have_user && clnt->have_password) {
        password_rc = bdb_user_password_check(clnt->user, clnt->password, &valid_user);
    }

    if (!clnt->have_user || !clnt->have_password || password_rc != 0) {
        bzero(resp, sizeof(*resp));
        resp->response = FSQL_ERROR;
        resp->rcode = CDB2ERR_ACCESS;
        if (valid_user)
            err = "invalid password";
        fsql_write_response(clnt, resp, err, strlen(err) + 1, 1, __func__,
                            __LINE__);
        return 1;
    }
    return 0;
}

static int need_flush(struct sqlclntstate *clnt)
{
    return (clnt->conninfo.pid == 0 && sbuf2fileno(clnt->sb) == fileno(stdout))
               ? 0
               : 1;
}

static void get_return_row_schema(struct sqlthdstate *thd,
                                  struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    int col;
    int ncols;

    ncols = sqlite3_column_count(stmt);

    thd->cinfo = realloc(thd->cinfo, ncols * sizeof(struct column_info));

    for (col = 0; col < ncols; col++) {
        if (clnt->req.parm) {
            thd->cinfo[col].type = clnt->type_overrides[col];
            if (!comdb2_is_valid_type(thd->cinfo[col].type)) {
                thd->cinfo[col].type = sqlite3_column_type(stmt, col);
            }
        } else {
            thd->cinfo[col].type = sqlite3_column_type(stmt, col);
            if ((gbl_surprise) || thd->cinfo[col].type == SQLITE_NULL) {
                thd->cinfo[col].type =
                    typestr_to_type(sqlite3_column_decltype(stmt, col));
            }
        }
        if (thd->cinfo[col].type == SQLITE_DECIMAL)
            thd->cinfo[col].type = SQLITE_TEXT;

        strncpy(thd->cinfo[col].column_name, sqlite3_column_name(stmt, col),
                sizeof(thd->cinfo[col].column_name));
        thd->cinfo[col].column_name[sizeof(thd->cinfo[col].column_name) - 1] =
            '\0';
    }
}

void thr_set_current_sql(const char *sql)
{
    char *prevsql;
    pthread_once(&current_sql_query_once, init_current_current_sql_key);
    if (gbl_debug_temptables) {
        prevsql = pthread_getspecific(current_sql_query_key);
        if (prevsql) {
            free(prevsql);
            pthread_setspecific(current_sql_query_key, NULL);
        }
        pthread_setspecific(current_sql_query_key, strdup(sql));
    }
}

static void setup_reqlog_new_sql(struct sqlthdstate *thd,
                                 struct sqlclntstate *clnt)
{
    char info_nvreplays[40];
    info_nvreplays[0] = '\0';

    if (clnt->verify_retries)
        snprintf(info_nvreplays, sizeof(info_nvreplays), "vreplays=%d ",
                 clnt->verify_retries);

    thrman_wheref(thd->thr_self, "%ssql: %s", info_nvreplays, clnt->sql);

    reqlog_new_sql_request(thd->logger, NULL);
    log_client_context(thd->logger, clnt);
    log_queue_time(thd->logger, clnt);
}

static void query_stats_setup(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt)
{
    /* debug */
    thr_set_current_sql(clnt->sql);

    /* debug */
    clnt->debug_sqlclntstate = pthread_self();

    clnt->nrows = 0;

    /* berkdb stats */
    bdb_reset_thread_stats();

    /* node stats */
    if (!clnt->rawnodestats) {
        clnt->rawnodestats = get_raw_node_stats(clnt->origin);
    }
    if (clnt->rawnodestats)
        clnt->rawnodestats->sql_queries++;

    /* global stats */
    if (clnt->is_newsql) {
        ATOMIC_ADD(gbl_nnewsql, 1);
    } else {
        ATOMIC_ADD(gbl_nsql, 1);
    }

    /* sql thread stats */
    thd->sqlthd->startms = time_epochms();
    thd->sqlthd->stime = time_epoch();
    thd->sqlthd->nmove = thd->sqlthd->nfind = thd->sqlthd->nwrite = 0;

    /* reqlog */
    setup_reqlog_new_sql(thd, clnt);

    /* fingerprint info */
    if (gbl_fingerprint_queries)
        sqlite3_fingerprint_enable(thd->sqldb);
    else
        sqlite3_fingerprint_disable(thd->sqldb);

    /* using case sensitive like? enable */
    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(thd->sqldb, 1);

    if (gbl_dump_sql_dispatched)
        logmsg(LOGMSG_USER, "SQL mode=%d [%s]\n", clnt->dbtran.mode, clnt->sql);

    if (clnt->sql_query)
        reqlog_set_request(thd->logger, clnt->sql_query);
}

#define HINT_LEN 127
enum cache_status {
    CACHE_DISABLED = 0,
    CACHE_HAS_HINT = 1,
    CACHE_FOUND_STMT = 2,
    CACHE_FOUND_STR = 4,
};
struct sql_state {
    enum cache_status status;          /* populated by get_prepared_stmt */
    sqlite3_stmt *stmt;                /* cached engine, if any */
    char cache_hint[HINT_LEN];         /* hint copy, if any */
    const char *sql;                   /* the actual string used */
    stmt_hash_entry_type *stmt_entry;  /* fast pointer to hashed record */
    struct schema *parameters_to_bind; /* fast pointer to parameters */
};

static void get_cached_stmt(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                            struct sql_state *rec)
{
    int hint_len;

    bzero(rec, sizeof(*rec));

    rec->status = CACHE_DISABLED;
    rec->sql = clnt->sql;

    if ((gbl_enable_sql_stmt_caching == STMT_CACHE_ALL) ||
        ((gbl_enable_sql_stmt_caching == STMT_CACHE_PARAM) && clnt->tag)) {
        hint_len = sizeof(rec->cache_hint);
        if (extract_sqlcache_hint(clnt->sql, rec->cache_hint, &hint_len)) {
            rec->status = CACHE_HAS_HINT;
            if (find_stmt_table(thd->stmt_table, rec->cache_hint,
                                &rec->stmt_entry) == 0) {
                rec->status |= CACHE_FOUND_STMT;
                rec->stmt = rec->stmt_entry->stmt;
                rec->sql = (char *)sqlite3_sql(rec->stmt);
                rec->parameters_to_bind = rec->stmt_entry->params_to_bind;
            } else {
                /* We are not able to find the statement in cache,
                 * and this is a partial statement. */
                /* Try to find sql string stored in hash table */
                if (find_sql_hint_table(rec->cache_hint, (char **)&rec->sql,
                                        (char **)&(clnt->tag)) == 0) {
                    rec->status |= CACHE_FOUND_STR;
                } else {
                    /* the above call doesn set rec->sql if not found */
                }
            }
        } else {
            if (find_stmt_table(thd->stmt_table, rec->sql, &rec->stmt_entry) ==
                0) {
                rec->status = CACHE_FOUND_STMT;
                rec->stmt = rec->stmt_entry->stmt;
            }
        }
    }
}

static void get_cached_params(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt,
                              struct sql_state *rec)
{
    int hint_len;

    bzero(rec, sizeof(*rec));

    rec->status = CACHE_DISABLED;
    rec->sql = clnt->sql;

    if ((gbl_enable_sql_stmt_caching == STMT_CACHE_PARAM) && clnt->tag) {
        hint_len = sizeof(rec->cache_hint);
        if (extract_sqlcache_hint(clnt->sql, rec->cache_hint, &hint_len)) {
            rec->status = CACHE_HAS_HINT;
            if (find_stmt_table(thd->stmt_table, rec->cache_hint,
                                &rec->stmt_entry) == 0) {
                rec->status |= CACHE_FOUND_STMT;
                rec->parameters_to_bind = rec->stmt_entry->params_to_bind;
            } else {
                if (find_sql_hint_table(rec->cache_hint, (char **)&rec->sql,
                                        &(clnt->tag)) == 0) {
                    rec->status |= CACHE_FOUND_STR;
                }
            }
        }
    }
}


static int _dbglog_send_cost(struct sqlclntstate *clnt)
{
    dump_client_query_stats(clnt->dbglog, clnt->query_stats);
    return 0;
}

enum {
  ERR_GENERIC = -1,
  ERR_PREPARE = -2,
  ERR_PREPARE_RETRY = -3,
  ERR_ROW_HEADER = -4,
  ERR_CONVERSION_DT = -5,
};

struct client_comm_if {
    int (*send_row_format)(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                           struct sql_state *rec, int ncols,
                           CDB2SQLRESPONSE__Column **columns);
    int (*send_row_data)(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                         int new_row_data_type, int ncols, int row_id, int rc,
                         CDB2SQLRESPONSE__Column **columns);
    void (*send_prepare_error)(struct sqlclntstate *clnt, const char *errstr,
                              int clnt_retry);
    int (*send_run_error)(struct sqlclntstate *clnt, const char *errstr,
                          int client_rc);
    int (*flush)(struct sqlclntstate *clnt);
    int (*send_cost)(struct sqlclntstate *clnt);
    int (*send_effects)(struct sqlclntstate *clnt);
    void (*send_done)(struct sqlthdstate *thd, struct sqlclntstate *clnt, 
                     const char *func, int line);
    int (*send_dummy)(struct sqlclntstate *clnt);
};

struct client_comm_if client_sql_api = {
    &send_ret_column_info,
    &send_row,
    &send_prepare_error,
    &send_err_but_msg, 
    &flush_row,
    &_dbglog_send_cost,
    &send_query_effects,
    &send_last_row,
    &send_dummy,
};

static int get_prepared_bound_param(struct sqlthdstate *thd,
                                    struct sqlclntstate *clnt,
                                    struct sql_state *rec, 
                                    struct errstat *err)
{
    get_cached_params(thd, clnt, rec);

    /* bind values here if it was a parametrized query */
    if (clnt->tag && (rec->parameters_to_bind == NULL)) {
        if ((rec->status & CACHE_HAS_HINT) && clnt->in_client_trans &&
            strlen(clnt->tag) <= 1) {
            /* Make the client retry the query.*/
            errstat_set_rcstrf(err, ERR_PREPARE_RETRY, "%s",
                               "invalid parametrized tag (api bug?)");
            return -1;
        }

        rec->parameters_to_bind = new_dynamic_schema(
            clnt->tag, strlen(clnt->tag), gbl_dump_sql_dispatched);
        if (rec->parameters_to_bind == NULL) {
            errstat_set_rcstrf(err, ERR_PREPARE, "%s",
                               "invalid parametrized tag (api bug?)");
            return -1;
        }
    }

    return 0;
}

static void clear_stmt_record(struct sql_state *rec)
{
    if (rec->parameters_to_bind) {
        free_tag_schema(rec->parameters_to_bind);
        rec->parameters_to_bind = NULL;
    }
}

/**
 * Cache a stmt if needed; struct sql_state is prepared by
 * get_prepared_stmt(), and it is cleaned here as well
 *
 */
static void put_prepared_stmt(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt,
                              struct sql_state *rec, int outrc,
                              int stored_proc)
{
    sqlite3_stmt *stmt = rec->stmt;
    if (stmt || ((stored_proc == 1) && (rec->status & CACHE_HAS_HINT))) {
        if ((gbl_enable_sql_stmt_caching == STMT_CACHE_ALL) ||
            ((gbl_enable_sql_stmt_caching == STMT_CACHE_PARAM) && clnt->tag)) {
            if (stmt)
                sqlite3_reset(stmt);

            if (!bdb_attr_get(thedb->bdb_attr,
                              BDB_ATTR_DISABLE_CACHING_STMT_WITH_FDB) ||
                sqlite3_stmt_has_remotes(stmt) == 0) {
                if (!rec->stmt_entry) {
                    if (outrc == 0) {
                        if (rec->status & CACHE_HAS_HINT) {
                            if (add_stmt_table(
                                    thd, rec->cache_hint,
                                    (char *)(gbl_debug_temptables ? rec->sql
                                                                  : NULL),
                                    stmt, rec->parameters_to_bind) == 0) {
                                rec->parameters_to_bind = NULL;
                            }
                            if (!(rec->status & CACHE_FOUND_STR)) {
                                add_sql_hint_table(rec->cache_hint, clnt->sql,
                                                   clnt->tag);
                            }
                        } else {
                            if (add_stmt_table(
                                    thd, clnt->sql,
                                    (char *)(gbl_debug_temptables ? rec->sql
                                                                  : NULL),
                                    stmt, rec->parameters_to_bind) == 0) {
                                rec->parameters_to_bind = NULL;
                            }
                        }
                    } else if (stmt) {
                        sqlite3_finalize(stmt);
                    }
                } else {
                    if (outrc == 0)
                        touch_stmt_entry(thd, rec->stmt_entry);

                    if (rec->status & CACHE_HAS_HINT)
                        rec->parameters_to_bind = NULL;
                }
            } else {
                if (stmt)
                    sqlite3_finalize(stmt);
            }
        } else {
            if (stmt)
                sqlite3_finalize(stmt);
        }
    }

    clear_stmt_record(rec);

    if ((rec->status & CACHE_HAS_HINT) && (rec->status & CACHE_FOUND_STR)) {
        char *k = rec->cache_hint;
        pthread_mutex_lock(&gbl_sql_lock);
        {
            lrucache_release(sql_hints, &k);
        }
        pthread_mutex_unlock(&gbl_sql_lock);
    }
}

/* return 0 if continue, 1 if done */
static int ha_retrieve_snapshot(struct sqlclntstate *clnt)
{
    int is_snap_retry;

    if (!clnt->is_newsql)
        return 0;

    /* MOHIT -- Check here that we are in high availablity, its cdb2api, and
     * is its a retry. */
    if (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS && clnt->sql_query) {
        if (clnt->sql_query->retry) {
            clnt->num_retry = clnt->sql_query->retry;
            if (clnt->sql_query->snapshot_info) {
                clnt->snapshot_file = clnt->sql_query->snapshot_info->file;
                clnt->snapshot_offset = clnt->sql_query->snapshot_info->offset;
                if (gbl_extended_sql_debug_trace) {
                    char cnonce[256];
                    snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
                    logmsg(LOGMSG_USER, "%s: retry cnonce '%s' at [%d][%d]\n",
                            __func__, cnonce, clnt->snapshot_file,
                            clnt->snapshot_offset);
                }
            } else {
                clnt->snapshot_file = 0;
                clnt->snapshot_offset = 0;
            }
        } else {
            clnt->num_retry = 0;
            clnt->snapshot_file = 0;
            clnt->snapshot_offset = 0;
        }
    }

    is_snap_retry = is_snap_uid_retry(clnt);

    if (is_snap_retry == 1) {
        /* Give number of changes as reply. */
        if (gbl_extended_sql_debug_trace) {
            logmsg(LOGMSG_USER, "%s: this is a snap-retry - sql is '%s', "
                            "ctrlengine_state=%d: i am sending the last row\n",
                    __func__, clnt->sql, clnt->ctrl_sqlengine);
        }
        newsql_send_dummy_resp(clnt, __func__, __LINE__);
        newsql_send_last_row(clnt, 0, __func__, __LINE__);
        return 1;
    } else if (is_snap_retry == -1) {
        send_prepare_error(clnt,
                           "Comdb2 server not setup for high availability", 0);
        return 1;
    }

    return 0;
}

static void update_schema_remotes(struct sqlclntstate *clnt,
                                  struct sql_state *rec)
{
    /* reset set error since this is a retry */
    clnt->osql.error_is_remote = 0;
    clnt->osql.xerr.errval = 0;

    /* if I jump here because of sqlite3_step failure, I have local
     * cache already freed */
    sqlite3UnlockStmtTablesRemotes(clnt); /*lose all the locks boyo! */

    /* terminate the current statement; we are gonna reprepare */
    sqlite3_finalize(rec->stmt);
    rec->stmt = NULL;
}

static void _prepare_error(struct sqlthdstate *thd, 
                                struct sqlclntstate *clnt,
                                struct sql_state *rec, int rc, 
                                struct errstat *err)
{
    int flush_resp;
    const char *errstr;

    if (clnt->in_client_trans && (rec->status & CACHE_HAS_HINT ||
                                  has_sqlcache_hint(clnt->sql, NULL, NULL)) &&
        !(rec->status & CACHE_FOUND_STR) &&
        (clnt->osql.replay == OSQL_RETRY_NONE)) {

        errstr = (char *)sqlite3_errmsg(thd->sqldb);
        reqlog_logf(thd->logger, REQL_TRACE, "sqlite3_prepare failed %d: %s\n",
                    rc, errstr);
        errstat_set_rcstrf(err, ERR_PREPARE_RETRY, "%s", errstr);

        srs_tran_del_last_query(clnt);
        return;
    }

    flush_resp = need_flush(clnt);
    if(rc == FSQL_PREPARE && !rec->stmt)
        errstr = "no statement";
    else if (clnt->fdb_state.xerr.errval) {
        errstr = clnt->fdb_state.xerr.errstr;
    } else {
        errstr = (char *)sqlite3_errmsg(thd->sqldb);
    }
    reqlog_logf(thd->logger, REQL_TRACE, "sqlite3_prepare failed %d: %s\n", rc,
                errstr);
    if (flush_resp) {
        errstat_set_rcstrf(err, ERR_PREPARE, "%s", errstr);
    } else {
        if (clnt->saved_errstr)
            free(clnt->saved_errstr);
        clnt->saved_errstr = strdup(errstr);
    }
    if (clnt->want_query_effects) {
        clnt->had_errors = 1;
    }
    if (gbl_print_syntax_err) {
        logmsg(LOGMSG_WARN, "sqlite3_prepare() failed for: %s [%s]\n", clnt->sql,
                errstr);
    }

    if (clnt->ctrl_sqlengine != SQLENG_NORMAL_PROCESS) {
        /* multiple query transaction
           keep sending back error */
        handle_sql_intrans_unrecoverable_error(clnt);
    }
}

/**
 * Get a sqlite engine, either from cache or building a new one
 * Locks tables to prevent any schema changes for them
 *
 */
static int get_prepared_stmt(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                             struct sql_state *rec, struct errstat *err)
{
    const char *rest_of_sql = NULL;
    int rc = SQLITE_OK;

    /* checked current sql thread engine and make sure it is up to date */
    clnt->no_transaction = 1;
    rdlock_schema_lk();
    if ((rc = check_thd_gen(thd, clnt)) != SQLITE_OK) {
        clnt->no_transaction = 0;
        unlock_schema_lk();
        return rc;
    }

    /* sqlite handled request, do we have an engine already? */
    get_cached_stmt(thd, clnt, rec);
    if (rec->stmt) {
        /* we found a cached engine */
        if ((rc = sqlite3_resetclock(rec->stmt)) != SQLITE_OK)
            goto done;

        /* thr set the actual used sql */
        thr_set_current_sql(rec->sql);
    }

    /* Set to the expanded version */
    reqlog_set_sql(thd->logger, (char *)rec->sql);

    /* if don't have a stmt */
    do {
        if (!rec->stmt) {
            if (clnt->tag || clnt->is_newsql) {
                rc = sqlite3_prepare_v2(thd->sqldb, rec->sql, -1, &rec->stmt,
                                        &rest_of_sql);
            } else {
                rc = sqlite3_prepare(thd->sqldb, rec->sql, -1, &rec->stmt,
                                     &rest_of_sql);
            }
        }

        if (rc == SQLITE_OK) {
            rc = sqlite3LockStmtTables(rec->stmt);
            if (rc == SQLITE_SCHEMA_REMOTE) {
                /* need to update remote schema */
                sql_remote_schema_changed(clnt, rec->stmt);
                update_schema_remotes(clnt, rec);
                continue;
            }
        }
    } while (rc == SQLITE_SCHEMA_REMOTE);

done:
    if (gbl_fingerprint_queries) {
        unsigned char fingerprint[16];
        sqlite3_fingerprint(thd->sqldb, (unsigned char *)fingerprint);
        reqlog_set_fingerprint(thd->logger, fingerprint);
    }

    unlock_schema_lk();
    clnt->no_transaction = 0;

    if(!rc && !rec->stmt) rc = FSQL_PREPARE;
    if(rc)
        _prepare_error(thd, clnt, rec, rc, err);

    if (rest_of_sql && *rest_of_sql) {
        logmsg(LOGMSG_WARN, 
                "SQL TRAILING CHARACTERS AFTER QUERY TERMINATION: \"%s\"\n",
                rest_of_sql);
    }

    return rc;
}

static int check_client_specified_conversions(struct sqlthdstate *thd,
                                              struct sqlclntstate *clnt,
                                              int ncols, struct errstat *err)
{
    if ((clnt->req.parm && clnt->req.parm != ncols) ||
        (clnt->is_newsql && clnt->sql_query && clnt->sql_query->n_types &&
         clnt->sql_query->n_types != ncols)) {
        char errstr[128];
        int loc_cols = clnt->req.parm;
        if (clnt->is_newsql)
            loc_cols = clnt->sql_query->n_types;
        snprintf(errstr, sizeof(errstr),
                 "Number of overrides:%d doesn't match number of columns:%d",
                 loc_cols, ncols);
        reqlog_logf(thd->logger, REQL_TRACE, "%s\n", errstr);

        errstat_set_rcstrf(err, ERR_PREPARE, "%s", errstr);

        return -1;
    }

    return 0;
}

static int bind_params(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                       struct sql_state *rec, struct errstat *err)
{
    char *errstr = NULL;
    int rc = 0;

    if (clnt->is_newsql && clnt->sql_query && clnt->sql_query->n_bindvars) {
        assert(rec->parameters_to_bind == NULL);
        eventlog_params(thd->logger, rec->stmt, rec->parameters_to_bind, clnt);
        rc = bind_parameters(rec->stmt, rec->parameters_to_bind, clnt, &errstr);
        if (rc) {
            errstat_set_rcstrf(err, ERR_PREPARE, "%s", errstr);
        }
    } else if (rec->parameters_to_bind) {
        eventlog_params(thd->logger, rec->stmt, rec->parameters_to_bind, clnt);
        rc = bind_parameters(rec->stmt, rec->parameters_to_bind, clnt, &errstr);
        if(rc) {
            errstat_set_rcstrf(err, ERR_PREPARE, "%s", errstr);
        }
    } else if (clnt->verify_indexes) {
        bind_verify_indexes_query(rec->stmt, clnt->schema_mems);
    } else {
        if (sqlite3_bind_parameter_count(rec->stmt)) {
            reqlog_logf(thd->logger, REQL_TRACE, "parameter bind failed \n");
            errstat_set_rcstrf(err, ERR_PREPARE, "%s", 
                               "Query specified parameters, but no values"
                               " provided.");
            rc = -1;
        }
    }

    return rc;
}

/**
 * Get a sqlite engine with bound parameters set, if any
 *
 */
static int get_prepared_bound_stmt(struct sqlthdstate *thd,
                                   struct sqlclntstate *clnt,
                                   struct sql_state *rec,
                                   struct errstat *err)
{
    int ncols;
    int rc;

    if (clnt->verify_indexes) {
        /* short circuit for partial/expression indexes */
        assert(!rec->stmt);
        bzero(rec, sizeof(*rec));

        rec->status = CACHE_DISABLED;
        rec->sql = clnt->sql;
        rc = sqlite3_prepare_v2(thd->sqldb, rec->sql, -1, &rec->stmt, NULL);
    } else
        rc = get_prepared_stmt(thd, clnt, rec, err);
    if (rc)
        return rc;

    /* bind values here if it was a parametrized query */
    if (clnt->tag && (rec->parameters_to_bind == NULL)) {
        rec->parameters_to_bind = new_dynamic_schema(
            clnt->tag, strlen(clnt->tag), gbl_dump_sql_dispatched);
        if (rec->parameters_to_bind == NULL) {
            errstat_set_str(err, "invalid parametrized tag (api bug?)");
            errstat_set_rc(err, ERR_PREPARE_RETRY);
            return -1;
        }
    }

    ncols = sqlite3_column_count(rec->stmt);
    reqlog_logf(thd->logger, REQL_INFO, "ncols=%d", ncols);

    rc = check_client_specified_conversions(thd, clnt, ncols, err);
    if (rc)
        return rc;

    rc = bind_params(thd, clnt, rec, err);
    if (rc)
        return rc;

    return 0;
}

static void handle_stored_proc(struct sqlthdstate *thd,
                               struct sqlclntstate *clnt);

/* return 0 continue, 1 return *outrc */
static int handle_non_sqlite_requests(struct sqlthdstate *thd,
                                      struct sqlclntstate *clnt, int *outrc)
{
    int rc;
    int stored_proc;
    int flush_resp = need_flush(clnt);

    sql_update_usertran_state(clnt);

    switch (clnt->ctrl_sqlengine) {

    case SQLENG_PRE_STRT_STATE:
        *outrc = handle_sql_begin(thd, clnt, flush_resp);
        return 1;

    case SQLENG_WRONG_STATE:
        *outrc = handle_sql_wrongstate(thd, clnt);
        return 1;

    case SQLENG_FNSH_STATE:
    case SQLENG_FNSH_RBK_STATE:
        *outrc = handle_sql_commitrollback(thd, clnt, flush_resp);
        return 1;

    case SQLENG_STRT_STATE:
        /* FALL-THROUGH for update query execution */
        break;
    }

    /* additional non-sqlite requests */
    stored_proc = 0;
    if ((rc = check_sql(clnt, &stored_proc)) != 0) {
        return rc;
    }

    if (stored_proc) {
        clnt->trans_has_sp = 1;
    }

    /* this is a regular sql query, add it to history */
    if (srs_tran_add_query(clnt))
        logmsg(LOGMSG_ERROR, "Fail to add query to transaction replay session\n");


    if (stored_proc) {
        handle_stored_proc(thd, clnt);
        *outrc = 0;
        return 1;
    } else if (clnt->is_explain) { // only via newsql--cdb2api
        *outrc = newsql_dump_query_plan(clnt, thd->sqldb);
        return 1;
    }

    /* 0, this is an sqlite request, use an engine */
    return 0;
}

static void set_ret_column_info(struct sqlthdstate *thd,
                                struct sqlclntstate *clnt,
                                struct sql_state *rec, int ncols)
{
    sqlite3_stmt *stmt = rec->stmt;
    int col;

    thd->cinfo = realloc(thd->cinfo, ncols * sizeof(struct column_info));

    for (col = 0; col < ncols; col++) {

        /* set the return column type */
        if (clnt->req.parm || (clnt->is_newsql && clnt->sql_query->n_types)) {
            if (clnt->is_newsql) {
                thd->cinfo[col].type = clnt->sql_query->types[col];
            } else {
                thd->cinfo[col].type = clnt->type_overrides[col];
            }
            if (!comdb2_is_valid_type(thd->cinfo[col].type)) {
                thd->cinfo[col].type = sqlite3_column_type(stmt, col);
            }
        } else {
            thd->cinfo[col].type = sqlite3_column_type(stmt, col);
            if ((!clnt->is_newsql && gbl_surprise) ||
                thd->cinfo[col].type == SQLITE_NULL) {
                thd->cinfo[col].type =
                    typestr_to_type(sqlite3_column_decltype(stmt, col));
                /*thd->cinfo[col].type = sqlite3_column_type(stmt, col);*/
            }
        }
        if (thd->cinfo[col].type == SQLITE_DECIMAL)
            thd->cinfo[col].type = SQLITE_TEXT;

        /* set return column name */
        strncpy(thd->cinfo[col].column_name, sqlite3_column_name(stmt, col),
                sizeof(thd->cinfo[col].column_name));
        thd->cinfo[col].column_name[sizeof(thd->cinfo[col].column_name) - 1] =
            '\0';
    }
}

static int send_ret_column_info_oldsql(struct sqlthdstate *thd,
                                       struct sqlclntstate *clnt, int ncols,
                                       int flush_resp)
{
    struct fsqlresp resp;
    size_t buf_cinfos_len;
    uint8_t *p_buf_cinfos_start;
    uint8_t *p_buf_cinfos;
    uint8_t *p_buf_cinfos_end;
    int did_malloc = 0;
    int rc = 0;
    int i;

    if (!flush_resp)
        return 0;

    resp.response = FSQL_COLUMN_DATA;
    resp.flags = clnt->conninfo.pid;
    resp.rcode = 0;
    resp.parm = ncols;

    /* pack column info */
    buf_cinfos_len = ncols * COLUMN_INFO_LEN;
    if (buf_cinfos_len > 4096) {
        p_buf_cinfos_start = sqlite3_malloc(buf_cinfos_len);
        did_malloc = 1;
    } else {
        p_buf_cinfos_start = alloca(buf_cinfos_len);
    }
    p_buf_cinfos = p_buf_cinfos_start;
    p_buf_cinfos_end = p_buf_cinfos_start + buf_cinfos_len;

    for (i = 0; i < ncols; ++i) {
        if (!(p_buf_cinfos = column_info_put(&thd->cinfo[i], p_buf_cinfos,
                                             p_buf_cinfos_end))) {
            rc = -1;
            if (did_malloc)
                sqlite3_free(p_buf_cinfos_start);
            p_buf_cinfos_start = NULL;
            goto error;
        }
    }

    if (clnt->osql.replay != OSQL_RETRY_DO)
        rc =
            fsql_write_response(clnt, &resp, p_buf_cinfos_start, buf_cinfos_len,
                                0 /*flush*/, __func__, __LINE__);

    if (did_malloc)
        sqlite3_free(p_buf_cinfos_start);

    p_buf_cinfos_start = NULL;
    if (rc)
        goto error;

error:
    return rc;
}

static int send_ret_column_info(struct sqlthdstate *thd,
                                struct sqlclntstate *clnt,
                                struct sql_state *rec, int ncols,
                                CDB2SQLRESPONSE__Column **columns)
{
    int flush_resp = need_flush(clnt);
    int rc = 0;
    if ((!clnt->osql.replay || (clnt->dbtran.mode == TRANLEVEL_RECOM &&
                                !clnt->osql.sent_column_data)) &&
        !((clnt->want_query_effects && !is_with_statement(clnt->sql)) &&
          clnt->in_client_trans && !clnt->isselect)) {
        if (clnt->is_newsql) {
            newsql_send_column_info(clnt, thd->cinfo, ncols, rec->stmt,
                                    columns);
            rc = 0;
        } else {
            rc = send_ret_column_info_oldsql(thd, clnt, ncols, flush_resp);
            if (rc)
                goto error;
        }

        if (flush_resp) {
            rc = fsql_write_response(clnt, NULL /*resp*/, NULL /*dta*/,
                                     0 /*len*/, 1, __func__, __LINE__);
            if (rc < 0)
                goto error;
        }

        clnt->osql.sent_column_data = 1;
    }
error:
    return rc;
}

static inline int is_new_row_data(struct sqlclntstate *clnt)
{
    return (!clnt->iswrite && (clnt->req.flags & SQLF_WANT_NEW_ROW_DATA) &&
            gbl_new_row_data);
}

static int send_err_but_msg_new(struct sqlclntstate *clnt, const char *errstr,
                                int irc)
{
    int rc = 0;

    clnt->had_errors = 1;
    if (clnt->num_retry == clnt->sql_query->retry) {
        CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;
        if (clnt->osql.sent_column_data) {
            sql_response.response_type = RESPONSE_TYPE__COLUMN_VALUES;
        } else {
            sql_response.response_type = RESPONSE_TYPE__COLUMN_NAMES;
        }
        sql_response.n_value = 0;
        sql_response.error_code = irc;
        sql_response.error_string = (char*)errstr;

        rc = newsql_write_response(clnt, RESPONSE_HEADER__SQL_RESPONSE,
                                   &sql_response, 1 /*flush*/, malloc, __func__,
                                   __LINE__);
    }
    return rc;
}

static int send_err_but_msg_old(struct sqlclntstate *clnt, const char *errstr,
                                int irc)
{
    int rc = 0;
    struct fsqlresp resp;
    int flush_resp = need_flush(clnt);
    if (flush_resp) {
        resp.rcode = irc;
        resp.response = FSQL_ERROR;
        rc = fsql_write_response(clnt, &resp, (void *)errstr,
                                 strlen(errstr) + 1, 0, __func__, __LINE__);
    } else {
        if (clnt->saved_errstr)
            free(clnt->saved_errstr);
        clnt->saved_errstr = strdup(errstr);
    }
    return rc;
}

static int send_err_but_msg(struct sqlclntstate *clnt, const char *errstr, int irc)
{
    int rc;
    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->ready_for_heartbeats = 0;
    pthread_mutex_unlock(&clnt->wait_mutex);

    if (clnt->is_newsql) {
        rc = send_err_but_msg_new(clnt, errstr, irc);
    } else {
        rc = send_err_but_msg_old(clnt, errstr, irc);
    }

    return rc;
}

static inline int new_retrow_has_header(int is_null, int type)
{
    if (is_null)
        return 1;

    switch (type) {
    case SQLITE_INTEGER:
    case SQLITE_FLOAT:
    case SQLITE_INTERVAL_YM:
    case CLIENT_INTV_DS_LEN:
        return 0;
    }
    return 1;
}

static int new_retrow_get_column_nulls_and_count(sqlite3_stmt *stmt,
                                                 int new_row_data_type,
                                                 int ncols,
                                                 struct column_info *cinfo,
                                                 uint8_t *isNullCol)
{
    int total_col, col;

    total_col = 0;
    for (col = 0; col < ncols; col++) {
        /* need to know if *this* col content is SQLITE_NULL */
        isNullCol[col] = sqlite3_isColumnNullType(stmt, col);
#ifdef DEBUG
        if ((sqlite3_column_type(stmt, col) == SQLITE_NULL) != isNullCol[col]) {
            logmsg(LOGMSG_FATAL, "Did not evaluate to NULL col %d \n", col);
            abort();
        }
#endif
        if (new_row_data_type) {
            total_col += new_retrow_has_header(isNullCol[col], cinfo[col].type);
        }
    }
    return total_col;
}

static void *thd_adjust_space(struct sqlthdstate *thd)
{
    /* does the thread buffer fits ? */
    if (thd->buflen > thd->maxbuflen) {
        char *p;
        thd->maxbuflen = thd->buflen;
        if (thd->buf == NULL && thd->maxbuflen > gbl_blob_sz_thresh_bytes)
            p = comdb2_bmalloc(blobmem, thd->maxbuflen);
        else
            p = realloc(thd->buf, thd->maxbuflen);
        if (!p) {
            logmsg(LOGMSG_ERROR, "%s: out of memory realloc %d\n", __func__,
                    thd->maxbuflen);
            return NULL;
        }
        thd->buf = p;
    }
    return thd->buf;
}

static int update_retrow_schema(struct sqlthdstate *thd,
                                struct sqlclntstate *clnt, sqlite3_stmt *stmt,
                                int new_row_data_type, int ncols, int total_col,
                                uint8_t *isNullCol, struct errstat *err)
{
    int offset, fldlen;
    int buflen = 0;
    int col;

    /* header */
    if (new_row_data_type)
        offset = sizeof(int) + total_col * sizeof(struct sqlfield);
    else
        offset = ncols * sizeof(struct sqlfield);

    /* compute offsets and buffer size */
    for (col = 0, buflen = 0; col < ncols; col++) {
        if (isNullCol[col]) {
            thd->offsets[col].offset = -1;
            if (new_row_data_type) {
                thd->offsets[col].offset = col;
            }
            thd->offsets[col].len = -1;
            buflen = offset;
            continue;
        }

        switch (thd->cinfo[col].type) {
        case SQLITE_INTEGER:
            if (!new_row_data_type) {
                if (offset % sizeof(long long))
                    offset += sizeof(long long) - offset % sizeof(long long);
            }
            buflen = offset + sizeof(long long);
            thd->offsets[col].offset = offset;
            thd->offsets[col].len = sizeof(long long);
            offset += sizeof(long long);
            break;
        case SQLITE_FLOAT:
            if (!new_row_data_type) {
                if (offset % sizeof(double))
                    offset += sizeof(double) - offset % sizeof(double);
            }
            buflen = offset + sizeof(double);
            thd->offsets[col].len = sizeof(double);
            thd->offsets[col].offset = offset;
            offset += sizeof(double);
            break;
        case SQLITE_DATETIME:
        case SQLITE_DATETIMEUS:
            if (offset & 3) { /* align on 32 bit boundary */
                offset = (offset | 3) + 1;
            }
            if (clnt->have_extended_tm && clnt->extended_tm) {
                fldlen = CLIENT_DATETIME_EXT_LEN;
            } else {
                fldlen = CLIENT_DATETIME_LEN;
            }
            buflen = offset + fldlen;
            thd->offsets[col].offset = offset;
            thd->offsets[col].len = fldlen;
            offset += fldlen;
            break;
        case SQLITE_INTERVAL_YM:
            if (offset & 3) { /* align on 32 bit boundary */
                offset = (offset | 3) + 1;
            }
            fldlen = CLIENT_INTV_YM_LEN;
            buflen = offset + fldlen;
            thd->offsets[col].offset = offset;
            thd->offsets[col].len = fldlen;
            offset += fldlen;
            break;
        case SQLITE_INTERVAL_DS:
        case SQLITE_INTERVAL_DSUS:
            if (offset & 3) { /* align on 32 bit boundary */
                offset = (offset | 3) + 1;
            }
            fldlen = CLIENT_INTV_DS_LEN;
            buflen = offset + fldlen;
            thd->offsets[col].offset = offset;
            thd->offsets[col].len = fldlen;
            offset += fldlen;
            break;
        case SQLITE_TEXT:
        case SQLITE_BLOB:
            fldlen = sqlite3_column_bytes(stmt, col);
            /* sqlite3_column_bytes returns a 0 on error - check for
             * error or if it's really 0 */
            if (fldlen == 0 &&
                sqlite3_errcode(thd->sqldb) == SQLITE_CONV_ERROR) {
                errstat_set_rcstrf(err, ERR_ROW_HEADER, "%s", 
                                   (char*)sqlite3_errmsg(thd->sqldb));
                return -1;
            }
            if (thd->cinfo[col].type == SQLITE_TEXT)
                fldlen++;
            buflen = offset + fldlen;
            thd->offsets[col].offset = offset;
            thd->offsets[col].len = fldlen;
            offset += fldlen;
            break;
        }

        if (new_row_data_type) {
            thd->offsets[col].offset = col;
        }
    }

    thd->buflen = buflen;
    return 0;
}

/* creates the row header */
static int set_retrow_header(struct sqlthdstate *thd, int new_row_data_type,
                             int ncols, int total_col, uint8_t *isNullCol)
{
    uint8_t *p_buf;
    uint8_t *p_buf_end;
    int i;

    p_buf = (uint8_t *)thd->buf;
    p_buf_end = (uint8_t *)(thd->buf + thd->buflen);

    /* new row format has a counter as first field */
    if (new_row_data_type) {
        p_buf = buf_put(&total_col, sizeof(total_col), p_buf, p_buf_end);
    }

    /* columns that have header info are added next */
    for (i = 0; i < ncols; ++i) {
        if (!new_row_data_type ||
            new_retrow_has_header(isNullCol[i], thd->cinfo[i].type)) {
            if (!(p_buf = sqlfield_put(&thd->offsets[i], p_buf, p_buf_end)))
                return -1;
        }
    }

    return 0;
}

static int set_retrow_columns(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt, sqlite3_stmt *stmt,
                              int new_row_data_type, int ncols,
                              uint8_t *isNullCol, int total_col, int rowcount,
                              struct errstat *err)
{
    uint8_t *p_buf_end = thd->buf + thd->buflen;
    int little_endian = 0;
    int fldlen;
    double dval;
    long long ival;
    char *tval;
    int offset;
    int col;

    /* flip our data if the client asked for little endian data */
    if (clnt->have_endian && clnt->endian == FSQL_ENDIAN_LITTLE_ENDIAN) {
        little_endian = 1;
    }

    if (new_row_data_type) {
        offset = sizeof(int) + total_col * sizeof(struct sqlfield);
    } else {
        offset = ncols * sizeof(struct sqlfield);
    }

    reqlog_logf(thd->logger, REQL_RESULTS, "row %d = (", rowcount + 1);

    for (col = 0; col < ncols; col++) {

        if (col > 0) {
            reqlog_logl(thd->logger, REQL_RESULTS, ", ");
        }

        if (isNullCol[col]) {
            reqlog_logl(thd->logger, REQL_RESULTS, "NULL");
            continue;
        }

        switch (thd->cinfo[col].type) {
        case SQLITE_INTEGER:
            if (!new_row_data_type) {
                if (offset % sizeof(long long))
                    offset += sizeof(long long) - offset % sizeof(long long);
            }
            ival = sqlite3_column_int64(stmt, col);
            reqlog_logf(thd->logger, REQL_RESULTS, "%lld", ival);
            if (little_endian)
                ival = flibc_llflip(ival);
            if (!buf_put(&ival, sizeof(ival), (uint8_t *)(thd->buf + offset),
                         p_buf_end))
                goto error;
            offset += sizeof(long long);
            break;
        case SQLITE_FLOAT:
            if (!new_row_data_type) {
                if (offset % sizeof(double))
                    offset += sizeof(double) - offset % sizeof(double);
            }
            dval = sqlite3_column_double(stmt, col);
            reqlog_logf(thd->logger, REQL_RESULTS, "%f", dval);
            if (little_endian)
                dval = flibc_dblflip(dval);
            if (!buf_put(&dval, sizeof(dval), (uint8_t *)(thd->buf + offset),
                         p_buf_end))
                goto error;
            offset += sizeof(double);
            break;
        case SQLITE_TEXT:
            fldlen = sqlite3_column_bytes(stmt, col) + 1;
            tval = (char *)sqlite3_column_text(stmt, col);
            reqlog_logf(thd->logger, REQL_RESULTS, "'%s'", tval);
            /* text doesn't need to be packed */
            if (tval)
                memcpy(thd->buf + offset, tval, fldlen);
            else
                thd->buf[offset] = 0; /* null string */
            offset += fldlen;
            break;
        case SQLITE_DATETIME:
        case SQLITE_DATETIMEUS:
            if (offset & 3) { /* align on 32 bit boundary */
                offset = (offset | 3) + 1;
            }
            if (clnt->have_extended_tm && clnt->extended_tm) {
                fldlen = CLIENT_DATETIME_EXT_LEN;
            } else {
                fldlen = CLIENT_DATETIME_LEN;
            }
            {
                cdb2_client_datetime_t cdt;

                tval = (char *)sqlite3_column_datetime(stmt, col);
                if (!tval ||
                    convDttz2ClientDatetime((dttz_t *)tval,
                                            ((Vdbe *)stmt)->tzname, &cdt,
                                            thd->cinfo[col].type)) {
                    bzero(thd->buf + offset, fldlen);
                    logmsg(LOGMSG_ERROR, "%s: datetime conversion "
                                    "failure\n",
                            __func__);
                    errstat_set_rcstrf(err, ERR_CONVERSION_DT, 
                                       "failed to convert sqlite to client "
                                       "datetime for field \"%s\"",
                                       sqlite3_column_name(stmt, col));
                    goto error;
                }

                if (little_endian) {
                    cdt.tm.tm_sec = flibc_intflip(cdt.tm.tm_sec);
                    cdt.tm.tm_min = flibc_intflip(cdt.tm.tm_min);
                    cdt.tm.tm_hour = flibc_intflip(cdt.tm.tm_hour);
                    cdt.tm.tm_mday = flibc_intflip(cdt.tm.tm_mday);
                    cdt.tm.tm_mon = flibc_intflip(cdt.tm.tm_mon);
                    cdt.tm.tm_year = flibc_intflip(cdt.tm.tm_year);
                    cdt.tm.tm_wday = flibc_intflip(cdt.tm.tm_wday);
                    cdt.tm.tm_yday = flibc_intflip(cdt.tm.tm_yday);
                    cdt.tm.tm_isdst = flibc_intflip(cdt.tm.tm_isdst);
                    cdt.msec = flibc_intflip(cdt.msec);
                }

                /* Old linux sql clients will have extended_tms's. */
                if (clnt->have_extended_tm && clnt->extended_tm) {
                    if (!client_extended_datetime_put(
                            &cdt, (uint8_t *)(thd->buf + offset), p_buf_end))
                        goto error;
                } else {

                    if (!client_datetime_put(
                            &cdt, (uint8_t *)(thd->buf + offset), p_buf_end))
                        goto error;
                }
            }
            offset += fldlen;
            break;
        case SQLITE_INTERVAL_YM:
            if (offset & 3) { /* align on 32 bit boundary */
                offset = (offset | 3) + 1;
            }
            fldlen = CLIENT_INTV_YM_LEN;
            {
                cdb2_client_intv_ym_t ym;
                const intv_t *tv =
                    sqlite3_column_interval(stmt, col, SQLITE_AFF_INTV_MO);

                if (little_endian) {
                    ym.sign = flibc_intflip(tv->sign);
                    ym.years = flibc_intflip(tv->u.ym.years);
                    ym.months = flibc_intflip(tv->u.ym.months);
                } else {
                    ym.sign = tv->sign;
                    ym.years = tv->u.ym.years;
                    ym.months = tv->u.ym.months;
                }

                if (!client_intv_ym_put(&ym, (uint8_t *)(thd->buf + offset),
                                        p_buf_end))
                    goto error;
                offset += fldlen;
            }
            break;
        case SQLITE_INTERVAL_DS:
        case SQLITE_INTERVAL_DSUS:
            if (offset & 3) { /* align on 32 bit boundary */
                offset = (offset | 3) + 1;
            }
            fldlen = CLIENT_INTV_DS_LEN;
            {
                cdb2_client_intv_ds_t ds;
                intv_t *tv;
                tv = (intv_t *)sqlite3_column_interval(stmt, col,
                                                       SQLITE_AFF_INTV_SE);

                /* Adjust fraction based on client precision. */
                if (thd->cinfo[col].type == SQLITE_INTERVAL_DS && tv->u.ds.prec == 6)
                    tv->u.ds.frac /= 1000;
                else if (thd->cinfo[col].type == SQLITE_INTERVAL_DSUS && tv->u.ds.prec == 3)
                    tv->u.ds.frac *= 1000;

                if (little_endian) {
                    ds.sign = flibc_intflip(tv->sign);
                    ds.days = flibc_intflip(tv->u.ds.days);
                    ds.hours = flibc_intflip(tv->u.ds.hours);
                    ds.mins = flibc_intflip(tv->u.ds.mins);
                    ds.sec = flibc_intflip(tv->u.ds.sec);
                    ds.msec = flibc_intflip(tv->u.ds.frac);
                } else {
                    ds.sign = tv->sign;
                    ds.days = tv->u.ds.days;
                    ds.hours = tv->u.ds.hours;
                    ds.mins = tv->u.ds.mins;
                    ds.sec = tv->u.ds.sec;
                    ds.msec = tv->u.ds.frac;
                }

                if (!client_intv_ds_put(&ds, (uint8_t *)(thd->buf + offset),
                                        p_buf_end))
                    goto error;
                offset += fldlen;
            }
            break;
        case SQLITE_BLOB:
            fldlen = sqlite3_column_bytes(stmt, col);
            tval = (char *)sqlite3_column_blob(stmt, col);
            memcpy(thd->buf + offset, tval, fldlen);
            reqlog_logl(thd->logger, REQL_RESULTS, "x'");
            reqlog_loghex(thd->logger, REQL_RESULTS, tval, fldlen);
            reqlog_logl(thd->logger, REQL_RESULTS, "'");
            offset += fldlen;
            break;
        }
    }
    reqlog_logl(thd->logger, REQL_RESULTS, ")\n");
    return 0;
error:
    return -1;
}

/* based on current sqlite result row, create a client formatted row */
int make_retrow(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                struct sql_state *rec, int new_row_data_type, int ncols,
                int rowcount, int *fast_error, struct errstat *err)
{
    uint8_t *isNullCol = NULL;
    int total_col;
    int offset;
    int rc;

    *fast_error = 1;

    isNullCol = (uint8_t *)malloc(sizeof(uint8_t) * ncols);
    if (!isNullCol)
        return -1;

    total_col = new_retrow_get_column_nulls_and_count(
        rec->stmt, new_row_data_type, ncols, thd->cinfo, isNullCol);

    rc = update_retrow_schema(thd, clnt, rec->stmt, new_row_data_type, ncols,
                              total_col, isNullCol, err);
    if (rc) { /* done */
        assert(errstat_get_rc(err) == ERR_ROW_HEADER);
        *fast_error = 0;
        goto out;
    }

    if (!thd_adjust_space(thd)) {
        rc = -1; /* error */
        goto out;
    }

    rc = set_retrow_header(thd, new_row_data_type, ncols, total_col, isNullCol);
    if (rc) /* error */
        goto out;

    rc = set_retrow_columns(thd, clnt, rec->stmt, new_row_data_type, ncols,
                            isNullCol, total_col, rowcount, err);
out:
    free(isNullCol);
    return rc; /*error */
}

static void *blob_alloc_override(size_t len)
{
    return comdb2_bmalloc(blobmem, len + 1);
}

static int send_row_new(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                        int ncols, int row_id,
                        CDB2SQLRESPONSE__Column **columns)
{
    CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;
    int rc = 0;
    int i;

    for (i = 0; i < ncols; i++) {
        cdb2__sqlresponse__column__init(columns[i]);
        columns[i]->has_type = 0;
        if (thd->offsets[i].len == -1) {
            columns[i]->has_isnull = 1;
            columns[i]->isnull = 1;
            columns[i]->value.len = 0;
            columns[i]->value.data = NULL;
        } else {
            columns[i]->value.len = thd->offsets[i].len;
            columns[i]->value.data = thd->buf + thd->offsets[i].offset;
        }
    }

    if (clnt->num_retry) {
        sql_response.has_row_id = 1;
        sql_response.row_id = row_id;
    }

    _has_snapshot(clnt, sql_response);

    if (gbl_extended_sql_debug_trace) {
        int sz = clnt->sql_query->cnonce.len;
        int file = -1, offset = -1;
        if (sql_response.snapshot_info) {
            file = sql_response.snapshot_info->file;
            offset = sql_response.snapshot_info->offset;
        }
        char cnonce[256];
        snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
        logmsg(LOGMSG_USER, "%s: cnonce '%s' snapshot is [%d][%d]\n", __func__,
                cnonce, file, offset);
    }

    sql_response.value = columns;
    return _push_row_new(clnt, RESPONSE_TYPE__COLUMN_VALUES, &sql_response, 
                         columns, ncols, 
                         ((cdb2__sqlresponse__get_packed_size(&sql_response)+1)
                            > gbl_blob_sz_thresh_bytes) 
                            ?  blob_alloc_override: malloc,
                         0);
}

static int send_row_old(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                        int new_row_data_type)
{
    struct fsqlresp resp;

    if (new_row_data_type) {
        resp.response = FSQL_NEW_ROW_DATA;
    } else {
        resp.response = FSQL_ROW_DATA;
    }
    resp.flags = 0;
    resp.rcode = FSQL_OK;
    resp.parm = 0;

    return fsql_write_response(clnt, &resp, thd->buf, thd->buflen, 0, __func__,
                               __LINE__);
}

static int send_row(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                    int new_row_data_type, int ncols, int row_id, int rc,
                    CDB2SQLRESPONSE__Column **columns)
{
    int irc = 0;
    if (clnt->is_newsql) {
        if (!rc && (clnt->num_retry == clnt->sql_query->retry) &&
                (clnt->num_retry == 0 || clnt->sql_query->has_skip_rows == 0 ||
                 (clnt->sql_query->skip_rows < row_id)))
            irc = send_row_new(thd, clnt, ncols, row_id, columns);
    } else {
        irc = send_row_old(thd, clnt, new_row_data_type);
    }
    return irc;
}

static int flush_row(struct sqlclntstate *clnt)
{
    if (gbl_sqlflush_freq && clnt->recno % gbl_sqlflush_freq == 0) {
        if (fsql_write_response(clnt, NULL, NULL, 0, 1, __func__,
                                __LINE__) < 0)
            return -1;
    }
    return 0;
}

/* will do a tiny cleanup of clnt */
void run_stmt_setup(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    Vdbe *v = (Vdbe *)stmt;
    clnt->isselect = sqlite3_stmt_readonly(stmt);
    clnt->has_recording |= v->recording;
    comdb2_set_sqlite_vdbe_tzname_int(v, clnt);
    comdb2_set_sqlite_vdbe_dtprec_int(v, clnt);
    clnt->iswrite = 0; /* reset before step() */

#ifdef DEBUG
    if (gbl_debug_sql_opcodes) {
        fprintf(stderr, "%s () sql: '%s'\n", __func__, v->zSql);
    }
#endif
}

static int rc_sqlite_to_client(struct sqlthdstate *thd,
                               struct sqlclntstate *clnt,
                               struct sql_state *rec, char **perrstr)
{
    sqlite3_stmt *stmt = rec->stmt;
    int irc;

    /* get the engine error code, which is what we should pass
       back to the client!
     */
    irc = sql_check_errors(clnt, thd->sqldb, stmt, (const char **)perrstr);
    if (irc) {
        irc = sqlserver2sqlclient_error(irc);
    }

    if (!irc) {
        irc = errstat_get_rc(&clnt->osql.xerr);
        if (irc) {
            *perrstr = (char *)errstat_get_str(&clnt->osql.xerr);
            /* convert this to a user code */
            irc = (clnt->osql.error_is_remote)
                      ? irc
                      : blockproc2sql_error(irc, __func__, __LINE__);
            if (irc == DB_ERR_TRN_VERIFY &&
                clnt->dbtran.mode != TRANLEVEL_SNAPISOL &&
                clnt->dbtran.mode != TRANLEVEL_SERIAL && !clnt->has_recording &&
                clnt->osql.replay == OSQL_RETRY_NONE &&
                (clnt->verifyretry_off == 0)) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_DO);
            }
        }
        /* if this is a single query, we need to send back an answer here */
        if (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
            /* if this is still a verify error but we tried to many times,
               send error back anyway by resetting the replay field */
            if (irc == DB_ERR_TRN_VERIFY &&
                clnt->osql.replay == OSQL_RETRY_LAST) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
            }
            /* if this is still an error, but not verify, pass it back to
               client */
            else if (irc && irc != DB_ERR_TRN_VERIFY) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
            }
            /* if this is a successful retrial of a previous verified-failed
               query, reset here replay so we can send success message back
               to client */
            else if (!irc && clnt->osql.replay != OSQL_RETRY_NONE) {
                osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_NONE);
            }
        }
    } else if (clnt->osql.replay != OSQL_RETRY_NONE) {
        /* this was a retry that got an different error;
           send error to the client and stop retrying */
        osql_set_replay(__FILE__, __LINE__, clnt, OSQL_RETRY_HALT);
    }

    return irc;
}

static void send_last_row_old(struct sqlclntstate *clnt, int now,
                              const char *func, int line)
{
    struct fsqlresp resp;
    int flush_resp = need_flush(clnt);
    int rc;

    if (!clnt->intrans && clnt->want_query_effects) {
        if (!clnt->iswrite)
            rc = send_query_effects(clnt);
        reset_query_effects(clnt);
    }
    if (flush_resp) {
        bzero(&resp, sizeof resp);
        resp.response = FSQL_ROW_DATA;
        resp.flags = 0;
        resp.rcode = FSQL_DONE;
        resp.parm = now;
        fsql_write_response(clnt, &resp, NULL, 0, 1, func, line);
    }
}

static void send_last_row(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                          const char *func, int line)
{
    if (clnt->is_newsql) {
        newsql_send_last_row(clnt, 0, func, line);
    } else {
        send_last_row_old(clnt, (thd->sqlthd) ? thd->sqlthd->stime : 0, func,
                          line);
    }
}

static int post_sqlite_processing(struct sqlthdstate *thd,
                                  struct sqlclntstate *clnt,
                                  struct sql_state *rec, int postponed_write,
                                  int ncols, int row_id,
                                  CDB2SQLRESPONSE__Column **columns,
                                  struct client_comm_if *comm)
{
    char *errstr = NULL;
    int rc;
    test_no_btcursors(thd);

    if (clnt->client_understands_query_stats) {
        record_query_cost(thd->sqlthd, clnt);
        if(comm->send_cost)
            comm->send_cost(clnt);
    } else if (clnt->get_cost) {
        if (clnt->prev_cost_string) {
            free(clnt->prev_cost_string);
            clnt->prev_cost_string = NULL;
        }
        clnt->prev_cost_string = get_query_cost_as_string(thd->sqlthd, clnt);
    }

    rc = rc_sqlite_to_client(thd, clnt, rec, &errstr);

    if ((clnt->osql.replay == OSQL_RETRY_NONE ||
         clnt->osql.replay == OSQL_RETRY_HALT) &&
        !((clnt->want_query_effects && !is_with_statement(clnt->sql)) &&
          clnt->in_client_trans && !clnt->isselect &&
          !(rc && !clnt->had_errors))) {

        if (clnt->iswrite && postponed_write) {
            if (comm->send_row_data) {
                int irc;
                irc = comm->send_row_data(thd, clnt, rc, ncols, row_id, rc, columns);
                if (irc)
                    return irc;
            }
        }

        if (rc == SQLITE_OK) {

            pthread_mutex_lock(&clnt->wait_mutex);
            clnt->ready_for_heartbeats = 0;
            pthread_mutex_unlock(&clnt->wait_mutex);

            if(comm->send_done)
                comm->send_done(thd, clnt, __func__, __LINE__);
        } else {
            if(comm->send_run_error) {
                comm->send_run_error(clnt, errstr, rc);
                if (rc)
                    return rc;
            }
        }
    } else if ((clnt->osql.replay == OSQL_RETRY_NONE ||
                clnt->osql.replay == OSQL_RETRY_HALT) &&
               clnt->want_query_effects &&
               (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) &&
               !clnt->isselect) {
        if(comm->send_effects) {
            rc = comm->send_effects(clnt);
            if (rc)
                return rc;
        }
    } else if (clnt->want_query_effects && !clnt->isselect &&
               clnt->send_one_row) {
        assert(clnt->is_newsql);
        if(comm->send_dummy)
            comm->send_dummy(clnt);
        clnt->send_one_row = 0;
        clnt->skip_feature = 1;
        if(comm->send_done)
            comm->send_done(thd, clnt, __func__, __LINE__);
    }

    return 0;
}

/* The design choice here for communication is to send row data inside this function,
   and delegate the error sending to the caller (since we send multiple rows, but we 
   send error only once and stop processing at that time)
 */   
static int run_stmt(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                    struct sql_state *rec, int *fast_error, 
                    struct errstat *err,
                    struct client_comm_if *comm)
{
    CDB2SQLRESPONSE__Column **columns;
    sqlite3_stmt *stmt = rec->stmt;
    int new_row_data_type = 0;
    int ncols;
    int steprc;
    int rowcount = 0;
    ;
    int row_id = 0;
    int postponed_write = 0;
    int rc;

    reqlog_set_event(thd->logger, "sql");
    run_stmt_setup(clnt, rec->stmt);

    new_row_data_type = is_new_row_data(clnt);
    ncols = sqlite3_column_count(rec->stmt);

    /* Get first row to figure out column structure */
    steprc = sqlite3_step(stmt);
    if (steprc == SQLITE_SCHEMA_REMOTE) {
        /* remote schema changed;
           Only safe to recover here
           NOTE: not a fast error;
         */
        return steprc;
    }

    *fast_error = 1;

    if (clnt->verify_indexes && steprc == SQLITE_ROW) {
        clnt->has_sqliterow = 1;
        verify_indexes_column_value(stmt, clnt->schema_mems);
        return 0;
    } else if (clnt->verify_indexes && steprc == SQLITE_DONE) {
        clnt->has_sqliterow = 0;
        return 0;
    }

    /* create the row format and send it to client */
    columns = newsql_alloc_row(ncols);
    if (!columns)
        return -1;

    set_ret_column_info(thd, clnt, rec, ncols);
    if(comm->send_row_format) {
        rc = comm->send_row_format(thd, clnt, rec, ncols, columns);
        if (rc)
            goto out;
    }

    if (clnt->intrans == 0) {
        bzero(&clnt->effects, sizeof(clnt->effects));
        bzero(&clnt->log_effects, sizeof(clnt->log_effects));
    }

    /* no rows actually ? */
    if (steprc != SQLITE_ROW) {
        rc = steprc;
        goto postprocessing;
    }

    thd->offsets = realloc(thd->offsets, ncols * sizeof(struct sqlfield));
    if(!thd->offsets)
        return -1;

    do {
        /* replication contention reduction */
        release_locks_on_emit_row(thd, clnt);

        if (clnt->isselect == 1) {
            clnt->effects.num_selected++;
            clnt->log_effects.num_selected++;
            clnt->nrows++;
        }

#ifdef DEBUG
        int hasn;
        if (!(hasn = sqlite3_hasNColumns(stmt, ncols))) {
            printf("Does not have %d cols\n", ncols);
            abort();
        }
#endif

        /* create return row */
        rc = make_retrow(thd, clnt, rec, new_row_data_type, ncols, rowcount,
                         fast_error, err);
        if (rc)
            goto out;

        char cnonce[256];
        cnonce[0] = '\0';

        if (gbl_extended_sql_debug_trace && clnt->sql_query) {
            bzero(cnonce, sizeof(cnonce));
            snprintf(cnonce, 256, "%s", clnt->sql_query->cnonce.data);
            logmsg(LOGMSG_USER, "%s: cnonce '%s': iswrite=%d replay=%d "
                    "want_query_effects is %d, isselect is %d\n", 
                    __func__, cnonce, clnt->iswrite, clnt->osql.replay, 
                    clnt->want_query_effects, 
                    clnt->isselect);
        }

        /* return row, if needed */
        if (!clnt->iswrite && clnt->osql.replay != OSQL_RETRY_DO) {
            postponed_write = 0;
            row_id++;

            if (!clnt->want_query_effects || clnt->isselect) {
                if(comm->send_row_data) {
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER, "%s: cnonce '%s' sending row\n", __func__, cnonce);
                    }
                    rc = comm->send_row_data(thd, clnt, new_row_data_type, 
                                             ncols, row_id, rc, columns);
                    if (rc)
                        goto out;
                }
            }
        } else {
            if (gbl_extended_sql_debug_trace) {
                logmsg(LOGMSG_USER, "%s: cnonce '%s' setting postponed_write\n", 
                        __func__, cnonce);
            }
            postponed_write = 1;
        }

        rowcount++;
        reqlog_set_rows(thd->logger, rowcount);
        clnt->recno++;
        if (clnt->rawnodestats)
            clnt->rawnodestats->sql_rows++;

        /* flush */
        if(comm->flush && !clnt->iswrite) {
            rc = comm->flush(clnt);
            if (rc)
                goto out;
        }
    } while ((rc = sqlite3_step(stmt)) == SQLITE_ROW);

/* whatever sqlite returns in sqlite3_step is only used to step out of the loop,
   otherwise ignored; we are gonna
   get it from sqlite (or osql.xerr) */

postprocessing:
    if (rc == SQLITE_DONE)
        rc = 0;

    /* closing: error codes, postponed write result and so on*/
    rc =
        post_sqlite_processing(thd, clnt, rec, postponed_write, ncols, row_id,
                               columns, comm);

out:
    newsql_dealloc_row(columns, ncols);
    return rc;
}

static void handle_sqlite_error(struct sqlthdstate *thd,
                                struct sqlclntstate *clnt,
                                struct sql_state *rec)
{
    if (thd->sqlthd)
        thd->sqlthd->nmove = thd->sqlthd->nfind = thd->sqlthd->nwrite =
            thd->sqlthd->ntmpread = thd->sqlthd->ntmpwrite = 0;

    clear_stmt_record(rec);

    if (clnt->want_query_effects) {
        clnt->had_errors = 1;
    }

    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(thd->sqldb, 0);

    reqlog_end_request(thd->logger, -1, __func__, __LINE__);
}

static void sqlite_done(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                        struct sql_state *rec, int outrc)
{
    sqlite3_stmt *stmt = rec->stmt;

    sql_statement_done(thd->sqlthd, thd->logger, clnt->osql.rqid, outrc);

    if (clnt->rawnodestats && thd->sqlthd) {
        clnt->rawnodestats->sql_steps +=
            thd->sqlthd->nmove + thd->sqlthd->nfind + thd->sqlthd->nwrite;
    }

    if (stmt && !((Vdbe *)stmt)->explain && ((Vdbe *)stmt)->nScan > 1 &&
        (BDB_ATTR_GET(thedb->bdb_attr, PLANNER_WARN_ON_DISCREPANCY) == 1 ||
         BDB_ATTR_GET(thedb->bdb_attr, PLANNER_SHOW_SCANSTATS) == 1)) {
        compare_estimate_cost(stmt);
    }

    put_prepared_stmt(thd, clnt, rec, outrc, 0);

    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(thd->sqldb, 0);

#ifdef DEBUG_SQLITE_MEMORY
    int had_blocks = 0;
    hash_for(sql_blocks, dump_block, &had_blocks);
    if (had_blocks)
        printf("end of blocks\n");
#endif

    /* the ethereal sqlite objects insert into clnt->...Ddl fields
       we need to clear them out after the statement is done, or else
       the next read in sqlite master will find them and try to use them
     */
    clearClientSideRow(clnt);
}

static void handle_stored_proc(struct sqlthdstate *thd,
                               struct sqlclntstate *clnt)
{
    struct sql_state rec;
    struct errstat err;
    char *errstr;
    int rc;

    reqlog_set_event(thd->logger, "sp");

    rc = get_prepared_bound_param(thd, clnt, &rec, &err);
    if (rc) {
        int irc = errstat_get_rc(&err);
        assert(irc == ERR_PREPARE || irc == ERR_PREPARE_RETRY);
        send_prepare_error(clnt, err.errstr, (irc == ERR_PREPARE_RETRY));
        return;
    }

    rc = exec_procedure(clnt->sql, &errstr, thd, rec.parameters_to_bind, clnt);
    if (rc) {
        clnt->had_errors = 1;
        if (rc == -1)
            rc = -3;
        send_fsql_error(clnt, rc, (errstr) ? errstr : "");
        if (errstr) {
            free(errstr);
        }
    }

    test_no_btcursors(thd);

    sqlite_done(thd, clnt, &rec, 0);
}

static int handle_sqlite_requests(struct sqlthdstate *thd,
                                  struct sqlclntstate *clnt,
                                  struct client_comm_if *comm)
{
    struct sql_state rec;
    int rc;
    int fast_error;
    struct errstat err = {0};

    bzero(&rec, sizeof(rec));

    /* loop if possible in case when cached remote schema becomes stale */
    do {
        /* get an sqlite engine */
        rc = get_prepared_bound_stmt(thd, clnt, &rec, &err);
        if (rc) {
            int irc = errstat_get_rc(&err);
            /* certain errors are saved, in that case we don't send anything */
            if (irc == ERR_PREPARE || irc == ERR_PREPARE_RETRY) {
                if(comm->send_prepare_error)
                    comm->send_prepare_error(clnt, err.errstr, 
                                             (irc == ERR_PREPARE_RETRY));
                reqlog_set_error(thd->logger, sqlite3_errmsg(thd->sqldb));
            }
            goto errors;
        }

        /* run the engine */
        fast_error = 0;
        rc = run_stmt(thd, clnt, &rec, &fast_error, &err, comm);
        if (rc) {
            int irc = errstat_get_rc(&err);
            switch(irc) {
                case ERR_ROW_HEADER:
                    if(comm->send_run_error)
                        comm->send_run_error(clnt, errstat_get_str(&err), 
                                             CDB2ERR_CONV_FAIL);
                    break;
                case ERR_CONVERSION_DT:
                    if(comm->send_run_error)
                        comm->send_run_error(clnt, errstat_get_str(&err), 
                                             DB_ERR_CONV_FAIL);
                    break;
            }
            if (fast_error)
                goto errors;
        }

        if (rc == SQLITE_SCHEMA_REMOTE) {
            update_schema_remotes(clnt, &rec);
        }

    } while (rc == SQLITE_SCHEMA_REMOTE);

done:
    sqlite_done(thd, clnt, &rec, rc);

    return rc;

errors:
    handle_sqlite_error(thd, clnt, &rec);
    goto done;
}

static int check_sql_access(struct sqlclntstate *clnt)
{
    struct fsqlresp resp;
    if (gbl_check_access_controls) {
        check_access_controls(thedb);
        gbl_check_access_controls = 0;
    }
    return check_user_password(clnt, &resp);
}

/**
 * Main driver of SQL processing, for both sqlite and non-sqlite requests
 */
int execute_sql_query(struct sqlthdstate *thd, struct sqlclntstate *clnt)
{
    int outrc;
    int rc;

    /* access control */
    rc = check_sql_access(clnt);
    if (rc)
        return rc;

    /* setup */
    query_stats_setup(thd, clnt);

    /* is this a snapshot? special processing */
    rc = ha_retrieve_snapshot(clnt);
    if (rc)
        return 0;

    /* All requests that do not require a sqlite engine
       are processed below.  A return != 0 means processing
       done
     */
    rc = handle_non_sqlite_requests(thd, clnt, &outrc);
    if (rc)
        return outrc;

    /* This is a request that require a sqlite engine */
    rc = handle_sqlite_requests(thd, clnt, &client_sql_api);

    return rc;
}

void sqlengine_prepare_engine(struct sqlthdstate *thd,
                              struct sqlclntstate *clnt)
{
    struct errstat xerr;
    int rc;

    /* Do this here, before setting up Btree structures!
       so we can get back at our "session" information */
    clnt->debug_sqlclntstate = pthread_self();
    clnt->no_transaction = 1;
    struct sql_thread *sqlthd;
    if ((sqlthd = pthread_getspecific(query_info_key)) != NULL) {
        sqlthd->sqlclntstate = clnt;
    }

    if (thd->sqldb && (rc = check_thd_gen(thd, clnt)) != SQLITE_OK) {
        if (rc != SQLITE_SCHEMA_REMOTE) {
            delete_prepared_stmts(thd);
            sqlite3_close(thd->sqldb);
            thd->sqldb = NULL;
        }
    }

    if (gbl_enable_sql_stmt_caching && (thd->stmt_table == NULL)) {
        thd->param_cache_entries = 0;
        thd->noparam_cache_entries = 0;
        init_stmt_table(&thd->stmt_table);
    }

    if (!thd->sqldb || (rc == SQLITE_SCHEMA_REMOTE)) {
        if (!thd->sqldb) {
            clnt->no_transaction = 1;
            int rc = sqlite3_open_serial("db", &thd->sqldb, thd);
            clnt->no_transaction = 0;
            if (rc != 0) {
                logmsg(LOGMSG_ERROR, "%s:sqlite3_open_serial failed %d\n", __func__,
                        rc);
                thd->sqldb = NULL;
            }
            thd->dbopen_gen = gbl_dbopen_gen;
            thd->analyze_gen = gbl_analyze_gen;
        }

        get_copy_rootpages_nolock(thd->sqlthd);
        int got_curtran = rc = 0;
        if (!clnt->dbtran.cursor_tran) {
            got_curtran = 1;
            rc = get_curtran(thedb->bdb_env, clnt);
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s: unable to get a CURSOR transaction, rc = %d!\n",
                    __func__, rc);
        } else {
            if (thedb->timepart_views) {
                /* how about we are gonna add the views ? */
                rc = views_sqlite_update(thedb->timepart_views, thd->sqldb,
                                         &xerr);
                if (rc != VIEW_NOERR) {
                    logmsg(LOGMSG_ERROR, 
                            "failed to create views rc=%d errstr=\"%s\"\n",
                            xerr.errval, xerr.errstr);
                }
            }

            /* save the views generation number */
            thd->views_gen = gbl_views_gen;

            if (got_curtran && put_curtran(thedb->bdb_env, clnt)) {
                logmsg(LOGMSG_ERROR, "%s: unable to destroy a CURSOR transaction!\n",
                        __func__);
            }
        }
    }
    clnt->no_transaction = 0;
}

static void clean_queries_not_cached_in_srs(struct sqlclntstate *clnt)
{
    CDB2QUERY *query = NULL;

    /* Tell appsock thread to wake up again */
    pthread_mutex_lock(&clnt->wait_mutex);
    /* Take ownership of query, and free it. */
    if (clnt->added_to_hist == 0) {
        query = clnt->query;
        /* clear sql before cond_signal(). */
        if (query && /* query could not NULL. */
            clnt->sql == query->sqlquery->sql_query)
            clnt->sql = NULL;
    }
    clnt->query = NULL;
    clnt->done = 1;
    /* Must yield before signal */
    comdb2bma_yield_all();
    pthread_cond_signal(&clnt->wait_cond);
    pthread_mutex_unlock(&clnt->wait_mutex);

    /* Free it while we are reading new query. */
    if (query) {
        cdb2__query__free_unpacked(query, &pb_alloc);
    }
}

static void thr_set_user(int id)
{
    char thdinfo[40];
    snprintf(thdinfo, sizeof(thdinfo), "appsock %u", id);
    thrman_setid(thrman_self(), thdinfo);
}

static void debug_close_sb(struct sqlclntstate *clnt)
{
    static int once = 0;

    if (debug_switch_sql_close_sbuf()) {
        if (!once) {
            once = 1;
            sbuf2close(clnt->sb);
        }
    } else
        once = 0;
}

static void sqlengine_work_lua_thread(struct thdpool *pool, void *work,
                                      void *thddata)
{
    struct sqlthdstate *thd = thddata;
    struct sqlclntstate *clnt = work;

    if (!clnt->exec_lua_thread)
        abort();

    thr_set_user(clnt->appsock_id);

    clnt->osql.timings.query_dispatched = osql_log_time();
    clnt->deque_timeus = time_epochus();

    rdlock_schema_lk();
    sqlengine_prepare_engine(thd, clnt);
    unlock_schema_lk();

    reqlog_set_origin(thd->logger, "%s", clnt->origin);

    clnt->query_rc = exec_thread(thd, clnt);

    sql_reset_sqlthread(thd->sqldb, thd->sqlthd);

    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);

    clean_queries_not_cached_in_srs(clnt);

    debug_close_sb(clnt);

    thrman_setid(thrman_self(), "[done]");
}

int gbl_debug_sqlthd_failures;

static void sqlengine_work_appsock(struct thdpool *pool, void *work,
                                   void *thddata)
{
    struct sqlthdstate *thd = thddata;
    struct sqlclntstate *clnt = work;
    int rc;

    /* running this in a non sql thread */
    if (!thd->sqlthd)
        abort();

    thr_set_user(clnt->appsock_id);

    clnt->osql.timings.query_dispatched = osql_log_time();
    clnt->deque_timeus = time_epochus();

    /* make sure we have an sqlite engine sqldb */
    if (clnt->verify_indexes) {
        /* short circuit for partial/expression indexes */
        struct sql_thread *sqlthd;
        clnt->debug_sqlclntstate = pthread_self();
        clnt->no_transaction = 1;
        if ((sqlthd = pthread_getspecific(query_info_key)) != NULL) {
            sqlthd->sqlclntstate = clnt;
        }
        if (!thd->sqldb) {
            rc = sqlite3_open_serial("db", &thd->sqldb, thd);
            if (unlikely(rc != 0)) {
                fprintf(stderr, "%s:sqlite3_open_serial failed %d\n", __func__,
                        rc);
                thd->sqldb = NULL;
            } else {
                thd->dbopen_gen = gbl_dbopen_gen;
                thd->analyze_gen = gbl_analyze_gen;
            }
        }
    } else {
        rdlock_schema_lk();
        sqlengine_prepare_engine(thd, clnt);
        unlock_schema_lk();
    }

    int debug_appsock;
    if (unlikely(!thd->sqldb) ||
        (gbl_debug_sqlthd_failures && (debug_appsock = !(rand() % 1000)))) {
        /* unplausable, but anyway */
        logmsg(LOGMSG_ERROR, "%s line %d: exiting on null thd->sqldb\n",
               __func__, __LINE__);
        if (debug_appsock) {
            logmsg(LOGMSG_ERROR,
                   "%s line %d: testing null thd->sqldb codepath\n", __func__,
                   __LINE__);
        }
        clnt->query_rc = -1;
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->done = 1;
        pthread_cond_signal(&clnt->wait_cond);
        pthread_mutex_unlock(&clnt->wait_mutex);
        return;
    }

    reqlog_set_origin(thd->logger, "%s", clnt->origin);

    if (clnt->dbtran.mode == TRANLEVEL_SOSQL &&
        clnt->client_understands_query_stats && clnt->osql.rqid)
        osql_query_dbglog(thd->sqlthd, clnt->queryid);

    /* everything going in is cursor based */
    rc = get_curtran(thedb->bdb_env, clnt);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: unable to get a CURSOR transaction, rc=%d!\n",
                __func__, rc);
        /* Tell newsql client to CHANGENODE */
        if (clnt->is_newsql) {
            char *errstr = "Client api should change nodes";
            client_sql_api.send_run_error(clnt, errstr, CDB2ERR_CHANGENODE);
        }
        clnt->query_rc = -1;
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->done = 1;
        pthread_cond_signal(&clnt->wait_cond);
        pthread_mutex_unlock(&clnt->wait_mutex);
        clnt->osql.timings.query_finished = osql_log_time();
        osql_log_time_done(clnt);
        return;
    }

    /* it is a new query, it is time to clean the error */
    if (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS)
        bzero(&clnt->osql.xerr, sizeof(clnt->osql.xerr));

    /* Set whatever mode this client needs */
    rc = sql_set_transaction_mode(thd->sqldb, clnt, clnt->dbtran.mode);
    if (rc ||
        (gbl_debug_sqlthd_failures && (debug_appsock = !(rand() % 1000)))) {
        logmsg(LOGMSG_ERROR,
               "%s line %d: unable to set_transaction_mode rc=%d!\n", __func__,
               __LINE__, rc);
        if (debug_appsock) {
            logmsg(LOGMSG_ERROR, "%s line %d: testing failed set-transaction "
                                 "codepath\n",
                   __func__, __LINE__);
        }
        send_prepare_error(clnt, "Failed to set transaction mode.", 0);
        reqlog_logf(thd->logger, REQL_TRACE,
                    "Failed to set transaction mode.\n");
        if (put_curtran(thedb->bdb_env, clnt)) {
            logmsg(LOGMSG_ERROR,
                   "%s: unable to destroy a CURSOR transaction!\n", __func__);
        }
        clnt->query_rc = 0;
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->done = 1;
        pthread_cond_signal(&clnt->wait_cond);
        pthread_mutex_unlock(&clnt->wait_mutex);
        clnt->osql.timings.query_finished = osql_log_time();
        osql_log_time_done(clnt);
        return;
    }

    /* this could be done on sql_set_transaction_mode, but it
       affects all code paths and I don't like it */
    if ((clnt->dbtran.mode == TRANLEVEL_RECOM ||
         clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
         clnt->dbtran.mode == TRANLEVEL_SERIAL) || /* socksql has special
                                                      needs because of
                                                      inlining */
        (clnt->dbtran.mode == TRANLEVEL_SOSQL &&
         (clnt->ctrl_sqlengine == SQLENG_STRT_STATE ||
          clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS))) {
        clnt->osql.host = (thedb->master == gbl_mynode) ? 0 : thedb->master;
    }

    /* assign this query a unique id */
    sql_get_query_id(thd->sqlthd);

    /* actually execute the query */
    thrman_setfd(thd->thr_self, sbuf2fileno(clnt->sb));

    if (clnt->dbtran.mode == TRANLEVEL_RECOM ||
        clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
        clnt->dbtran.mode == TRANLEVEL_SERIAL ||
        clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        osql_shadtbl_begin_query(thedb->bdb_env, clnt);
    }

    if (clnt->fdb_state.remote_sql_sb)
        clnt->query_rc = execute_sql_query_offload(clnt, thd);
    else
        clnt->query_rc = execute_sql_query(thd, clnt);

    /* execute sql query might have generated an overriding fdb error;
       reset it here before returning */
    bzero(&clnt->fdb_state.xerr, sizeof(clnt->fdb_state.xerr));
    clnt->fdb_state.preserve_err = 0;

    if (clnt->dbtran.mode == TRANLEVEL_RECOM ||
        clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
        clnt->dbtran.mode == TRANLEVEL_SERIAL ||
        clnt->dbtran.mode == TRANLEVEL_SOSQL) {
        osql_shadtbl_done_query(thedb->bdb_env, clnt);
    }

    thrman_setfd(thd->thr_self, -1);

    sql_reset_sqlthread(thd->sqldb, thd->sqlthd);

    /* verify indexes queries use short circuit code,
       open a new sqlite3 db for new queries.
    */
    if (clnt->verify_indexes) {
        sqlite3_close(thd->sqldb);
        thd->sqldb = NULL;
    }

    /* this is a compromise; we release the curtran here, even though
       we might have a begin/commit transaction pending
       any query inside the begin/commit will be performed under its
       own locker id;
    */
    if (put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s: unable to destroy a CURSOR transaction!\n",
                __func__);
    }

    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);

    clean_queries_not_cached_in_srs(clnt);

    debug_close_sb(clnt);

    thrman_setid(thrman_self(), "[done]");
}

static void sqlengine_work_appsock_pp(struct thdpool *pool, void *work,
                                      void *thddata, int op)
{
    struct sqlclntstate *clnt = work;
    int rc = 0;

    switch (op) {
    case THD_RUN:
        if (clnt->exec_lua_thread)
            sqlengine_work_lua_thread(pool, work, thddata);
        else
            sqlengine_work_appsock(pool, work, thddata);
        break;
    case THD_FREE:
        /* we just mark the client done here, with error */
        ((struct sqlclntstate *)work)->query_rc = CDB2ERR_IO_ERROR;
        ((struct sqlclntstate *)work)->done =
            1; /* that's gonna revive appsock thread */
        break;
    }
}

static int send_heartbeat(struct sqlclntstate *clnt)
{
    int rc;
    struct fsqlresp resp;

    /* if client didnt ask for heartbeats, dont send them */
    if (!clnt->heartbeat)
        return 0;

    if (!clnt->ready_for_heartbeats) {
        return 0;
    }

    if (clnt->is_newsql) {
        newsql_write_response(clnt, FSQL_HEARTBEAT, NULL, 1, malloc, __func__,
                              __LINE__);
    } else {
        bzero(&resp, sizeof(resp));
        resp.response = FSQL_HEARTBEAT;
        resp.parm = 1;

        /* send a noop packet on clnt->sb */
        fsql_write_response(clnt, &resp, NULL, 0, 1, __func__, __LINE__);
    }

    return 0;
}

static int process_debug_pragma(struct sqlclntstate *clnt)
{
    int is_debug = 0;
    char *sql = clnt->sql;
    int rc = 0;

    is_debug = (strncasecmp(sql, "set debug ", 10) == 0);
    if (!is_debug)
        return is_debug;

    if (strncasecmp(sql + 10, "bdb ", 4) == 0) {
        rc = bdb_osql_trak(sql + 14, &clnt->bdb_osql_trak);

        return (rc == 0) ? 1 : rc;
    } else if (strncasecmp(sql + 10, "dbglog ", 6) == 0) {
        unsigned long long cookie;
        int queryid;
        cookie = strtoull(sql + 17, NULL, 16);
        clnt->client_understands_query_stats = 1;
        if (clnt->dbglog) {
            sbuf2close(clnt->dbglog);
            clnt->dbglog = NULL;
        }
        queryid = strtoul(sql + 34, NULL, 10);
        clnt->dbglog = open_dbglog_file(cookie);
        clnt->queryid = queryid;
        clnt->dbglog_cookie = cookie;
        return 1;
    }

    return -1;
}

/* timeradd() for struct timespec*/
#define TIMESPEC_ADD(a, b, result)                                             \
    do {                                                                       \
        (result).tv_sec = (a).tv_sec + (b).tv_sec;                             \
        (result).tv_nsec = (a).tv_nsec + (b).tv_nsec;                          \
        if ((result).tv_nsec >= 1000000000) {                                  \
            ++(result).tv_sec;                                                 \
            (result).tv_nsec -= 1000000000;                                    \
        }                                                                      \
    } while (0)

/* timersub() for struct timespec*/
#define TIMESPEC_SUB(a, b, result)                                             \
    do {                                                                       \
        (result).tv_sec = (a).tv_sec - (b).tv_sec;                             \
        (result).tv_nsec = (a).tv_nsec - (b).tv_nsec;                          \
        if ((result).tv_nsec < 0) {                                            \
            --(result).tv_sec;                                                 \
            (result).tv_nsec += 1000000000;                                    \
        }                                                                      \
    } while (0)

int dispatch_sql_query(struct sqlclntstate *clnt)
{
    int done;
    char msg[1024];
    char thdinfo[40];
    int rc;
    struct thr_handle *self = thrman_self();
    if (self) {
        if (clnt->exec_lua_thread)
            thrman_set_subtype(self, THRSUBTYPE_LUA_SQL);
        else
            thrman_set_subtype(self, THRSUBTYPE_TOPLEVEL_SQL);
    }

    bzero(&clnt->osql.timings, sizeof(osqltimings_t));
    bzero(&clnt->osql.fdbtimes, sizeof(fdbtimings_t));
    clnt->osql.timings.query_received = osql_log_time();

    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->deadlock_recovered = 0;
    clnt->done = 0;
    clnt->statement_timedout = 0;

    /* keep track so we can display it in stat thr */
    clnt->appsock_id = getarchtid();

    pthread_mutex_unlock(&clnt->wait_mutex);

    snprintf(msg, sizeof(msg), "%s \"%s\"", clnt->origin, clnt->sql);
    clnt->enque_timeus = time_epochus();

    char *sqlcpy;
    if ((rc = thdpool_enqueue(gbl_sqlengine_thdpool, sqlengine_work_appsock_pp,
                              clnt, (clnt->req.flags & SQLF_QUEUE_ME) ? 1 : 0,
                              sqlcpy = strdup(msg))) != 0) {
        free(sqlcpy);
        sqlcpy = NULL;
        if ((clnt->in_client_trans || clnt->osql.replay == OSQL_RETRY_DO) &&
            gbl_requeue_on_tran_dispatch) {
            /* force this request to queue */
            rc = thdpool_enqueue(gbl_sqlengine_thdpool,
                                 sqlengine_work_appsock_pp, clnt, 1,
                                 sqlcpy = strdup(msg));
        }

        if (rc) {
            if (sqlcpy)
                free(sqlcpy);
            /* say something back, if the client expects it */
            if (clnt->req.flags & SQLF_FAILDISPATCH_ON) {
                snprintf(msg, sizeof(msg), "%s: unable to dispatch sql query\n",
                         __func__);
                handle_failed_dispatch(clnt, msg);
            }

            return -1;
        }
    }

    /* successful dispatch or queueing, enable heartbeats */
    pthread_mutex_lock(&clnt->wait_mutex);
    clnt->ready_for_heartbeats = 1;
    pthread_mutex_unlock(&clnt->wait_mutex);

    /* SQL thread will unlock mutex when it is done, allowing us to lock it
     * again.  We block until then. */
    if (self)
        thrman_where(self, "waiting for query");

    if (clnt->heartbeat) {
        if ((clnt->osql.replay != OSQL_RETRY_NONE) ||
            (clnt->is_newsql && clnt->in_client_trans)) {
            send_heartbeat(clnt);
        }
        const struct timespec ms100 = {.tv_sec = 0, .tv_nsec = 100000000};
        struct timespec first, last;
        clock_gettime(CLOCK_REALTIME, &first);
        last = first;
        while (1) {
            struct timespec now, st;
            clock_gettime(CLOCK_REALTIME, &now);
            TIMESPEC_ADD(now, ms100, st);

            pthread_mutex_lock(&clnt->wait_mutex);
            if (clnt->done) {
                pthread_mutex_unlock(&clnt->wait_mutex);
                goto done;
            }
            int rc;
            rc = pthread_cond_timedwait(&clnt->wait_cond, &clnt->wait_mutex,
                                        &st);
            if (clnt->done) {
                pthread_mutex_unlock(&clnt->wait_mutex);
                goto done;
            }
            if (rc == ETIMEDOUT) {
                struct timespec diff;
                TIMESPEC_SUB(st, last, diff);
                if (diff.tv_sec >= clnt->heartbeat) {
                    last = st;
                    send_heartbeat(clnt);
                    fdb_heartbeats(clnt);
                }
                if (clnt->query_timeout > 0 && !clnt->statement_timedout) {
                    TIMESPEC_SUB(st, first, diff);
                    if (diff.tv_sec >= clnt->query_timeout) {
                        clnt->statement_timedout = 1;
                        logmsg(LOGMSG_WARN, "%s:%d Query exceeds max allowed time %d.\n",
                                __FILE__, __LINE__, clnt->query_timeout);
                    }
                }
            } else if (rc) {
                logmsg(LOGMSG_FATAL, "%s:%d pthread_cond_timedwait rc %d", __FILE__,
                        __LINE__, rc);
                exit(1);
            }

            if (pthread_mutex_trylock(&clnt->write_lock) == 0) {
                sbuf2flush(clnt->sb);
                pthread_mutex_unlock(&clnt->write_lock);
            }
            pthread_mutex_unlock(&clnt->wait_mutex);
        }
    } else {
        pthread_mutex_lock(&clnt->wait_mutex);
        while (!clnt->done) {
            pthread_cond_wait(&clnt->wait_cond, &clnt->wait_mutex);
        }
        pthread_mutex_unlock(&clnt->wait_mutex);
    }

done:
    if (self)
        thrman_where(self, "query done");
    return clnt->query_rc;
}

static void sqlengine_thd_start(struct thdpool *pool, void *thddata)
{
    struct sqlthdstate *thd = thddata;

    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);

    sql_mem_init(NULL);

    if (!gbl_use_appsock_as_sqlthread)
        thd->thr_self = thrman_register(THRTYPE_SQLENGINEPOOL);

    thd->logger = thrman_get_reqlogger(thd->thr_self);
    thd->buf = NULL;
    thd->maxbuflen = 0;
    thd->buflen = 0;
    thd->ncols = 0;
    // thd->stmt = NULL;
    thd->cinfo = NULL;
    thd->offsets = NULL;
    thd->sqldb = NULL;
    thd->stmt_table = NULL;
    thd->param_stmt_head = NULL;
    thd->param_stmt_tail = NULL;
    thd->noparam_stmt_head = NULL;
    thd->noparam_stmt_tail = NULL;
    thd->param_cache_entries = 0;
    thd->noparam_cache_entries = 0;

    start_sql_thread();

    thd->sqlthd = pthread_getspecific(query_info_key);
    void rcache_init(size_t, size_t);
    rcache_init(bdb_attr_get(thedb->bdb_attr, BDB_ATTR_RCACHE_COUNT),
                bdb_attr_get(thedb->bdb_attr, BDB_ATTR_RCACHE_PGSZ));
}

int gbl_abort_invalid_query_info_key;

static void sqlengine_thd_end(struct thdpool *pool, void *thddata)
{
    void rcache_destroy(void);
    rcache_destroy();
    struct sqlthdstate *thd = thddata;
    struct sql_thread *sqlthd;

    if ((sqlthd = pthread_getspecific(query_info_key)) != NULL) {
        /* sqlclntstate shouldn't be set: sqlclntstate is memory on another
         * thread's stack that will not be valid at this point. */

        if (sqlthd->sqlclntstate) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d sqlthd->sqlclntstate set in thd-teardown\n", __FILE__,
                   __LINE__);
            if (gbl_abort_invalid_query_info_key) {
                abort();
            }
            sqlthd->sqlclntstate = NULL;
        }
    }

    if (thd->stmt_table)
        delete_stmt_table(thd->stmt_table);
    if (thd->sqldb)
        sqlite3_close(thd->sqldb);
    if (thd->buf)
        free(thd->buf);
    if (thd->cinfo)
        free(thd->cinfo);
    if (thd->offsets)
        free(thd->offsets);

    /* AZ moved after the close which uses thd for rollbackall */
    done_sql_thread();

    sql_mem_shutdown(NULL);

    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
}

static inline int tdef_to_tranlevel(int tdef)
{
    switch (tdef) {
    case SQL_TDEF_SOCK:
        return TRANLEVEL_SOSQL;

    case SQL_TDEF_RECOM:
        return TRANLEVEL_RECOM;

    case SQL_TDEF_SERIAL:
        return TRANLEVEL_SERIAL;

    case SQL_TDEF_SNAPISOL:
        return TRANLEVEL_SNAPISOL;

    default:
        logmsg(LOGMSG_FATAL, "%s: line %d Unknown modedef: %d", __func__, __LINE__,
                tdef);
        abort();
    }
}

void reset_clnt(struct sqlclntstate *clnt, SBUF2 *sb, int initial)
{
    int wrtimeoutsec = 0;

    if (initial) {
        bzero(clnt, sizeof(*clnt));
    }
    clnt->sb = sb;
    clnt->must_close_sb = 1;
    clnt->recno = 1;
    strcpy(clnt->tzname, "America/New_York");
    clnt->dtprec = gbl_datetime_precision;
    bzero(&clnt->conninfo, sizeof(clnt->conninfo));
    clnt->rawnodestats = NULL;
    clnt->using_case_insensitive_like = 0;

    if (clnt->ctrl_sqlengine != SQLENG_INTRANS_STATE)
        clnt->intrans = 0;

    /* start off in comdb2 mode till we're told otherwise */
    clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    clnt->iswrite = 0;
    clnt->heartbeat = 0;
    clnt->limits.maxcost = gbl_querylimits_maxcost;
    clnt->limits.tablescans_ok = gbl_querylimits_tablescans_ok;
    clnt->limits.temptables_ok = gbl_querylimits_temptables_ok;
    clnt->limits.maxcost_warn = gbl_querylimits_maxcost_warn;
    clnt->limits.tablescans_warn = gbl_querylimits_tablescans_warn;
    clnt->limits.temptables_warn = gbl_querylimits_temptables_warn;

    bzero(&clnt->effects, sizeof(clnt->effects));
    bzero(&clnt->log_effects, sizeof(clnt->log_effects));

    /* reset the user */
    clnt->have_user = 0;
    bzero(clnt->user, sizeof(clnt->user));

    /* reset the password */
    clnt->have_password = 0;
    bzero(clnt->password, sizeof(clnt->user));

    /* reset endianess */
    clnt->have_endian = 0;
    clnt->endian = 0;

    /* reset extended_tm */
    clnt->have_extended_tm = 0;
    clnt->extended_tm = 0;

    /* reset page-order. */
    clnt->pageordertablescan =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN);

    /* let's reset osql structure as well */
    osql_clean_sqlclntstate(clnt);
    /* clear dbtran after aborting unfinished shadow transactions. */
    bzero(&clnt->dbtran, sizeof(dbtran_type));

    clnt->origin = intern(get_origin_mach_by_buf(sb));

    clnt->in_client_trans = 0;
    clnt->had_errors = 0;
    clnt->statement_timedout = 0;
    clnt->query_timeout = 0;
    if (clnt->saved_errstr) {
        free(clnt->saved_errstr);
        clnt->saved_errstr = NULL;
    }
    clnt->saved_rc = 0;
    clnt->want_stored_procedure_debug = 0;
    clnt->want_stored_procedure_trace = 0;
    clnt->want_query_effects = 0;
    clnt->send_one_row = 0;
    clnt->verifyretry_off = 0;

    /* Reset the version, we have to set it for every run */
    clnt->spname[0] = 0;
    clnt->spversion.version_num = 0;
    free(clnt->spversion.version_str);
    clnt->spversion.version_str = NULL;

    clnt->is_explain = 0;
    clnt->get_cost = 0;
    clnt->snapshot = 0;
    clnt->num_retry = 0;
    clnt->early_retry = 0;
    clnt->sql_query = NULL;
    clnt_reset_cursor_hints(clnt);

    clnt->skip_feature = 0;

    bzero(clnt->dirty, sizeof(clnt->dirty));

    if (gbl_sqlwrtimeoutms == 0)
        wrtimeoutsec = 0;
    else
        wrtimeoutsec = gbl_sqlwrtimeoutms / 1000;

    if (sb && sbuf2fileno(sb) > 2) // if valid sb and sb->fd is not stdio
    {
        net_add_watch_warning(
            clnt->sb, bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME),
            wrtimeoutsec, clnt, watcher_warning_function);
    }
    clnt->planner_effort =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_PLANNER_EFFORT);
    clnt->osql_max_trans = g_osql_max_trans;

    clnt->arr = NULL;
    clnt->selectv_arr = NULL;
    clnt->file = 0;
    clnt->offset = 0;
    clnt->enque_timeus = clnt->deque_timeus = 0;
    reset_clnt_flags(clnt);

    clnt->ins_keys = 0ULL;
    clnt->del_keys = 0ULL;
    clnt->has_sqliterow = 0;
    clnt->verify_indexes = 0;
    clnt->schema_mems = NULL;
    clnt->init_gen = 0;
    for (int i = 0; i < clnt->ncontext; i++) {
        free(clnt->context[i]);
    }
    free(clnt->context);
    clnt->context = NULL;
    clnt->ncontext = 0;
}

void reset_clnt_flags(struct sqlclntstate *clnt)
{
    clnt->writeTransaction = 0;
    clnt->has_recording = 0;
}

static void handle_sql_intrans_unrecoverable_error(struct sqlclntstate *clnt)
{
    int osqlrc = 0;
    int rc = 0;
    int bdberr = 0;

    if (clnt && clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
        switch (clnt->dbtran.mode) {
        case TRANLEVEL_SOSQL:
            osql_sock_abort(clnt, OSQL_SOCK_REQ);
            if (clnt->selectv_arr) {
                currangearr_free(clnt->selectv_arr);
                clnt->selectv_arr = NULL;
            }
            break;

        case TRANLEVEL_RECOM:
            recom_abort(clnt);
            break;

        case TRANLEVEL_SNAPISOL:
        case TRANLEVEL_SERIAL:
            serial_abort(clnt);
            if (clnt->arr) {
                currangearr_free(clnt->arr);
                clnt->arr = NULL;
            }
            if (clnt->selectv_arr) {
                currangearr_free(clnt->selectv_arr);
                clnt->selectv_arr = NULL;
            }

            break;

        default:
            /* I don't expect this */
            abort();
        }

        clnt->intrans = 0;
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__,
                                SQLENG_NORMAL_PROCESS);
    }
}

static int sbuf_is_local(SBUF2 *sb)
{
    struct sockaddr_in addr;

    if (net_appsock_get_addr(sb, &addr))
        return 1;

    if (addr.sin_addr.s_addr == gbl_myaddr.s_addr)
        return 1;

    if (addr.sin_addr.s_addr == INADDR_LOOPBACK)
        return 1;

    return 0;
}

/* This function is now just the i/o loop.  Queries are executed in the function
 * above. */
static int handle_fastsql_requests_io_loop(struct sqlthdstate *thd,
                                           struct sqlclntstate *clnt)
{
    int rc;
    int i;
    int *endian;
    enum transaction_level newlvl;
    int sz, sqllen, tagsz, nullbitmapsz;
    char *p;
    uint8_t *p_buf, *p_buf_end;
    int bloblen;
    int blobno;
    struct sqlthdstate sqlthd;
    int do_master_check = 1;

#ifdef OSQL_TRACK_ALL
    bdb_osql_trak("ALL", &clnt->bdb_osql_trak);
#endif
    if (gbl_use_appsock_as_sqlthread) {
        sqlthd.thr_self = thd->thr_self;
        sqlengine_thd_start(NULL, &sqlthd);
    }

    if (thedb->rep_sync == REP_SYNC_NONE)
        do_master_check = 0;

    if (do_master_check && sbuf_is_local(clnt->sb))
        do_master_check = 0;

    if (do_master_check && bdb_master_should_reject(thedb->bdb_env) &&
        (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS)) {
        logmsg(LOGMSG_INFO, "new query on master, dropping socket\n");
        goto done;
    }

    if (active_appsock_conns >
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXAPPSOCKSLIMIT)) {
        logmsg(LOGMSG_WARN, 
                "%s: Exhausted appsock connections, total %d connections \n",
                __func__, active_appsock_conns);
        char *err = "Exhausted appsock connections.";
        struct fsqlresp resp;
        bzero(&resp, sizeof(resp));
        resp.response = FSQL_ERROR;
        resp.rcode = SQLHERR_APPSOCK_LIMIT;
        rc = fsql_write_response(clnt, &resp, err, strlen(err) + 1, 1, __func__,
                                 __LINE__);
        goto done;
    }

    for (;;) {
        /* reset blosql conversion flags */
        if (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS) {
            clnt->had_errors = 0;
            if (clnt->saved_errstr) {
                free(clnt->saved_errstr);
                clnt->saved_errstr = NULL;
            }
            clnt->saved_rc = 0;
        }
        thrman_wheref(thd->thr_self, "blocked reading next query from %s",
                      clnt->origin);

        rc = handle_fastsql_requests_io_read(thd, clnt, FSQLREQ_LEN);
        if (rc != 0) {
            /*printf("%s: rc %d\n", __func__, rc);*/
            logmsg(LOGMSG_ERROR, "%s line %d td %u: rc %d\n", __func__, __LINE__,
                   (uint32_t)pthread_self(), rc);
            goto done;
        }

        /* avoid new accepting new queries/transaction on opened connections
           if we are incoherent (and not in a transaction). */
        if (!bdb_am_i_coherent(thedb->bdb_env) &&
            (clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS)) {
            logmsg(LOGMSG_ERROR, "%s line %d td %u: new query on incoherent node, "
                            "dropping socket\n",
                    __func__, __LINE__, (uint32_t)pthread_self());
            goto done;
        }

        if (do_master_check && bdb_master_should_reject(thedb->bdb_env) &&
            !clnt->intrans) {
            logmsg(LOGMSG_USER, 
                    "%s line %d td %u: new query on master, dropping socket\n",
                    __func__, __LINE__, (uint32_t)pthread_self());
            goto done;
        }

        /* unpack the request */
        if (!fsqlreq_get(&clnt->req, (uint8_t *)thd->buf,
                         (const uint8_t *)(thd->buf + FSQLREQ_LEN))) {
            rc = -1;
            logmsg(LOGMSG_ERROR, "%s line %d td %u: fsqlreq_get failure\n", __func__,
                    __LINE__, (uint32_t)pthread_self());
            goto done;
        }

        /* read in the data */
        rc = handle_fastsql_requests_io_read(thd, clnt, clnt->req.followlen);
        if (rc != 0) {
            logmsg(LOGMSG_ERROR,
                "%s line %d td %u: failed to get data following request %d\n",
                __func__, __LINE__, (uint32_t)pthread_self(),
                clnt->req.request);
            goto done;
        }

        if (thedb->stopped)
            goto done;

        switch (clnt->req.request) {
        case FSQL_EXECUTE_INLINE_PARAMS_TZ:
            rc = sbuf2fread(clnt->tzname, sizeof(clnt->tzname), 1, clnt->sb);
            if (rc != 1) {
                logmsg(LOGMSG_ERROR, "%s line %d td %u: error reading tzname in request %d\n",
                       __func__, __LINE__, (uint32_t)pthread_self(),
                       clnt->req.request);
                goto done;
            }
            clnt->tzname[TZNAME_MAX - 1] = 0;
        /* there is a mismatch between client tzname (36 bytes)
           and server representation -minimum value == 33 bytes
           from now on tzname will be up to TZNAME_MAX
           */
        /* fall through */
        case FSQL_EXECUTE_INLINE_PARAMS:
            if (clnt->req.followlen - clnt->req.parm * sizeof(int) <= 0) {
                logmsg(LOGMSG_ERROR, "%s line %d td %u: incorrect data length sent\n",
                       __func__, __LINE__, (uint32_t)pthread_self());
                goto done;
            }

            if (clnt->wrong_db) {
                struct fsqlresp rsp;
                char *err = "SQL connection handshake error.";
                rsp.response = FSQL_ERROR;
                rsp.rcode = SQLHERR_WRONG_DB;
                rsp.flags = 0;
                rsp.parm = 0;
                fsql_write_response(clnt, &rsp, err, strlen(err) + 1, 1,
                                    __func__, __LINE__);
                logmsg(LOGMSG_ERROR, "%s line %d td %u: wrong db error\n", __func__, __LINE__,
                       (uint32_t)pthread_self());
                goto done;
            }

            /* Reset sent column data */
            clnt->osql.sent_column_data = 0;

            if (clnt->req.flags & SQLF_CONVERTED_BLOSQL) {
                logmsg(LOGMSG_ERROR, "Unsupported transactional mode blosql!\n");
                send_prepare_error(clnt,
                                   "Unsupported transactional mode blosql", 0);
                goto done;
            }

            if (clnt->req.flags & SQLF_WANT_QUERY_EFFECTS)
                clnt->want_query_effects = 1;

            if (clnt->req.flags & SQLREQ_FLAG_WANT_SP_TRACE)
                clnt->want_stored_procedure_trace = 1;

            if (clnt->req.flags & SQLREQ_FLAG_WANT_SP_DEBUG)
                clnt->want_stored_procedure_debug = 1;

            if (clnt->req.flags & SQLF_WANT_READONLY_ACCESS) {
                clnt->is_readonly = 1;
            } else {
                clnt->is_readonly = 0;
            }

            if (clnt->req.flags & SQLF_WANT_VERIFYRETRY_OFF) {
                clnt->verifyretry_off = 1;
            } else {
                clnt->verifyretry_off = 0;
            }

            /* We already had an error, but have no way to indicate this to the
             * client
             * until they commit/abort.  So consume all incoming statements and
             * ignore them until they do. */
            if (clnt->had_errors) {
                if (strncasecmp(clnt->sql, "commit", 6) &&
                    strncasecmp(clnt->sql, "rollback", 8))
                    continue;
            }

            clnt->sql = thd->buf + clnt->req.parm * sizeof(int);
            while (isspace(*clnt->sql))
                clnt->sql++;
            clnt->type_overrides = (int *)thd->buf;
            for (i = 0; i < clnt->req.parm; ++i)
                clnt->type_overrides[i] = ntohl(clnt->type_overrides[i]);

            clnt->tag = NULL;
            clnt->tagbuf = NULL;
            clnt->tagbufsz = 0;
            if (gbl_notimeouts)
                net_add_watch(clnt->sb, 0, 0);

            /* We're running from an appsock pool which has tiny
             * threads, so we need to pass this query to an sql engine
             * thread to execute it and take control back once it's
             * done. */
            if (gbl_use_appsock_as_sqlthread) {
                sqlengine_work_appsock(NULL, clnt, &sqlthd);
                rc = 0;
            } else {
                /* tell blobmem that I want my priority back
                   when the sql thread is done */
                comdb2bma_pass_priority_back(blobmem);
                rc = dispatch_sql_query(clnt);
            }
            if (clnt->osql.replay == OSQL_RETRY_DO) {
                rc = srs_tran_replay(clnt, thd->thr_self);
            } else {
                /* if this transaction is done (marked by
                   SQLENG_NORMAL_PROCESS),
                   clean transaction sql history
                 */
                if (clnt->osql.history &&
                    clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS)
                    srs_tran_destroy(clnt);
            }
            if (rc) {
                clnt->had_errors = 1;
                /* we won't get here for subsequent errors under this
                 * transaction */
            }

            if (fsql_write_response(clnt, NULL, NULL, 0, 1, __func__,
                                    __LINE__) < 0)
                rc = -1;

            if (rc && !(clnt->want_query_effects && clnt->in_client_trans)) {
                //                printf("%s line %d td %u: erroring out\n",
                //                __func__, __LINE__,
                //                        (uint32_t)pthread_self());
                goto done;
            }

            break;

        case FSQL_EXECUTE_STOP: /* unused (and useless) */
            reqlog_logl(thd->logger, REQL_INFO, "FSQL_EXECUTE_STOP");
            fsql_write_response(clnt, NULL, NULL, 0, 1, __func__, __LINE__);
            break;

        case FSQL_SET_PASSWORD:
            clnt->have_password = 1;
            memcpy(clnt->password, thd->buf, 16);
            break;

        case FSQL_SET_USER:
            clnt->have_user = 1;
            memcpy(clnt->user, thd->buf, 16);
            break;

        case FSQL_SET_ENDIAN:
            endian = (int *)thd->buf;
            clnt->have_endian = 1;
            clnt->endian = ntohl(*endian);
            break;

        case FSQL_SET_HEARTBEAT:
            if (clnt->req.parm > 0)
                clnt->heartbeat = clnt->req.parm;
            break;

        /*fprintf(stderr, "setting client heartbeat to %d\n",
          clnt->req.parm);*/

        case FSQL_EXECUTE_REPLACEABLE_PARAMS_TZ:
            rc = sbuf2fread(clnt->tzname, sizeof(clnt->tzname), 1, clnt->sb);
            if (rc != 1) {
                logmsg(LOGMSG_ERROR, "%s line %d td %u: error reading tzname in request %d\n",
                       __func__, __LINE__, (uint32_t)pthread_self(),
                       clnt->req.request);
                goto done;
            }
            clnt->tzname[TZNAME_MAX - 1] = 0;
        /* there is a mismatch between client tzname (36 bytes)
           and server representation -minimum value == 33 bytes
           from now on tzname will be up to TZNAME_MAX
         */
        /* Don't break */
        case FSQL_EXECUTE_REPLACEABLE_PARAMS:
            /* TODO: a little more size checking here, endian correctness */
            /* sql length */
            p_buf = (uint8_t *)thd->buf;
            p_buf_end = (p_buf + thd->maxbuflen);

            /* overrides */
            clnt->type_overrides = (int *)thd->buf;
            for (i = 0; i < clnt->req.parm; ++i)
                clnt->type_overrides[i] = ntohl(clnt->type_overrides[i]);

            /* sql */
            p_buf = (unsigned char *)(thd->buf + clnt->req.parm * sizeof(int));
            p_buf =
                (uint8_t *)buf_get(&sqllen, sizeof(sqllen), p_buf, p_buf_end);
            clnt->sqllen = sqllen;
            clnt->sql = (char *)p_buf;
            while (isspace(*clnt->sql)) {
                clnt->sql++;
                clnt->sqllen--;
            }

            if (clnt->had_errors) {
                if (strncasecmp(clnt->sql, "commit", 6) &&
                    strncasecmp(clnt->sql, "rollback", 8))
                    continue;
            }

            p_buf += sqllen;

            /* dyntag header size */
            p_buf = (uint8_t *)buf_get(&tagsz, sizeof(tagsz), p_buf, p_buf_end);

            /* dyntag header */
            clnt->tag = (char *)p_buf;
            p_buf += tagsz;

            /* dyntag buffer size */
            p_buf = (uint8_t *)buf_get(&clnt->tagbufsz, sizeof(clnt->tagbufsz),
                                       p_buf, p_buf_end);

            /* dyntag buffer */
            clnt->tagbuf = p_buf;
            p_buf += clnt->tagbufsz;

            /* timezone */
            /* Moved to top.
            p_buf = (uint8_t *)buf_no_net_get(&clnt->tzname,
                    sizeof(clnt->tzname), p_buf, p_buf_end);
            */

            /* num null bytes */
            p_buf = (uint8_t *)buf_get(&nullbitmapsz, sizeof(nullbitmapsz),
                                       p_buf, p_buf_end);
            clnt->numnullbits = nullbitmapsz;

            /* null bytes */
            clnt->nullbits = p_buf;
            p_buf += nullbitmapsz;

            /* num blobs */
            p_buf = (uint8_t *)buf_get(&clnt->numblobs, sizeof(clnt->numblobs),
                                       p_buf, p_buf_end);

            /* number of blobs shouldn't be fixed.
             * Try to keep 16 (the old limit) inline, allocate more
             * if the client has more.
             * */
            if (clnt->numblobs) {
                if (clnt->numblobs <= MAXBLOBS) {
                    clnt->blobs = clnt->inline_blobs;
                    clnt->bloblens = clnt->inline_bloblens;
                } else {
                    if (clnt->numallocblobs < clnt->numblobs) {
                        clnt->alloc_blobs = realloc(
                            clnt->alloc_blobs, sizeof(void *) * clnt->numblobs);
                        clnt->alloc_bloblens = realloc(
                            clnt->alloc_bloblens, sizeof(int) * clnt->numblobs);
                        clnt->numallocblobs = clnt->numblobs;
                    }
                    clnt->blobs = clnt->alloc_blobs;
                    clnt->bloblens = clnt->alloc_bloblens;
                }
            }

            /* blobs */
            for (blobno = 0; blobno < clnt->numblobs; blobno++) {
                /* blob size */
                p_buf = (uint8_t *)buf_get(&(clnt->bloblens[blobno]),
                                           sizeof(int), p_buf, p_buf_end);

                /* blob data */
                clnt->blobs[blobno] = p_buf;
                p_buf += clnt->bloblens[blobno];
            }

            /* Reset sent column data */
            clnt->osql.sent_column_data = 0;

            if (gbl_use_appsock_as_sqlthread) {
                sqlengine_work_appsock(NULL, clnt, &sqlthd);
                rc = 0;
            } else {
                /* tell blobmem that I want my priority back
                   when the sql thread is done */
                comdb2bma_pass_priority_back(blobmem);
                rc = dispatch_sql_query(clnt);
            }
            if (clnt->osql.replay == OSQL_RETRY_DO) {
                rc = srs_tran_replay(clnt, thd->thr_self);
            } else {
                /* if this transaction is done (marked by
                   SQLENG_NORMAL_PROCESS),
                   clean transaction sql history
                 */
                if (clnt->osql.history &&
                    clnt->ctrl_sqlengine == SQLENG_NORMAL_PROCESS)
                    srs_tran_destroy(clnt);
            }
            if (rc) {
                clnt->had_errors = 1;
                /* we won't get here for subsequent errors under this
                 * transaction */
            }

            if (fsql_write_response(clnt, NULL, NULL, 0, 1, __func__,
                                    __LINE__) < 0)
                rc = -1;

            if (rc && !(clnt->want_query_effects && clnt->in_client_trans)) {
                goto done;
            }

            break;

        case FSQL_SET_TIMEOUT: {
            /* parm contains timeout. just set it.  no response to write. */
            int notimeout = disable_server_sql_timeouts();
            if (notimeout)
                reqlog_logf(thd->logger, REQL_INFO,
                            "IGNORING FSQL_SET_TIMEOUT");
            else
                reqlog_logf(thd->logger, REQL_INFO, "FSQL_SET_TIMEOUT %d",
                            clnt->req.parm);
            sbuf2settimeout(clnt->sb, 0, notimeout ? 0 : clnt->req.parm);

            if (clnt->req.parm == 0)
                net_add_watch(clnt->sb, 0, 0);
            else
                net_add_watch_warning(
                    clnt->sb,
                    bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME),
                    notimeout ? 0 : clnt->req.parm / 1000, clnt,
                    watcher_warning_function);

            break;
        }
        case FSQL_SET_PLANNER_EFFORT:
            clnt->planner_effort = clnt->req.parm;
            break;

        case FSQL_OSQL_MAX_TRANS:
            clnt->osql_max_trans = clnt->req.parm;
#ifdef DEBUG
            printf("setting clnt->osql_max_trans to %d\n",
                   clnt->osql_max_trans);
#endif
            break;

        case FSQL_SET_DATETIME_PRECISION:
            if (clnt->req.parm == DTTZ_PREC_MSEC ||
                clnt->req.parm == DTTZ_PREC_USEC)
                clnt->dtprec = clnt->req.parm;
            break;

        case FSQL_SET_REMOTE_ACCESS: {
            char *settings = thd->buf;

            rc = fdb_access_control_create(clnt, settings);
            if (rc) {
                logmsg(LOGMSG_ERROR,
                        "%s: failed to process remote access settings \"%s\"\n",
                        __func__, settings);
            }
        }

        break;

        case FSQL_SET_SQL_DEBUG:
            clnt->sql = thd->buf;
            rc = process_debug_pragma(clnt);
            if (rc != 1)
                logmsg(LOGMSG_ERROR,
                        "Error interpreting FSQL_SET_SQL_DEBUG (%d) rc=%d\n",
                        clnt->req.request, rc);
            break;

        case FSQL_SET_ISOLATION_LEVEL:
            newlvl = (enum transaction_level)clnt->req.parm;
            reqlog_logf(thd->logger, REQL_INFO, "FSQL_SET_ISOLATION_LEVEL %d",
                        newlvl);

            switch (newlvl) {
            case TRANLEVEL_SERIAL:
            case TRANLEVEL_SNAPISOL:
            case TRANLEVEL_RECOM:
            case TRANLEVEL_SOSQL:
                /* these connections shouldn't time out */
                sbuf2settimeout(
                    clnt->sb,
                    bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME) *
                        1000,
                    0);

                /* pragma count_changes=1 */
                clnt->osql.count_changes = 1;

                if (gbl_rowlocks && newlvl != TRANLEVEL_SERIAL) {
                    clnt->dbtran.mode = TRANLEVEL_SNAPISOL;
                } else {
                    clnt->dbtran.mode = newlvl;
                }
                if (thd->sqldb) {
                    rc = sql_set_transaction_mode(thd->sqldb, clnt,
                                                  clnt->dbtran.mode);
                    if (rc) {
                        send_prepare_error(
                            clnt, "Failed to set transaction mode.", 0);
                        goto done;
                    }
                }
                break;

            default:
                break;
            }
            break;

        case FSQL_RESET:
            if (clnt && clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
                ctrace("Client has malformed transactions, socket sent to "
                       "sockpool before its time\n");
                rc = -1;
                goto done;
            }
            reset_clnt(clnt, clnt->sb, 0);
            reqlog_set_origin(thd->logger, "");
            break;

        case FSQL_GET_EFFECTS: {
            rc = send_query_effects(clnt);
        } break;

        case FSQL_GRAB_DBGLOG: {
            unsigned long long cookie;
            p_buf = (uint8_t *)thd->buf;
            p_buf_end = (p_buf + thd->maxbuflen);
            buf_get(&cookie, sizeof(cookie), p_buf, p_buf_end);
            grab_dbglog_file(clnt->sb, cookie, clnt);
            break;
        }

        case FSQL_SET_INFO:
            reqlog_logl(thd->logger, REQL_INFO, "FSQL_SET_INFO");
            if (!conninfo_get(
                    &clnt->conninfo, (const uint8_t *)thd->buf,
                    (const uint8_t *)(thd->buf + sizeof(struct conninfo)))) {
                rc = -1;
                goto done;
            }

            char *from;
            if (clnt->conninfo.node == 0)
                snprintf(clnt->origin_space, sizeof(clnt->origin_space),
                         "machine %s pid %d task %.8s", clnt->origin,
                         clnt->conninfo.pid, clnt->conninfo.pename);
            else
                snprintf(clnt->origin_space, sizeof(clnt->origin_space),
                         "machine %d pid %d task %.8s", clnt->conninfo.node,
                         clnt->conninfo.pid, clnt->conninfo.pename);

            reqlog_set_origin(thd->logger, clnt->origin_space);

            if (thd->sqlthd) {
                memcpy(&thd->sqlthd->sqlclntstate->conn, &clnt->conninfo,
                       sizeof(struct conninfo));
                thd->sqlthd->sqlclntstate->origin = clnt->origin;
            }
            break;

        case FSQL_PRAGMA: {
            double cost = 0;
            int onoff = 1;
            int version = 0;
            clnt->have_query_limits = 1;
            /* TODO: endianize */
            switch (clnt->req.parm) {
#if 0
                case SQL_PRAGMA_ERROR:
                    fprintf(stderr, "Thread %d got SQL_PRAGMA_ERROR, '%s'\n",
                          pthread_self(), thd->buf);
                    break;
#endif
            case SQL_PRAGMA_MAXCOST:
                memcpy(&cost, thd->buf, sizeof(double));
                cost = flibc_ntohd(cost);
                clnt->limits.maxcost = cost;
                break;
            case SQL_PRAGMA_TABLESCAN_OK:
                memcpy(&onoff, thd->buf, sizeof(int));
                clnt->limits.tablescans_ok = htonl(onoff);
                break;
            case SQL_PRAGMA_TEMPTABLES_OK:
                memcpy(&onoff, thd->buf, sizeof(int));
                clnt->limits.temptables_ok = htonl(onoff);
                break;

            case SQL_PRAGMA_MAXCOST_WARNING:
                memcpy(&cost, thd->buf, sizeof(double));
                cost = flibc_ntohd(cost);
                clnt->limits.maxcost_warn = cost;
                break;
            case SQL_PRAGMA_TABLESCAN_WARNING:
                memcpy(&onoff, thd->buf, sizeof(int));
                clnt->limits.tablescans_warn = onoff;
                break;
            case SQL_PRAGMA_TEMPTABLES_WARNING:
                memcpy(&onoff, thd->buf, sizeof(int));
                clnt->limits.temptables_warn = onoff;
                break;
            case SQL_PRAGMA_EXTENDED_TM:
                clnt->have_extended_tm = 1;
                memcpy(&onoff, thd->buf, sizeof(int));
                clnt->extended_tm = ntohl(onoff);
                if (clnt->extended_tm)
                    gbl_extended_tm_from_sql++;
                break;
            case SQL_PRAGMA_SP_VERSION:
                if (clnt->spversion.version_str) {
                    free(clnt->spversion.version_str);
                    clnt->spversion.version_str = NULL;
                }
                memcpy(&version, thd->buf, sizeof(int));
                clnt->spversion.version_num = ntohl(version);
                strncpy(clnt->spname, thd->buf + sizeof(int), MAX_SPNAME);
                clnt->spname[MAX_SPNAME] = '\0';
                break;

            default:
                /* don't know what it is, ignore it */
                break;
            }
            break;
        }

        default:
            logmsg(LOGMSG_ERROR, "Unknown request: %d\n", clnt->req.request);
            break;
        }
        reqlog_end_request(thd->logger, 0, __func__, __LINE__);
    }

done:
    /* we end up with an open transaction here for
       - conversion errors
       - timeouts on client socket
     */
    if (gbl_use_appsock_as_sqlthread) {
        sqlengine_thd_end(NULL, &sqlthd);
    }
    if (clnt && clnt->ctrl_sqlengine == SQLENG_INTRANS_STATE) {
        handle_sql_intrans_unrecoverable_error(clnt);
    }

    reqlog_end_request(thd->logger, rc, __func__, __LINE__);
    thrman_where(thd->thr_self, NULL);
    if (thd->buf) {
        if (clnt->sql >= thd->buf &&
            clnt->sql < ((char *)thd->buf + thd->maxbuflen)) {
            // clnt->sql was using thd->buf
            clnt->sql = NULL;
        }
        free(thd->buf);
        thd->buf = NULL;
        thd->maxbuflen = 0;
    }
    if (thd->cinfo) {
        free(thd->cinfo);
        thd->cinfo = NULL;
    }
    if (thd->offsets) {
        free(thd->offsets);
        thd->offsets = NULL;
    }

    /* Release client resources if we still own them */
    if (clnt) {
        if (clnt->using_case_insensitive_like) {
            if (thd->sqldb)
                toggle_case_sensitive_like(thd->sqldb, 0);
            clnt->using_case_insensitive_like = 0;
        }

        if (clnt->sb) {
            if (clnt->must_close_sb) {
                close_appsock(clnt->sb);
            } else {
                fsql_write_response(clnt, NULL, NULL, 0, 1, __func__, __LINE__);
            }
            clnt->sb = NULL;
        }

        if (clnt->saved_errstr) {
            free(clnt->saved_errstr);
            clnt->saved_errstr = NULL;
        }
        if (clnt->numallocblobs) {
            free(clnt->alloc_blobs);
            clnt->alloc_blobs = NULL;
            free(clnt->alloc_bloblens);
            clnt->alloc_bloblens = NULL;
        }
    }

    return rc;
}

static void send_dbinforesponse(SBUF2 *sb)
{
    struct newsqlheader hdr;
    CDB2DBINFORESPONSE *dbinfo_response = malloc(sizeof(CDB2DBINFORESPONSE));
    cdb2__dbinforesponse__init(dbinfo_response);

    fill_dbinfo(dbinfo_response, thedb->bdb_env);

    int len = cdb2__dbinforesponse__get_packed_size(dbinfo_response);
    void *buf = malloc(len);
    cdb2__dbinforesponse__pack(dbinfo_response, buf);

    hdr.type = ntohl(RESPONSE_HEADER__DBINFO_RESPONSE);
    hdr.compression = 0;
    hdr.dummy = 0;
    hdr.length = ntohl(len);

    sbuf2write((char *)&hdr, sizeof(hdr), sb);

    sbuf2write(buf, len, sb);
    sbuf2flush(sb);
    free(buf);
    cdb2__dbinforesponse__free_unpacked(dbinfo_response, &pb_alloc);
}

CDB2QUERY *read_newsql_query(struct sqlclntstate *clnt, SBUF2 *sb)
{
    struct newsqlheader hdr;
    int rc;
    int pre_enabled = 0;
    int was_timeout = 0;
    char ssl_able;

retry_read:
    rc = sbuf2fread_timeout((char *)&hdr, sizeof(hdr), 1, sb, &was_timeout);
    if (rc != 1) {
        if (was_timeout) {
            handle_failed_dispatch(clnt, "Socket read timeout.");
        }
        return NULL;
    }

    hdr.type = ntohl(hdr.type);
    hdr.compression = ntohl(hdr.compression);
    hdr.length = ntohl(hdr.length);

    if (hdr.type == FSQL_SSLCONN) {
        /* If client requires SSL and we haven't done that,
           do SSL_accept() now. handle_newsql_requests()
           will close the sb if SSL_accept() fails. */

        /* Can't SSL_accept twice - probably a client API logic error.
           Let it disconnect. */
        if (sslio_has_ssl(sb)) {
            logmsg(LOGMSG_WARN, "The connection is already SSL encrypted.\n");
            return NULL;
        }

        /* Flush the SSL ability byte. We need to do this because:
           1) The `require_ssl` field in dbinfo may not reflect the
              actual status of this node;
           2) Doing SSL_accept() immediately would cause too many
              unnecessary EAGAIN/EWOULDBLOCK's for non-blocking BIO. */
        ssl_able = (gbl_client_ssl_mode >= SSL_ALLOW) ? 'Y' : 'N';
        if ((rc = sbuf2putc(sb, ssl_able)) < 0 || (rc = sbuf2flush(sb)) < 0)
            return NULL;

        if (ssl_able == 'Y' &&
            sslio_accept(sb, gbl_ssl_ctx, SSL_REQUIRE, NULL, 0) != 1)
            return NULL;

        if (sslio_verify(sb, gbl_client_ssl_mode, NULL, 0) != 0) {
            char *err = "Client certificate authentication failed.";
            struct fsqlresp resp;
            bzero(&resp, sizeof(resp));
            resp.response = FSQL_ERROR;
            resp.rcode = CDB2ERR_CONNECT_ERROR;
            rc = fsql_write_response(clnt, &resp, err, strlen(err) + 1, 1,
                                     __func__, __LINE__);
            return NULL;
        }

        goto retry_read;
    } else if (hdr.type == FSQL_RESET) { /* Reset from sockpool.*/
        reset_clnt(clnt, sb, 0);
        clnt->tzname[0] = '\0';
        clnt->osql.count_changes = 1;
        clnt->heartbeat = 1;
        clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
        goto retry_read;
    }

    if (hdr.type > 2) {
        logmsg(LOGMSG_ERROR, "%s: Invalid message  %d\n", __func__, hdr.type);
        return NULL;
    }

    int bytes = hdr.length;

    if (bytes <= 0) {
        logmsg(LOGMSG_ERROR, "%s: Junk message  %d\n", __func__, bytes);
        return NULL;
    }

    CDB2QUERY *query;

    char *p;

    if (bytes <= gbl_blob_sz_thresh_bytes)
        p = malloc(bytes);
    else
        while (1) { // big buffer. most certainly it is a huge blob.
            p = comdb2_timedmalloc(blobmem, bytes, 1000);

            if (p != NULL || errno != ETIMEDOUT)
                break;

            pthread_mutex_lock(&clnt->wait_mutex);
            clnt->heartbeat = 1;
            if (clnt->ready_for_heartbeats == 0) {
                pre_enabled = 1;
                clnt->ready_for_heartbeats = 1;
            }
            send_heartbeat(clnt);
            fdb_heartbeats(clnt);
            pthread_mutex_unlock(&clnt->wait_mutex);
        }

    if (pre_enabled) {
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        pthread_mutex_unlock(&clnt->wait_mutex);
        pre_enabled = 0;
    }

    if (!p) {
        logmsg(LOGMSG_ERROR, "%s: out of memory malloc %d\n", __func__, bytes);
        return NULL;
    }

    if (bytes) {
        rc = sbuf2fread(p, bytes, 1, sb);
        if (rc != 1) {
            free(p);
            return NULL;
        }
    }

    while (1) {
        query = cdb2__query__unpack(&pb_alloc, bytes, p);

        if (query != NULL || errno != ETIMEDOUT)
            break;

        pthread_mutex_lock(&clnt->wait_mutex);
        if (clnt->heartbeat == 0)
            clnt->heartbeat = 1;
        if (clnt->ready_for_heartbeats == 0) {
            pre_enabled = 1;
            clnt->ready_for_heartbeats = 1;
        }
        send_heartbeat(clnt);
        fdb_heartbeats(clnt);
        pthread_mutex_unlock(&clnt->wait_mutex);
    }

    if (pre_enabled) {
        pthread_mutex_lock(&clnt->wait_mutex);
        clnt->ready_for_heartbeats = 0;
        pthread_mutex_unlock(&clnt->wait_mutex);
    }

    if (query && query->dbinfo) {
        if (query->dbinfo->has_want_effects &&
            query->dbinfo->want_effects == 1) {
            CDB2SQLRESPONSE sql_response = CDB2__SQLRESPONSE__INIT;
            CDB2EFFECTS effects = CDB2__EFFECTS__INIT;
            sql_response.response_type =
                RESPONSE_TYPE__COMDB2_INFO; /* Last Row. */
            sql_response.n_value = 0;
            if (clnt->verifyretry_off == 1 ||
                clnt->dbtran.mode == TRANLEVEL_SNAPISOL ||
                clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                clnt->effects.num_affected = clnt->effects.num_updated +
                                             clnt->effects.num_deleted +
                                             clnt->effects.num_inserted;
                effects.num_affected = clnt->effects.num_affected;
                effects.num_selected = clnt->effects.num_selected;
                effects.num_updated = clnt->effects.num_updated;
                effects.num_deleted = clnt->effects.num_deleted;
                effects.num_inserted = clnt->effects.num_inserted;
                sql_response.effects = &effects;
                sql_response.error_code = 0;
            } else {
                sql_response.error_code = -1;
                sql_response.error_string = "Get effects not supported in "
                                            "transaction with verifyretry on";
            }

            newsql_write_response(clnt, RESPONSE_HEADER__SQL_EFFECTS,
                                  &sql_response, 1 /*flush*/, malloc, __func__,
                                  __LINE__);

            goto retry_read;
        }
        send_dbinforesponse(sb);
        cdb2__query__free_unpacked(query, &pb_alloc);
        query = NULL;
        free(p);
        goto retry_read;
    }

    free(p);

    /* Do security check before we return. We do it only after
       the query has been unpacked so that we know whether
       it is a new client (new clients have SSL feature).
       The check must be done for every query, otherwise
       attackers could bypass it by using pooled connections
       from sockpool. The overhead of the check is negligible. */
    if (gbl_client_ssl_mode >= SSL_REQUIRE && !sslio_has_ssl(sb)) {
        /* The code block does 2 things:
           1. Return an error to outdated clients;
           2. Send dbinfo to new clients to trigger SSL.
              It may happen when require_ssl is first time
              enabled across the cluster. */
        int client_supports_ssl = 0;
        for (int ii = 0; ii < query->sqlquery->n_features; ++ii) {
            if (CDB2_CLIENT_FEATURES__SSL == query->sqlquery->features[ii]) {
                client_supports_ssl = 1;
                break;
            }
        }

        if (client_supports_ssl) {
            newsql_write_response(clnt, RESPONSE_HEADER__SQL_RESPONSE_SSL,
                                  NULL, 1 , malloc, __func__, __LINE__);
            /* Client is going to reuse the connection. Don't drop it. */
            goto retry_read;
        } else {
            char *err = "The database requires SSL connections.";
            struct fsqlresp resp;
            bzero(&resp, sizeof(resp));
            resp.response = FSQL_ERROR;
            resp.rcode = CDB2ERR_CONNECT_ERROR;
            rc = fsql_write_response(clnt, &resp, err, strlen(err) + 1, 1,
                                     __func__, __LINE__);
        }
        return NULL;
    }

    return query;
}

static int process_set_commands(struct sqlclntstate *clnt)
{
    CDB2SQLQUERY *sql_query = NULL;
    int num_commands = 0;
    sql_query = clnt->sql_query;
    char *sqlstr = NULL;
    char *endp;
    int rc = 0;
    num_commands = sql_query->n_set_flags;
    for (int ii = 0; ii < num_commands && rc == 0; ii++) {
        sqlstr = sql_query->set_flags[ii];
        sqlstr = cdb2_skipws(sqlstr);
        if (strncasecmp(sqlstr, "set", 3) == 0) {
            if (gbl_extended_sql_debug_trace) {
                logmsg(LOGMSG_ERROR, "td %u %s line %d processing set command '%s'\n", 
                        pthread_self(), __func__, __LINE__, sqlstr);
            }
            sqlstr += 3;
            sqlstr = cdb2_skipws(sqlstr);
            if (strncasecmp(sqlstr, "transaction", 11) == 0) {
                sqlstr += 11;
                sqlstr = cdb2_skipws(sqlstr);
                clnt->dbtran.mode = TRANLEVEL_INVALID;
                clnt->high_availability = 0;
                if (strncasecmp(sqlstr, "read", 4) == 0) {
                    sqlstr += 4;
                    sqlstr = cdb2_skipws(sqlstr);
                    if (strncasecmp(sqlstr, "committed", 4) == 0) {
                        clnt->dbtran.mode = TRANLEVEL_RECOM;
                    }
                } else if (strncasecmp(sqlstr, "serial", 6) == 0) {
                    clnt->dbtran.mode = TRANLEVEL_SERIAL;
                    if (clnt->hasql_on == 1) {
                        clnt->high_availability = 1;
                    }
                } else if (strncasecmp(sqlstr, "blocksql", 7) == 0) {
                    clnt->dbtran.mode = TRANLEVEL_SOSQL;
                } else if (strncasecmp(sqlstr, "snap", 4) == 0) {
                    sqlstr += 4;
                    clnt->dbtran.mode = TRANLEVEL_SNAPISOL;
                    clnt->verify_retries = 0;
                    if (clnt->hasql_on == 1) {
                        clnt->high_availability = 1;
                        logmsg(LOGMSG_ERROR, 
                                "Enabling snapshot isolation high availability\n");
                    }
                }
                if (clnt->dbtran.mode == TRANLEVEL_INVALID)
                    rc = ii + 1;
            } else if (strncasecmp(sqlstr, "timeout", 7) == 0) {
                sqlstr += 7;
                sqlstr = cdb2_skipws(sqlstr);
                int timeout = strtol(sqlstr, &endp, 10);
                int notimeout = disable_server_sql_timeouts();
                sbuf2settimeout(clnt->sb, 0, notimeout ? 0 : timeout);
                if (timeout == 0)
                    net_add_watch(clnt->sb, 0, 0);
                else
                    net_add_watch_warning(
                        clnt->sb, bdb_attr_get(thedb->bdb_attr,
                                               BDB_ATTR_MAX_SQL_IDLE_TIME),
                        notimeout ? 0 : (timeout / 1000), clnt,
                        watcher_warning_function);
            } else if (strncasecmp(sqlstr, "maxquerytime", 12) == 0) {
                sqlstr += 12;
                sqlstr = cdb2_skipws(sqlstr);
                int timeout = strtol(sqlstr, &endp, 10);
                if (timeout >= 0)
                    clnt->query_timeout = timeout;
            } else if (strncasecmp(sqlstr, "timezone", 8) == 0) {
                sqlstr += 8;
                sqlstr = cdb2_skipws(sqlstr);
                strncpy(clnt->tzname, sqlstr, sizeof(clnt->tzname));
            } else if (strncasecmp(sqlstr, "datetime", 8) == 0) {
                sqlstr += 8;
                sqlstr = cdb2_skipws(sqlstr);

                if (strncasecmp(sqlstr, "precision", 9) == 0) {
                    sqlstr += 9;
                    sqlstr = cdb2_skipws(sqlstr);
                    DTTZ_TEXT_TO_PREC(sqlstr, clnt->dtprec, 0, return -1);
                } else {
                    rc = ii + 1;
                }
            } else if (strncasecmp(sqlstr, "user", 4) == 0) {
                sqlstr += 4;
                sqlstr = cdb2_skipws(sqlstr);
                clnt->have_user = 1;
                strncpy(clnt->user, sqlstr, sizeof(clnt->user));
            } else if (strncasecmp(sqlstr, "password", 8) == 0) {
                sqlstr += 8;
                sqlstr = cdb2_skipws(sqlstr);
                clnt->have_password = 1;
                strncpy(clnt->password, sqlstr, sizeof(clnt->password));
                sqlite3Dequote(clnt->password);
            } else if (strncasecmp(sqlstr, "spversion", 9) == 0) {
                clnt->spversion.version_num = 0;
                free(clnt->spversion.version_str);
                clnt->spversion.version_str = NULL;
                sqlstr += 9;
                sqlstr = cdb2_skipws(sqlstr);
                char *spname = sqlstr;
                while (!isspace(*sqlstr)) {
                    ++sqlstr;
                }
                *sqlstr = 0;
                if ((sqlstr - spname) < MAX_SPNAME) {
                    strncpy(clnt->spname, spname, MAX_SPNAME);
                    clnt->spname[MAX_SPNAME] = '\0';
                } else {
                    rc = ii + 1;
                }
                ++sqlstr;

                sqlstr = cdb2_skipws(sqlstr);
                int ver = strtol(sqlstr, &endp, 10);
                if (*sqlstr == '\'' || *sqlstr == '"') { // looks like a str
                    if (strlen(sqlstr) < MAX_SPVERSION_LEN) {
                        clnt->spversion.version_str = strdup(sqlstr);
                        sqlite3Dequote(clnt->spversion.version_str);
                    } else {
                        rc = ii + 1;
                    }
                } else if (*endp == 0) { // parsed entire number successfully
                    clnt->spversion.version_num = ver;
                } else {
                    rc = ii + 1;
                }
            } else if (strncasecmp(sqlstr, "readonly", 8) == 0) {
                sqlstr += 8;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->is_readonly = 0;
                } else {
                    clnt->is_readonly = 1;
                }
            } else if (strncasecmp(sqlstr, "sptrace", 7) == 0) {
                sqlstr += 7;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->want_stored_procedure_trace = 0;
                } else {
                    clnt->want_stored_procedure_trace = 1;
                }
            } else if (strncasecmp(sqlstr, "spdebug", 7) == 0) {
                sqlstr += 7;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "off", 3) == 0) {
                    clnt->want_stored_procedure_debug = 0;
                } else {
                    clnt->want_stored_procedure_debug = 1;
                }
            } else if (strncasecmp(sqlstr, "HASQL", 5) == 0) {
                sqlstr += 5;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->hasql_on = 1;
                    if (clnt->dbtran.mode == TRANLEVEL_SERIAL ||
                        clnt->dbtran.mode == TRANLEVEL_SNAPISOL) {
                        clnt->high_availability = 1;
                        if (gbl_extended_sql_debug_trace) {
                            logmsg(LOGMSG_USER, "td %u %s line %d setting high_availability\n", 
                                    pthread_self(), __func__, __LINE__);
                        }
                    }
                } else {
                    clnt->hasql_on = 0;
                    clnt->high_availability = 0;
                    if (gbl_extended_sql_debug_trace) {
                        logmsg(LOGMSG_USER, "td %u %s line %d clearing high_availability\n", 
                                pthread_self(), __func__, __LINE__);
                    }
                }
            } else if (strncasecmp(sqlstr, "verifyretry", 11) == 0) {
                sqlstr += 11;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->verifyretry_off = 0;
                } else {
                    clnt->verifyretry_off = 1;
                }
            } else if (strncasecmp(sqlstr, "remote", 6) == 0) {
                sqlstr += 6;
                sqlstr = cdb2_skipws(sqlstr);

                int rc = fdb_access_control_create(clnt, sqlstr);
                if (rc) {
                    logmsg(LOGMSG_ERROR,
                        "%s: failed to process remote access settings \"%s\"\n",
                        __func__, sqlstr);
                }
                rc = ii + 1;
            } else if (strncasecmp(sqlstr, "getcost", 7) == 0) {
                sqlstr += 7;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->get_cost = 1;
                } else {
                    clnt->get_cost = 0;
                }
            } else if (strncasecmp(sqlstr, "explain", 7) == 0) {
                sqlstr += 7;
                sqlstr = cdb2_skipws(sqlstr);
                if (strncasecmp(sqlstr, "on", 2) == 0) {
                    clnt->is_explain = 1;
                } else if (strncasecmp(sqlstr, "verbose", 7) == 0) {
                    clnt->is_explain = 2;
                } else {
                    clnt->is_explain = 0;
                }
            } else if (strncasecmp(sqlstr, "maxtransize", 11) == 0) {
                sqlstr += 11;
                int maxtransz = strtol(sqlstr, &endp, 10);
                if (endp != sqlstr && maxtransz >= 0)
                    clnt->osql_max_trans = maxtransz;
                else
                    logmsg(LOGMSG_ERROR, "Error: bad value for maxtransize %s\n", sqlstr);
#ifdef DEBUG
                printf("setting clnt->osql_max_trans to %d\n",
                       clnt->osql_max_trans);
#endif
            } else if (strncasecmp(sqlstr, "plannereffort", 13) == 0) {
                sqlstr += 13;
                int effort = strtol(sqlstr, &endp, 10);
                if (0 < effort && effort <= 10)
                    clnt->planner_effort = effort;
#ifdef DEBUG
                printf("setting clnt->planner_effort to %d\n",
                       clnt->planner_effort);
#endif
            } else {
                rc = ii + 1;
            }

            if (rc) {
                char err[256];
                snprintf(err, sizeof(err) - 1, "Invalid set command '%s'",
                         sqlstr);
                send_prepare_error(clnt, err, 0);
            }
        }
    }
    return rc;
}

static int fsql_writer(SBUF2 *sb, const char *buf, int nbytes)
{
    extern int gbl_sql_release_locks_on_slow_reader;
    ssize_t nwrite, written = 0;
    int retry = -1;
    int released_locks = 0;
retry:
    retry++;
    while (written < nbytes) {
        struct pollfd pd;
        int fd = pd.fd = sbuf2fileno(sb);
        pd.events = POLLOUT;
        errno = 0;
        int rc = poll(&pd, 1, 100);
        if (rc < 0) {
            if (errno == EINTR || errno == EAGAIN) {
                if (gbl_sql_release_locks_on_slow_reader && !released_locks) {
                    release_locks("slow reader");
                    released_locks = 1;
                }
                goto retry;
            }
            logmsg(LOGMSG_ERROR, "%s poll rc:%d errno:%d errstr:%s\n", __func__, rc,
                    errno, strerror(errno));
            return rc;
        }
        if (rc == 0) { // descriptor not ready, write will block
            if (gbl_sql_release_locks_on_slow_reader && !released_locks) {
                rc = release_locks("slow reader");
                if (rc) {
                    logmsg(LOGMSG_ERROR,
                           "%s release_locks generation changed\n", __func__);
                    return -1;
                }
                released_locks = 1;
            }

            if (bdb_lock_desired(thedb->bdb_env)) {
                struct sql_thread *thd = pthread_getspecific(query_info_key);
                if (thd) {
                    rc = recover_deadlock(thedb->bdb_env, thd, NULL, 0);
                    if (rc) {
                        logmsg(LOGMSG_ERROR,
                               "%s recover_deadlock generation changed\n",
                               __func__);
                        return -1;
                    }
                }
            }

#ifdef _SUN_SOURCE
            if (gbl_flush_check_active_peer) {
                /* On Solaris, we end up with sockets with
                 * no peer, on which poll cheerfully returns
                 * no events. So after several retries check if
                 * still connected. */
                if (retry % 10 == 0) {
                    /* if we retried for a second, see if
                     * the connection is still around.
                     */
                    struct sockaddr_in peeraddr;
                    socklen_t len = sizeof(peeraddr);
                    rc = getpeername(fd, (struct sockaddr *)&peeraddr, &len);
                    if (rc == -1 && errno == ENOTCONN) {
                        ctrace("fd %d disconnected\n", fd);
                        return -1;
                    }
                }
            }
#endif
            goto retry;
        }
        if (pd.revents & POLLOUT) {
            /* I dislike this code in this layer - it should be in net. */
            nwrite = sbuf2unbufferedwrite(sb, &buf[written], nbytes - written);
            if (nwrite < 0)
                return -1;
            written += nwrite;
        } else if (pd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            return -1;
        }
    }
    return written;
}

int handle_newsql_requests(struct thr_handle *thr_self, SBUF2 *sb,
                           int *keepsocket)
{
    int rc = 0;
    int do_master_check = 1;

    struct sqlclntstate clnt;
    reset_clnt(&clnt, sb, 1);
    clnt.tzname[0] = '\0';

    clnt.is_newsql = 1;

    if (keepsocket)
        *keepsocket = 1;

    if (thedb->rep_sync == REP_SYNC_NONE)
        do_master_check = 0;

    if (do_master_check && sbuf_is_local(clnt.sb))
        do_master_check = 0;

    if (active_appsock_conns >
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXAPPSOCKSLIMIT)) {
        logmsg(LOGMSG_WARN,
                "%s: Exhausted appsock connections, total %d connections \n",
                __func__, active_appsock_conns);
        char *err = "Exhausted appsock connections.";
        struct fsqlresp resp;
        bzero(&resp, sizeof(resp));
        resp.response = FSQL_ERROR;
        resp.rcode = SQLHERR_APPSOCK_LIMIT;
        rc = fsql_write_response(&clnt, &resp, err, strlen(err) + 1, 1,
                                 __func__, __LINE__);
        goto done;
    }

    /* avoid new accepting new queries/transaction on opened connections
       if we are incoherent (and not in a transaction). */
    if (!bdb_am_i_coherent(thedb->bdb_env) &&
        (clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS)) {
        logmsg(LOGMSG_ERROR, 
               "%s line %d td %u new query on incoherent node, dropping socket\n",
               __func__, __LINE__, (uint32_t)pthread_self());
        goto done;
    }

    CDB2QUERY *query = read_newsql_query(&clnt, sb);

    if (query == NULL) {
        goto done;
    }

    assert(query->sqlquery);
    CDB2SQLQUERY *sql_query = query->sqlquery;

    int allow_master_exec = 0;
    int allow_master_dbinfo = 0;
    for (int ii = 0; ii < sql_query->n_features; ii++) {
        if (CDB2_CLIENT_FEATURES__ALLOW_MASTER_EXEC ==
            sql_query->features[ii]) {
            allow_master_exec = 1;
        } else if (CDB2_CLIENT_FEATURES__ALLOW_MASTER_DBINFO ==
                   sql_query->features[ii]) {
            allow_master_dbinfo = 1;
        } else if (CDB2_CLIENT_FEATURES__ALLOW_QUEUING ==
                   sql_query->features[ii]) {
            clnt.req.flags |= SQLF_QUEUE_ME;
        }
    }

    if (do_master_check && bdb_master_should_reject(thedb->bdb_env) &&
        (clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS)) {
        if (allow_master_exec == 0) {
            logmsg(LOGMSG_ERROR, 
                   "%s line %d td %u new query on master, dropping socket\n",
                   __func__, __LINE__, (uint32_t)pthread_self());
            if (allow_master_dbinfo) {
                send_dbinforesponse(sb);
            }
            /* Send sql response with dbinfo. */
            goto done;
        }
    }

    /*printf("\n Query %s length %d" , sql_query->sql_query.data,
     * sql_query->sql_query.len);*/

    int wrtimeoutsec;
    int notimeout = disable_server_sql_timeouts();

    /* these connections shouldn't time out */
    sbuf2settimeout(clnt.sb, 0, 0);

    pthread_mutex_init(&clnt.wait_mutex, NULL);
    pthread_cond_init(&clnt.wait_cond, NULL);
    pthread_mutex_init(&clnt.write_lock, NULL);
    pthread_mutex_init(&clnt.dtran_mtx, NULL);

    clnt.osql.count_changes = 1;
    clnt.dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    clnt.high_availability = 0;

    sbuf2settimeout(
        sb, bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME) * 1000,
        notimeout ? 0 : gbl_sqlwrtimeoutms);
    sbuf2flush(sb);
    net_set_writefn(sb, fsql_writer);

    if (gbl_sqlwrtimeoutms == 0 || notimeout)
        wrtimeoutsec = 0;
    else
        wrtimeoutsec = gbl_sqlwrtimeoutms / 1000;

    net_add_watch_warning(
        sb, bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME),
        wrtimeoutsec, &clnt, watcher_warning_function);

    /* appsock threads aren't sql threads so for appsock pool threads
     * sqlthd will be NULL */
    struct sql_thread *sqlthd = pthread_getspecific(query_info_key);
    if (sqlthd) {
        bzero(&sqlthd->sqlclntstate->conn, sizeof(struct conninfo));
        sqlthd->sqlclntstate->origin[0] = 0;
    }

    while (query) {
        assert(query->sqlquery);
        sql_query = query->sqlquery;

        clnt.sql = sql_query->sql_query;
        if (!clnt.in_client_trans) {
            bzero(&clnt.effects, sizeof(clnt.effects));
            bzero(&clnt.log_effects, sizeof(clnt.log_effects));
            clnt.trans_has_sp = 0;
        }
        clnt.is_newsql = 1;
        if (clnt.dbtran.mode < TRANLEVEL_SOSQL) {
            clnt.dbtran.mode = TRANLEVEL_SOSQL;
        }
        clnt.osql.sent_column_data = 0;
        clnt.stop_this_statement = 0;
        clnt.sql_query = sql_query;

        if ((clnt.tzname[0] == '\0') && sql_query->tzname)
            strncpy(clnt.tzname, sql_query->tzname, sizeof(clnt.tzname));

        if (sql_query->dbname && thedb->envname &&
            strcasecmp(sql_query->dbname, thedb->envname)) {
            char errstr[64 + (2 * MAX_DBNAME_LENGTH)];
            snprintf(errstr, sizeof(errstr),
                     "DB name mismatch query:%s actual:%s", sql_query->dbname,
                     thedb->envname);
            logmsg(LOGMSG_ERROR, "%s\n", errstr);
            struct fsqlresp resp;

            resp.response = FSQL_COLUMN_DATA;
            resp.flags = 0;
            resp.rcode = CDB2__ERROR_CODE__WRONG_DB;
            fsql_write_response(&clnt, &resp, (void *)errstr, strlen(errstr) + 1, 1 /*flush*/, __func__, __LINE__);
            goto done;
        }

        if (clnt.sql_query->client_info) {
            if (clnt.conninfo.pid &&
                clnt.conninfo.pid != clnt.sql_query->client_info->pid) {
                /* Different pid is coming without reset. */
                logmsg(LOGMSG_WARN, "Multiple processes using same socket PID 1 %d "
                                "PID 2 %d Host %.8x\n",
                        clnt.conninfo.pid, clnt.sql_query->client_info->pid,
                        clnt.sql_query->client_info->host_id);
            }
            clnt.conninfo.pid = clnt.sql_query->client_info->pid;
            clnt.conninfo.node = clnt.sql_query->client_info->host_id;
        }

        rc = process_set_commands(&clnt);
        if (rc) {
            /*
            fprintf(stderr, "%s line %d td %u process_set_commands error\n",
                    __func__, __LINE__, (uint32_t) pthread_self());
                    */
            goto done;
        }

        if (gbl_rowlocks && clnt.dbtran.mode != TRANLEVEL_SERIAL) {
            clnt.dbtran.mode = TRANLEVEL_SNAPISOL;
        }

        if (sql_query->little_endian) {
            clnt.have_endian = 1;
            clnt.endian = FSQL_ENDIAN_LITTLE_ENDIAN;
        } else {
            clnt.have_endian = 0;
        }

        clnt.query = query;
        clnt.added_to_hist = 0;

        /* avoid new accepting new queries/transaction on opened connections
           if we are incoherent (and not in a transaction). */
        if (!bdb_am_i_coherent(thedb->bdb_env) &&
            (clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS)) {
            logmsg(LOGMSG_ERROR, "%s line %d td %u new query on incoherent node, "
                            "dropping socket\n",
                    __func__, __LINE__, (uint32_t)pthread_self());
            goto done;
        }

        clnt.heartbeat = 1;

        if (clnt.had_errors && strncasecmp(clnt.sql, "commit", 6) &&
            strncasecmp(clnt.sql, "rollback", 8)) {
            if (clnt.in_client_trans == 0) {
                clnt.had_errors = 0;
                /* tell blobmem that I want my priority back
                   when the sql thread is done */
                comdb2bma_pass_priority_back(blobmem);
                rc = dispatch_sql_query(&clnt);
            } else {
                /* Do Nothing */
                send_heartbeat(&clnt);
            }
        } else if (clnt.had_errors) {
            /* Do Nothing */
            if (clnt.ctrl_sqlengine == SQLENG_STRT_STATE)
                clnt.ctrl_sqlengine = SQLENG_NORMAL_PROCESS;

            clnt.had_errors = 0;
            clnt.in_client_trans = 0;
            rc = -1;
        } else {
            /* tell blobmem that I want my priority back
               when the sql thread is done */
            comdb2bma_pass_priority_back(blobmem);
            rc = dispatch_sql_query(&clnt);
        }

        if (clnt.osql.replay == OSQL_RETRY_DO) {
            if (clnt.trans_has_sp == 0) {
                srs_tran_replay(&clnt, thr_self);
            } else {
                osql_set_replay(__FILE__, __LINE__, &clnt, OSQL_RETRY_NONE);
                srs_tran_destroy(&clnt);
            }
        } else {
            /* if this transaction is done (marked by SQLENG_NORMAL_PROCESS),
               clean transaction sql history
            */
            if (clnt.osql.history &&
                clnt.ctrl_sqlengine == SQLENG_NORMAL_PROCESS)
                srs_tran_destroy(&clnt);
        }

        if (rc && !clnt.in_client_trans)
            goto done;

        pthread_mutex_lock(&clnt.wait_mutex);
        if (clnt.query) {
            if (clnt.added_to_hist == 1) {
                clnt.query = NULL;
            } else {
                cdb2__query__free_unpacked(clnt.query, &pb_alloc);
                clnt.query = NULL;
            }
        }
        pthread_mutex_unlock(&clnt.wait_mutex);

        query = read_newsql_query(&clnt, sb);
    }

done:
    if (clnt.ctrl_sqlengine == SQLENG_INTRANS_STATE) {
        handle_sql_intrans_unrecoverable_error(&clnt);
    }

    close_sp(&clnt);
    osql_clean_sqlclntstate(&clnt);

    if (clnt.dbglog) {
        sbuf2close(clnt.dbglog);
        clnt.dbglog = NULL;
    }

    if (clnt.query) {
        if (clnt.added_to_hist == 1) {
            clnt.query = NULL;
        } else {
            cdb2__query__free_unpacked(clnt.query, &pb_alloc);
            clnt.query = NULL;
        }
    }

    /* XXX free logical tran?  */
    close_appsock(sb);

    clnt.dbtran.mode = TRANLEVEL_INVALID;
    clnt.high_availability = 0;
    if (clnt.query_stats)
        free(clnt.query_stats);

    pthread_mutex_destroy(&clnt.wait_mutex);
    pthread_cond_destroy(&clnt.wait_cond);
    pthread_mutex_destroy(&clnt.write_lock);
    pthread_mutex_destroy(&clnt.dtran_mtx);

    return 0;
}

int handle_fastsql_requests(struct thr_handle *thr_self, SBUF2 *sb,
                            int *keepsocket, int wrong_db)
{
    struct sqlthdstate thd;
    struct sqlclntstate clnt;
    int rc = 0;
    int wrtimeoutsec;
    int notimeout = disable_server_sql_timeouts();

    if (keepsocket)
        *keepsocket = 1;

    /*fprintf(stderr, "handle_fastsql_requests clnt %x\n", &clnt);*/

    reset_clnt(&clnt, sb, 1);

    pthread_mutex_init(&clnt.wait_mutex, NULL);
    pthread_cond_init(&clnt.wait_cond, NULL);
    pthread_mutex_init(&clnt.write_lock, NULL);
    pthread_mutex_init(&clnt.dtran_mtx, NULL);

    /* start off in comdb2 mode till we're told otherwise */
    clnt.dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    clnt.wrong_db = wrong_db;
    clnt.high_availability = 0;

    sbuf2settimeout(
        sb, bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME) * 1000,
        notimeout ? 0 : gbl_sqlwrtimeoutms);
    sbuf2flush(sb);
    net_set_writefn(sb, fsql_writer);

    if (gbl_sqlwrtimeoutms == 0 || notimeout)
        wrtimeoutsec = 0;
    else
        wrtimeoutsec = gbl_sqlwrtimeoutms / 1000;

    net_add_watch_warning(
        sb, bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAX_SQL_IDLE_TIME),
        wrtimeoutsec, &clnt, watcher_warning_function);

    thd.logger = thrman_get_reqlogger(thr_self);
    thd.buf = NULL;
    thd.maxbuflen = 0;
    thd.cinfo = NULL;
    thd.offsets = NULL;
    thd.thr_self = thr_self;
    thd.sqldb = NULL;
    // thd.stmt = NULL;
    thd.stmt_table = NULL;
    thd.param_stmt_head = NULL;
    thd.param_stmt_tail = NULL;
    thd.noparam_stmt_head = NULL;
    thd.noparam_stmt_tail = NULL;
    thd.param_cache_entries = 0;
    thd.noparam_cache_entries = 0;

    /* appsock threads aren't sql threads so for appsock pool threads
     * thd.sqlthd will be NULL */
    thd.sqlthd = pthread_getspecific(query_info_key);
    if (thd.sqlthd) {
        bzero(&thd.sqlthd->sqlclntstate->conn, sizeof(struct conninfo));
        thd.sqlthd->sqlclntstate->origin[0] = 0;
    }

    rc = handle_fastsql_requests_io_loop(&thd, &clnt);

    clnt_reset_cursor_hints(&clnt);
    close_sp(&clnt);
    osql_clean_sqlclntstate(&clnt);

    if (clnt.dbglog) {
        sbuf2close(clnt.dbglog);
        clnt.dbglog = NULL;
    }

    /* XXX free logical tran?  */

    clnt.dbtran.mode = TRANLEVEL_INVALID;
    if (clnt.query_stats)
        free(clnt.query_stats);

    pthread_mutex_destroy(&clnt.wait_mutex);
    pthread_cond_destroy(&clnt.wait_cond);
    pthread_mutex_destroy(&clnt.write_lock);
    pthread_mutex_destroy(&clnt.dtran_mtx);

    return rc;
}

pthread_mutex_t gbl_sql_lock;

/* Write sql interface.  This will replace sqlnet.c */

/* don't let connections get more than this much memory */
static int alloc_lim = 1024 * 1024 * 8;

/* any state associated with a connection (including open db handles)
   needs to be stored here.  everything is cleaned up on end of thread
   in destroy_sqlconn */
struct sqlconn {
    pthread_t tid;
    SBUF2 *sb;
    hash_t *handles;
    sqlite3 *db;
    int reqsz;

    struct statement_handle *last_handle;

    /* all reads are done to this buffer */
    char *buf;
    int bufsz;

    /* for debugging/stats: current state and timestamp when it was entered */
    char *state;
    int tm;

    LINKC_T(struct sqlconn) lnk;
};

static int write_str(struct sqlconn *conn, char *err);

static void conn_set_state(struct sqlconn *conn, char *state)
{
    conn->state = state;
    conn->tm = time(NULL);
}

static void conn_alloc(struct sqlconn *conn, int sz)
{
    if (conn->bufsz >= sz)
        return;
    conn->bufsz = sz;
    conn->buf = realloc(conn->buf, conn->bufsz);
}

static LISTC_T(struct sqlconn) conns;

/* handles are always a per-connection deal, and a connection
   always has a dedicated thread, so no need to lock around
   handles */
typedef unsigned long long handle_tp;
enum req_code {
    REQ_EOF = -2,
    REQ_INVALID = -1,
    REQ_CONNECT = 0,
    REQ_PREPARE = 1,
    REQ_VERSION = 2,
    REQ_CHANGES = 3,
    REQ_FINALIZE = 4,
    REQ_STEP = 5,
    REQ_RESET = 6
};

/* request and responses go back in this format */
struct reqhdr {
    int rq;
    int followlen;
};

/* column for results coming back */

struct statement_handle {
    /* context: need to swap these when switching between handles */
    struct sqlthdstate sqlstate;
    struct sqlclntstate clnt;

    handle_tp hid;
    sqlite3_stmt *p;
    int *types;
};

static void switch_context(struct sqlconn *conn, struct statement_handle *h)
{
    struct sql_thread *thd;
    sqlite3 *db;
    int i;

    return;

#if 0
    /* don't do anything if we are working with the same statemtn as last time */
    if (conn->last_handle == h)
        return;

    conn->last_handle = h;

    thd = pthread_getspecific(query_info_key);
    h->sqlstate.sqlthd = thd;
    h->clnt.debug_sqlclntstate = pthread_self();


    db = conn->db;
    /* reset client handle - we need one per statement */
    for (i = 0; i < db->nDb; i++) {
         if (db->aDb[i].pBt) {
            db->aDb[i].pBt->sqlclntstate = &h->clnt;
         }
    }
#endif
}

#if 0
static int closehndl(void *obj, void *arg) {
    struct sqlconn *conn;
    struct statement_handle *h;

    conn = (struct sqlconn*) arg;
    h = (struct statement_handle*) obj;
    
    sqlite3_finalize(h->p);
    free(h);
}
#endif

/* read request from connection, write to connection's buffer. return request
 * type */
static enum req_code read_req(struct sqlconn *conn)
{
    struct reqhdr rq;
    int rc;

    conn->reqsz = 0;

    /* header */
    rc = sbuf2fread((char *)&rq, sizeof(struct reqhdr), 1, conn->sb);
    if (rc == 0)
        return REQ_EOF;

    if (rc != 1)
        return REQ_INVALID;

    rq.rq = ntohl(rq.rq);
    rq.followlen = ntohl(rq.followlen);

    /* sanity check buffer size required */
    if (rq.followlen < 0 || rq.followlen > alloc_lim)
        return REQ_INVALID;

    conn_alloc(conn, rq.followlen);

    conn->reqsz = rq.followlen;
    rc = sbuf2fread((char *)conn->buf, rq.followlen, 1, conn->sb);
    if (rc != 1)
        return REQ_INVALID;

    return rq.rq;
}

struct client_query_stats *get_query_stats_from_thd()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (!thd)
        return NULL;

    struct sqlclntstate *clnt = thd->sqlclntstate;
    if (!clnt)
        return NULL;

    if (!clnt->query_stats)
        record_query_cost(thd, clnt);

    return clnt->query_stats;
}

char *comdb2_get_prev_query_cost()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (!thd)
        return NULL;

    struct sqlclntstate *clnt = thd->sqlclntstate;
    if (!clnt)
        return NULL;

    return clnt->prev_cost_string;
}

void comdb2_free_prev_query_cost()
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (!thd)
        return;

    struct sqlclntstate *clnt = thd->sqlclntstate;
    if (!clnt)
        return;

    if (clnt->prev_cost_string) {
        free(clnt->prev_cost_string);
        clnt->prev_cost_string = NULL;
    }
}

/* get sql query cost and return it as char *
 * function will allocate memory for string
 * and caller should free that memory area
 */
static char *get_query_cost_as_string(struct sql_thread *thd,
                                      struct sqlclntstate *clnt)
{
    if (!clnt || !thd)
        return NULL;
    record_query_cost(thd, clnt);

    if (!clnt->query_stats)
        return NULL;

    strbuf *out = strbuf_new();
    struct client_query_stats *st = clnt->query_stats;

    strbuf_appendf(out, "Cost: %.2lf NRows: %d\n", st->cost, clnt->nrows);
    for (int ii = 0; ii < st->n_components; ii++) {
        strbuf_append(out, "    ");
        if (st->path_stats[ii].table[0] == '\0') {
            strbuf_appendf(out, "temp index finds %d ",
                           st->path_stats[ii].nfind);
            if (st->path_stats[ii].nnext)
                strbuf_appendf(out, "next/prev %d ", st->path_stats[ii].nnext);
            if (st->path_stats[ii].nwrite)
                strbuf_appendf(out, "nwrite %d ", st->path_stats[ii].nwrite);
        } else {
            if (st->path_stats[ii].ix >= 0)
                strbuf_appendf(out, "index %d on ", st->path_stats[ii].ix);
            strbuf_appendf(out, "table %s finds %d", st->path_stats[ii].table,
                           st->path_stats[ii].nfind);
            if (st->path_stats[ii].nnext > 0) {
                strbuf_appendf(out, " next/prev %d", st->path_stats[ii].nnext);
                if (st->path_stats[ii].ix < 0)
                    strbuf_appendf(out, "[TABLE SCAN]");
            }
        }
        strbuf_append(out, "\n");
    }

    char *str = strbuf_disown(out);
    strbuf_free(out);
    return str;
}

static int execute_sql_query_offload_inner_loop(struct sqlclntstate *clnt,
                                                struct sqlthdstate *poolthd,
                                                sqlite3_stmt *stmt)
{
    int ret;
    UnpackedRecord upr;
    Mem res;
    char *cid;
    int rc = 0;
    int tmp;
    int sent;

    if (!clnt->fdb_state.remote_sql_sb) {
        while ((ret = sqlite3_step(stmt)) == SQLITE_ROW)
            ;
    } else {
        bzero(&res, sizeof(res));

        if (clnt->osql.rqid == OSQL_RQID_USE_UUID)
            cid = (char *)&clnt->osql.uuid;
        else
            cid = (char *)&clnt->osql.rqid;

        sent = 0;
        while (1) {
            /* NOTE: in the recom and serial mode, the cursors look at the
            shared shadow_tran
            while the transaction updates are arriving! Mustering the parallel
            computing!
            Get the LOCK!
            */
            if (clnt->dbtran.mode == TRANLEVEL_RECOM ||
                clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                pthread_mutex_lock(&clnt->dtran_mtx);
            }

            ret = sqlite3_step(stmt);

            if (clnt->dbtran.mode == TRANLEVEL_RECOM ||
                clnt->dbtran.mode == TRANLEVEL_SERIAL) {
                pthread_mutex_unlock(&clnt->dtran_mtx);
            }

            if (ret != SQLITE_ROW) {
                break;
            }

            if (res.z) {
                /* now we have the packed sqlite row in Mem->z */
                rc = fdb_svc_sql_row(clnt->fdb_state.remote_sql_sb, cid, res.z,
                                     res.n, IX_FNDMORE,
                                     clnt->osql.rqid == OSQL_RQID_USE_UUID);
                if (rc) {
                    /*
                    fprintf(stderr, "%s: failed to send back sql row\n",
                    __func__);
                    */
                    break;
                }
            }

            bzero(&upr, sizeof(upr));
            sqlite3VdbeMemRelease(&res);
            bzero(&res, sizeof(res));

            upr.aMem = sqlite3GetCachedResultRow(stmt, &tmp);
            if (!upr.aMem) {
                logmsg(LOGMSG_ERROR, "%s: failed to retrieve result set\n",
                        __func__);
                return -1;
            }

            upr.nField = tmp;

            /* special treatment for sqlite_master */
            if (clnt->fdb_state.flags == FDB_RUN_SQL_SCHEMA) {
                rc = fdb_svc_alter_schema(clnt, stmt, &upr);
                if (rc) {
                    /* break; Ignore for now, this will run less optimized */
                }
            }

            sqlite3VdbeRecordPack(&upr, &res);
            sent = 1;
        }

        /* send the last row, marking flag as such */
        if (!rc) {
            if (sent == 1) {
                rc = fdb_svc_sql_row(clnt->fdb_state.remote_sql_sb, cid, res.z,
                                     res.n, IX_FND,
                                     clnt->osql.rqid == OSQL_RQID_USE_UUID);
            } else {
                rc = fdb_svc_sql_row(clnt->fdb_state.remote_sql_sb, cid, res.z,
                                     res.n, IX_EMPTY,
                                     clnt->osql.rqid == OSQL_RQID_USE_UUID);
            }
            if (rc) {
                /*
                fprintf(stderr, "%s: failed to send back sql row\n", __func__);
                */
            }
        }

        /* cleanup last row */
        sqlite3VdbeMemRelease(&res);
    }

    /* blocksql doesn't look at sqlite3_step, result of transaction commit
       are submitted by lower levels; we need to fix this for remote cursors

       NOTE: when caller closes the socket early, ret == SQLITE_ROW.  This is
       not
       an error, caller decided it needs no more rows.

       */

    if (ret == SQLITE_DONE || ret == SQLITE_ROW) {
        ret = 0;
    }

    return ret;
}

static int execute_sql_query_offload(struct sqlclntstate *clnt,
                                     struct sqlthdstate *poolthd)
{
    int rc = 0, ret = 0, irc = 0;
    sqlite3_stmt *stmt = NULL;
    const char *rest_of_sql = NULL;
    struct sql_thread *thd = poolthd->sqlthd;
    sqlite3 *sqldb = poolthd->sqldb;
    char *sql = clnt->sql;
    char *sql_str = NULL;
    struct sql_thread *thdcmp = pthread_getspecific(query_info_key);
    char *errstr = NULL;
    struct schema *parameters_to_bind = NULL;
    int have_our_own_error = 0;
    int sqlcache_hint = 0;
    char cache_hint[128];
    stmt_hash_entry_type *stmt_entry = NULL;
    char *err = NULL;
    int i;
    int sql_cache_hint_found = 0;
    int hint_len;

    /* asserts */
    if (thdcmp != thd) {
        logmsg(LOGMSG_ERROR, "%s:Wrong sql_thread for the wrong sql thread!\n",
                __func__);
        return SQLITE_INTERNAL;
    }
    if (!thd || !sqldb) {
        logmsg(LOGMSG_ERROR, "%s: no sql_thread or sqlite3 structure\n", __func__);
        return SQLITE_INTERNAL;
    }

    if (clnt->is_newsql) {
        ATOMIC_ADD(gbl_nnewsql, 1);
    } else {
        ATOMIC_ADD(gbl_nsql, 1);
    }
    thd->startms = time_epochms();
    thd->stime = time_epoch();
    thd->nmove = thd->nfind = thd->nwrite = 0;

    if (clnt->tag) {
        parameters_to_bind = new_dynamic_schema(clnt->tag, strlen(clnt->tag),
                                                gbl_dump_sql_dispatched);
        if (parameters_to_bind == NULL) {
            logmsg(LOGMSG_ERROR, "%s:%d invalid parametrized sql tag: %s\n", __FILE__,
                   __LINE__, clnt->tag);
            return SQLITE_INTERNAL;
        }
    }

    reqlog_new_sql_request(poolthd->logger, sql);
    log_queue_time(poolthd->logger, clnt);

    rc = sql_set_transaction_mode(sqldb, clnt, clnt->dbtran.mode);
    if (rc) {
        ret = FSQL_PREPARE;
        err = sqlite3_mprintf("Failed to set transaction mode.");
        have_our_own_error = 1;
        goto done_here;
    }

    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(sqldb, 1);

    /* reset error */
    bzero(&clnt->fail_reason, sizeof(clnt->fail_reason));
    bzero(&clnt->osql.xerr, sizeof(clnt->osql.xerr));

    if (clnt->rawnodestats)
        clnt->rawnodestats->sql_queries++;

    thrman_wheref(poolthd->thr_self, "%s", clnt->sql);

    user_request_begin(REQUEST_TYPE_QTRAP, FLAG_REQUEST_TRACK_EVERYTHING);

    if (gbl_dump_sql_dispatched)
        logmsg(LOGMSG_USER, "BLOCKSQL mode=%d [%s]\n", clnt->dbtran.mode,
                clnt->sql);

    clnt->no_transaction = 1;

    rdlock_schema_lk();
    if ((rc = check_thd_gen(poolthd, clnt)) != SQLITE_OK) {
        unlock_schema_lk();
        clnt->no_transaction = 0;
        return rc;
    }

    if ((gbl_enable_sql_stmt_caching == STMT_CACHE_ALL) ||
        ((gbl_enable_sql_stmt_caching == STMT_CACHE_PARAM) && clnt->tag)) {
        hint_len = sizeof(cache_hint);
        sqlcache_hint = extract_sqlcache_hint(clnt->sql, cache_hint, &hint_len);
        if (sqlcache_hint) {
            if (find_stmt_table(poolthd->stmt_table, cache_hint, &stmt_entry) ==
                0) {
                stmt = stmt_entry->stmt;
            }
        } else {
            if (find_stmt_table(poolthd->stmt_table, clnt->sql, &stmt_entry) ==
                0) {
                stmt = stmt_entry->stmt;
            }
        }
    }

    if (!stmt) {
        if (clnt->tag) {
            if (sql_str) {
                rc =
                    sqlite3_prepare_v2(sqldb, sql_str, -1, &stmt, &rest_of_sql);
            } else {
                rc = sqlite3_prepare_v2(sqldb, clnt->sql, -1, &stmt,
                                        &rest_of_sql);
            }
        } else {
            if (sql_str) {
                rc = sqlite3_prepare(sqldb, sql_str, -1, &stmt, &rest_of_sql);
            } else {
                rc = sqlite3_prepare(sqldb, sql, -1, &stmt, &rest_of_sql);
            }
        }
        stmt_entry = NULL;
    } else {
        rc = sqlite3_resetclock(stmt);
    }
    clnt->no_transaction = 0;

    if (ret || !stmt) {
        ret = ERR_SQL_PREP;
    failed_locking:
        unlock_schema_lk();
        errstr = (char *)sqlite3_errmsg(sqldb);
        goto done_here;
    }

    irc = sqlite3LockStmtTables(stmt);
    if (irc) {
        goto failed_locking;
    }
    unlock_schema_lk();

    if (rest_of_sql && *rest_of_sql) {
        logmsg(LOGMSG_WARN, 
                "SQL TRAILING CHARACTERS AFTER QUERY TERMINATION: \"%s\"\n",
                rest_of_sql);
    }

    if (parameters_to_bind) {
        eventlog_params(poolthd->logger, stmt, parameters_to_bind, clnt);
        rc = bind_parameters(stmt, parameters_to_bind, clnt, &err);
        if (rc) {
            ret = ERR_SQL_PREP;
            have_our_own_error = 1;
            goto done_here;
        }
    } else {
        int nfields = sqlite3_bind_parameter_count(stmt);
        if (nfields) {
            ret = ERR_SQL_PREP;
            err = sqlite3_mprintf(
                "Query specified parameters, but no values provided.");
            have_our_own_error = 1;
            goto done_here;
        }
    }

    if (clnt->tzname) {
        memcpy(((Vdbe *)stmt)->tzname, clnt->tzname, TZNAME_MAX);
    }
    ((Vdbe *)stmt)->dtprec = clnt->dtprec;

    ret = execute_sql_query_offload_inner_loop(clnt, poolthd, stmt);

    if ((gbl_who > 0) || debug_this_request(gbl_debug_until)) {
        struct per_request_stats *st;
        st = user_request_get_stats();
        if (st)
            logmsg(LOGMSG_USER, 
                    "nreads %d (%lld bytes) nwrites %d (%lld bytes) nmempgets %d\n",
                    st->nreads, st->readbytes, st->nwrites, st->writebytes,
                    st->mempgets);
        gbl_who--;
    }

    if (clnt->client_understands_query_stats)
        record_query_cost(thd, thd->sqlclntstate);

done_here:
    /* if we turned on case sensitive like, turn it off since the sql handle we
       just used may be used by another connection with this disabled */
    if (clnt->using_case_insensitive_like)
        toggle_case_sensitive_like(sqldb, 0);

    /* check for conversion errors;
       in the case of an error, osql.xerr.errval will be set probably to
       SQLITE_INTERNAL
     */
    rc = sql_check_errors(clnt, poolthd->sqldb, stmt, (const char **)&errstr);
    if (rc) {
        /* check for prepare errors */
        if (ret == ERR_SQL_PREP)
            rc = ERR_SQL_PREP;
        errstat_set_rc(&clnt->osql.xerr, rc);
        errstat_set_str(&clnt->osql.xerr, errstr);
    }

    /* error binding. our own in the sense that it didn't come from sqlite or
     * Berkeley. */
    if (have_our_own_error) {
        ret = ERR_SQL_PREP;
        errstat_set_rc(&clnt->osql.xerr, ret);
        errstat_set_str(&clnt->osql.xerr, err);
    }

    if (err)
        sqlite3_free(err);

    if (!clnt->fdb_state.remote_sql_sb) {
        rc = osql_block_commit(thd);
        if (rc)
            logmsg(LOGMSG_ERROR, 
                    "%s: sqloff_block_send_done failed to write reply\n",
                    __func__);
    }

    if (stmt) {
        if ((gbl_enable_sql_stmt_caching == STMT_CACHE_ALL) ||
            ((gbl_enable_sql_stmt_caching == STMT_CACHE_PARAM) && clnt->tag)) {
            sqlite3_reset(stmt);
            if (!stmt_entry) {
                {
                    if (sqlcache_hint == 1) {
                        if (add_stmt_table(poolthd, cache_hint, NULL, stmt,
                                           parameters_to_bind) == 0) {
                            /* Its now not our problem. */
                            parameters_to_bind = NULL;
                        }
                    } else {
                        if (add_stmt_table(poolthd, clnt->sql, NULL, stmt,
                                           parameters_to_bind) == 0) {
                            parameters_to_bind = NULL;
                        }
                    }
                }
            } else {
                touch_stmt_entry(poolthd, stmt_entry);
                if (sqlcache_hint == 1) {
                    parameters_to_bind = NULL;
                }
            }
        } else {
            sqlite3_finalize(stmt);
        }
    }

    if (parameters_to_bind) {
        free_tag_schema((struct schema *)parameters_to_bind);
        parameters_to_bind = NULL;
    }

    sql_statement_done(thd, poolthd->logger, clnt->osql.rqid, rc);

    if (clnt->rawnodestats && thd) {
        clnt->rawnodestats->sql_steps += thd->nmove + thd->nfind + thd->nwrite;
    }
    sql_reset_sqlthread(sqldb, thd);

    if (sql_cache_hint_found) {
        char *k = cache_hint;
        pthread_mutex_lock(&gbl_sql_lock);
        {
            lrucache_release(sql_hints, &k);
        }
        pthread_mutex_unlock(&gbl_sql_lock);
    }

    return ret;
}

static void sqlengine_work_blocksock(struct thdpool *pool, void *work,
                                     void *thddata)
{
    struct sqlclntstate *clnt = work;
    struct sqlthdstate *thd = thddata;
    int bdberr;
    int rc;

    clnt->osql.timings.query_dispatched = osql_log_time();
    rdlock_schema_lk();
    sqlengine_prepare_engine(thd, clnt);
    unlock_schema_lk();

    rc = get_curtran(thedb->bdb_env, clnt);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "%s: unable to get a CURSOR transaction, rc = %d!\n",
                __func__, rc);
    }

    if (thd->sqldb) {
        /* Set whatever mode this client needs */
        rc = sql_set_transaction_mode(thd->sqldb, clnt, clnt->dbtran.mode);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: Failed to set transaction mode %d\n", __func__,
                    clnt->dbtran.mode);
        }
        /* assign this query a unique id */

        /* assign this query a unique id */
        sql_get_query_id(thd->sqlthd);

        thrman_setfd(thd->thr_self, -1);
        clnt->query_rc = execute_sql_query_offload(clnt, thd);
        thrman_setfd(thd->thr_self, -1);
    }

    thd->sqlthd->sqlclntstate = NULL; /* the thread does not need this */

    clnt_reset_cursor_hints(clnt);

    /* TODO: */
    /* record query cost business here */

    /* this thread goes back to pool, but it is on charge of sqlclntstate
        as provided by net reader or blockprocessor thread;
        cleaning it here
     */
    if (osql_unregister_sqlthr(clnt))
        logmsg(LOGMSG_ERROR, "%s: unable to unregister blocksql thread %llx\n",
                __func__, clnt->osql.rqid);

    if (put_curtran(thedb->bdb_env, clnt)) {
        logmsg(LOGMSG_ERROR, "%s: unable to destroy a CURSOR transaction!\n",
                __func__);
    }

    if (osql_clean_sqlclntstate(clnt))
        logmsg(LOGMSG_ERROR, "fail to clean osql???\n"); /* and ignore for now */
    if (clnt->sql)
        free(clnt->sql);

    rc = pthread_mutex_destroy(&clnt->write_lock);
    if (rc) {
        logmsg(LOGMSG_FATAL, "%s:%d pthread_mutex_destroy rc %d\n", __FILE__, __LINE__, rc);
        exit(1);
    }

    if (clnt->tag) {
        int blobno;
        free(clnt->tag);
        free(clnt->tagbuf);
        free(clnt->nullbits);
        for (blobno = 0; blobno < clnt->numblobs; blobno++)
            free(clnt->blobs[blobno]);
    }

    clnt->osql.timings.query_finished = osql_log_time();
    osql_log_time_done(clnt);

    clnt->dbtran.mode = TRANLEVEL_INVALID;
    free(clnt);
}

/* please follow minimal code encapsulation guidelines;
   please no more new waves of variables thrown everywhere
   in structs and function arguments, I get nauseous
 */
static void init_tag_info(struct sqlclntstate *clnt, struct tag_osql *taginfo)
{
    int blobno;

    if (taginfo) {
        clnt->tag = strdup(taginfo->tag);
        clnt->tagbuf = malloc(taginfo->tagbuflen);
        memcpy(clnt->tagbuf, taginfo->tagbuf, taginfo->tagbuflen);
        clnt->tagbufsz = taginfo->tagbuflen;
        clnt->numnullbits = taginfo->numnullbits;
        clnt->nullbits = malloc(taginfo->numnullbits);
        memcpy(clnt->nullbits, taginfo->nullbits, taginfo->numnullbits);
        clnt->numblobs = taginfo->numblobs;
        if (clnt->bloblens)
            free(clnt->bloblens);
        if (clnt->blobs)
            free(clnt->blobs);

        clnt->blobs = malloc(sizeof(void *) * taginfo->numblobs);
        clnt->bloblens = malloc(sizeof(int) * taginfo->numblobs);

        memcpy(clnt->bloblens, taginfo->bloblens,
               sizeof(int) * taginfo->numblobs);

        for (blobno = 0; blobno < taginfo->numblobs; blobno++) {
            clnt->blobs[blobno] = malloc(clnt->bloblens[blobno]);
            memcpy(clnt->blobs[blobno], taginfo->blobs[blobno],
                   clnt->bloblens[blobno]);
        }
    }
}

static void init_query_limits_info(struct sqlclntstate *clnt,
                                   struct query_limits *limits)
{
    clnt->limits.maxcost = gbl_querylimits_maxcost;
    clnt->limits.tablescans_ok = gbl_querylimits_tablescans_ok;
    clnt->limits.temptables_ok = gbl_querylimits_temptables_ok;
    clnt->limits.maxcost_warn = gbl_querylimits_maxcost_warn;
    clnt->limits.tablescans_warn = gbl_querylimits_tablescans_warn;
    clnt->limits.temptables_warn = gbl_querylimits_temptables_warn;

    if (limits) {
        clnt->have_query_limits = 1;
        clnt->limits = *limits;
    }
}

int sql_testrun(char *sql, int sqllen) { return 0; }

int sqlpool_init(void)
{
    gbl_sqlengine_thdpool =
        thdpool_create("SQL engine pool", sizeof(struct sqlthdstate));

    if (gbl_exit_on_pthread_create_fail)
        thdpool_set_exit(gbl_sqlengine_thdpool);

    /* big fat stack to handle big queries */
    thdpool_set_stack_size(gbl_sqlengine_thdpool, 4 * 1024 * 1024);
    thdpool_set_init_fn(gbl_sqlengine_thdpool, sqlengine_thd_start);
    thdpool_set_delt_fn(gbl_sqlengine_thdpool, sqlengine_thd_end);
    thdpool_set_minthds(gbl_sqlengine_thdpool, 4);
    thdpool_set_maxthds(gbl_sqlengine_thdpool, 48);
    thdpool_set_linger(gbl_sqlengine_thdpool, 30);
    thdpool_set_maxqueueoverride(gbl_sqlengine_thdpool, 500);
    thdpool_set_maxqueueagems(gbl_sqlengine_thdpool, 5 * 60 * 1000);
    thdpool_set_dump_on_full(gbl_sqlengine_thdpool, 1);

    return 0;
}

/* we have to clear
      - sqlclntstate (key, pointers in Bt, thd)
      - thd->tran and mode (this is actually done in Commit/Rollback)
 */
static void sql_reset_sqlthread(sqlite3 *db, struct sql_thread *thd)
{
    int i;

    if (thd) {
        thd->sqlclntstate = NULL;
    }
}

/**
 * Resets sqlite engine to retrieve the error code
 */
int sql_check_errors(struct sqlclntstate *clnt, sqlite3 *sqldb,
                     sqlite3_stmt *stmt, const char **errstr)
{

    int rc = SQLITE_OK;

    rc = sqlite3_reset(stmt);

    switch (rc) {
    case 0:
        rc = sqlite3_errcode(sqldb);
        if (rc)
            *errstr = sqlite3_errmsg(sqldb);
        break;

    case SQLITE_DEADLOCK:
        gbl_sql_deadlock_failures++;
        *errstr = sqlite3_errmsg(sqldb);
        break;

    case SQLITE_TOOBIG:
        *errstr = "transaction too big";
        rc = ERR_TRAN_TOO_BIG;
        break;

    case SQLITE_ABORT:
        /* no error in this case, regular abort or
           block processor failure to commit */
        rc = SQLITE_OK;
        break;

    case SQLITE_ERROR:
        /* check for convertion failure, stored in clnt->fail_reason */
        if (clnt->fail_reason.reason != CONVERT_OK) {
            rc = ERR_CONVERT_DTA;
        }
        *errstr = sqlite3_errmsg(sqldb);
        break;

    case SQLITE_LIMIT:
        *errstr = "Query exceeded set limits";
        break;

    case SQLITE_ACCESS:
        *errstr = errstat_get_str(&clnt->osql.xerr);
        if (!*errstr || (*errstr)[0] == 0) {
            *errstr = sqlite3_errmsg(sqldb);
            /* hate it please fix */
            if (*errstr == NULL || strcmp(*errstr, "not an error") == 0)
                *errstr = "access denied";
        }
        break;

    case SQLITE_CONV_ERROR:
        if (!*errstr)
            *errstr = "type conversion failure";
        break;

    case SQLITE_TRANTOOCOMPLEX:
        *errstr = "Transaction rollback too large";
        break;

    case SQLITE_TRAN_CANCELLED:
        *errstr = "Unable to maintain snapshot, too many resources blocked";
        break;

    case SQLITE_TRAN_NOLOG:
        *errstr = "Unable to maintain snapshot, too many log files";
        break;

    case SQLITE_TRAN_NOUNDO:
        *errstr = "Database changed due to sc or fastinit; snapshot failure";
        break;

    case SQLITE_CLIENT_CHANGENODE:
        *errstr = "Client api should run query against a different node";
        break;

    case SQLITE_SCHEMA_REMOTE:
        rc = SQLITE_OK; /* this is processed based on clnt->osql.xerr */
        break;
    default:
        rc = SQLITE_INTERNAL;
        *errstr = sqlite3_errmsg(sqldb);
        break;
    }

    return rc;
}

/*
 * convert a block processor code error
 * to an sql code error
 * this is also done for blocksql on the client side
 */
int blockproc2sql_error(int rc, const char *func, int line)
{
    switch (rc) {
    case 0:
        return CDB2_OK;
    /* error dispatched by the block processor */
    case 102:
        return CDB2ERR_NOMASTER;
    case 105:
        return DB_ERR_TRN_BUF_INVALID;
    case 106:
        return DB_ERR_TRN_BUF_OVERFLOW;
    case 195:
        return DB_ERR_INTR_READ_ONLY;
    case 199:
        return DB_ERR_BAD_REQUEST;
    case 208:
        return DB_ERR_TRN_OPR_OVERFLOW;
    case 212:
        return DB_ERR_NONKLESS;
    case 220:
        return DB_ERR_TRN_FAIL;
    case 222:
        return DB_ERR_TRN_DUP;
    case 224:
        return DB_ERR_TRN_VERIFY;
    case 225:
        return DB_ERR_TRN_DB_FAIL;
    case 230:
        return DB_ERR_TRN_NOT_SERIAL;
    case 301:
        return DB_ERR_CONV_FAIL;
    case 998:
        return DB_ERR_BAD_COMM_BUF;
    case 999:
        return DB_ERR_BAD_COMM;
    case 2000:
        return DB_ERR_TRN_DB_FAIL;
    case 2001:
        return CDB2ERR_PREPARE_ERROR;

    /* hack for now; if somehow we get a 300/RC_INTERNAL_RETRY
       it means that due to schema change or similar issues
       and we report deadlock error;
       in the future, this could be retried
     */
    case 300:
        return CDB2ERR_DEADLOCK;

    /* error dispatched on the sql side */
    case ERR_NOMASTER:
        return CDB2ERR_NOMASTER;

    case ERR_CONSTR:
        return CDB2ERR_CONSTRAINTS;

    case ERR_NULL_CONSTRAINT:
        return DB_ERR_TRN_NULL_CONSTRAINT;

    case SQLITE_ACCESS:
        return CDB2ERR_ACCESS;

    case 1229: /* ERR_BLOCK_FAILED + OP_FAILED_INTERNAL + ERR_FIND_CONSTRAINT */
        return DB_ERR_TRN_FKEY;

    case ERR_UNCOMMITABLE_TXN:
        return DB_ERR_TRN_VERIFY;

    case ERR_REJECTED:
        return SQLHERR_MASTER_QUEUE_FULL;

    case SQLHERR_MASTER_TIMEOUT:
        return SQLHERR_MASTER_TIMEOUT;

    case ERR_NOT_DURABLE:
        return CDB2ERR_CHANGENODE;

    default:
        return DB_ERR_INTR_GENERIC;
    }
}

int sqlserver2sqlclient_error(int rc)
{
    switch (rc) {
    case SQLITE_DEADLOCK:
        return CDB2ERR_DEADLOCK;
    case SQLITE_BUSY:
        return CDB2ERR_DEADLOCK;
    case SQLITE_LIMIT:
        return SQLHERR_LIMIT;
    case SQLITE_TRANTOOCOMPLEX:
        return SQLHERR_ROLLBACKTOOLARGE;
    case SQLITE_CLIENT_CHANGENODE:
        return CDB2ERR_CHANGENODE;
    case SQLITE_TRAN_CANCELLED:
        return SQLHERR_ROLLBACK_TOOOLD;
    case SQLITE_TRAN_NOLOG:
        return SQLHERR_ROLLBACK_NOLOG;
    case SQLITE_ACCESS:
        return CDB2ERR_ACCESS;
    case ERR_TRAN_TOO_BIG:
        return DB_ERR_TRN_OPR_OVERFLOW;
    case SQLITE_INTERNAL:
        return CDB2ERR_INTERNAL;
    case ERR_CONVERT_DTA:
        return DB_ERR_CONV_FAIL;
    case SQLITE_TRAN_NOUNDO:
        return SQLHERR_ROLLBACK_NOLOG; /* this will suffice */
    default:
        return CDB2ERR_UNKNOWN;
    }
}

static int test_no_btcursors(struct sqlthdstate *thd)
{

    sqlite3 *db;
    if ((db = thd->sqldb) == NULL) {
        return 0;
    }
    BtCursor *pCur = NULL;
    int leaked = 0;
    int i = 0;
    int rc = 0;

    for (i = 0; i < db->nDb; i++) {

        Btree *pBt = db->aDb[i].pBt;

        if (!pBt)
            continue;
        if (pBt->cursors.count) {
            logmsg(LOGMSG_ERROR, "%s: detected %d leaked btcursors\n", __func__,
                    pBt->cursors.count);
            leaked = 1;
            while (pBt->cursors.count) {

                pCur = listc_rtl(&pBt->cursors);
                if (pCur->bdbcur) {
                    logmsg(LOGMSG_ERROR, "%s: btcursor has bdbcursor opened\n",
                            __func__);
                }
                rc = sqlite3BtreeCloseCursor(pCur);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "sqlite3BtreeClose:sqlite3BtreeCloseCursor rc %d\n",
                           rc);
                }
            }
        }
    }

    return leaked;
}

unsigned long long osql_log_time(void)
{
    if (0) {
        return 1000 * ((unsigned long long)time_epoch()) + time_epochms();
    } else {
        struct timeval tv;

        gettimeofday(&tv, NULL);

        return 1000 * ((unsigned long long)tv.tv_sec) +
               ((unsigned long long)tv.tv_usec) / 1000;
    }
}

void osql_log_time_done(struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    osqltimings_t *tms = &osql->timings;
    fdbtimings_t *fdbtms = &osql->fdbtimes;

    if (!gbl_time_osql)
        goto fdb;

    /* fix short paths */
    if (tms->commit_end == 0)
        tms->commit_end = tms->query_finished;

    if (tms->commit_start == 0)
        tms->commit_start = tms->commit_end;

    if (tms->commit_prep == 0)
        tms->commit_prep = tms->commit_start;

    logmsg(LOGMSG_USER, "rqid=%llu total=%llu (queued=%llu) sql=%llu (exec=%llu "
            "prep=%llu commit=%llu)\n",
            osql->rqid, tms->query_finished - tms->query_received, /*total*/
            tms->query_dispatched - tms->query_received,           /*queued*/
            tms->query_finished - tms->query_dispatched, /*sql processing*/
            tms->commit_prep -
                tms->query_dispatched, /*local sql execution, before commit*/
            tms->commit_start - tms->commit_prep, /*time to ship shadows*/
            tms->commit_end -
                tms->commit_start /*ship commit, replicate, received rc*/
            );
fdb:
    if (!gbl_time_fdb)
        return;

    logmsg(LOGMSG_USER, "total=%llu msec (longest=%llu msec) calls=%llu\n",
            fdbtms->total_time, fdbtms->max_call, fdbtms->total_calls);
}

static void sql_thread_describe(void *obj, FILE *out)
{
    struct sqlclntstate *clnt = (struct sqlclntstate *)obj;
    char *host;

    if (!clnt) {
        logmsg(LOGMSG_USER, "non sql thread ???\n");
        return;
    }

    if (clnt->origin[0]) {
        logmsg(LOGMSG_USER, "%s \"%s\"\n", clnt->origin, clnt->sql);
    } else {
        host = get_origin_mach_by_buf(clnt->sb);
        logmsg(LOGMSG_USER, "(old client) %s \"%s\"\n", host, clnt->sql);
    }
}

const char *get_saved_errstr_from_clnt(struct sqlclntstate *clnt)
{
    return clnt ? clnt->saved_errstr : NULL;
}

static int watcher_warning_function(void *arg, int timeout, int gap)
{
    struct sqlclntstate *clnt = (struct sqlclntstate *)arg;

    logmsg(LOGMSG_WARN, 
            "WARNING: appsock idle for %d seconds (%d), connected from %s\n",
            gap, timeout, (clnt->origin) ? clnt->origin : "(unknown)");

    return 1; /* cancel recurrent */
}

static void dump_sql_hint_entry(void *item, void *p)
{
    int *count = (int *)p;
    sql_hint_hash_entry_type *entry = (sql_hint_hash_entry_type *)item;

    logmsg(LOGMSG_USER, "%d hit %d ref %d   %s  => %s\n", *count, entry->lnk.hits,
           entry->lnk.ref, entry->sql_hint, entry->sql_str);
    (*count)++;
}

void sql_dump_hints(void)
{
    int count = 0;
    pthread_mutex_lock(&gbl_sql_lock);
    {
        lrucache_foreach(sql_hints, dump_sql_hint_entry, &count);
    }
    pthread_mutex_unlock(&gbl_sql_lock);
}

/* Fetch a row from sqlite, and pack it into a buffer ready to send back to a
 * client.
 * My hope is that one day lua and sqlinterfaces will call this code. */
int emit_sql_row(struct sqlthdstate *thd, struct column_info *cols,
                 struct sqlfield *offsets, struct sqlclntstate *clnt,
                 sqlite3_stmt *stmt, int *irc, char *errstr, int maxerrstr)
{
    int offset;
    int ncols;
    int col;
    int fldlen;
    int buflen;
    int little_endian = 0;
    uint8_t *p_buf, *p_buf_end;
    int rc = 0;
    int created_cols = 0;
    int created_offsets = 0;

    *irc = 0;

    /* flip our data if the client asked for little endian data */
    if (clnt->have_endian && clnt->endian == FSQL_ENDIAN_LITTLE_ENDIAN) {
        little_endian = 1;
    }

    ncols = sqlite3_column_count(stmt);

    if (offsets == NULL) {
        offsets = malloc(ncols * sizeof(struct sqlfield));
        created_offsets = 1;
    }
    if (cols == NULL) {
        cols = malloc(ncols * sizeof(struct column_info));
        for (col = 0; col < ncols; col++) {
            const char *ctype = sqlite3_column_decltype(stmt, col);
            cols[col].type = typestr_to_type(ctype);
            snprintf(cols[col].column_name, sizeof(cols[col]), "%s",
                     sqlite3_column_name(stmt, col));
            cols[col].column_name[sizeof(cols[col]) - 1] = 0;
        }
        created_cols = 1;
    }

    offset = ncols * sizeof(struct sqlfield);
    /* compute offsets and buffer size */
    for (col = 0, buflen = 0; col < ncols; col++) {
        if (sqlite3_column_type(stmt, col) == SQLITE_NULL) {
            offsets[col].offset = -1;
            offsets[col].len = -1;
            buflen = offset;
            continue;
        }

        switch (cols[col].type) {
        case SQLITE_INTEGER:
            if (offset % sizeof(long long))
                offset += sizeof(long long) - offset % sizeof(long long);
            buflen = offset + sizeof(long long);
            offsets[col].offset = offset;
            offsets[col].len = sizeof(long long);
            offset += sizeof(long long);
            break;
        case SQLITE_FLOAT:
            if (offset % sizeof(double))
                offset += sizeof(double) - offset % sizeof(double);
            buflen = offset + sizeof(double);
            offsets[col].len = sizeof(double);
            offsets[col].offset = offset;
            offset += sizeof(double);
            break;
        case SQLITE_DATETIME:
        case SQLITE_DATETIMEUS:
            if (offset & 3) { /* align on 32 bit boundary */
                offset = (offset | 3) + 1;
            }
            if (clnt->have_extended_tm && clnt->extended_tm) {
                fldlen = CLIENT_DATETIME_EXT_LEN;
            } else {
                fldlen = CLIENT_DATETIME_LEN;
            }
            buflen = offset + fldlen;
            offsets[col].offset = offset;
            offsets[col].len = fldlen;
            offset += fldlen;
            break;
        case SQLITE_INTERVAL_YM:
            if (offset & 3) { /* align on 32 bit boundary */
                offset = (offset | 3) + 1;
            }
            fldlen = CLIENT_INTV_YM_LEN;
            buflen = offset + fldlen;
            offsets[col].offset = offset;
            offsets[col].len = fldlen;
            offset += fldlen;
            break;
        case SQLITE_INTERVAL_DS:
        case SQLITE_INTERVAL_DSUS:
            if (offset & 3) { /* align on 32 bit boundary */
                offset = (offset | 3) + 1;
            }
            fldlen = CLIENT_INTV_DS_LEN;
            buflen = offset + fldlen;
            offsets[col].offset = offset;
            offsets[col].len = fldlen;
            offset += fldlen;
            break;
        case SQLITE_TEXT:
        case SQLITE_BLOB:
            fldlen = sqlite3_column_bytes(stmt, col);
            if (cols[col].type == SQLITE_TEXT)
                fldlen++;
            buflen = offset + fldlen;
            offsets[col].offset = offset;
            offsets[col].len = fldlen;
            offset += fldlen;
            break;
        default:
            logmsg(LOGMSG_ERROR, "unknown type\n");
            break;
        }
    }
    /*buflen += ncols * sizeof(struct sqlfield);*/
    if (buflen > thd->maxbuflen) {
        char *p;
        thd->maxbuflen = buflen;
        p = realloc(thd->buf, thd->maxbuflen);
        if (!p) {
            logmsg(LOGMSG_ERROR, "%s: out of memory realloc %d\n", __func__,
                    thd->maxbuflen);
            buflen = -1;
            goto done;
        }
        thd->buf = p;
    }
    p_buf = (uint8_t *)thd->buf;
    p_buf_end = (uint8_t *)(thd->buf + buflen);
    for (col = 0; col < ncols; ++col) {
        if (!(p_buf = sqlfield_put(&offsets[col], p_buf, p_buf_end))) {
            buflen = -1;
            goto done;
        }
    }
    offset = ncols * sizeof(struct sqlfield);

    /* copy record to buffer */
    for (col = 0; col < ncols; col++) {
        double dval;
        long long ival;
        char *tval;

        if (col > 0) {
            reqlog_logl(thd->logger, REQL_RESULTS, ", ");
        }

        if (sqlite3_column_type(stmt, col) != SQLITE_NULL) {
            switch (cols[col].type) {
            case SQLITE_INTEGER:
                if (offset % sizeof(long long))
                    offset += sizeof(long long) - offset % sizeof(long long);
                ival = sqlite3_column_int64(stmt, col);
                reqlog_logf(thd->logger, REQL_RESULTS, "%lld", ival);
                if (little_endian)
                    ival = flibc_llflip(ival);
                if (!buf_put(&ival, sizeof(ival),
                             (uint8_t *)(thd->buf + offset), p_buf_end)) {
                    buflen = -1;
                    goto done;
                }
                offset += sizeof(long long);
                break;
            case SQLITE_FLOAT:
                if (offset % sizeof(double))
                    offset += sizeof(double) - offset % sizeof(double);
                dval = sqlite3_column_double(stmt, col);
                reqlog_logf(thd->logger, REQL_RESULTS, "%f", dval);
                if (little_endian)
                    dval = flibc_dblflip(dval);
                if (!buf_put(&dval, sizeof(dval),
                             (uint8_t *)(thd->buf + offset), p_buf_end)) {
                    buflen = -1;
                    goto done;
                }
                offset += sizeof(double);
                break;
            case SQLITE_TEXT:
                fldlen = sqlite3_column_bytes(stmt, col) + 1;
                tval = (char *)sqlite3_column_text(stmt, col);
                reqlog_logf(thd->logger, REQL_RESULTS, "'%s'", tval);
                /* text doesn't need to be packed */
                if (tval)
                    memcpy(thd->buf + offset, tval, fldlen);
                else
                    thd->buf[offset] = 0; /* null string */
                offset += fldlen;
                break;
            case SQLITE_DATETIME:
            case SQLITE_DATETIMEUS:
                if (offset & 3) { /* align on 32 bit boundary */
                    offset = (offset | 3) + 1;
                }
                if (clnt->have_extended_tm && clnt->extended_tm) {
                    fldlen = CLIENT_DATETIME_EXT_LEN;
                } else {
                    fldlen = CLIENT_DATETIME_LEN;
                }
                {
                    cdb2_client_datetime_t cdt;

                    tval = (char *)sqlite3_column_datetime(stmt, col);
                    if (!tval ||
                        convDttz2ClientDatetime((dttz_t *)tval,
                                                ((Vdbe *)stmt)->tzname, &cdt,
                                                cols[col].type)) {
                        bzero(thd->buf + offset, fldlen);
                        logmsg(LOGMSG_ERROR, "%s: datetime conversion "
                                "failure\n",
                                __func__);
                        snprintf(errstr, maxerrstr, "failed to convert sqlite "
                                                    "to client datetime for "
                                                    "field \"%s\"",
                                 sqlite3_column_name(stmt, col));
                        rc = DB_ERR_CONV_FAIL;
                        buflen = -2;
                        goto done;
                    }

                    if (little_endian) {
                        cdt.tm.tm_sec = flibc_intflip(cdt.tm.tm_sec);
                        cdt.tm.tm_min = flibc_intflip(cdt.tm.tm_min);
                        cdt.tm.tm_hour = flibc_intflip(cdt.tm.tm_hour);
                        cdt.tm.tm_mday = flibc_intflip(cdt.tm.tm_mday);
                        cdt.tm.tm_mon = flibc_intflip(cdt.tm.tm_mon);
                        cdt.tm.tm_year = flibc_intflip(cdt.tm.tm_year);
                        cdt.tm.tm_wday = flibc_intflip(cdt.tm.tm_wday);
                        cdt.tm.tm_yday = flibc_intflip(cdt.tm.tm_yday);
                        cdt.tm.tm_isdst = flibc_intflip(cdt.tm.tm_isdst);
                        cdt.msec = flibc_intflip(cdt.msec);
                    }

                    /* Old linux sql clients will have extended_tms's. */
                    if (clnt->have_extended_tm && clnt->extended_tm) {
                        if (!client_extended_datetime_put(
                                &cdt, (uint8_t *)(thd->buf + offset),
                                p_buf_end)) {
                            buflen = -1;
                            goto done;
                        }
                    } else {

                        if (!client_datetime_put(&cdt,
                                                 (uint8_t *)(thd->buf + offset),
                                                 p_buf_end)) {
                            buflen = -1;
                            goto done;
                        }
                    }
                }
                offset += fldlen;
                break;
            case SQLITE_INTERVAL_YM:
                if (offset & 3) { /* align on 32 bit boundary */
                    offset = (offset | 3) + 1;
                }
                fldlen = CLIENT_INTV_YM_LEN;
                {
                    cdb2_client_intv_ym_t ym;
                    const intv_t *tv =
                        sqlite3_column_interval(stmt, col, SQLITE_AFF_INTV_MO);

                    if (little_endian) {
                        ym.sign = flibc_intflip(tv->sign);
                        ym.years = flibc_intflip(tv->u.ym.years);
                        ym.months = flibc_intflip(tv->u.ym.months);
                    } else {
                        ym.sign = tv->sign;
                        ym.years = tv->u.ym.years;
                        ym.months = tv->u.ym.months;
                    }

                    if (!client_intv_ym_put(&ym, (uint8_t *)(thd->buf + offset),
                                            p_buf_end)) {
                        buflen = -1;
                        goto done;
                    }
                    offset += fldlen;
                }
                break;
            case SQLITE_INTERVAL_DS:
            case SQLITE_INTERVAL_DSUS:
                if (offset & 3) { /* align on 32 bit boundary */
                    offset = (offset | 3) + 1;
                }
                fldlen = CLIENT_INTV_DS_LEN;
                {
                    cdb2_client_intv_ds_t ds;
                    intv_t *tv;
                    tv = (intv_t *)sqlite3_column_interval(stmt, col,
                                                           SQLITE_AFF_INTV_SE);

                    /* Adjust fraction based on client precision. */
                    if (cols[col].type == SQLITE_INTERVAL_DS && tv->u.ds.prec == 6)
                        tv->u.ds.frac /= 1000;
                    else if (cols[col].type == SQLITE_INTERVAL_DSUS && tv->u.ds.prec == 3)
                        tv->u.ds.frac *= 1000;

                    if (little_endian) {
                        ds.sign = flibc_intflip(tv->sign);
                        ds.days = flibc_intflip(tv->u.ds.days);
                        ds.hours = flibc_intflip(tv->u.ds.hours);
                        ds.mins = flibc_intflip(tv->u.ds.mins);
                        ds.sec = flibc_intflip(tv->u.ds.sec);
                        ds.msec = flibc_intflip(tv->u.ds.frac);
                    } else {
                        ds.sign = tv->sign;
                        ds.days = tv->u.ds.days;
                        ds.hours = tv->u.ds.hours;
                        ds.mins = tv->u.ds.mins;
                        ds.sec = tv->u.ds.sec;
                        ds.msec = tv->u.ds.frac;
                    }

                    if (!client_intv_ds_put(&ds, (uint8_t *)(thd->buf + offset),
                                            p_buf_end)) {
                        buflen = -1;
                        goto done;
                    }
                    offset += fldlen;
                }
                break;
            case SQLITE_BLOB:
                fldlen = sqlite3_column_bytes(stmt, col);
                tval = (char *)sqlite3_column_blob(stmt, col);
                memcpy(thd->buf + offset, tval, fldlen);
                reqlog_logl(thd->logger, REQL_RESULTS, "x'");
                reqlog_loghex(thd->logger, REQL_RESULTS, tval, fldlen);
                reqlog_logl(thd->logger, REQL_RESULTS, "'");
                offset += fldlen;
                break;
            }
        } else {
            reqlog_logl(thd->logger, REQL_RESULTS, "NULL");
        }
    }

done:
    if (created_cols)
        free(cols);
    if (created_offsets)
        free(offsets);
    return buflen;
}

/**
 * Callback for sqlite during prepare, to retrieve default tzname
 * Required by stat4 which need datetime conversions during prepare
 *
 */
void comdb2_set_sqlite_vdbe_tzname(Vdbe *p)
{
    struct sql_thread *sqlthd = pthread_getspecific(query_info_key);
    if (!sqlthd)
        return;
    comdb2_set_sqlite_vdbe_tzname_int(p, sqlthd->sqlclntstate);
}

void comdb2_set_sqlite_vdbe_dtprec(Vdbe *p)
{
    struct sql_thread *sqlthd = pthread_getspecific(query_info_key);
    if (!sqlthd)
        return;
    comdb2_set_sqlite_vdbe_dtprec_int(p, sqlthd->sqlclntstate);
}

void run_internal_sql(char *sql)
{
    struct sqlclntstate clnt;
    reset_clnt(&clnt, NULL, 1);

    pthread_mutex_init(&clnt.wait_mutex, NULL);
    pthread_cond_init(&clnt.wait_cond, NULL);
    pthread_mutex_init(&clnt.write_lock, NULL);
    pthread_mutex_init(&clnt.dtran_mtx, NULL);
    clnt.dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    clnt.high_availability = 0;
    clnt.sql = sql;

    dispatch_sql_query(&clnt);
    if (clnt.query_rc || clnt.saved_errstr) {
        logmsg(LOGMSG_ERROR, "%s: Error from query: '%s' (rc = %d) \n", __func__, sql,
               clnt.query_rc);
        if (clnt.saved_errstr)
            logmsg(LOGMSG_ERROR, "%s: Error: '%s' \n", __func__, clnt.saved_errstr);
    }
    clnt_reset_cursor_hints(&clnt);
    osql_clean_sqlclntstate(&clnt);

    if (clnt.dbglog) {
        sbuf2close(clnt.dbglog);
        clnt.dbglog = NULL;
    }

    /* XXX free logical tran?  */

    clnt.dbtran.mode = TRANLEVEL_INVALID;
    if (clnt.query_stats)
        free(clnt.query_stats);

    pthread_mutex_destroy(&clnt.wait_mutex);
    pthread_cond_destroy(&clnt.wait_cond);
    pthread_mutex_destroy(&clnt.write_lock);
    pthread_mutex_destroy(&clnt.dtran_mtx);
}

void start_internal_sql_clnt(struct sqlclntstate *clnt)
{
    reset_clnt(clnt, NULL, 1);

    pthread_mutex_init(&clnt->wait_mutex, NULL);
    pthread_cond_init(&clnt->wait_cond, NULL);
    pthread_mutex_init(&clnt->write_lock, NULL);
    pthread_mutex_init(&clnt->dtran_mtx, NULL);
    clnt->dbtran.mode = tdef_to_tranlevel(gbl_sql_tranlevel_default);
    clnt->high_availability = 0;
    clnt->is_newsql = 0;
}

int run_internal_sql_clnt(struct sqlclntstate *clnt, char *sql)
{
#ifdef DEBUG
    printf("run_internal_sql_clnt() sql '%s'\n", sql);
#endif
    clnt->sql = sql;
    dispatch_sql_query(clnt);
    int rc = 0;

    if (clnt->query_rc || clnt->saved_errstr) {
        logmsg(LOGMSG_ERROR, "%s: Error from query: '%s' (rc = %d) \n", __func__, sql,
               clnt->query_rc);
        if (clnt->saved_errstr)
            logmsg(LOGMSG_ERROR, "%s: Error: '%s' \n", __func__, clnt->saved_errstr);
        rc = 1;
    }
    return rc;
}

void end_internal_sql_clnt(struct sqlclntstate *clnt)
{
    clnt_reset_cursor_hints(clnt);
    osql_clean_sqlclntstate(clnt);

    if (clnt->dbglog) {
        sbuf2close(clnt->dbglog);
        clnt->dbglog = NULL;
    }

    clnt->dbtran.mode = TRANLEVEL_INVALID;
    if (clnt->query_stats)
        free(clnt->query_stats);

    pthread_mutex_destroy(&clnt->wait_mutex);
    pthread_cond_destroy(&clnt->wait_cond);
    pthread_mutex_destroy(&clnt->write_lock);
    pthread_mutex_destroy(&clnt->dtran_mtx);
}

static int send_dummy(struct sqlclntstate *clnt)
{
    if(clnt->is_newsql)
        return newsql_send_dummy_resp(clnt, __func__, __LINE__);
    return 0;
}
