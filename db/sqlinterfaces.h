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

#ifndef __SQLINTERFACES_H__
#define __SQLINTERFACES_H__

/* comment for commit */

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <pthread.h>

#include <plhash.h>
#include <segstr.h>

#include <list.h>

#include <sbuf2.h>
#include <bdb_api.h>

#include "comdb2.h"
#include "types.h"
#include "tag.h"

#include <dynschematypes.h>
#include <dynschemaload.h>

#include <sqlite3.h>
#include "comdb2uuid.h"
#include <sqlresponse.pb-c.h>


struct newsqlheader {
    int type;        /*  newsql request/response type */
    int compression; /*  Some sort of compression done? */
    int dummy;       /*  Make it equal to fsql header. */
    int length;      /*  length of response */
};

struct column_info {
    int type; /* SQLITE_INTEGER   SQLITE_FLOAT   SQLITE_DOUBLE   SQLITE_BLOB
                 (not supported yet) */
    char column_name[32]; /* fixed column sizes */
};

enum { COLUMN_INFO_LEN = 4 + (1 * 32) };
BB_COMPILE_TIME_ASSERT(column_info_size,
                       sizeof(struct column_info) == COLUMN_INFO_LEN);

/* higher numbers to not clash with sqlnet.c.  In fact, this should
   merge into sqlnet at some point in the distant future */
/* requests for fastsql */
enum fsql_request {
    FSQL_EXECUTE_INLINE_PARAMS = 100,
    FSQL_EXECUTE_STOP = 101,
    FSQL_SET_ISOLATION_LEVEL = 102,
    FSQL_SET_TIMEOUT = 103,
    FSQL_SET_INFO = 104,
    FSQL_EXECUTE_INLINE_PARAMS_TZ = 105,
    FSQL_SET_HEARTBEAT = 106,
    FSQL_PRAGMA = 107,
    FSQL_RESET = 108,
    FSQL_EXECUTE_REPLACEABLE_PARAMS = 109,
    FSQL_SET_SQL_DEBUG = 110,
    FSQL_GRAB_DBGLOG = 111,
    FSQL_SET_USER = 112,
    FSQL_SET_PASSWORD = 113,
    FSQL_SET_ENDIAN = 114,
    FSQL_EXECUTE_REPLACEABLE_PARAMS_TZ = 115,
    FSQL_GET_EFFECTS = 116,
    FSQL_SET_PLANNER_EFFORT = 117,
    FSQL_SET_REMOTE_ACCESS = 118,
    FSQL_OSQL_MAX_TRANS = 119,
    FSQL_SET_DATETIME_PRECISION = 120,
    FSQL_SSLCONN = 121
};

/* responses for fastsql */
enum fsql_response {
    FSQL_COLUMN_DATA = 200,
    FSQL_ROW_DATA = 201,
    FSQL_NO_MORE_DATA = 202,
    FSQL_ERROR = 203,
    FSQL_QUERY_STATS = 204,
    FSQL_HEARTBEAT = 205,
    FSQL_SOSQL_TRAN_RESPONSE = 206,
    FSQL_DEBUG_TRACE = 207,
    FSQL_NEW_ROW_DATA = 208,
    FSQL_QUERY_EFFECTS = 209
};

/* Response types for an FSQL_QUERY_STATS response.  Must correspond to values
   in comdb2_api.c. */
enum { FSQL_RESP_QUERY_COST = 1, FSQL_RESP_QUERY_INFO = 2 };

/* Endian values for FSQL_SET_ENDIAN */
enum { FSQL_ENDIAN_BIG_ENDIAN = 1, FSQL_ENDIAN_LITTLE_ENDIAN = 2 };

union sql_dbglog_info {
    struct {
        int dbgtype;
        int sqltype;
    } type;

    struct record_per_info {
        int dbgtype;
        int sqltype;

        int nixmv;   /* index first/next/prev/last */
        int nixfnd;  /* index finds */
        int ndtamv;  /* dta first/next/prev/last */
        int ndtafnd; /* dta finds */
    } record_per_info;

    unsigned char pad[128];
};

struct fsqlreq {
    int request;   /* enum fsql_request */
    int flags;     /* request flags */
    int parm;      /* extra word of into differs per request */
    int followlen; /* how much data follows header*/
};
enum { FSQLREQ_LEN = 4 + 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(fsqlreq_size, sizeof(struct fsqlreq) == FSQLREQ_LEN);

/* fsqslreq is needed for dbglog_support */
uint8_t *fsqlreq_put(const struct fsqlreq *p_fsqlreq, uint8_t *p_buf,
                     const uint8_t *p_buf_end);

struct fsqlresp {
    int response; /* enum fsql_response */
    int flags;    /* response flags */
    int rcode;
    int parm;      /* extra word of info differs per request type */
    int followlen; /* how much data follows header*/
};
enum { FSQLRESP_LEN = 4 + 4 + 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(fsqlresp_size, sizeof(struct fsqlresp) == FSQLRESP_LEN);

/* fsqlresp_put is needed for dbglog_support */
uint8_t *fsqlresp_put(const struct fsqlresp *p_fsqlresp, uint8_t *p_buf,
                      const uint8_t *p_buf_end);

/* fsqlresp_get is needed for dbglog_support */
const uint8_t *fsqlresp_get(struct fsqlresp *p_fsqlresp, const uint8_t *p_buf,
                            const uint8_t *p_buf_end);

/* return codes */
enum fsql_return_code {
    FSQL_OK = 0,
    FSQL_DONE = 1,       /* no more records to read */
    FSQL_CONNECT = 1002, /* can't connect to db (shouldn't happen) */
    FSQL_PREPARE = 1003, /* prepare failed */
    FSQL_EXEC = 1004,    /* error during execute */
};

struct sqlfield {
    int offset;
    int len;
};
enum { SQLFIELD_LEN = 4 + 4 };
BB_COMPILE_TIME_ASSERT(sqlfield_size, sizeof(struct sqlfield) == SQLFIELD_LEN);

/* SQL REQUEST FLAGS */
enum {
    /* don't use 1-3 */
    SQLREQ_FLAG_CLIENT_UNDERSTANDS_STATS = 4
};

struct tag_osql {
    char *tag;
    void *tagbuf;
    int tagbuflen;
    void *nullbits;
    int numnullbits;
    int numblobs;
    void **blobs;
    int *bloblens;
};

struct sqlclntstate;
struct sql_thread;
struct query_limits;

void set_lua_mspace(void *space);
void set_thread_mspace(void *space);
void *get_sql_mspace();

char *tranlevel_tostr(int lvl);

int newsql_send_column_info(struct sqlclntstate *clnt, struct column_info *col, 
                             int ncols, sqlite3_stmt *stmt, 
                             CDB2SQLRESPONSE__Column **columns);
void newsql_send_strbuf_response(struct sqlclntstate *clnt, const char *str,
                                 int slen);
int newsql_send_dummy_resp(struct sqlclntstate *clnt, const char *func, int line);
int newsql_send_last_row(struct sqlclntstate *clnt, int is_begin, const char *func, int line);

int newsql_write_response(struct sqlclntstate *clnt, int type,
                          CDB2SQLRESPONSE *sql_response,
                          int flush, void* (*alloc)(size_t size),
                          const char *func, int line);

/* requests for fastsql */
int fsql_write_response(struct sqlclntstate *clnt, struct fsqlresp *resp,
                        void *dta, int len, int flush, const char *func, uint32_t line);

int handle_fastsql_requests(struct thr_handle *thr_self, SBUF2 *sb,
                            int *keepsock, int wrong_db);

int handle_newsql_requests(struct thr_handle *thr_self, SBUF2 *sb);

int sql_check_errors(struct sqlclntstate *clnt, sqlite3 *sqldb,
                     sqlite3_stmt *stmt, const char **errstr);

void sql_dump_hist_statements(void);

int handle_offloadsql_pool(char *sql, int sqllen, char *host,
                           unsigned long long rqid, uuid_t uuid, char *tzname,
                           int queryid, int flags, struct query_limits *limits,
                           struct tag_osql *taginfo);

int handle_offloadsql_remotesql(char *sql, int sqllen, char *host,
                                unsigned long long rqid, uuid_t uuid,
                                char *tzname, int queryid, int flags,
                                SBUF2 *sb);

enum {
    SQL_PRAGMA_CASE_SENSITIVE_LIKE = 1,
    SQL_PRAGMA_MAXCOST = 2,
    SQL_PRAGMA_TABLESCAN_OK = 3,
    SQL_PRAGMA_TEMPTABLES_OK = 4,
    SQL_PRAGMA_MAXCOST_WARNING = 5,
    SQL_PRAGMA_TABLESCAN_WARNING = 6,
    SQL_PRAGMA_TEMPTABLES_WARNING = 7,
    SQL_PRAGMA_EXTENDED_TM = 8,
    TAGGED_PRAGMA_CLIENT_ENDIAN = 9,
    TAGGED_PRAGMA_EXTENDED_TM = 10,
    SQL_PRAGMA_SP_VERSION = 11,
    SQL_PRAGMA_ERROR = 12
};

double query_cost(struct sql_thread *thd);

const char *get_saved_errstr_from_clnt(struct sqlclntstate *);

void run_internal_sql(char *sql);
void start_internal_sql_clnt(struct sqlclntstate *clnt);
int run_internal_sql_clnt(struct sqlclntstate *clnt, char *sql);
void end_internal_sql_clnt(struct sqlclntstate *clnt);
CDB2SQLRESPONSE__Column** newsql_alloc_row(int ncols);
void newsql_dealloc_row(CDB2SQLRESPONSE__Column **columns, int ncols);
void reset_clnt_flags(struct sqlclntstate *);
int request_sequence_num(const char *name, long long *val);
#endif
