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

/*
 * CDB2 API
 *
 */

#ifndef INCLUDED_CDB2API_H
#define INCLUDED_CDB2API_H

#include <stdio.h>
#include <stdlib.h>

#if defined __cplusplus
extern "C" {
#endif

enum cdb2_hndl_alloc_flags {
    CDB2_READ_INTRANS_RESULTS = 2,
    CDB2_DIRECT_CPU = 4,
    CDB2_RANDOM = 8,
    CDB2_RANDOMROOM = 16,
    CDB2_ROOM = 32,
    CDB2_ADMIN = 64,
    CDB2_SQL_ROWS = 128,
    CDB2_TYPE_IS_FD = 256,
    CDB2_REQUIRE_FASTSQL = 512,
    CDB2_MASTER = 1024,
};

enum cdb2_request_type {
    CDB2_REQUEST_CDB2QUERY = 1,
    CDB2_REQUEST_SQLQUERY = 2,
    CDB2_REQUEST_DBINFO = 3
};

enum cdb2_errors {
    CDB2_OK = 0,
    CDB2_OK_DONE = 1,
    CDB2ERR_CONNECT_ERROR = -1,
    CDB2ERR_NOTCONNECTED = -2,
    CDB2ERR_PREPARE_ERROR = -3,
    CDB2ERR_IO_ERROR = -4,
    CDB2ERR_INTERNAL = -5,
    CDB2ERR_NOSTATEMENT = -6,
    CDB2ERR_BADCOLUMN = -7,
    CDB2ERR_BADSTATE = -8,
    CDB2ERR_ASYNCERR = -9,
    CDB2_OK_ASYNC = -10,

    CDB2ERR_INVALID_ID = -12,
    CDB2ERR_RECORD_OUT_OF_RANGE = -13,

    CDB2ERR_REJECTED = -15,
    CDB2ERR_STOPPED = -16,
    CDB2ERR_BADREQ = -17,
    CDB2ERR_DBCREATE_FAILED = -18,

    CDB2ERR_THREADPOOL_INTERNAL = -20, /* some error in threadpool code */
    CDB2ERR_ANALYZE_ALREADY_RUNNING = -22,
    CDB2ERR_APPSOCK_LIMIT = -23,

    CDB2ERR_NOMASTER = -101,
    CDB2ERR_UNTAGGED_DATABASE = -102,
    CDB2ERR_CONSTRAINTS = -103,
    CDB2ERR_DEADLOCK = 203,

    CDB2ERR_TRAN_IO_ERROR = -105,
    CDB2ERR_ACCESS = -106,

    CDB2ERR_QUERYLIMIT = -107,

    CDB2ERR_SCHEMA = -110,

    CDB2ERR_VERIFY_ERROR = 2,
    CDB2ERR_FKEY_VIOLATION = 3,
    CDB2ERR_NULL_CONSTRAINT = 4,

    CDB2ERR_CONV_FAIL = 113,
    CDB2ERR_NONKLESS = 114,
    CDB2ERR_MALLOC = 115,
    CDB2ERR_NOTSUPPORTED = 116,

    CDB2ERR_SCHEMACHANGE = 240,
    CDB2ERR_DUPLICATE = 299,
    CDB2ERR_READONLY = 305,
    CDB2ERR_NOTSERIAL = 230,
    CDB2ERR_TZNAME_FAIL = 401,
    CDB2ERR_CHANGENODE = 402,
    CDB2ERR_CHECK_CONSTRAINT = 403,
    CDB2ERR_DIST_ABORT = 410,
    CDB2ERR_QUERY_REJECTED = 451,

    CDB2ERR_UNKNOWN = 300

};

enum cdb2_api_const {
    CDB2_MAX_KEYS = 28,
    CDB2_MAX_SERVER_KEY_SIZE = 512,
    CDB2_MAX_CLIENT_KEY_SIZE = 256,
    CDB2_MAX_ASK_ARRAY = 1024,
    CDB2_MAX_ASK_SEGS = (CDB2_MAX_ASK_ARRAY - 2) / 2,
    CDB2_MAX_BLOB_FIELDS = 15,
    CDB2_MAX_TZNAME = 36,
    CDB2_MAX_BIND_ARRAY = 32767
};

/* New comdb2tm definition. */
typedef struct cdb2_tm {
    int tm_sec;
    int tm_min;
    int tm_hour;
    int tm_mday;
    int tm_mon;
    int tm_year;
    int tm_wday;
    int tm_yday;
    int tm_isdst;
} cdb2_tm_t;

struct cdb2_effects_type {
    int num_affected;
    int num_selected;
    int num_updated;
    int num_deleted;
    int num_inserted;
};

/* datetime type definition */
typedef struct cdb2_client_datetime {
    cdb2_tm_t tm;
    unsigned int msec;
    char tzname[CDB2_MAX_TZNAME];
} cdb2_client_datetime_t;

/* microsecond-precision datetime type definition */
typedef struct cdb2_client_datetimeus {
    cdb2_tm_t tm;
    unsigned int usec;
    char tzname[CDB2_MAX_TZNAME];
} cdb2_client_datetimeus_t;

/* interval types definition */
typedef struct cdb2_client_intv_ym {
    int sign;            /* sign of the interval, +/-1 */
    unsigned int years;  /* interval year */
    unsigned int months; /* interval months [0-11] */
} cdb2_client_intv_ym_t;

typedef struct cdb2_client_intv_ds {
    int sign;           /* sign of the interval, +/-1 */
    unsigned int days;  /* interval days    */
    unsigned int hours; /* interval hours   */
    unsigned int mins;  /* interval minutes */
    unsigned int sec;   /* interval sec     */
    unsigned int msec;  /* msec             */
} cdb2_client_intv_ds_t;

typedef struct cdb2_client_intv_dsus {
    int sign;           /* sign of the interval, +/-1 */
    unsigned int days;  /* interval days    */
    unsigned int hours; /* interval hours   */
    unsigned int mins;  /* interval minutes */
    unsigned int sec;   /* interval sec     */
    unsigned int usec;  /* usec             */
} cdb2_client_intv_dsus_t;

/* Map from protobuf */
typedef enum cdb2_coltype {
    CDB2_INTEGER = 1,
    CDB2_REAL = 2,
    CDB2_CSTRING = 3,
    CDB2_BLOB = 4,
    CDB2_DATETIME = 6,
    CDB2_INTERVALYM = 7,
    CDB2_INTERVALDS = 8,
    CDB2_DATETIMEUS = 9,
    CDB2_INTERVALDSUS = 10
} cdb2_coltype;

typedef struct cdb2_hndl cdb2_hndl_tp;
typedef struct cdb2_effects_type cdb2_effects_tp;

#ifndef INCLUDED_COMDB2_API_H
typedef struct cdb2_effects_type effects_tp;
#endif

void cdb2_set_comdb2db_config(const char *cfg_file);
void cdb2_set_comdb2db_info(const char *cfg_info);
int cdb2_get_comdb2db(char **comdb2db_name);

int cdb2_open(cdb2_hndl_tp **hndl, const char *dbname, const char *type,
              int flags);
int cdb2_clone(cdb2_hndl_tp **hndl, cdb2_hndl_tp *c_hndl);

int cdb2_next_record(cdb2_hndl_tp *hndl);

int cdb2_get_effects(cdb2_hndl_tp *hndl, cdb2_effects_tp *effects);

int cdb2_close(cdb2_hndl_tp *hndl);

int cdb2_run_statement(cdb2_hndl_tp *hndl, const char *sql);
int cdb2_run_statement_typed(cdb2_hndl_tp *hndl, const char *sql, int ntypes,
                             int *types);

int cdb2_numcolumns(cdb2_hndl_tp *hndl);
const char *cdb2_column_name(cdb2_hndl_tp *hndl, int col);
int cdb2_column_type(cdb2_hndl_tp *hndl, int col);
int cdb2_column_size(cdb2_hndl_tp *hndl, int col);
void *cdb2_column_value(cdb2_hndl_tp *hndl, int col);
const char *cdb2_errstr(cdb2_hndl_tp *hndl);
const char *cdb2_cnonce(cdb2_hndl_tp *hndl);
void cdb2_set_debug_trace(cdb2_hndl_tp *hndl);
void cdb2_dump_ports(cdb2_hndl_tp *hndl, FILE *out);
void cdb2_cluster_info(cdb2_hndl_tp *hndl, char **cluster, int *ports, int max, int *count);
int cdb2_snapshot_file(cdb2_hndl_tp *hndl, int *file, int *offset);
void cdb2_getinfo(cdb2_hndl_tp *hndl, int *intrans, int *hasql);
void cdb2_set_max_retries(int max_retries);
void cdb2_set_min_retries(int min_retries);
void cdb2_hndl_set_max_retries(cdb2_hndl_tp *hndl, int max_retries);
void cdb2_hndl_set_min_retries(cdb2_hndl_tp *hndl, int min_retries);

void cdb2_use_hint(cdb2_hndl_tp *hndl);

int cdb2_bind_param(cdb2_hndl_tp *hndl, const char *name, int type,
                    const void *varaddr, int length);
int cdb2_bind_index(cdb2_hndl_tp *hndl, int index, int type,
                    const void *varaddr, int length);
int cdb2_bind_array(cdb2_hndl_tp *, const char *, cdb2_coltype, const void *, size_t count, size_t typelen);
int cdb2_clearbindings(cdb2_hndl_tp *hndl);

const char *cdb2_dbname(cdb2_hndl_tp *hndl);
const char *cdb2_host(cdb2_hndl_tp *hndl);

void cdb2_enable_sockpool(void);
void cdb2_disable_sockpool(void);

int cdb2_push_context(cdb2_hndl_tp *hndl, const char *msg);
int cdb2_pop_context(cdb2_hndl_tp *hndl);
int cdb2_clear_contexts(cdb2_hndl_tp *hndl);

int cdb2_init_ssl(int init_libssl, int init_libcrypto);
int cdb2_is_ssl_encrypted(cdb2_hndl_tp *hndl);

void cdb2_setIdentityBlob(cdb2_hndl_tp *hndl, void *id);

int cdb2_clear_ack(cdb2_hndl_tp *hndl);

char *cdb2_string_escape(cdb2_hndl_tp *hndl, const char *str);

int cdb2_send_2pc(cdb2_hndl_tp *hndl, char *dbname, char *pname, char *ptier, char *source, unsigned int op,
                  char *dist_txnid, int rcode, int outrc, char *errmsg, int async);

typedef enum cdb2_event_ctrl {
    CDB2_OVERWRITE_RETURN_VALUE = 1,
    CDB2_AS_HANDLE_SPECIFIC_ARG = 1 << 1
} cdb2_event_ctrl;

typedef enum cdb2_event_type {
    /* Network events */
    CDB2_BEFORE_NEWSQL_CONNECT = 1,
    CDB2_AFTER_NEWSQL_CONNECT = 1 << 1,
    CDB2_BEFORE_PMUX = 1 << 2,
    CDB2_AFTER_PMUX = 1 << 3,
    CDB2_BEFORE_DBINFO = 1 << 4,
    CDB2_AFTER_DBINFO = 1 << 5,
    CDB2_BEFORE_SEND_QUERY = 1 << 6,
    CDB2_AFTER_SEND_QUERY = 1 << 7,
    CDB2_BEFORE_READ_RECORD = 1 << 8,
    CDB2_AFTER_READ_RECORD = 1 << 9,
    CDB2_AT_RECEIVE_HEARTBEAT = 1 << 10,
    CDB2_BEFORE_TCP_CONNECT = 1 << 11,
    CDB2_AFTER_TCP_CONNECT = 1 << 12,

    /* Logical operation events.
       A logicial operation event typically
       consists of multiple network events. */
    CDB2_AT_ENTER_RUN_STATEMENT = 1 << 13,
    CDB2_AT_EXIT_RUN_STATEMENT = 1 << 14,
    CDB2_AT_ENTER_NEXT_RECORD = 1 << 15,
    CDB2_AT_EXIT_NEXT_RECORD = 1 << 16,

    /* Lifecycle events */
    CDB2_BEFORE_DISCOVERY = 1 << 27,
    CDB2_AFTER_DISCOVERY = 1 << 28,
    CDB2_AT_OPEN = 1 << 29,
    CDB2_AT_CLOSE = 1 << 30
} cdb2_event_type;

typedef enum cdb2_event_arg {
    CDB2_HOSTNAME = 1,
    CDB2_PORT,
    CDB2_SQL,
    CDB2_RETURN_VALUE,
    CDB2_QUERY_STATE,
    CDB2_FINGERPRINT
} cdb2_event_arg;

typedef struct cdb2_event cdb2_event;

typedef void *(*cdb2_event_callback)(cdb2_hndl_tp *hndl, void *user_arg,
                                     int argc, void **argv);

cdb2_event *cdb2_register_event(cdb2_hndl_tp *hndl, cdb2_event_type types,
                                cdb2_event_ctrl ctrls, cdb2_event_callback cb,
                                void *user_arg, int argc, ...);
int cdb2_unregister_event(cdb2_hndl_tp *hndl, cdb2_event *e);

struct cdb2_identity {
    void  (*resetIdentity_start)();
    void  (*resetIdentity_end)(int);
    void *(*getIdentity)();
};

#if defined __cplusplus
}
#endif

#endif /* INCLUDED_CDB2_API_H */
