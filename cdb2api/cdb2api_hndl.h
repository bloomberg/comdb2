/*
   Copyright 2025 Bloomberg Finance L.P.

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
 * CDB2API HNDL
 *
 */

#ifndef INCLUDED_CDB2API_HNDL_H
#define INCLUDED_CDB2API_HNDL_H
#include <sbuf2.h>

#include <sys/types.h>
#include <sys/queue.h>

#define MAX_NODES 48

#define DBNAME_LEN 64
#define TYPE_LEN 64
#define POLICY_LEN 24

#define CDB2HOSTNAME_LEN 128

#define MAX_STACK 512 /* Size of call-stack which opened the handle */

#include "sqlquery.pb-c.h"
#include "sqlresponse.pb-c.h"
#include "cdb2api.h"

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

#define MAX_CONTEXTS 10     /* Maximum stack size for storing context messages */
#define MAX_CONTEXT_LEN 100 /* Maximum allowed length of a context message */

struct context_messages {
    char *message[MAX_CONTEXTS];
    int count;
    int has_changed;
};

struct cdb2_stmt_types;

struct cdb2_query {
    TAILQ_ENTRY(cdb2_query) entry;
    void *buf;
    int len;
    int is_read;
    char *sql;
};
TAILQ_HEAD(query_list, cdb2_query);

struct cdb2_ssl_sess {
    struct cdb2_ssl_sess *next;
    char dbname[64];
    char cluster[64];
    int ref;
    SSL_SESSION *sessobj;
};
typedef struct cdb2_ssl_sess cdb2_ssl_sess;

#define TYPESTR_LEN DBNAME_LEN + TYPE_LEN + POLICY_LEN + 16

struct cdb2_hndl {
    char dbname[DBNAME_LEN];
    char cluster[64];
    char type[TYPE_LEN];
    char hosts[MAX_NODES][CDB2HOSTNAME_LEN];
    uint64_t timestampus; // client query timestamp of first try
    int ports[MAX_NODES];
    int hosts_connected[MAX_NODES];
    char shards[MAX_NODES][DBNAME_LEN];
    char cached_host[CDB2HOSTNAME_LEN]; /* hostname of a sockpool connection */
    int cached_port;                    /* port of a sockpool connection */
    SBUF2 *sb;
    int dbnum;
    int num_hosts;          /* total number of hosts */
    int num_hosts_sameroom; /* number of hosts that are in my datacenter (aka room) */
    int num_shards;
    int num_shards_sameroom;
    int node_seq; /* fail over to the `node_seq'-th host */
    int in_trans;
    int temp_trans;
    int is_retry;
    int is_chunk;
    int is_set;
    char newsql_typestr[TYPESTR_LEN];
    char policy[POLICY_LEN];
    int master;
    int connected_host;
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
    struct query_list queries;
    int snapshot_file;
    int snapshot_offset;
    int query_no;
    int retry_all;
    int num_set_commands;
    int num_set_commands_sent;
    int is_read;
    int is_invalid;
    int is_rejected;
    unsigned long long rows_read;
    int read_intrans_results;
    int first_record_read;
    char **commands;
    int ack;
    int is_hasql;
    int is_admin;
    int sockpool_enabled;
    int db_default_type_override_env;
    int clear_snap_line;
    int debug_trace;
    int max_retries;
    int min_retries;
    ssl_mode c_sslmode;      /* client SSL mode */
    peer_ssl_mode s_sslmode; /* server SSL mode */
    int sslerr;              /* 1 if unrecoverable SSL error. */
    char *sslpath;           /* SSL certificates */
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
    int sockpool_send_timeoutms;
    int sockpool_recv_timeoutms;
    int request_fp; /* 1 if requesting the fingerprint; 0 otherwise. */
    cdb2_event *events;
    pid_t pid;
    int got_dbinfo;

    /* per handle iaaap::IIdentity */
    const void *identity;

    // Protobuf allocator data used only for row data i.e. lastresponse
    ProtobufCAllocator s_allocator;
    void *protobuf_data;
    int protobuf_size;
    int protobuf_offset;
    ProtobufCAllocator *allocator;
    int auto_consume_timeout_ms;
    int max_auto_consume_rows;
    struct cdb2_hndl *fdb_hndl;
    int is_child_hndl;
    CDB2SQLQUERY__IdentityBlob *id_blob;
    struct cdb2_stmt_types *stmt_types;
    RETRY_CALLBACK retry_clbk;
};

#endif
