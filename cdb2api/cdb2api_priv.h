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

#ifndef INCLUDED_CDB2API_PRIV_H
#define INCLUDED_CDB2API_PRIV_H


#include "sqlquery.pb-c.h"
#include "sqlresponse.pb-c.h"
#include "sbuf2.h"


#define DB_TZNAME_DEFAULT "America/New_York"

#define MAX_NODES 128
#define MAX_CONTEXTS 10 /* Maximum stack size for storing context messages */
#define MAX_CONTEXT_LEN 100 /* Maximum allowed length of a context message */


#define MAX_CNONCE_LEN 100
struct context_messages {
    char *message[MAX_CONTEXTS];
    int count;
    int has_changed;
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
typedef struct cdb2_ssl_sess_list cdb2_ssl_sess_list;

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


struct cdb2_hndl {
    char dbname[64];
    char cluster[64];
    char type[64];
    char hosts[MAX_NODES][64];
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
    int skip_feature;
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
    cdb2_ssl_sess_list *sess_list;
#endif
    struct context_messages context_msgs;
    char *env_tz;
};

#endif
