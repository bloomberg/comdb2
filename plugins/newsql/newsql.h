/*
   Copyright 2017-2020 Bloomberg Finance L.P.

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

#ifndef INCLUDED_NEWSQL_H
#define INCLUDED_NEWSQL_H

#include <sqlquery.pb-c.h>
#include <sqlresponse.pb-c.h>

struct sqlclntstate;
struct newsql_appdata;

struct newsql_stmt {
    CDB2QUERY *query;
    char tzname[CDB2_MAX_TZNAME];
};

struct newsqlheader {
    int type;        /*  newsql request/response type */
    int compression; /*  Some sort of compression done? */
    int state;       /*  query state - whether it's progressing, etc. */
    int length;      /*  length of response */
};

typedef enum {
    NEWSQL_STATE_NONE,
    /* Query is making progress. Applicable only when type is HEARTBEAT */
    NEWSQL_STATE_ADVANCING,
    /* RESET from in-process cache. Applicable only when type is RESET */
    NEWSQL_STATE_LOCALCACHE
} newsql_state;

struct newsql_postponed_data {
    size_t len;
    struct newsqlheader hdr;
    uint8_t *row;
};

#define NEWSQL_APPDATA_COMMON                                                  \
    int (*ping_pong)(struct sqlclntstate *);                                   \
    int (*read)(struct sqlclntstate *, void *, int len, int nitems);           \
    int (*write)(struct sqlclntstate *, int type, int state, const CDB2SQLRESPONSE *, int flush); \
    int (*write_dbinfo)(struct sqlclntstate *);                                \
    int (*write_hdr)(struct sqlclntstate *, int type, int state);              \
    int (*write_postponed)(struct sqlclntstate *);                             \
    CDB2QUERY *query;                                                          \
    CDB2SQLQUERY *sqlquery;                                                    \
    int8_t send_intrans_response;                                              \
    int8_t protocol_version;                                              \
    struct newsql_postponed_data *postponed;                                   \
    struct sql_col_info col_info;

void newsql_setup_clnt(struct sqlclntstate *);
void newsql_destroy_clnt(struct sqlclntstate *);
int process_set_commands(struct sqlclntstate *, CDB2SQLQUERY *);
void handle_sql_intrans_unrecoverable_error(struct sqlclntstate *);
int newsql_heartbeat(struct sqlclntstate *);
void setup_newsql_evbuffer_handlers(void);
int newsql_first_run(struct sqlclntstate *, CDB2SQLQUERY *);
typedef enum {
    NEWSQL_SUCCESS = 0,
    NEWSQL_ERROR = 1,
    NEWSQL_INCOHERENT = 2,
    NEWSQL_NO_LEADER = 3,
    NEWSQL_NEW_LEADER = 4
} newsql_loop_result;
newsql_loop_result newsql_loop(struct sqlclntstate *, CDB2SQLQUERY *);
int is_commit_rollback(struct sqlclntstate *);
int newsql_should_dispatch(struct sqlclntstate *, int *is_commit_rollback);
void newsql_reset(struct sqlclntstate *, int);
void free_newsql_appdata(struct sqlclntstate *);
void newsql_effects(CDB2SQLRESPONSE *, CDB2EFFECTS *, struct sqlclntstate *);

#define plugin_set_callbacks_newsql(name)                                      \
    clnt->plugin.close = newsql_close##_##name;                                \
    clnt->plugin.destroy_stmt = newsql_destroy_stmt##_##name;                  \
    clnt->plugin.flush = newsql_flush##_##name;                                \
    clnt->plugin.get_fileno = newsql_get_fileno##_##name;                      \
    clnt->plugin.get_x509_attr = newsql_get_x509_attr##_##name;                \
    clnt->plugin.has_ssl = newsql_has_ssl##_##name;                            \
    clnt->plugin.has_x509 = newsql_has_x509##_##name;                          \
    clnt->plugin.local_check = newsql_local_check##_##name;                    \
    clnt->plugin.peer_check = newsql_peer_check##_##name;                      \
    clnt->plugin.set_timeout = newsql_set_timeout##_##name;                    \
    appdata->ping_pong = newsql_ping_pong##_##name;                            \
    appdata->read = newsql_read##_##name;                                      \
    appdata->write = newsql_write##_##name;                                    \
    appdata->write_dbinfo = newsql_write_dbinfo##_##name;                      \
    appdata->write_hdr = newsql_write_hdr##_##name;                            \
    appdata->write_postponed = newsql_write_postponed##_##name;

int leader_is_new(void);
#endif /* INCLUDED_NEWSQL_H */
