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

struct NewsqlProtobufCAllocator {
    ProtobufCAllocator protobuf_allocator;
    uint8_t *protobuf_data;
    int protobuf_offset;
    int protobuf_size;
    int alloced_outside_buffer;
};

#define APPDATA_MINCOLS 32

struct dbenv;
struct sbuf2;
struct sqlclntstate;

struct newsqlheader {
    int type;        /*  newsql request/response type */
    int compression; /*  Some sort of compression done? */
    int state;       /*  query state - whether it's progressing, etc. */
    int length;      /*  length of response */
};

struct newsql_postponed_data {
    size_t len;
    struct newsqlheader hdr;
    uint8_t *row;
};

struct newsql_appdata {
    int (*close_impl)(struct sqlclntstate *);
    int (*flush_impl)(struct sqlclntstate *);
    int (*get_fileno_impl)(struct sqlclntstate *);
    int (*get_x509_attr_impl)(struct sqlclntstate *, int, void *, int);
    int (*has_ssl_impl)(struct sqlclntstate *);
    int (*has_x509_impl)(struct sqlclntstate *);
    int (*local_check_impl)(struct sqlclntstate *);
    int (*peer_check_impl)(struct sqlclntstate *);
    int (*ping_pong_impl)(struct sqlclntstate *);
    int (*read_impl)(struct sqlclntstate *, void *, int len, int nitems);
    int (*set_timeout_impl)(struct sqlclntstate *, int);
    int (*write_impl)(struct sqlclntstate *, int type, int state, const CDB2SQLRESPONSE *, int flush);
    int (*write_hdr_impl)(struct sqlclntstate *, int type, int state);
    int (*write_postponed_impl)(struct sqlclntstate *);

    struct sbuf2 *sb; /* Leaving here for now - until newsql_appdata_sbuf exists */
    CDB2QUERY *query;
    CDB2SQLQUERY *sqlquery;
    int8_t send_intrans_response;
    struct newsql_postponed_data *postponed;
    struct NewsqlProtobufCAllocator newsql_protobuf_allocator;

    /* columns */
    int count;
    int capacity;
    int type[0]; /* must be last */
};

void setup_newsql_clnt(struct sqlclntstate *);
struct newsql_appdata *get_newsql_appdata(struct sqlclntstate *, int ncols);
int process_set_commands(struct sqlclntstate *, CDB2SQLQUERY *);
void handle_sql_intrans_unrecoverable_error(struct sqlclntstate *);
int newsql_heartbeat(struct sqlclntstate *);
#endif /* INCLUDED_NEWSQL_H */
