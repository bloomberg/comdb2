/*
   Copyright 2021 Bloomberg Finance L.P.

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

#ifndef INCLUDED_SQLWRITER_H
#define INCLUDED_SQLWRITER_H

#include <stdint.h>
#include <pthread.h>

//writer will block if outstanding data hits:
#define SQLWRITER_MAX_BUF KB(256)

struct dispatch_sql_arg;
struct evbuffer;
struct event_base;
struct iovec;
struct sqlclntstate;
struct sqlwriter;
struct ssl_data;

int  sql_flush(struct sqlwriter *);

void sql_enable_heartbeat(struct sqlwriter *);
void sql_disable_heartbeat(struct sqlwriter *);

void sql_enable_timeout(struct sqlwriter *, int);
void sql_disable_timeout(struct sqlwriter *);

int sql_write(struct sqlwriter *, void *, int);
int sql_writev(struct sqlwriter *, struct iovec *, int);
int sql_write_buffer(struct sqlwriter *, struct evbuffer *);
int sql_append_packed(struct sqlwriter *, const void *, size_t);

typedef int(sql_pack_fn)(struct sqlwriter *, void *pack_arg);
struct sqlwriter_arg {
    int fd;
    struct sqlclntstate *clnt;
    struct event_base *timer_base;
    sql_pack_fn *pack;
    sql_pack_fn *pack_hb;
};
struct sqlwriter *sqlwriter_new(struct sqlwriter_arg *);
void sqlwriter_free(struct sqlwriter *);
void sql_reset(struct sqlwriter *);

int sql_done(struct sqlwriter *);
int sql_peer_check(struct sqlwriter *);

struct evbuffer *sql_wrbuf(struct sqlwriter *);

int done_cb_evbuffer(struct sqlclntstate *);
int recover_deadlock_evbuffer(struct sqlclntstate *);

void sql_enable_ssl(struct sqlwriter *, struct ssl_data *);
void sql_disable_ssl(struct sqlwriter *);

typedef void(sql_dispatch_timeout_fn)(struct sqlclntstate *);
void sql_wait_for_leader(struct sqlwriter *, sql_dispatch_timeout_fn *);

void clnt_increase_netwaitus(struct sqlclntstate *clnt, int this_many_us);
#endif /* INCLUDED_SQLWRITER_H */
