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

#ifndef INCLUDED_DBGLOG_H
#define INCLUDED_DBGLOG_H

#include <inttypes.h>
struct ireq;
struct sbuf2;
struct dbglog_hdr;
struct sql_thread;
struct sqlclntstate;
struct client_query_stats;

int dbglog_init_write_counters(struct ireq *);
int grab_dbglog_file(struct sbuf2 *, unsigned long long, struct sqlclntstate *);
struct sbuf2 *open_dbglog_file(unsigned long long);
void append_debug_logs_from_master(struct sbuf2 *, unsigned long long);
void dbglog_dump_write_stats(struct ireq *);
void dbglog_record_db_write(struct ireq *, char *);
void dump_client_query_stats(struct sbuf2 *, struct client_query_stats *);
void dump_client_query_stats_packed(struct sbuf2 *, const uint8_t *);

struct dbglog_impl {
    int (*dbglog_init_write_counters)(struct ireq *);
    int (*grab_dbglog_file)(struct sbuf2 *, unsigned long long, struct sqlclntstate *);
    struct sbuf2 *(*open_dbglog_file)(unsigned long long);
    void (*append_debug_logs_from_master)(struct sbuf2 *, unsigned long long);
    void (*dbglog_dump_write_stats)(struct ireq *);
    void (*dbglog_record_db_write)(struct ireq *, char *);
    void (*dump_client_query_stats)(struct sbuf2 *, struct client_query_stats *);
    void (*dump_client_query_stats_packed)(struct sbuf2 *, const uint8_t *);
};

void set_dbglog_impl(struct dbglog_impl *);
#endif
