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
#include <stddef.h>
struct ireq;
struct comdb2buf;
struct dbglog_hdr;
struct sql_thread;
struct sqlclntstate;
struct client_query_stats;

int dbglog_init_write_counters(struct ireq *);
int grab_dbglog_file(unsigned long long, struct sqlclntstate *);
struct comdb2buf *open_dbglog_file(unsigned long long);
void append_debug_logs_from_master(struct comdb2buf *, unsigned long long);
void dbglog_dump_write_stats(struct ireq *);
void dbglog_record_db_write(struct ireq *, char *);
void dump_client_query_stats(struct comdb2buf *, struct client_query_stats *);
void dump_client_query_stats_packed(struct comdb2buf *, const uint8_t *);
int dbglog_process_debug_pragma(struct sqlclntstate *, const char *);
int dbglog_mmap_dbglog_file(unsigned long long, void **, size_t *, int *);
int dbglog_munmap_dbglog_file(unsigned long long, void *, size_t, int);

struct dbglog_impl {
    int (*dbglog_init_write_counters)(struct ireq *);
    int (*grab_dbglog_file)(unsigned long long, struct sqlclntstate *);
    struct comdb2buf *(*open_dbglog_file)(unsigned long long);
    void (*append_debug_logs_from_master)(struct comdb2buf *, unsigned long long);
    void (*dbglog_dump_write_stats)(struct ireq *);
    void (*dbglog_record_db_write)(struct ireq *, char *);
    void (*dump_client_query_stats)(struct comdb2buf *, struct client_query_stats *);
    void (*dump_client_query_stats_packed)(struct comdb2buf *, const uint8_t *);
    int (*process_debug_pragma)(struct sqlclntstate *, const char *);
    int (*mmap_dbglog_file)(unsigned long long, void **, size_t *, int *);
    int (*munmap_dbglog_file)(unsigned long long, void *, size_t, int);
};

void set_dbglog_impl(struct dbglog_impl *);
#endif
