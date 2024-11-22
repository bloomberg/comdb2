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

#ifndef INCLUDED_REQUEST_STATS_H
#define INCLUDED_REQUEST_STATS_H

#include <pthread.h>

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include <limits.h>

#include "comdb2.h"

/* Low level stats.  The idea is when something like a disk write or a malloc,
   or something
   happens, we call a callback that updates this struct, which is allocated on a
   per-request
   (sql query, qtrap, etc.) basis. It's intentionally vague. Yes, another
   pthread key. */
enum request_type { REQUEST_TYPE_SQL_QUERY = 1, REQUEST_TYPE_QTRAP = 2 };

enum {
    FLAG_REQUEST_TRACK_READS = 0x01,
    FLAG_REQUEST_TRACK_WRITES = 0x02,

    FLAG_REQUEST_TRACK_EVERYTHING = INT_MAX
};

struct per_request_stats {
    enum request_type type;
    int flags;
    int nreads;
    int nwrites;
    int failed_nreads;
    int failed_nwrites;
    int nfsyncs;
    int mempgets;
    long long readbytes;
    long long writebytes;
    long long failed_readbytes;
    long long failed_writebytes;
};

struct global_stats {
    int64_t page_reads;
    int64_t page_writes;
    int64_t failed_page_reads;
    int64_t failed_page_writes;
    int64_t fsyncs;
    int64_t mempgets;
    int64_t page_bytes_read;
    int64_t page_bytes_written;
    int64_t failed_page_bytes_read;
    int64_t failed_page_bytes_written;
};

void user_request_begin(enum request_type type, int flags);
struct per_request_stats *user_request_get_stats(void);
void user_request_end(void);
void user_request_read_callback(int);
void user_request_write_callback(int);
void user_request_memp_callback(void);
void user_request_init(void);
void global_request_stats(struct global_stats *stats);

void user_request_on(void);
void user_request_off(void);

#endif
