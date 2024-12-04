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

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <stddef.h>
#include <comdb2_atomic.h>
#include <stackutil.h>

#include "request_stats.h"

static pthread_key_t key;
static int enabled = 1;

extern void __berkdb_register_read_callback(void (*callback)(int bytes));
extern void __berkdb_register_write_callback(void (*callback)(int bytes));
extern void __berkdb_register_failed_read_callback(void (*callback)(int bytes));
extern void __berkdb_register_failed_write_callback(void (*callback)(int bytes));
extern void __berkdb_register_memp_callback(void (*callback)(void));
extern void __berkdb_register_fsync_callback(void (*callback)(int fd));

static void user_request_done(void *st) { free(st); }

static struct global_stats global = {0};

int gbl_stack_at_page_read = 0;
int gbl_stack_at_page_write = 0;

void user_request_begin(enum request_type type, int flags)
{
    struct per_request_stats *st;
    st = pthread_getspecific(key);
    if (st == NULL) {
        st = malloc(sizeof(struct per_request_stats));
        if (st == NULL)
            return;
        Pthread_setspecific(key, st);
    }
    bzero(st, sizeof(struct per_request_stats));
    st->type = type;
    st->flags = flags;
}

struct per_request_stats *user_request_get_stats(void)
{
    struct per_request_stats *st;
    st = pthread_getspecific(key);
    return st;
}

void user_request_fsync_callback(int fd)
{
    struct per_request_stats *st;

    if (!enabled)
        return;

    st = pthread_getspecific(key);
    if (st) {
        st->nfsyncs++;
    }

    ATOMIC_ADD64(global.fsyncs, 1);
}

void user_request_failed_read_callback(int bytes)
{
    struct per_request_stats *st;

    if (!enabled)
        return;

    st = pthread_getspecific(key);
    if (st) {
        st->failed_nreads++;
        st->failed_readbytes += bytes;
    }

    ATOMIC_ADD64(global.failed_page_reads, 1);
    ATOMIC_ADD64(global.failed_page_bytes_read, bytes);
}

void user_request_failed_write_callback(int bytes)
{
    struct per_request_stats *st;

    if (!enabled)
        return;

    st = pthread_getspecific(key);
    if (st) {
        st->failed_nwrites++;
        st->failed_writebytes += bytes;
    }

    ATOMIC_ADD64(global.failed_page_writes, 1);
    ATOMIC_ADD64(global.failed_page_bytes_written, bytes);
}

void user_request_read_callback(int bytes)
{
    struct per_request_stats *st;

    if (!enabled)
        return;

    st = pthread_getspecific(key);
    if (st) {
        st->nreads++;
        st->readbytes += bytes;
    }

    ATOMIC_ADD64(global.page_reads, 1);
    ATOMIC_ADD64(global.page_bytes_read, bytes);

    if (gbl_stack_at_page_read) {
        stackutil_get_stack_id("page_read");
    }
}

void user_request_write_callback(int bytes)
{
    struct per_request_stats *st;

    if (!enabled)
        return;

    st = pthread_getspecific(key);
    if (st) {
        st->nwrites++;
        st->writebytes += bytes;
    }

    ATOMIC_ADD64(global.page_writes, 1);
    ATOMIC_ADD64(global.page_bytes_written, bytes);

    if (gbl_stack_at_page_write) {
        stackutil_get_stack_id("page_write");
    }
}

void user_request_memp_callback(void)
{
    struct per_request_stats *st;

    if (!enabled)
        return;

    st = pthread_getspecific(key);
    if (st)
        st->mempgets++;

    ATOMIC_ADD64(global.mempgets, 1);
}

void global_request_stats(struct global_stats *stats)
{
    stats->page_reads = ATOMIC_LOAD64(global.page_reads);
    stats->page_writes = ATOMIC_LOAD64(global.page_writes);
    stats->failed_page_reads = ATOMIC_LOAD64(global.failed_page_reads);
    stats->failed_page_writes = ATOMIC_LOAD64(global.failed_page_writes);
    stats->fsyncs = ATOMIC_LOAD64(global.fsyncs);
    stats->mempgets = ATOMIC_LOAD64(global.mempgets);
    stats->page_bytes_read = ATOMIC_LOAD64(global.page_bytes_read);
    stats->page_bytes_written = ATOMIC_LOAD64(global.page_bytes_written);
    stats->failed_page_bytes_read = ATOMIC_LOAD64(global.failed_page_bytes_read);
    stats->failed_page_bytes_written = ATOMIC_LOAD64(global.failed_page_bytes_written);
}

void user_request_init(void)
{
    Pthread_key_create(&key, user_request_done);

    __berkdb_register_read_callback(user_request_read_callback);
    __berkdb_register_failed_read_callback(user_request_failed_read_callback);
    __berkdb_register_write_callback(user_request_write_callback);
    __berkdb_register_failed_write_callback(user_request_failed_write_callback);
    __berkdb_register_memp_callback(user_request_memp_callback);
    __berkdb_register_fsync_callback(user_request_fsync_callback);
}

void user_request_on(void) { enabled = 1; }

void user_request_off(void) { enabled = 0; }
