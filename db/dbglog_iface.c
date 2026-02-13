/*
   Copyright 2018 Bloomberg Finance L.P.

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

#include <dbglog.h>

static struct dbglog_impl impl;

int dbglog_init_write_counters(struct ireq *iq)
{
    if (impl.dbglog_init_write_counters)
        return impl.dbglog_init_write_counters(iq);
    return 0;
}

int grab_dbglog_file(unsigned long long cookie, struct sqlclntstate *clnt)
{
    if (impl.grab_dbglog_file)
        return impl.grab_dbglog_file(cookie, clnt);
    return 0;
}

SBUF2 *open_dbglog_file(unsigned long long cookie)
{
    if (impl.open_dbglog_file)
        return impl.open_dbglog_file(cookie);
    return 0;
}
void append_debug_logs_from_master(SBUF2 *sb, unsigned long long cookie)
{
    if (impl.append_debug_logs_from_master)
        impl.append_debug_logs_from_master(sb, cookie);
}

void dbglog_dump_write_stats(struct ireq *iq)
{
    if (impl.dbglog_dump_write_stats)
        impl.dbglog_dump_write_stats(iq);
}

void dbglog_record_db_write(struct ireq *iq, char *c)
{
    if (impl.dbglog_record_db_write)
        impl.dbglog_record_db_write(iq, c);
}

void dump_client_query_stats(SBUF2 *sb, struct client_query_stats *stats)
{
    if (impl.dump_client_query_stats)
        impl.dump_client_query_stats(sb, stats);
}

void dump_client_query_stats_packed(SBUF2 *sb, const uint8_t *pakd)
{
    if (impl.dump_client_query_stats_packed)
        impl.dump_client_query_stats_packed(sb, pakd);
}

int dbglog_process_debug_pragma(struct sqlclntstate *clnt, const char *pragma)
{
    if (impl.process_debug_pragma)
        return impl.process_debug_pragma(clnt, pragma);
    return 0;
}

int dbglog_mmap_dbglog_file(unsigned long long c, void **a, size_t *s, int *f)
{
    if (impl.mmap_dbglog_file)
        return impl.mmap_dbglog_file(c, a, s, f);
    return -1;
}

int dbglog_munmap_dbglog_file(unsigned long long c, void *a, size_t s, int f)
{
    if (impl.munmap_dbglog_file)
        return impl.munmap_dbglog_file(c, a, s, f);
    return -1;
}

void set_dbglog_impl(struct dbglog_impl *i)
{
    impl = *i;
}
