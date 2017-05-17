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

#include <sbuf2.h>

#include "comdb2.h"
#include <compile_time_assert.h>

enum { DBGLOG_QUERYSTATS = 1 };

struct dbglog_hdr {
    int type;
    int len;
};
enum { DBGLOG_HDR_LEN = 4 + 4 };
BB_COMPILE_TIME_ASSERT(dbglog_hdr_len,
                       sizeof(struct dbglog_hdr) == DBGLOG_HDR_LEN);

SBUF2 *open_dbglog_file(unsigned long long cookie);
int record_query_cost(struct sql_thread *thd, struct sqlclntstate *clnt);
void dump_client_query_stats(SBUF2 *sb, struct client_query_stats *st);
void dump_client_query_stats_packed(SBUF2 *db,
                                    const uint8_t *p_buf_client_query_stats);
int grab_dbglog_file(SBUF2 *sb, unsigned long long cookie,
                     struct sqlclntstate *clnt);
int dbglog_init_write_counters(struct ireq *iq);
void dbglog_record_db_write(struct ireq *iq, char *optype);
void append_debug_logs_from_master(SBUF2 *oursb,
                                   unsigned long long master_dbglog_cookie);
void dbglog_dump_write_stats(struct ireq *);

#endif
