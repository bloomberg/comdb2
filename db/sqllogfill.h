/*
   Copyright 2025 Bloomberg Finance L.P.

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

#ifndef SQLLOGFILL_H
#define SQLLOGFILL_H

#include <bdb_int.h>

void sql_logfill_signal(bdb_state_type *bdb_state);

void create_sql_logfill_threads(bdb_state_type *bdb_state);

void sql_logfill_metrics(int64_t *applied_recs, int64_t *applied_bytes, int64_t *finds, int64_t *nexts, int64_t *qsize,
                         int64_t *blocks);

#endif
