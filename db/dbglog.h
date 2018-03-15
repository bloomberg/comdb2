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

#define open_dbglog_file(...) NULL
#define dbglog_init_write_counters(...) 0

#define dump_client_query_stats_packed(...) do { } while (0)
#define dump_client_query_stats(...) do { } while (0)
#define append_debug_logs_from_master(...) do { } while (0)
#define dbglog_record_db_write(...) do { } while (0)
#define dbglog_dump_write_stats(...) do { } while (0)

#endif
