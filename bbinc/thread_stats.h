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

#ifndef _THREAD_STATS_H_
#define _THREAD_STATS_H_

#include <stdint.h>

struct berkdb_thread_stats {
    uint64_t n_locks;
    unsigned n_lock_waits;
    uint64_t lock_wait_time_us;
    uint64_t worst_lock_wait_time_us;

    unsigned n_preads;
    unsigned pread_bytes;
    uint64_t pread_time_us;

    unsigned n_pwrites;
    unsigned pwrite_bytes;
    uint64_t pwrite_time_us;

    unsigned n_memp_fgets;
    uint64_t memp_fget_time_us;

    /* number of times data had to be paged into / out of the buffer pool */
    unsigned n_memp_pgs;
    uint64_t memp_pg_time_us;

    unsigned n_shallocs;
    uint64_t shalloc_time_us;

    unsigned n_shalloc_frees;
    uint64_t shalloc_free_time_us;

    uint64_t rep_lock_time_us;
    uint64_t rep_deadlock_retries;

    uint64_t rep_log_cnt;
    uint64_t rep_log_bytes;

    uint64_t rep_collect_time_us;
    uint64_t rep_exec_time_us;
};

#endif
