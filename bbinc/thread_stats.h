/*
 */

#ifndef _THREAD_STATS_H_
#define _THREAD_STATS_H_

#include <stdint.h>

struct berkdb_thread_stats {
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

    unsigned n_memp_pgs;
    uint64_t memp_pg_time_us;

    unsigned n_shallocs;
    uint64_t shalloc_time_us;

    unsigned n_shalloc_frees;
    uint64_t shalloc_free_time_us;
};

#endif
