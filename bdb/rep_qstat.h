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

#ifndef __rep_qstat_h
#define __rep_qstat_h
#include <net.h>

typedef struct net_queue_stat {
    char *nettype;
    char *hostname;

    pthread_mutex_t lock;

    /* Keep track of the minimum and maximum lsn */
    DB_LSN min_lsn;
    DB_LSN max_lsn;

    /* Keep track of how many of each type of record */
    int max_type;
    int *type_counts;

    /* Other counts */
    int64_t unknown_count;
    int64_t total_count;
} net_queue_stat_t;

void net_rep_qstat_init(netinfo_type *netinfo_ptr);
#endif
