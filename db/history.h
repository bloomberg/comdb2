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

#ifndef __HISTORY_H
#define __HISTORY_H

#include <pthread.h>
#include <pool.h>

/* bigsnd/bigrcv history module */

typedef struct {
    int rq[3];
    int rsp[3];
    char *fullrq;
    char *fullrsp;
    int fromnode;
    int frompid;
    int master;
    int opcode;
    int rcode;
} history_request;

typedef struct {
    int size;
    int head, tail;
    int total;
    history_request **hist;
    pool_t *pool;
    pool_t *bufs;
    pthread_mutex_t lock;
    int wholereq;
} history;

/* history.c */
int init_history(history *h, size_t size);
history_request *hist_get_event(history *h);
void hist_add_event(history *h, history_request *req);
void hist_dump(history *h, unsigned int skip, unsigned int count);
void search_history(history *list, char *msg);
int hist_is_empty(history *hist);
int hist_num_entries(history *hist);
void add_hit_to_history(int dbnum, int node);
void disable_hist_hits_updates(void);
void enable_hist_hits_updates(void);
int is_hits_hist_enabled(void);
void hist_print_event(history *h, history_request *req);
#endif
