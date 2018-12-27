/*
   Copyright 2015, 2018 Bloomberg Finance L.P.

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

#ifndef INCLUDED_PHYSWRITE_H
#define INCLUDED_PHYSWRITE_H
#include <pthread.h>

typedef struct physwrite_results_s {
    pthread_mutex_t lk;
    pthread_cond_t cd;
    int dispatched;
    int done;
    int commit_file;
    int commit_offset;
    int rcode;
    int errval;
    char *errstr;
    int inserts;
    int updates;
    int deletes;
    int cupdates;
    int cdeletes;
} physwrite_results_t;

int physwrite_route_packet(int usertype, void *data, int datalen,
        uint32_t flags);

int physwrite_route_packet_tails(int usertype, void *data, int datalen,
        int ntails, void *tail, int tailen);

void physwrite_init(char *name, char *type, char *host);

int physwrite_exec(char *host, int usertype, void *data, int datalen,
        int *rcode, int *errval, char **errstr, int *inserts, int *updates,
        int *deletes, int *cupdates, int *cdeletes, int *commit_file,
        int *commit_offset, uint32_t flags);

#endif
