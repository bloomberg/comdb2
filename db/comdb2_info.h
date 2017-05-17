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

#ifndef INCLUDED_COMDB2_INFO_H
#define INCLUDED_COMDB2_INFO_H

enum comdb2_state {
    COMDB2_DOWN = 0,             /* shouldn't happen */
    COMDB2_STARTING = 1,         /* just started, not yet in election */
    COMDB2_ELECTION_STARTUP = 2, /* initial election */
    COMDB2_CATCHUP = 3,          /* catching up to master */
    COMDB2_READY = 4,            /* ready and accepting requests */
    COMDB2_ELECTION = 5          /* master down/electing */
};

struct comdb2info_hdr {
    int sz;
    double pad;
};

typedef unsigned long long lsn_type;

/* NOTE: if changing this structure, never delete anything, and always add new
 * members to the end */
struct comdb2info {
    int sz;
    int state;
    char dbname[8];
    int dbnum;
    int nconnected;
    int nreqs;
    int nsql;
    int nreads;
    int nwrites;
    int master;
    int reqrate;
    lsn_type lsn;
    lsn_type master_lsn;

    /* These are extras, check that they are within sz before writing to
     * them */
    int sc_running;
    int cachekb;
    int retries;
    short cachehitrate;
    long long nread_ios;
    long long nwrite_ios;
    unsigned long long sqlticks;
    long long nfsyncs;

    /* long long versions of some of the int fields above - they overflow for
     * some databases */
    long long nreqs_l;
    long long nsql_l;
    long long nreads_l;
    long long nwrites_l;
    long long retries_l;
};

struct comdb2info *get_my_comdb2_info(void);
struct comdb2info *attach_comdb2_info_area(int create);

#define COMDB2_INFO_HAS_FIELD(ptr_info, field)                                 \
    ((ptr_info) &&                                                             \
     offsetof(struct comdb2info, field) + sizeof((ptr_info)->field) <=         \
         (ptr_info)->sz)

#endif

void comdb2info_clear_my_area(void);
