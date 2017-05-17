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

#ifndef __COMDB2_TRN_INTRL_H__
#define __COMDB2_TRN_INTRL_H__

enum COMDB2_TRN_CNST {
    CDB2_TRN_MAXPURGE = 256,
};

typedef struct longblk_trn {
    unsigned long long tranid;
    void *trn_data;
    int blocking;
    int timestamp;
    int datasz;
    int numsegs;
    int expseg;
    int numreqs;
    struct longblk_trn *mylistlink;
    int first_scsmsk_offset;
} longblk_trans_type;

struct long_trn_stat {
    int ltrn_fulltrans;
    int ltrn_npurges;
    int ltrn_npurged;
    double ltrn_avgpurge;
    int ltrn_maxnseg;
    int ltrn_avgnseg;
    int ltrn_minnseg;
    int ltrn_maxsize;
    int ltrn_avgsize;
    int ltrn_minsize;
    int ltrn_maxnreq;
    int ltrn_avgnreq;
    int ltrn_minnreq;
};

struct purge_trn_type {
    longblk_trans_type *purgearr[CDB2_TRN_MAXPURGE];
    int count;
};

extern int longblk_trans_purge_interval;

#endif
