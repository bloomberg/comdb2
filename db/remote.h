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

#ifndef INCLUDED_REMOTE_H
#define INCLUDED_REMOTE_H

#include "comdb2.h"

#define COMDB2_SO_ENTRY "csp_comdb2_lcl"
/* for comdbg csp */
enum comdb2_rqs {
    CDB2_OPINIT = 0,
    CDB2_OPFIND = 1,
    CDB2_OPNEXT = 2,
    CDB2_OPPREV = 3,
    CDB2_OPFRRN = 4,
    CDB2_OPLDUP = 5
};

/* comdb2_api interface for finds. New opcode for ix_find_last_dup_rnum .*/
union find {
    struct find_req {
        short prccom[3];
        short opcode; /* comdb2: specific req,  comdbg: OP_STORED */
        int opcode2;  /* ureq in comdbg (one of comdb2_rqs) */
        int internal[12];
        int ixnum;
        int keylen;
        char table[MAXTABLELEN + 1];
        char dta[1]; /* key */
    } req;

    struct find_rsp {
        short prccom[4];
        int keylen;
        int dtalen;
        int cksum;
        int rrn;
        unsigned long long genid;
        char dta[1]; /* fndkey + fnddta */
    } rsp;
};

union findrrn {
    struct findrrn_req {
        short prccom[3];
        short opcode;
        int opcode2;
        int internal[12];
        char table[MAXTABLELEN + 1];
        int rrn;
        unsigned long long genid;
    } req;

    struct findrrn_rsp {
        short prccom[4];
        int cksum;
        int dtalen;
        char dta[1]; /* fnddta */
    } rsp;
};

union findnext {
    struct findnext_req {
        short prccom[3];
        short opcode;
        int opcode2;
        int internal[12];
        char table[MAXTABLELEN + 1];
        int ixnum;
        int keylen;
        int lastrrn;
        unsigned long long lastgenid;
        char dta[1]; /* key + lastkey */
    } req;

    struct findnext_rsp {
        short prccom[4];
        int foundrrn;
        unsigned long long genid;
        int cksum;
        int keylen;
        int dtalen;
        char dta[1]; /* key + data */
    } rsp;
};

int ix_next_remote(struct ireq *iq, int ixnum, void *key, int keylen,
                   void *last, int lastrrn, unsigned long long lastgenid,
                   void *fndkey, int *fndrrn, unsigned long long *genid,
                   void *fnddta, int *fndlen, int maxlen);

int ix_prev_remote(struct ireq *iq, int ixnum, void *key, int keylen,
                   void *last, int lastrrn, unsigned long long lastgenid,
                   void *fndkey, int *fndrrn, unsigned long long *genid,
                   void *fnddta, int *fndlen, int maxlen);

int ix_find_last_dup_rnum_remote(struct ireq *iq, int ixnum, void *key,
                                 int keylen, void *fndkey, int *fndrrn,
                                 unsigned long long *genid, void *fnddta,
                                 int *fndlen, int *recnum, int maxlen);

int ix_find_remote(struct ireq *iq, int ixnum, void *key, int keylen,
                   void *fndkey, int *fndrrn, unsigned long long *genid,
                   void *fnddta, int *fndlen, int maxlen);

int ix_find_by_rrn_and_genid_remote(struct ireq *iq, int rrn,
                                    unsigned long long genid, void *fnddta,
                                    int *fndlen, int maxlen);

#endif
