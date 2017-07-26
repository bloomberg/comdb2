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

#ifndef __SOCKET_INTERFACES_H__
#define __SOCKET_INTERFACES_H__

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <pthread.h>

#include <plhash.h>
#include <segstr.h>

#include <list.h>

#include <sbuf2.h>
#include <bdb_api.h>

#include <compile_time_assert.h>

#include "comdb2.h"
#include "types.h"
#include "tag.h"

#include <dynschematypes.h>
#include <dynschemaload.h>

enum sockreq_types {
    SOCKREQ_BLKLONGBEGIN = 100,
    SOCKREQ_SETTIMEOUT = 101,
    SOCKREQ_HBEAT = 102
};

typedef struct sockrsp {
    int response; /* enum sock_response */
    int flags;    /* response flags */
    int rcode;
    int parm;      /* extra word of info differs per request type */
    int followlen; /* how much data follows header*/
} sockrsp_t;

enum { SOCKRSP_LEN = 4 + 4 + 4 + 4 + 4 };

BB_COMPILE_TIME_ASSERT(sockrsp_len, sizeof(sockrsp_t) == SOCKRSP_LEN);

typedef struct sockreq {
    int request;     /* enum sockreq_types */
    int flags;       /* any flags */
    int parm;        /* parameter? */
    int fromcpu;     /* from cpu */
    int frompid;     /* from pid */
    int fromtask[2]; /* from task */
    int followlen;   /* length following */
} sockreq_t;

enum { SOCKREQ_LEN = 4 + 4 + 4 + 4 + 4 + (2 * 4) + 4 };

BB_COMPILE_TIME_ASSERT(sockreq_len, sizeof(sockreq_t) == SOCKREQ_LEN);

int handle_socket_txbuf(struct thr_handle *thr_self, SBUF2 *sb, struct dbtable *db,
                        int *keepsock);
/*int reterr_socket(struct thd * thd, struct ireq* iq, int rc);*/
int sndbak_socket(SBUF2 *sb, u_char *buf, int buflen, int rc);
int sndbak_open_socket(SBUF2 *sb, u_char *buf, int buflen, int rc);

int handle_socketrequest(SBUF2 *sb, int *keepsocket, int wrongdb);

#endif /* #ifndef __SOCKET_INTERFACES_H__ */
