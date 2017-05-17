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

/* stored procedure compatibility */
#include "comdb2.h"
#include "logmsg.h"

struct debug_req {
    struct req_hdr hdr;
    int opcode;
    union {
        struct {
            int rrn;
            int attr;
            char data[MAX_METADATA_RECORD_LEN];
        } metadb_put_req;
    } req;
};

int todebug(struct ireq *iq)
{
    struct debug_req *req;

    req = (struct debug_req *)iq->rq;

    switch (req->opcode) {
    default:
        logmsg(LOGMSG_ERROR, "todebug: invalid opcode %d\n", req->opcode);
    }
    return 0;
}
