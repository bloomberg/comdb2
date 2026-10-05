/*
   Copyright 2026 Bloomberg Finance L.P.

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

#include <stdlib.h>

#include <build/db.h>

#include "bdb_int.h"
#include "comdb2_trace.h"
#include "epochlib.h"

struct comdb2_trace_hooks *gbl_trace_hooks = NULL;

int bdb_debug_log_trace(bdb_state_type *bdb_state, tran_type *tran, const void *payload, int len)
{
    DBT op = {0}, key = {0}, data = {0};

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    op.data = COMDB2_TRACE_DEBUG_OP;
    op.size = 4;
    key.data = "trace";
    key.size = sizeof("trace");
    data.data = (void *)payload;
    data.size = len;
    return bdb_state->dbenv->debug_log(bdb_state->dbenv, tran->tid, &op, &key, &data);
}

struct trace_rep {
    void *h;
    int startms;
};

void *bdb_trace_rep_start(const void *payload, int len)
{
    struct comdb2_trace_hooks *hooks = gbl_trace_hooks;
    struct trace_rep *r;
    void *h;

    if (hooks == NULL || (h = hooks->rep_start(payload, len)) == NULL)
        return NULL;
    if ((r = malloc(sizeof(*r))) == NULL) {
        hooks->release(h);
        return NULL;
    }
    r->h = h;
    r->startms = comdb2_time_epochms();
    return r;
}

void bdb_trace_rep_free(void *p)
{
    struct trace_rep *r = p;

    if (r == NULL)
        return;
    gbl_trace_hooks->release(r->h);
    free(r);
}

void bdb_trace_rep_done(void *p, int rc, uint64_t pagein, uint64_t pagein_io)
{
    struct trace_rep *r = p;

    if (r == NULL)
        return;
    gbl_trace_hooks->rep_end(r->h, comdb2_time_epochms() - r->startms, pagein, pagein_io, rc);
    bdb_trace_rep_free(r);
}
