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

#ifndef INCLUDED_COMDB2_TRACE_H
#define INCLUDED_COMDB2_TRACE_H

/* An opaque per-request payload carried client -> replicant sql -> master ->
 * replicants' replication. Core only transports it; a plugin installs the
 * hooks, and every call site is a no-op while gbl_trace_hooks is NULL. Each
 * start hook returns a handle that is eventually passed to release(). */

#include <stdint.h>

#define COMDB2_TRACE_MAXLEN 64

/* op of the __db_debug record carrying the master's payload; must stay 4 bytes
 * and not read as 1, 2 or 3, which recovery and addrem interpret */
#define COMDB2_TRACE_DEBUG_OP "TRCE"

struct reqlogger;

struct comdb2_trace_hooks {
    /* replicant sql thread */
    void *(*sql_start)(const void *trace, int len);
    void (*sql_end)(void *h, const struct reqlogger *logger);
    int (*sql_osql_payload)(void *h, void *buf, int sz);

    /* master block processor */
    void *(*master_start)(const void *payload, int len, const char *from_host);
    int (*master_log_payload)(void *h, void *buf, int sz);
    /* page-ins: this txn's on the block processor, from toblock() through commit */
    void (*master_end)(void *h, int localcommit_ms, int distcommit_ms, int64_t pagein, int64_t pagein_io, int rc);

    /* replicant replication */
    void *(*rep_start)(const void *payload, int len);
    /* page-ins: every thread's that applied the txn */
    void (*rep_end)(void *h, int apply_ms, int64_t pagein, int64_t pagein_io, int rc);

    void (*release)(void *h);
};

extern struct comdb2_trace_hooks *gbl_trace_hooks;

#endif
