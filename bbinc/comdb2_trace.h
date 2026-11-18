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

/*
 * An opaque per-request payload carried from client to replicant sql.
 * If a write schedule is generated for the client transaction, the payload
 * is sent to master and is replicated to replicants. Comdb2's core only
 * transports it. A plugin implements and installs the hooks.
 * Each start hook returns a handle that is eventually passed to release().
 *
 * How to add a metric to a span:
 * - sql: no code changes in the plugin. Simply add a new metric to `struct reqlogger'.
 *   the plugin tags it in sql_end().
 * - master/replication: Changes are required in the plugin to tag a new metric.
 *   A new parameter should also be added to master_end()/rep_end().
 */

#include <stdint.h>

#define COMDB2_TRACE_MAXLEN 64

struct reqlogger;

struct comdb2_trace_hooks {
    /* replicant sql thread */
    void *(*sql_start)(const void *trace, int len);
    void (*sql_end)(void *h, const struct reqlogger *logger);
    /* copy out the opaque client's trace-id payload to buf,
       which will be sent to master in the OSQL stream */
    int (*sql_osql_trace)(void *h, void *buf, int sz);

    /* master block processor */
    void *(*master_start)(const void *payload, int len, const char *from_host);
    void (*master_end)(int rc, void *h, int localcommit_ms, int distcommit_ms, int64_t pagein, int64_t pagein_io);
    /* copy out the opaque OSQL's trace-id payload to buf,
       which will be written to WAL and replcated to replicants */
    int (*wal_trace)(void *h, void *buf, int sz);

    /* replicant replication */
    void *(*rep_start)(const void *payload, int len);
    void (*rep_end)(int rc, void *h, int apply_ms, int64_t pagein, int64_t pagein_io);

    void (*release)(void *h);
};

extern struct comdb2_trace_hooks *gbl_trace_hooks;

#endif
