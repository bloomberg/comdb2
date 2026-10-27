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

/* comdb2_trace hooks for tests: pass the payload through unchanged and log
 * each stage with it, so a test can grep the db logs. */

#include <stdlib.h>
#include <string.h>

#include "comdb2_plugin.h"
#include "comdb2_initializer.h"
#include "comdb2_trace.h"
#include "tunables.h"
#include "logmsg.h"

static int gbl_tracetest = 0;

struct tracetest {
    int len;
    unsigned char payload[COMDB2_TRACE_MAXLEN];
    char hex[COMDB2_TRACE_MAXLEN * 2 + 1];
};

static void *tracetest_start(const void *payload, int len)
{
    struct tracetest *t;

    if (len <= 0 || len > COMDB2_TRACE_MAXLEN || (t = malloc(sizeof(*t))) == NULL)
        return NULL;
    t->len = len;
    memcpy(t->payload, payload, len);
    for (int i = 0; i < len; i++)
        sprintf(t->hex + 2 * i, "%02x", t->payload[i]);
    return t;
}

static int tracetest_payload(void *h, void *buf, int sz)
{
    struct tracetest *t = h;

    if (t->len > sz)
        return -1;
    memcpy(buf, t->payload, t->len);
    return t->len;
}

static void tracetest_sql_end(void *h, const struct reqlogger *logger)
{
    logmsg(LOGMSG_USER, "tracetest sql_end %s\n", ((struct tracetest *)h)->hex);
}

static void *tracetest_master_start(const void *payload, int len, const char *from_host)
{
    struct tracetest *t = tracetest_start(payload, len);

    if (t)
        logmsg(LOGMSG_USER, "tracetest master_start %s from %s\n", t->hex, from_host ? from_host : "local");
    return t;
}

static void tracetest_master_end(void *h, int localcommit_ms, int distcommit_ms, int64_t pagein, int64_t pagein_io,
                                 int rc)
{
    logmsg(LOGMSG_USER, "tracetest master_end %s rc %d\n", ((struct tracetest *)h)->hex, rc);
}

static void tracetest_rep_end(void *h, int apply_ms, int64_t pagein, int64_t pagein_io, int rc)
{
    logmsg(LOGMSG_USER, "tracetest rep_end %s rc %d\n", ((struct tracetest *)h)->hex, rc);
}

static struct comdb2_trace_hooks tracetest_hooks = {
    .sql_start = tracetest_start,
    .sql_end = tracetest_sql_end,
    .sql_osql_payload = tracetest_payload,
    .master_start = tracetest_master_start,
    .master_log_payload = tracetest_payload,
    .master_end = tracetest_master_end,
    .rep_start = tracetest_start,
    .rep_end = tracetest_rep_end,
    .release = free,
};

static int tracetest_init(void *unused)
{
    REGISTER_TUNABLE("tracetest", "Install the comdb2_trace test hooks (Default: off)", TUNABLE_BOOLEAN, &gbl_tracetest,
                     READONLY | INTERNAL | EXPERIMENTAL, NULL, NULL, NULL, NULL);
    return 0;
}

static int tracetest_destroy(void)
{
    return 0;
}

static int tracetest_postlrl(void)
{
    if (gbl_tracetest)
        gbl_trace_hooks = &tracetest_hooks;
    return 0;
}

comdb2_initializer_t tracetest_plugin = {.post_lrl = tracetest_postlrl};

#include "plugin.h"
