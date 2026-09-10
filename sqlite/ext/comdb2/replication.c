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
#include <string.h>
#include <stddef.h>
#include "comdb2.h"
#include "bdb_int.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "cdb2api.h"
#include "tohex.h"

/* One row per replication thread currently applying. The client comes from the
 * txn's DB_llog_clientinfo record, the fingerprint from whichever statement
 * this thread is on -- so sibling threads on one txn differ in fingerprint. */
typedef struct systable_replication {
    int64_t threadid;
    char *taskname;
    char *host;
    int64_t pid;
    int pid_isnull;
    char *fingerprint;
    int64_t commit_lsn_file;
    int64_t commit_lsn_offset;
} systable_replication_t;

typedef struct getreplication {
    int count;
    int alloc;
    systable_replication_t *records;
} getreplication_t;

static void collect(void *args, uint64_t tid, const char *taskname, const char *host, int pid,
                    const uint8_t *fingerprint, uint32_t lsn_file, uint32_t lsn_offset)
{
    getreplication_t *r = (getreplication_t *)args;
    systable_replication_t *t;

    r->count++;
    if (r->count >= r->alloc) {
        r->alloc = r->alloc ? r->alloc * 2 : 16;
        r->records = realloc(r->records, r->alloc * sizeof(systable_replication_t));
    }

    t = &r->records[r->count - 1];
    memset(t, 0, sizeof(*t));

    t->threadid = tid;
    t->taskname = taskname ? strdup(taskname) : NULL;
    t->host = host ? strdup(host) : NULL;
    if (pid)
        t->pid = pid;
    else
        t->pid_isnull = 1;

    if (fingerprint) {
        char hex[BDB_FINGERPRINTSZ * 2 + 1];
        util_tohex(hex, (const char *)fingerprint, BDB_FINGERPRINTSZ);
        t->fingerprint = strdup(hex);
    }

    t->commit_lsn_file = lsn_file;
    t->commit_lsn_offset = lsn_offset;
}

static int get_replication(void **data, int *records)
{
    getreplication_t r = {0};
    bdb_replication_foreach(collect, &r);
    *data = r.records;
    *records = r.count;
    return 0;
}

static void free_replication(void *p, int n)
{
    systable_replication_t *begin = p, *end = begin + n, *t;

    for (t = begin; t < end; ++t) {
        free(t->taskname);
        free(t->host);
        free(t->fingerprint);
    }
    free(p);
}

sqlite3_module systblReplicationModule = {
    .access_flag = CDB2_ALLOW_USER | CDB2_STRICT,
};

int systblReplicationInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_replication", &systblReplicationModule, get_replication, free_replication,
        sizeof(systable_replication_t),
        CDB2_INTEGER, "threadid", -1, offsetof(systable_replication_t, threadid),
        CDB2_CSTRING, "taskname", -1, offsetof(systable_replication_t, taskname),
        CDB2_CSTRING, "host", -1, offsetof(systable_replication_t, host),
        CDB2_INTEGER, "pid", offsetof(systable_replication_t, pid_isnull), offsetof(systable_replication_t, pid),
        CDB2_CSTRING, "fingerprint", -1, offsetof(systable_replication_t, fingerprint),
        CDB2_INTEGER, "commit_lsn_file", -1, offsetof(systable_replication_t, commit_lsn_file),
        CDB2_INTEGER, "commit_lsn_offset", -1, offsetof(systable_replication_t, commit_lsn_offset),
        SYSTABLE_END_OF_FIELDS);
}
