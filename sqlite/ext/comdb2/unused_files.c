/*
   Copyright 2025 Bloomberg Finance L.P.

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

#include <assert.h>
#include <comdb2systblInt.h>
#include <ezsystables.h>
#include <bdb_api.h>
 
typedef struct systable_unused_files {
    char *filename;
    int64_t reflog;
} systable_unused_files_t;

typedef struct getunusedfiles {
    int count;
    int alloc;
    systable_unused_files_t *records;
} getunusedfiles_t;

static int collect(void *args, int reflog, char *filename)
{
    getunusedfiles_t *a = (getunusedfiles_t *)args;
    systable_unused_files_t *u;
    a->count++;
    if (a->count >= a->alloc) {
        if (a->alloc == 0) a->alloc = 16;
        else a->alloc = a->alloc * 2;
        a->records = realloc(a->records, a->alloc * sizeof(systable_unused_files_t));
        if (!a->records) {
            logmsg(LOGMSG_FATAL, "%s: realloc failed\n", __func__);
            abort();
        }
    }
    u = &a->records[a->count - 1];
    u->filename = strdup(filename);
    if (!u->filename) {
        logmsg(LOGMSG_FATAL, "%s: strdup failed\n", __func__);
        abort();
    }
    u->reflog = reflog;
    return 0;
}

static int get_unused_files(void **data, int *records)
{
    getunusedfiles_t a = {0};
    oldfile_hash_collect(collect, &a);
    *data = a.records;
    *records = a.count;
    return 0;
}

static void free_unused_files(void *p, int n)
{
    systable_unused_files_t *a, *begin = p;
    systable_unused_files_t *end = begin + n;
    for (a = begin; a < end; ++a) {
        free(a->filename);
    }
    free(p);
}

sqlite3_module systblUnusedFilesModule = {
    .access_flag = CDB2_ALLOW_USER
};

int systblUnusedFilesInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_unused_files", &systblUnusedFilesModule,
            get_unused_files, free_unused_files, sizeof(systable_unused_files_t),
            CDB2_CSTRING, "filename", -1, offsetof(systable_unused_files_t, filename),
            CDB2_INTEGER, "reflog", -1, offsetof(systable_unused_files_t, reflog),
            SYSTABLE_END_OF_FIELDS);
}
