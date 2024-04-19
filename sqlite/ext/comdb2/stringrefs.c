/*
   Copyright 2024 Bloomberg Finance L.P.

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
#include "string_ref.h"
#include "cdb2api.h"
#include "str0.h"
#include <stackutil.h>

typedef struct systable_stringrefs {
    char *string;
    const char *func;
    int64_t line;
    int64_t refcnt;
    int64_t stackid;
    char *stack;
} systable_stringrefs_t;

typedef struct getstringrefs {
    int count;
    int alloc;
    systable_stringrefs_t *records;
} getstringrefs_t;

static int collect(void *args, char *string, const char *func, int line, int refcnt, int stackid)
{
    getstringrefs_t *g = (getstringrefs_t *)args;
    g->count++;
    if (g->count >= g->alloc) {
        if (g->alloc == 0) g->alloc = 16;
        else g->alloc = g->alloc * 2;
        g->records = realloc(g->records, g->alloc * sizeof(systable_stringrefs_t));
    }
    systable_stringrefs_t *s = &g->records[g->count - 1];
    s->string = strdup(string);
    s->func = func;
    s->line = line;
    s->refcnt = refcnt;
    s->stackid = stackid;
    if ((s->stack = stackutil_get_stack_str(stackid, NULL, NULL, NULL)) == NULL) {
        s->stack = strdup("(no-stack)");
    }
    return 0;
}

static int get_stringrefs(void **data, int *records)
{
    getstringrefs_t s = {0};
    collect_stringrefs(collect, &s);
    *data = s.records;
    *records = s.count;
    return 0;
}

static void free_stringrefs(void *p, int n)
{
    systable_stringrefs_t *s, *begin = p;
    systable_stringrefs_t *end = begin + n;
    for (s = begin; s < end; ++s) {
        free(s->string);
        free(s->stack);
    }
    free(p);
}

sqlite3_module systblStringRefsModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblStringRefsInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_stringrefs", &systblStringRefsModule,
            get_stringrefs, free_stringrefs, sizeof(systable_stringrefs_t),
            CDB2_CSTRING, "string", -1, offsetof(systable_stringrefs_t, string),
            CDB2_CSTRING, "func", -1, offsetof(systable_stringrefs_t, func),
            CDB2_INTEGER, "line", -1, offsetof(systable_stringrefs_t, line),
            CDB2_INTEGER, "refcnt", -1, offsetof(systable_stringrefs_t, refcnt),
            CDB2_CSTRING, "stack", -1, offsetof(systable_stringrefs_t, stack),
            SYSTABLE_END_OF_FIELDS);
}
