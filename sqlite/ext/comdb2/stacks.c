/*
   Copyright 2023 Bloomberg Finance L.P.

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
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "types.h"
#include "stackutil.h"

static sqlite3_module systblStacksModule = {
    .access_flag = CDB2_ALLOW_USER,
};

struct stack {
    int64_t id;
    int64_t frame;
    int64_t hits;
    char *code;
};

static int get_stacks(void **data, int *num_points) {
    struct stack *s;
    char *code[MAXFRAMES];
    int maxid = stackutil_get_num_stacks();
    int64_t hits;
    int nitems = 0;

    // overallocate at first, we'll free what we don't need in a second
    s = malloc(sizeof(struct stack) * MAXFRAMES * maxid);
    for (int id = 0; id < maxid; id++) {
        int nframes = stackutil_get_stack_description(id, code, &hits);
        if (nframes <= 0)
            continue;
        for (int frame = 0; frame < nframes; frame++) {
            s[nitems].id = id;
            s[nitems].frame = frame;
            s[nitems].hits = hits; // ew
            s[nitems].code = code[frame];
            nitems++;
        }
    }
    s = realloc(s, sizeof(struct stack) * nitems);
    *data = s;
    *num_points = nitems;
    return 0;
}

static void free_stacks(void *data, int num_points) {
    // "code" strings are owned by stackutil and are permanent - we don't free them
    free(data);
}

int systblStacks(sqlite3 *db) {
    return create_system_table(db, "comdb2_stacks",
            &systblStacksModule, get_stacks, free_stacks, sizeof(struct stack),
            CDB2_INTEGER, "id", -1, offsetof(struct stack, id),
            CDB2_INTEGER, "frame", -1, offsetof(struct stack, frame),
            CDB2_INTEGER, "hits", -1, offsetof(struct stack, hits),
            CDB2_CSTRING, "code", -1, offsetof(struct stack, code),
            SYSTABLE_END_OF_FIELDS);
}
