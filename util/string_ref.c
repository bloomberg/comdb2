/*
   Copyright 2020 Bloomberg Finance L.P.

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
#include <pthread.h>
#include "string_ref.h"
#include "sys_wrap.h"
#include "comdb2_atomic.h"
#include "mem_util.h"
#include "mem_override.h"
#include "logmsg.h"
#include "list.h"
#include "stackutil.h"

static pthread_mutex_t srl_mtx = PTHREAD_MUTEX_INITIALIZER;
int gbl_stack_string_refs = 0;

static int gbl_creation_count;

struct string_ref {
    int cnt;
    size_t len;
    const char *func;
    int line;
    LINKC_T(struct string_ref) lnk;
    int stackid;
    char str[1];
};

static int inited = 0;
LISTC_T(struct string_ref) sr_list;

/* Makes a copy of the string passed and uses that as a reference counted object
 */
struct string_ref * create_string_ref_internal(const char *str, const char *func, int line)
{
    assert(str);
    size_t len = strlen(str);
    struct string_ref *ref = malloc(sizeof(struct string_ref) + len);
    ref->cnt = 1;
    ref->len = len;
    ref->func = func;
    ref->line = line;
    ref->stackid = gbl_stack_string_refs ? stackutil_get_stack_id("strref") : -1;
    strcpy(ref->str, str);

    Pthread_mutex_lock(&srl_mtx);
    if (!inited) {
        listc_init(&sr_list, offsetof(struct string_ref, lnk));
        inited = 1;
    }
    listc_atl(&sr_list, ref);
    gbl_creation_count += 1;
    Pthread_mutex_unlock(&srl_mtx);
    return ref;
}

/* Get a reference by increasing the count */
struct string_ref * get_ref(struct string_ref *ref)
{
    assert(ref);
    int cnt = ATOMIC_ADD32(ref->cnt, 1);
    if(cnt <= 1) // create has a reference, this can only be > 1
        abort();

    return ref;
}

/* Release a reference and free if this is the last holder.
 * set *ref to NULL so it can no longer access this obj
 */
void put_ref(struct string_ref **ref_p)
{
    struct string_ref *ref = *ref_p;
    if (ref == NULL)
        return; // nothing to do

    int cnt = ATOMIC_ADD32(ref->cnt, -1);
    if (cnt < 0)
        abort();

    if (cnt == 0) {
        Pthread_mutex_lock(&srl_mtx);
        listc_rfl(&sr_list, ref);
        gbl_creation_count -= 1;
        Pthread_mutex_unlock(&srl_mtx);
        free(ref);
    }
    *ref_p = NULL;
}

/* Transfer ownership of the reference from pointer 'from' to 'to'
 * use this instead of assigning 'to = from'
 */
void transfer_ref(struct string_ref **from, struct string_ref **to)
{
    *to = *from;
    *from = NULL;
}

const char *string_ref_cstr(struct string_ref *ref)
{
    return ref->str;
}

size_t string_ref_len(struct string_ref *ref)
{
    return ref->len;
}

static int print_it(void *obj, void *arg)
{
    struct string_ref *ref = obj;
    char *stack = ref->stackid >= 0 ? stackutil_get_stack_str(ref->stackid, NULL, NULL, NULL) : NULL;
    if (stack) {
        logmsg(LOGMSG_USER, "%s:%d allocated %s:%d %s\n", ref->str, ref->cnt, ref->func, ref->line, stack);
    } else {
        logmsg(LOGMSG_USER, "%s:%d allocated %s:%d\n", ref->str, ref->cnt, ref->func, ref->line);
    }
    return 0;
}

void print_all_string_references()
{
    if (gbl_creation_count > 0) {
        assert(inited);
        struct string_ref *r;
        Pthread_mutex_lock(&srl_mtx);
        logmsg(LOGMSG_USER, "Remaining not-cleaned-up string references:\n");
        LISTC_FOR_EACH(&sr_list, r, lnk)
        {
            print_it(r, NULL);
        }
        Pthread_mutex_unlock(&srl_mtx);
    }
}

int collect_stringrefs(collect_stringrefs_t func, void *args)
{
    if (gbl_creation_count > 0) {
        assert(inited);
        struct string_ref *r;
        Pthread_mutex_lock(&srl_mtx);
        LISTC_FOR_EACH(&sr_list, r, lnk)
        {
            (*func)(args, r->str, r->func, r->line, r->cnt, r->stackid);
        }
        Pthread_mutex_unlock(&srl_mtx);
    }
    return 0;
}

int all_string_references_cleared()
{
    int res = (gbl_creation_count == 0);
    if (!res)
        print_all_string_references();
    return res;
}
