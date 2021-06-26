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
#include "locks_wrap.h"
#include "comdb2_atomic.h"
#include "mem_util.h"
#include "mem_override.h"
#include "logmsg.h"

#ifndef NDEBUG
#define TRACK_REFERENCES
#endif

#ifdef TRACK_REFERENCES
#include <plhash.h>

static pthread_mutex_t srh_mtx = PTHREAD_MUTEX_INITIALIZER;
static hash_t *sr_hash = NULL;
#endif

static int gbl_creation_count;

struct string_ref {
    int cnt;
    size_t len;
    char str[1];
};


/* Makes a copy of the string passed and uses that as a reference counted object
 */
struct string_ref * create_string_ref(const char *str)
{
    assert(str);
    size_t len = strlen(str);
    struct string_ref *ref = malloc(sizeof(struct string_ref) + len);
    ref->cnt = 1;
    ref->len = len;
    strcpy(ref->str, str);

#ifdef TRACK_REFERENCES
    Pthread_mutex_lock(&srh_mtx);
    if(!sr_hash)
        sr_hash = hash_init_ptr();
    hash_add(sr_hash, ref);
    gbl_creation_count += 1;
    Pthread_mutex_unlock(&srh_mtx);
#else
    ATOMIC_ADD32(gbl_creation_count, 1);
#endif
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
#ifdef TRACK_REFERENCES
        Pthread_mutex_lock(&srh_mtx);
        int rc = hash_del(sr_hash, ref);
        if (rc != 0) {
            abort();
        }
        gbl_creation_count -= 1;
        Pthread_mutex_unlock(&srh_mtx);
#else
        ATOMIC_ADD32(gbl_creation_count, -1);
#endif
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

#ifdef TRACK_REFERENCES
static int print_it(void *obj, void *arg)
{
    struct string_ref *ref = obj;
    logmsg(LOGMSG_USER, "%s:%d\n", ref->str, ref->cnt);
    return 0;
}

static void print_all_string_references()
{
    if (sr_hash) {
        Pthread_mutex_lock(&srh_mtx);
        logmsg(LOGMSG_USER, "Remaining not-cleaned-up string references:\n");
        hash_for(sr_hash, print_it, NULL);
        Pthread_mutex_unlock(&srh_mtx);
    }
}
#endif


int all_string_references_cleared()
{
    int res = (gbl_creation_count == 0);
#ifdef TRACK_REFERENCES
    int count = 0;
    if (sr_hash)
        hash_info(sr_hash, NULL, NULL, NULL, NULL, &count, NULL, NULL);
    if (!res || count != 0) {
        print_all_string_references();
        abort();
    }
    if (sr_hash) {
        hash_clear(sr_hash);
        hash_free(sr_hash);
        sr_hash = NULL;
    }
#endif
    return res;
}
