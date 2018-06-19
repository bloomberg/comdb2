/*
   Copyright 2015 Bloomberg Finance L.P.

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
#ifndef PER_THREAD_MALLOC

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <thread_malloc.h>
#include <dlmalloc.h>
#include <pthread.h>
#include <mem.h>

#include <logmsg.h>

static __thread comdb2ma m = NULL;
static void thread_memcreate_int(size_t sz)
{
    m = comdb2ma_create(sz, COMDB2MA_UNLIMITED, "thread_malloc",
                        COMDB2MA_MT_UNSAFE);
    if (m == NULL) {
        logmsg(LOGMSG_FATAL, "%s: comdb2ma_create failed sz:%lu bytes\n", __func__,
                sz);
        abort();
    }
}

void thread_memcreate(size_t sz)
{
    if (m) {
        logmsg(LOGMSG_ERROR, "%s already have memory allocator\n", __func__);
        return;
    }
    thread_memcreate_int(sz);
}

void thread_memcreate_notrace(size_t sz)
{
    if (m)
        return;
    thread_memcreate_int(sz);
}

void thread_memdestroy(void)
{
    if (m)
        comdb2ma_destroy(m);
    m = NULL;
}

void *thread_malloc(size_t n) { return comdb2_malloc(m, n); }

void thread_free(void *ptr) { comdb2_free(ptr); }

void *thread_calloc(size_t n_elem, size_t elem_sz)
{
    return comdb2_calloc(m, n_elem, elem_sz);
}

void *thread_realloc(void *ptr, size_t sz)
{
    return comdb2_realloc(m, ptr, sz);
}
#endif
