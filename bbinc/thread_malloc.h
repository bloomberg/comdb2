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

#ifndef INCLUDE_THREAD_MALLOC_H
#define INCLUDE_THREAD_MALLOC_H

#ifdef PER_THREAD_MALLOC

#define thread_memcreate(x)
#define thread_memcreate_notrace(x)
#define thread_memdestroy()
#define thread_malloc(x) malloc(x)
#define thread_free(x) free(x)
#define thread_calloc(x, y) calloc(x, y)
#define thread_realloc(x, y) realloc(x, y)

#else

void thread_memcreate(size_t);
void thread_memcreate_notrace(size_t);
void thread_memdestroy(void);
void *thread_malloc(size_t);
void thread_free(void *);
void *thread_calloc(size_t n_elem, size_t elem_sz);
void *thread_realloc(void *, size_t);

#endif // PER_THREAD_MALLOC

#endif
