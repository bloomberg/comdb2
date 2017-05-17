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

#ifndef INCLUDED_ARENA
#define INCLUDED_ARENA

#if defined __cplusplus
extern "C" {
#endif

#include <sys/types.h>

/* arena allocator.
   NOT THREADSAFE - users must lock externally if this is
   required.
   Johan N 807x3046

*/

/* constants for align_power */
#define SYSARENA_1_BYTE_ALIGN 0
#define SYSARENA_2_BYTE_ALIGN 1
#define SYSARENA_4_BYTE_ALIGN 2
#define SYSARENA_8_BYTE_ALIGN 3

struct arena;

typedef void *(*malloc_fun)(size_t sz);
typedef void (*free_fun)(void *ptr);

typedef struct arena arena_t;

arena_t *arena_new(malloc_fun m, free_fun f, size_t initsz, size_t increment);

/* set allocation alignment for this arena as a power of 2 e.g. an align_power
 * of 3 is 2^3 = 8 byte alignment.  Note that 3 is the maximum supported
 * align_power. */
void arena_set_alignment(arena_t *arena, size_t align_power);

/* allocate from arena.  the returned memory will be aligned according to the
 * arena's default alignment settings, or you can override the alignment. */
void *arena_alloc(arena_t *arena, size_t sz);
void *arena_alloc_align(arena_t *arena, size_t sz, size_t align_power);

/* duplicate a C string.  the returned pointer will be byte aligned. */
char *arena_strdup(arena_t *arena, const char *s);

/* free all user memory. The arena can be reused after
   this  operation.  */
void arena_free_all(arena_t *arena);

/* destroy the arena completely, freeing everything including
   infrastructure memory. */
void arena_destroy(arena_t *arena);

/* obtain  statistics. Input: arena.
   Output: all other arguments

num_allocs: Total number of allocations
num_chunks: Number of memory chunks (linked list)
max_allocsz, avg_allocsz: self explanatory
wasted: number of wasted  bytes
allocated: number of allocated bytes
initsz: size of initial chunk
increment: size of subsequent  chunks

*/

void arena_get_stats(const arena_t *arena, int *num_allocs, int *num_chunks,
                     int *max_allocsz, int *avg_allocsz, int *wasted,
                     int *allocated, int *initsz, int *increment);

#if defined __cplusplus
}
#endif

#endif
