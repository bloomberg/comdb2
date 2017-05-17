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

#include <arena.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include <logmsg.h>

/* all-purpose arena allocator. */

typedef struct chunk {
    /* base of memory allocated in this chunk. */
    char *mem;
    /* size of memory  allocated */
    size_t memsz;
    /* where to place the next object */
    char *bump;
    struct chunk *next;

} chunk_t;

struct arena {

    /* default allocation alignment */
    size_t align_power;

    /* size of increment if we run out of memory */
    size_t increment;

    malloc_fun malloc_f;
    free_fun free_f;

    /* point to the latest alloced chunk */
    struct chunk *current;

    struct chunk first;

    /* stats */
    int num_allocs;
    int num_chunks;
    int max_alloc_sz;
    int waste;
    int alloc;
};

static void arena_reset_stats(arena_t *a);

static void arena_reset_stats(arena_t *a)
{
    a->num_allocs = 0;
    a->num_chunks = 1;
    a->max_alloc_sz = a->waste = a->alloc = 0;
}

arena_t *arena_new(malloc_fun m, free_fun f, size_t initsz, size_t increment)
{
    arena_t *r;

    if (!m || !f) {
        logmsg(LOGMSG_ERROR, "%s: invalid arguments\n", __func__);
        return NULL;
    }

    r = m(sizeof(arena_t));
    if (!r) {
        logmsg(LOGMSG_ERROR, "%s: could not allocate arena\n", __func__);
        return NULL;
    }

    r->malloc_f = m;
    r->free_f = f;

    r->first.mem = m(initsz);
    if (!r->first.mem) {
        logmsg(LOGMSG_ERROR, "%s: could not allocate  initial memory\n", __func__);
        f(r);
        return NULL;
    }

    r->align_power = 3; /* 2^3 = 8 bytes */
    r->first.memsz = initsz;
    r->increment = increment;
    r->first.bump = r->first.mem;
    r->first.next = NULL;
    r->current = &r->first;

    arena_reset_stats(r);

    /*  printf("Created new arena: sz %d incr %d\n", initsz, increment);*/
    return r;
}

void arena_set_alignment(arena_t *arena, size_t align_power)
{
    if (arena) {
        arena->align_power = align_power;
    } else {
        logmsg(LOGMSG_ERROR, "%s:arena=NULL!\n", __func__);
    }
}

void *arena_alloc(arena_t *a, size_t sz)
{
    return arena_alloc_align(a, sz, a->align_power);
}

void *arena_alloc_align(arena_t *a, size_t sz, size_t align_power)
{
    char *r;
    chunk_t *curr;
    size_t freemem;
    size_t align, align_mask;

    if (!a) {
        logmsg(LOGMSG_ERROR, "%s: invalid arguments\n", __func__);
        return NULL;
    }
    if (align_power > 3) {
        logmsg(LOGMSG_ERROR, "%s: bad align_power %u\n", __func__,
                (unsigned)align_power);
    }
    align = 1 << align_power;
    align_mask = align - 1;
    curr = a->current;
    freemem = curr->memsz - (curr->bump - curr->mem);

    /* first make sure our bump is correctly aligned for this allocation */
    if (align_mask) {
        uintptr_t ibump = (uintptr_t)curr->bump;
        uintptr_t ibump_misalign = ibump & align_mask;
        if (ibump_misalign) {
            ibump_misalign = align - ibump_misalign;
            if (ibump_misalign > freemem) {
                ibump_misalign = freemem;
            }
            a->waste += ibump_misalign;
            curr->bump += ibump_misalign;
            freemem -= ibump_misalign;
        }
    }

    /* then ensure we have enough space */
    if (freemem < sz) {
        char *tmp;
        size_t incr;
        a->waste += freemem;

        incr = a->increment;
        if (incr < sz) {
            incr = sz;
        }
        incr = (incr + align_mask) & ~align_mask;

        tmp = a->malloc_f(incr);
        if (!tmp) {
            logmsg(LOGMSG_ERROR, "%s: memory allocation failed\n", __func__);
            return NULL;
        }

        curr->next = a->malloc_f(sizeof(chunk_t));
        if (!curr->next) {
            logmsg(LOGMSG_ERROR, "%s: memory allocation failed\n", __func__);
            a->free_f(tmp);
            return NULL;
        }

        a->current = curr = curr->next;
        curr->memsz = incr;
        curr->bump = tmp;
        curr->mem = tmp;
        curr->next = NULL;

        a->num_chunks++;

        /* we will assume that the newly allocated memory is aligned correctly
         * for alignments of 1, 2, 4 or 8 bytes (which is all we cater for) */
    }

    /* allocate. */
    r = curr->bump;
    curr->bump += sz;

    /* sanity check */
    if (curr->bump > curr->mem + curr->memsz) {
        logmsg(LOGMSG_ERROR, "%s: INTERNAL CORRUPTION - top %p bump %p\n", __func__,
                curr->mem + curr->memsz, curr->bump);
        curr->bump = curr->mem + curr->memsz;
    }

    a->num_allocs++;
    if (sz > a->max_alloc_sz)
        a->max_alloc_sz = sz;
    a->alloc += sz;

    return r;
}

char *arena_strdup(arena_t *a, const char *s)
{
    if (!a || !s) {
        logmsg(LOGMSG_ERROR, "%s: bad arguments %p %p\n", __func__, a, s);
        return NULL;
    } else {
        size_t len;
        chunk_t *curr = a->current;
        size_t freemem = curr->memsz - (curr->bump - curr->mem);
        char *p;

        /* if our current chunk has any space at all left in it, try to copy the
         * string into it.  if we hit the end of the free space before we hit
         * the
         * end of the string then we revert back to regular arena allocate and
         * copy.  usually this will be fast, occassionally this will be slow. */
        if (freemem > 0) {
            size_t pos;
            for (pos = 0; pos < freemem; pos++) {
                if (!(curr->bump[pos] = s[pos])) {
                    /* we hit the null terminator - success! */
                    p = curr->bump;
                    pos++;
                    curr->bump += pos;
                    a->num_allocs++;
                    a->alloc += pos;
                    if (pos > a->max_alloc_sz)
                        a->max_alloc_sz = pos;
                    return p;
                }
            }
            /* didn't fit.  continue scan to get string length */
            len = pos + strlen(s + pos);
        } else {
            len = strlen(s);
        }

        len += 1;
        p = arena_alloc_align(a, len, 1);
        if (p) {
            memcpy(p, s, len);
        }
        return p;
    }
}

void arena_free_all(arena_t *arena)
{
    chunk_t *c;

    if (!arena) {
        logmsg(LOGMSG_ERROR, "%s: invalid arguments\n", __func__);
        return;
    }

    c = arena->first.next;
    while (c) {
        chunk_t *tmp = c;
        arena->free_f(c->mem);
        c = c->next;
        arena->free_f(tmp);
    }

    arena->first.bump = arena->first.mem;
    arena->current = &arena->first;
    arena->first.next = NULL;

    arena_reset_stats(arena);

    /*  printf("Cleared arena\n");*/
}

void arena_destroy(arena_t *arena)
{
    free_fun f;
    if (!arena) {
        logmsg(LOGMSG_ERROR, "%s: invalid arguments\n", __func__);
        return;
    }
    arena_free_all(arena);

    f = arena->free_f;
    f(arena->first.mem);
    f(arena);
    /*  printf("Destroyed arena\n");*/
}

void arena_get_stats(const arena_t *arena, int *num_allocs, int *num_chunks,
                     int *max_allocsz, int *avg_allocsz, int *wasted,
                     int *allocated, int *initsz, int *increment)
{

    if (!arena) {
        logmsg(LOGMSG_ERROR, "%s: invalid arguments\n", __func__);
        return;
    }

    *num_allocs = arena->num_allocs;
    *num_chunks = arena->num_chunks;
    *max_allocsz = arena->max_alloc_sz;
    *avg_allocsz = arena->num_allocs > 0 ? arena->alloc / arena->num_allocs : 0;
    *wasted = arena->waste;
    *allocated = arena->alloc;

    *initsz = arena->first.memsz;
    *increment = arena->increment;
}

#ifdef TEST_PROG

static void chkptralign(const void *p, uintptr_t align)
{
    uintptr_t pi = (uintptr_t)p;
    if (pi & (align - 1)) {
        fprintf(stderr, "ptr %p is not %u byte aligned\n", p, (unsigned)align);
    }
}

static void arena_print_stats(FILE *fh, const arena_t *arena)
{
    int num_allocs, num_chunks, max_allocsz, avg_allocsz, wasted;
    int allocated, initsz, increment;
    arena_get_stats(arena, &num_allocs, &num_chunks, &max_allocsz, &avg_allocsz,
                    &wasted, &allocated, &initsz, &increment);
    fprintf(fh, "num allocs    %d\n", num_allocs);
    fprintf(fh, "num chunks    %d\n", num_chunks);
    fprintf(fh, "max alloc sz  %d\n", max_allocsz);
    fprintf(fh, "avg alloc sz  %d\n", avg_allocsz);
    fprintf(fh, "bytes wasted  %d\n", wasted);
    fprintf(fh, "bytes alloced %d\n", allocated);
    fprintf(fh, "initial size  %d\n", initsz);
    fprintf(fh, "chunk size    %d\n", increment);
}

int main(int argc, char **argv)
{

    int i, j;
    arena_t *arena;

    arena = arena_new(malloc, free, 1024, 1024);

    for (i = 0; i < 1000; ++i) {
        char *tmp = arena_alloc(arena, 30027);
        chkptralign(tmp, sizeof(double));
    }
    printf("----\n");
    arena_print_stats(stdout, arena);
    arena_free_all(arena);
    for (i = 0; i < 1000; ++i) {
        char *tmp = arena_alloc(arena, 30013);
        chkptralign(tmp, sizeof(double));
    }
    printf("----\n");
    arena_print_stats(stdout, arena);
    arena_free_all(arena);
    for (i = 0; i < 1000; ++i) {
        char *tmp = arena_alloc(arena, 30013);
        chkptralign(tmp, sizeof(double));
    }
    printf("----\n");
    arena_print_stats(stdout, arena);
    arena_free_all(arena);

    arena_destroy(arena);

    arena = arena_new(malloc, free, 64, 128);
    for (i = 1; i < 1000; i++) {
        char *tmp;
        for (j = 0; j < 10; j++) {
            tmp = arena_alloc_align(arena, i, 0);
            tmp = arena_strdup(arena, "Hello, world");
            if (strcmp(tmp, "Hello, world")) {
                fprintf(stderr, "I got %p = %s!\n", tmp, tmp);
            }
        }
    }
    printf("----\n");
    arena_print_stats(stdout, arena);
    arena_destroy(arena);
}

#endif
