/* Deliberate direct leak through a generated Comdb2 subsystem allocator.
 *
 * mem_berkdb.h is the header the berkdb subsystem itself is built with. Under
 * COMDB2_PER_THREAD_MALLOC=OFF (USE_SYS_ALLOC) it maps comdb2_malloc_berkdb to
 * malloc, so the leak must be visible to LeakSanitizer and Valgrind exactly
 * like leak.c. This proves the system-allocator variants really route Comdb2
 * allocations through malloc. With the native allocator the block would come
 * from a dlmalloc mspace, invisible to both tools, so this probe is only built
 * for system-allocator variants. */

#include <stdio.h>
#include <string.h>

#include <mem_berkdb.h>

#ifndef USE_SYS_ALLOC
#error "comdb2_allocator_leak requires COMDB2_PER_THREAD_MALLOC=OFF"
#endif

static void __attribute__((noinline)) leak_block(void)
{
    char *volatile p = comdb2_malloc_berkdb(64);
    if (p != NULL)
        memset(p, 'B', 64);
    p = NULL;
}

int main(void)
{
    printf("memory-probe: comdb2-allocator-leak\n");
    fflush(stdout);
    leak_block();
    return 0;
}
