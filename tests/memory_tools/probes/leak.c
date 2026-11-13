/* Deliberate direct leak of a plain malloc() block.
 * Expected detectors: LeakSanitizer "Direct leak" (asan-system) and
 * Valgrind "definitely lost" (valgrind-system).
 *
 * The only reference lives in a volatile local of a non-inlined function and
 * is overwritten before it returns, so nothing reachable points at the block
 * when the process exits. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void __attribute__((noinline)) leak_block(void)
{
    char *volatile p = malloc(64);
    if (p != NULL)
        memset(p, 'L', 64);
    p = NULL;
}

int main(void)
{
    printf("memory-probe: leak\n");
    fflush(stdout);
    leak_block();
    return 0;
}
