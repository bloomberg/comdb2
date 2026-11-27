/* Deliberate heap use-after-free.
 * Expected detector: AddressSanitizer "heap-use-after-free". */

#include <stdio.h>
#include <stdlib.h>

static int __attribute__((noinline)) read_slot(int *volatile p, int slot)
{
    return p[slot];
}

int main(void)
{
    int *p = malloc(16 * sizeof(int));
    /* A volatile copy hides the free from -Wuse-after-free and stops the
     * compiler from proving the read dead. */
    int *volatile stale = p;
    volatile int slot = 3;
    int i;

    if (p == NULL)
        return 2;
    for (i = 0; i < 16; i++)
        p[i] = i;
    free(p);

    printf("memory-probe: heap-use-after-free\n");
    fflush(stdout);
    return read_slot(stale, slot) == 3 ? 0 : 1;
}
