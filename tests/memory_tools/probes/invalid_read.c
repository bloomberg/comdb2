/* Deliberate heap read one element past the end of a malloc() block.
 * Expected detector: Valgrind Memcheck "Invalid read". */

#include <stdio.h>
#include <stdlib.h>

static int __attribute__((noinline)) read_slot(const int *p, int slot)
{
    return p[slot];
}

int main(void)
{
    int *p = malloc(8 * sizeof(int));
    volatile int slot = 8; /* runtime index hides the overrun from the compiler */
    int i, v;

    if (p == NULL)
        return 2;
    for (i = 0; i < 8; i++)
        p[i] = i;

    printf("memory-probe: invalid-read\n");
    fflush(stdout);
    v = read_slot(p, slot);
    free(p);
    printf("read: %d\n", v);
    return 0;
}
