/* Deliberate branch on an uninitialized heap value.
 * Expected detector: Valgrind Memcheck "Conditional jump or move depends on
 * uninitialised value(s)". */

#include <stdio.h>
#include <stdlib.h>

static int __attribute__((noinline)) is_positive(const int *p, int slot)
{
    if (p[slot] > 0)
        return 1;
    return 0;
}

int main(void)
{
    int *p = malloc(8 * sizeof(int)); /* never initialized */
    /* The volatile copy hides the uninitialized block from
     * -Wmaybe-uninitialized; the read itself is the point of the probe. */
    const int *volatile hidden = p;
    volatile int slot = 5;
    int r;

    if (p == NULL)
        return 2;

    printf("memory-probe: uninitialized-read\n");
    fflush(stdout);
    r = is_positive(hidden, slot);
    free(p);
    /* Printing keeps the read observable even if the branch becomes a setcc. */
    printf("is_positive: %d\n", r);
    return 0;
}
