/* Deliberate signed integer overflow on runtime values.
 * Expected detector: UndefinedBehaviorSanitizer "runtime error: signed
 * integer overflow". */

#include <limits.h>
#include <stdio.h>

static int __attribute__((noinline)) add(int a, int b)
{
    return a + b;
}

int main(void)
{
    /* volatile prevents constant folding of the overflow */
    volatile int big = INT_MAX;
    volatile int one = 1;

    printf("memory-probe: signed-overflow\n");
    fflush(stdout);
    return add(big, one) == INT_MIN ? 0 : 1;
}
