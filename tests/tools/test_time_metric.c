#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>

#include "perf.h"
#include "mem.h"

extern int gbl_metric_maxpoints;

#define VALUE 50
#define RUN_SECONDS 5
#define NADDERS 4

static struct time_metric *metric;
static volatile int stop;

/* Each add pushes the oldest point out, freeing it while readers may be looking at it */
static void *adder(void *arg)
{
    while (!stop)
        time_metric_add(metric, VALUE);
    return NULL;
}

/* Every point holds VALUE, so the max must be VALUE and the change between points 0
 * (or both 0 before the first add). A freed point reads as part of a memory address;
 * the max only notices addresses that read as positive, the delta notices either sign. */
static void test_readers_while_adding(void)
{
    pthread_t thds[NADDERS];
    time_t end = time(NULL) + RUN_SECONDS;
    long reads = 0;
    int max;
    double delta;

    gbl_metric_maxpoints = 16;
    metric = time_metric_new("test");
    for (int i = 0; i < NADDERS; i++)
        pthread_create(&thds[i], NULL, adder, NULL);

    do {
        max = time_metric_max(metric);
        delta = time_metric_delta_average(metric);
        ++reads;
    } while ((max == VALUE || max == 0) && delta == 0 && time(NULL) < end);

    stop = 1;
    for (int i = 0; i < NADDERS; i++)
        pthread_join(thds[i], NULL);

    if ((max != VALUE && max != 0) || delta != 0) {
        fprintf(stderr, "after %ld reads: max %d (expected %d), delta average %f (expected 0)\n", reads, max, VALUE,
                delta);
        exit(1);
    }
}

int main(int argc, char **argv)
{
    comdb2ma_init(0, 0);
    test_readers_while_adding();
    return 0;
}
