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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <limits.h>

#include "list.h"
#include "pool_c.h"

#include "averager.h"
#include "mem_util.h"
#include "mem_override.h"

struct tick {
    int value;
    int time_added;
    LINKC_T(struct tick) lnk;
};

struct averager {
    int limit;
    int64_t sum;
    pool_t *pool;
    int maxpoints;
    struct tick *max;
    struct tick *min;
    LISTC_T(struct tick) ticks;
};

struct averager *averager_new(int limit, int maxpoints)
{
    struct averager *avg;
    avg = malloc(sizeof(struct averager));
    avg->limit = limit;
    avg->sum = 0;
    avg->pool = pool_setalloc_init(sizeof(struct tick), 1000, malloc, free);
    avg->maxpoints = maxpoints;
    avg->max = NULL;
    avg->min = NULL;
    listc_init(&avg->ticks, offsetof(struct tick, lnk));
    return avg;
}

void averager_clear(struct averager *avg) {
    struct tick *t;
    t = listc_rtl(&avg->ticks);
    while (t) {
        pool_relablk(avg->pool, t);
        t = listc_rtl(&avg->ticks);
    }
    avg->sum = 0;
    avg->min = avg->max = NULL;
}

void averager_purge_old(struct averager *avg, int now)
{
    struct tick *t = NULL;

    if (avg->max != NULL && (((now - avg->max->time_added) > avg->limit) || (avg->maxpoints && avg->ticks.count > avg->maxpoints)))
        avg->max = NULL;
    if (avg->min != NULL && (((now - avg->min->time_added) > avg->limit) || (avg->maxpoints && avg->ticks.count > avg->maxpoints)))
        avg->min = NULL;

    t = avg->ticks.top;
    while (t && (((now - t->time_added) > avg->limit) ||
                 (avg->maxpoints && avg->ticks.count > avg->maxpoints))) {
        listc_rtl(&avg->ticks);
        avg->sum -= t->value;
        pool_relablk(avg->pool, t);
        t = avg->ticks.top;
    }

    if (avg->max == NULL || avg->min == NULL) {
        LISTC_FOR_EACH(&avg->ticks, t, lnk) {
            if (avg->max == NULL || t->value > avg->max->value)
                avg->max = t;
            if (avg->min == NULL || t->value < avg->min->value)
                avg->min = t;
        }
    }

}

void averager_add(struct averager *avg, int value, int now)
{
    struct tick *t;

    averager_purge_old(avg, now);

    t = pool_getablk(avg->pool);

    t->value = value;
    t->time_added = now;
    avg->sum += t->value;
    listc_abl(&avg->ticks, t);

    /* Update min/max - the very 1st element added to the average is both max and min. */
    if (avg->max == NULL || value > avg->max->value)
        avg->max = t;
    if (avg->min == NULL || value < avg->min->value)
        avg->min = t;

}

double averager_avg(struct averager *avg)
{
    if (avg->ticks.count == 0)
        return 0.0;
    return (double)avg->sum / (double)avg->ticks.count;
}

int averager_max(struct averager *avg)
{
    if (avg->max == NULL)
        return 0;
    return avg->max->value;
}

int averager_min(struct averager *avg)
{
    if (avg->min == NULL)
        return 0;
    return avg->min->value;
}

void averager_destroy(struct averager *avg) { pool_free(avg->pool); }

int averager_depth(struct averager *avg) { return avg->ticks.count; }

int averager_get_points(struct averager *avg, struct point **values, int *nvalues) {
    struct point *points;
    points = malloc(sizeof(struct point) * avg->ticks.count);
    if (points == NULL)
        return -1;
    int pt = 0;
    struct tick *t;
    LISTC_FOR_EACH(&avg->ticks, t, lnk) {
        points[pt].time_added = t->time_added;
        points[pt++].value = t->value;
    }
    *nvalues = pt;
    *values = points;
    return 0;
}



#ifdef TEST_AVERAGER
int main(int argc, char *argv[])
{
    struct averager avg;
    struct tick *t;

    averager_init(&avg, 10);
    for (int i = 0; i < 10000; i++) {
        averager_add(&avg, i, i);
        printf("%d avg %d\n", i, averager_avg(&avg));
    }
    printf("%d ticks:\n", avg.ticks.count);
    LISTC_FOR_EACH(&avg.ticks, t, lnk)
    {
        printf("time %d value %d\n", t->value, t->time_added);
    }
}
#endif
