#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stddef.h>

#include <epochlib.h>

#include "perf.h"
#include "averager.h"
#include "list.h"

int gbl_timeseries_metrics = 1;

static LISTC_T(struct time_metric) metrics;

/* TODO: tunabalize? */
int gbl_metric_maxpoints = 10000;
int gbl_metric_maxage = 3600;

struct time_metric {
    char *name;
    struct averager *avg;
    pthread_mutex_t lk;
    LINKC_T(struct time_metric) lnk;
};

pthread_once_t once = PTHREAD_ONCE_INIT;

static void init_time_metrics(void) {
    listc_init(&metrics, offsetof(struct time_metric, lnk));
}

struct time_metric* time_metric_new(char *name) {
    struct time_metric *t;
    pthread_once(&once, init_time_metrics);
    t = calloc(1, sizeof(struct time_metric));
    if (t == NULL)
        goto bad;
    t->name = strdup(name);
    if (t->name == NULL)
        goto bad;
    t->avg = averager_new(gbl_metric_maxage, gbl_metric_maxpoints);
    if (t->avg == NULL)
        goto bad;
    int rc = pthread_mutex_init(&t->lk, NULL);
    if (rc)
        goto bad;

    listc_abl(&metrics, t);

    return t;
bad:
    if (t) {
        free(t->name);
        if (t->avg)
            averager_destroy(t->avg);
    }
    return NULL;
}

void time_metric_add(struct time_metric *t, int value) {
    time_t now = comdb2_time_epoch();

    if (!gbl_timeseries_metrics)
        return;

    pthread_mutex_lock(&t->lk);
    averager_add(t->avg, value, now);
    pthread_mutex_unlock(&t->lk);
}

struct time_metric* time_metric_get(char *name) {
    struct time_metric *t;
    LISTC_FOR_EACH(&metrics, t, lnk) {
        if (strcmp(t->name, name) == 0)
            return t;
    }
    return NULL;
}

struct time_metric* time_metric_first(void) {
    return metrics.top;
}

struct time_metric* time_metric_next(struct time_metric *t) {
    return t->lnk.next;
}

char* time_metric_name(struct time_metric *t) {
    return t->name;
}

void time_metric_purge_old(struct time_metric *t) {
    time_t now = comdb2_time_epoch();

    pthread_mutex_lock(&t->lk);
    averager_purge_old(t->avg, now);
    pthread_mutex_unlock(&t->lk);
}

int time_metric_get_points(struct time_metric *t, struct point **values, int *nvalues) {
    int rc;
    pthread_mutex_lock(&t->lk);
    rc = averager_get_points(t->avg, values, nvalues);
    pthread_mutex_unlock(&t->lk);
    return rc;
}

double time_metric_average(struct time_metric *t) {
    return averager_avg(t->avg);
}
