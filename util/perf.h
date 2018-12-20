#ifndef INCLUDED_PERF_H
#define INCLUDED_PERF_H

#include "averager.h"

struct time_metric;

struct time_metric* time_metric_new(char *name);
void time_metric_free(struct time_metric *t);
void time_metric_add(struct time_metric *t, int value);
struct time_metric* time_metric_get(char *name);
struct time_metric* time_metric_first(void);
struct time_metric* time_metric_next(struct time_metric *t);
char* time_metric_name(struct time_metric *t); 
int time_metric_get_points(struct time_metric *t, struct point **values, int *nvalues);
double time_metric_average(struct time_metric *t);
int time_metric_max(struct time_metric *t);
void time_metric_purge_old(struct time_metric *t);

#endif
