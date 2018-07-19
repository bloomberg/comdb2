/*
   Copyright 2017 Bloomberg Finance L.P.

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

#ifndef _STATISTICS_H
#define _STATISTICS_H

typedef enum {
    STATISTIC_INTEGER,
    STATISTIC_DOUBLE,
    STATISTIC_BOOLEAN,
    STATISTIC_STRING,
    /* Must always be the last. */
    STATISTIC_INVALID,
} comdb2_metric_type;

typedef enum {
    STATISTIC_COLLECTION_TYPE_CUMULATIVE,
    STATISTIC_COLLECTION_TYPE_LATEST,
} comdb2_collection_type;

struct comdb2_metric {
    /* Name of the statistic. (Mandatory) */
    const char *name;

    /* Description of the statistic. (Mandatory) */
    const char *descr;

    /* Type of the statistic. (Mandatory) */
    comdb2_metric_type type;

    /* Counter. (Mandatory) */
    comdb2_collection_type collection_type;

    /* Pointer to the variable that stores the statistic's value. (Mandatory) */
    void *var;

    /* Returns the value of the statistic. (Optional) */
    void *(*value)(void *);
};
typedef struct comdb2_metric comdb2_metric;

/* Array of all comdb2 metrics */
extern comdb2_metric gbl_metrics[];

/* Total number of metrics. */
extern int gbl_metrics_count;

/* Refresh the values of all metrics. */
int refresh_metrics(void);

/* Initialize & reset the values of all metrics. */
int init_metrics(void);

/* Return the metric type in C-string. */
const char *metric_type(comdb2_metric_type type);

/* Return how we keep the counter for this metric (C-string) */
const char *metric_collection_type_string(comdb2_collection_type t);

extern int64_t gbl_last_checkpoint_ms;
extern int64_t gbl_total_checkpoint_ms;
extern int gbl_checkpoint_count;

extern int64_t gbl_rcache_hits;
extern int64_t gbl_rcache_misses;

#endif /* _STATISTICS_H */
