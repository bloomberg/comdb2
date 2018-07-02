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
} comdb2_statistic_type;

struct comdb2_statistic {
    /* Name of the statistic. (Mandatory) */
    const char *name;

    /* Description of the statistic. (Mandatory) */
    const char *descr;

    /* Type of the statistic. (Mandatory) */
    comdb2_statistic_type type;

    /* Pointer to the variable that stores the statistic's value. (Mandatory) */
    void *var;

    /* Returns the value of the statistic. (Optional) */
    void *(*value)(void *);
};
typedef struct comdb2_statistic comdb2_statistic;

/* Array of all comdb2 statistics */
extern comdb2_statistic gbl_statistics[];

/* Total number of statistics. */
extern int gbl_statistics_count;

/* Refresh the values of all statistics. */
int refresh_statistics(void);

/* Initialize & reset the values of all statistics. */
int init_statistics(void);

/* Return the statistic type in C-string. */
const char *statistic_type(comdb2_statistic_type type);

#endif /* _STATISTICS_H */
