/*
   Copyright 2019 Bloomberg Finance L.P.

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

#ifndef __LOGICAL_CRON_H_
#define __LOGICAL_CRON_H_

#include "cron.h"

#define LOGICAL_CRON_SYSTABLE "comdb2_logical_cron"

/**
 * Move logical clock one step forward
 *
 */
void logical_cron_incr(sched_if_t *impl);

/**
 * Set the logical clock to step "val"
 *
 */
void logical_cron_set(sched_if_t *impl, unsigned long long val);

/**
 * Restart a logical scheduler
 *
 */
int logical_cron_init(const char *name, struct errstat *err);

/**
 * Return a statement to update a cron
 *
 */
char *logical_cron_update_sql(const char *name, long long value,
                              int increment);

/**
 * Retrieve the persistent value of a cron counter
 *
 */
unsigned long long logical_cron_read_persistent(const char *name,
                                                struct errstat *err);

#endif
