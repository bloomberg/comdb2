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
 * Create a testing logical unit
 *
 */
int logical_cron_unit_test(FILE *out, const char *name);

#endif
