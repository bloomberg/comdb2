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

#ifndef __CRON_SYSTABLE_H_
#define __CRON_SYSTABLE_H_

typedef struct systable_cron_scheds {
    char *name;
    char *type;
    int running;
    int nevents;
    char *description;
} systable_cron_scheds_t;

typedef struct systable_cron_events {
    char *name;
    char *type;
    char *arg1;
    char *arg2;
    char *arg3;
    char *sourceid;
} systable_cron_events_t;

/**
 * Creates a snapshot of the existing cron schedulers
 *
 */
int cron_systable_schedulers_collect(void **data, int *nrecords);

/**
 * Free the cron schedulers snapshot
 *
 */
void cron_systable_schedulers_free(void *data, int nrecords);

/**
 * Creates a snapshot of the existing global cron events
 *
 */
int cron_systable_events_collect(void **data, int *nrecords);

/**
 * Free the global cron events snapshot
 *
 */
void cron_systable_events_free(void *data, int nrecords);


#endif
