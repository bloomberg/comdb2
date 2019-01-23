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

#ifndef __VIEWS_SYSTABLE_H_
#define __VIEWS_SYSTABLE_H_


typedef struct systable_timepartition {
    char *name;
    char *period;
    int64_t retention;
    int64_t nshards;
    int64_t version;
    char *shard0name;
    int starttime;
    char *sourceid;
} systable_timepartition_t;

typedef struct systable_timepartshard {
    char *name;
    char *shardname;
    int low;
    int high;
} systable_timepartshard_t;

/**
 * Creates a snapshot of the existing timepartitions
 *
 */
int timepart_systable_timepartitions_collect(void **data, int *nrecords);

/**
 * Free the timepartitions snapshot
 *
 */
void timepart_systable_timepartitions_free(void *data, int nrecords);

/**
 * Creates a snapshot of the existing timepartitions
 *
 */
int timepart_systable_timepartshards_collect(void **data, int *nrecords);

/**
 * Free the timepartitions snapshot
 *
 */
void timepart_systable_timepartshards_free(void *data, int nrecords);

/**
 * Creates a snapshot of the existing time partition events
 *
 */
int timepart_systable_timepartevents_collect(void **data, int *nrecords);

/**
 * Free the time partition events snapshot
 *
 */
void timepart_systable_timepartevents_free(void *data, int nrecords);

#endif
