/*
   Copyright 2024 Bloomberg Finance L.P.

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

#ifndef _API_HISTORY_H_
#define _API_HISTORY_H_

#include <sys/time.h>

typedef struct api_driver {
    char *name;
    char *version;
    time_t last_seen;
} api_driver_t;

typedef struct api_history api_history_t;

void acquire_api_history_lock(api_history_t*, int);
void release_api_history_lock(api_history_t*);
api_history_t *init_api_history();
int free_api_history(api_history_t*);
api_driver_t *get_next_api_history_entry(api_history_t*, void**, unsigned int*);
int get_num_api_history_entries(api_history_t*);
int update_api_history(api_history_t*, char*, char*);

#endif
