/*
   Copyright 2025 Bloomberg Finance L.P.

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

#ifndef LOG_INFO_H
#define LOG_INFO_H

struct __db_env;
struct bdb_state_tag;

#include <stdint.h>

typedef struct LOG_INFO LOG_INFO;

struct LOG_INFO {
    uint32_t file;
    uint32_t offset;
    uint32_t size;
    uint32_t gen;
};

LOG_INFO get_last_lsn(struct bdb_state_tag *);
LOG_INFO get_first_lsn(struct bdb_state_tag *);
uint32_t get_next_offset(struct __db_env *, LOG_INFO log_info);
int log_info_compare(LOG_INFO *a, LOG_INFO *b);

#endif
