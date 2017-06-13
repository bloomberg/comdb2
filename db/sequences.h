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

#ifndef INCLUDE_SEQUENCES_H
#define INCLUDE_SEQUENCES_H

#include "comdb2.h"
#include <stdbool.h>

int seq_next_val (char *name, long long *val);
int seq_prev_val (char *name, long long *val);
sequence_t *get_sequence(char *name);
int add_sequence (char *name, long long min_val, long long max_val, 
    long long increment, bool cycle, long long start_val, 
    long long chunk_size, char flags);
int drop_sequence (char *name);

#endif
