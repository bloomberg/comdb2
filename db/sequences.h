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
#include <sql.h>

int seq_next_val(tran_type *tran, char *name, long long *val,
                 bdb_state_type *bdb_state);
int sequences_master_change();
int sequences_master_upgrade();
int insert_sequence_range(sequence_t *seq, sequence_range_t *node);
int generate_replicant_sequence_range(char *name, sequence_range_t *range);

#endif
