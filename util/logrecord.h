/*
   Copyright 2026 Bloomberg Finance L.P.

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

/*
 * Fast field accessors for packed log record buffers.
 *
 * These functions retrieve individual fields from raw log record data without
 * fully unpacking the record, avoiding the overhead of a complete unpack.
 */

#ifndef INCLUDED_LOGRECORD_H
#define INCLUDED_LOGRECORD_H

#include <stdint.h>
#include <sys/types.h>

uint64_t logrecord_timestamp_regop_gen(char *data);
uint32_t logrecord_generation_regop_gen(char *data);

uint64_t logrecord_timestamp_dist_commit(char *data);
uint32_t logrecord_generation_dist_commit(char *data);

uint64_t logrecord_timestamp_dist_abort(char *data);
uint32_t logrecord_generation_dist_abort(char *data);

uint64_t logrecord_timestamp_regop_rowlocks(char *data);
uint32_t logrecord_generation_regop_rowlocks(char *data);

uint32_t logrecord_timestamp_regop(char *data);
uint32_t logrecord_timestamp_ckp(char *data);
uint32_t logrecord_generation_ckp(char *data);

/*
 * Returns the timestamp from whichever commit/checkpoint record type is
 * present.  Returns (uint64_t)-1 if the record type is not recognized.
 */
uint64_t logrecord_timestamp_matchable(char *data);

#endif /* INCLUDED_LOGRECORD_H */
