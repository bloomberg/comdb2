/*
   Copyright 2020 Bloomberg Finance L.P.

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

#ifndef INCLUDED_TRANLOG_H
#define INCLUDED_TRANLOG_H

#include "build/db.h"

/* Define flags for the third argument */
enum {
    TRANLOG_FLAGS_BLOCK             = 0x1,
    TRANLOG_FLAGS_DURABLE           = 0x2,
    TRANLOG_FLAGS_DESCENDING        = 0x4,
    TRANLOG_FLAGS_SENTINEL          = 0x8,
};

u_int64_t get_timestamp_from_matchable_record(char *data);

#endif
