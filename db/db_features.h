/*
   Copyright 2023 Bloomberg Finance L.P.

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

#ifndef DECLARE_FEATURE
#define DECLARE_FEATURE(id, description)
#define EMPTY_DECLARE_FEATURE
#endif

// ezsystables uses int64_t for integer values, so use that here
// for consistency.

// 0-999 are reserved for legacy things and plugins
//

DECLARE_FEATURE(FEATURE_REMOTE_WRITES, "Writes sent to this database to pass to a different database.")
DECLARE_FEATURE(FEATURE_TEST_COUNTER, "Calls to comdb2_test_counter function.")

#ifdef EMPTY_DECLARE_FEATURE
#undef EMPTY_DECLARE_FEATURE
#undef DECLARE_FEATURE
#endif

#ifndef INCLUDED_FEATURES_H
#define INCLUDED_FEATURES_H

#include <stdint.h>

enum {
    FEATURE_TEST_COUNTER = 1001,
    FEATURE_REMOTE_WRITES = 1002,
    FEATURE_MAX
};

struct feature_usage {
    char* id;
    char *description;
    int64_t num_uses;
    int defined; // not returned by ezsystable
};

void feature_usage_bump(int64_t feature_id);
int feature_usage_get(struct feature_usage **features, int *num_features);
void feature_usage_free(struct feature_usage *features, int num_features);
void feature_usage_register(int64_t feature_id, const char* name, const char *description);
#endif
