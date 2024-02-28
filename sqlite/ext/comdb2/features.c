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

#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "cdb2api.h"
#include "db_features.h"

sqlite3_module systblFeaturesModule = {
    .access_flag = CDB2_ALLOW_ALL,
};

int systblFeaturesInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_features_used",
            &systblFeaturesModule, (int(*)(void **, int*)) feature_usage_get, 
            (void(*)(void*, int)) feature_usage_free,
            sizeof(struct feature_usage),
            CDB2_CSTRING, "id", -1, offsetof(struct feature_usage, id),
            CDB2_CSTRING, "description", -1, offsetof(struct feature_usage, description),
            CDB2_INTEGER, "num_uses", -1, offsetof(struct feature_usage, num_uses),
            SYSTABLE_END_OF_FIELDS);
}
