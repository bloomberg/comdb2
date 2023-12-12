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

#include "db_features.h"
#include "comdb2_atomic.h"

static struct feature_usage features[FEATURE_MAX] = {
#define DECLARE_FEATURE(_id, _description) [_id] = { .id = #_id, .description = _description, .defined = 1 },
#include "db_features.h"
#undef DECLARE_FEATURE
};

void feature_usage_bump(int64_t feature_id) {
    if (feature_id < 0 || feature_id >= FEATURE_MAX)
        return;
    ATOMIC_ADD64(features[feature_id].num_uses, 1);
}

int feature_usage_get(struct feature_usage **features_out, int *num_features) {
    // collect all the used features
    int used_features = 0;
    for (int i = 0; i < FEATURE_MAX; i++) {
        if (features[i].defined)
            used_features++;
    }
    struct feature_usage *f;
    f = malloc(used_features * sizeof(struct feature_usage));
    if (f == NULL)
        return -1;
    int feature_ix  = 0;
    for (int i = 0; i < FEATURE_MAX; i++) {
        if (features[i].defined) {
            f[feature_ix].id = features[i].id;
            f[feature_ix].description = features[i].description;
            f[feature_ix++].num_uses = features[i].num_uses;
        }
        if (feature_ix >= used_features)
            break;
    }
    
    *num_features = used_features;
    *features_out = f;
    return 0;
}

void feature_usage_free(struct feature_usage *features, int num_features) {
    free(features);
}

void feature_usage_register(int64_t feature_id, const char* name, const char *description) {
    if (feature_id < 0 || feature_id >= FEATURE_MAX)
        return;
    features[feature_id].id = strdup(name);
    features[feature_id].description = strdup(description);
    features[feature_id].num_uses = 0;
}
