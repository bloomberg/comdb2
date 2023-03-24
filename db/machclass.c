/*
   Copyright 2017, 2020 Bloomberg Finance L.P.

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

#include <ctype.h>
#include <stdlib.h>
#include <pthread.h>
#include <plhash_glue.h>
#include "machclass.h"
#include "lockmacros.h"

char *gbl_machine_class = NULL;

typedef struct machine_class {
    char *name;
    int value;
} machine_class_t;

static pthread_mutex_t mach_mtx = PTHREAD_MUTEX_INITIALIZER;
static hash_t *classes;
static hash_t *class_names;

#define MAX_MACH_CLASS_NAME_LEN 10
static machine_class_t default_classes[] = {
    {"unknown", 0},     /* 0 indexed! */
    {"test", 1},        /* CLASS_TEST == 1 */
    {"dev", 1},         /* CLASS_TEST == 1 */
    {"alpha", 2},       /* CLASS_ALPHA == 2 */
    {"uat", 3},         /* CLASS_UAT == 3 */
    {"beta", 4},        /* CLASS_BETA == 4  */
    {"prod", 5},        /* CLASS_PROD == 5 */
    {"integration", 6}, /* CLASS_INTEGRATION == 6 */
};

static char *fdb_tiers[sizeof(default_classes) / sizeof(default_classes[0])];

int is_default = 0;

static int _mach_class_add(machine_class_t *class, int *added);

int mach_class_init(void)
{
    int i;
    int rc = 0;

    Pthread_mutex_lock(&mach_mtx);
    classes = hash_init_strcaseptr(offsetof(struct machine_class, name));
    class_names = hash_init_i4(offsetof(struct machine_class, value));
    if (!classes || !class_names) {
        return -1;
    }

    for (i = 0; i < sizeof(default_classes) / sizeof(default_classes[0]); i++) {
        rc = _mach_class_add(&default_classes[i], NULL);
        if (rc)
            break;
    }
    is_default = 1;
    Pthread_mutex_unlock(&mach_mtx);
    return rc;
}

static int _mach_class_add(machine_class_t *class, int *added)
{
    if (!hash_find(classes, &class->name)) {
        logmsg(LOGMSG_DEBUG, "Adding class %s value %d\n", class->name,
               class->value);
        hash_add(classes, class);
        hash_add(class_names, class);
        if (added)
            *added = 1;
    }
    return 0;
}

int mach_class_addclass(const char *name, int value)
{
    machine_class_t *class = calloc(1, sizeof(machine_class_t));
    int rc = 0;
    int added = 0;

    if (!class)
        return -1;

    class->name = strdup(name);
    if (!class->name) {
        free(class);
        return -1;
    }
    class->value = value;

    Pthread_mutex_lock(&mach_mtx);
    if (is_default) {
        /* override the default with client classes */
        hash_clear(classes);
        is_default = 0;
    }
    rc = _mach_class_add(class, &added);

    Pthread_mutex_unlock(&mach_mtx);

    if (!added) {
        free(class->name);
        free(class);
    }
    return rc;
}

int mach_class_name2class(const char *name)
{
    machine_class_t *class;
    int value = CLASS_UNKNOWN;

    Pthread_mutex_lock(&mach_mtx);

    class = hash_find(classes, &name);
    if (class)
        value = class->value;

    Pthread_mutex_unlock(&mach_mtx);

    return value;
}

const char *mach_class_class2name(int value)
{
    machine_class_t *class;
    const char *name = NULL;

    Pthread_mutex_lock(&mach_mtx);

    class = hash_find(class_names, &value);
    if (class)
        name = class->name;

    Pthread_mutex_unlock(&mach_mtx);

    return name;
}

int mach_class_remap_fdb_tier(const char *name, const char *tier)
{
    machine_class_t *class;
    int rc = 0;

    Pthread_mutex_lock(&mach_mtx);

    class = hash_find(classes, &name);
    if (class == NULL) {
        logmsg(LOGMSG_ERROR, "machine class '%s' does not exist", name);
        rc = -1;
    } else if (class->value >= sizeof(fdb_tiers) / sizeof(fdb_tiers[0])) {
        logmsg(LOGMSG_ERROR, "machine class '%s' is out of bound", name);
        rc = -1;
    } else {
        free(fdb_tiers[class->value]);
        fdb_tiers[class->value] = strdup(tier);
    }

    Pthread_mutex_unlock(&mach_mtx);

    return rc;
}

const char *mach_class_class2tier(int value)
{
    machine_class_t *class;
    const char *ret = NULL;

    Pthread_mutex_lock(&mach_mtx);

    if (fdb_tiers[value] != NULL)
        ret = fdb_tiers[value];
    else {
        class = hash_find(class_names, &value);
        if (class)
            ret = class->name;
    }

    Pthread_mutex_unlock(&mach_mtx);

    return ret;
}
