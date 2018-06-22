/*
   Copyright 2015 Bloomberg Finance L.P.

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

#include <pthread.h>
#include <string.h>
#include <stdlib.h>

#include "plhash.h"
#include "intern_strings.h"

#include "mem_util.h"
#include "mem_override.h"
#include "logmsg.h"

static pthread_once_t once = PTHREAD_ONCE_INIT;
static pthread_mutex_t intern_lk = PTHREAD_MUTEX_INITIALIZER;
static hash_t *interned_strings = NULL;

struct interned_string {
    char *str;
    int64_t ref;
};

static void init_interned_strings(void)
{
    interned_strings = hash_init_strptr(offsetof(struct interned_string, str));
    if (interned_strings == NULL) {
        logmsg(LOGMSG_FATAL, "can't create hash table for hostname strings\n");
        abort();
    }
}

/* Store a copy of parameter str in a hash tbl */
char *intern(const char *str)
{
    char *out;
    struct interned_string *s;

    pthread_once(&once, init_interned_strings);
    pthread_mutex_lock(&intern_lk);
    s = hash_find_readonly(interned_strings, &str);
    if (s == NULL) {
        s = malloc(sizeof(struct interned_string));
        if (s == NULL) {
            pthread_mutex_unlock(&intern_lk);
            return NULL;
        }
        s->str = strdup(str);
        if (s->str == NULL) {
            free(s);
            pthread_mutex_unlock(&intern_lk);
            return NULL;
        }
        hash_add(interned_strings, s);
    }
    s->ref++;
    pthread_mutex_unlock(&intern_lk);
    return s->str;
}

char *internn(const char *str, int len)
{
    char *s;
    char *out;
    s = malloc(len + 1);
    memcpy(s, str, len);
    s[len] = 0;
    out = intern(s);
    free(s);
    return out;
}

int isinterned(const char *node)
{
    struct interned_string *s;

    pthread_mutex_lock(&intern_lk);
    s = hash_find_readonly(interned_strings, &node);
    pthread_mutex_unlock(&intern_lk);

    if (s && s->str == node)
        return 1;

    return 0;
}

static int intern_free(void *ptr, void *unused)
{
    struct interned_string *obj = ptr;
    free(obj->str);
    obj->str = NULL;
    free(obj);
    return 0;
}

void cleanup_interned_strings()
{
    hash_for(interned_strings, intern_free, NULL);
    hash_clear(interned_strings);
    hash_free(interned_strings);
    interned_strings = NULL;
    pthread_mutex_destroy(&intern_lk);
}
