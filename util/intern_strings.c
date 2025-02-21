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
#include <alloca.h>

#include <plhash_glue.h>
#include "intern_strings.h"

#include "mem_util.h"
#include "mem_override.h"
#include "logmsg.h"
#include "sys_wrap.h"

static pthread_once_t once = PTHREAD_ONCE_INIT;
static pthread_mutex_t intern_lk = PTHREAD_MUTEX_INITIALIZER;
static hash_t *interned_strings = NULL;
static int node_ix;

static void init_interned_strings(void)
{
    interned_strings = hash_init_strptr(offsetof(struct interned_string, str));
    if (interned_strings == NULL) {
        logmsg(LOGMSG_FATAL, "can't create hash table for hostname strings\n");
        abort();
    }
}

/* Store a copy of parameter str in a hash tbl */
struct interned_string *intern_ptr(const char *str)
{
    struct interned_string *s;

    pthread_once(&once, init_interned_strings);
    Pthread_mutex_lock(&intern_lk);
    s = hash_find_readonly(interned_strings, &str);
    if (s == NULL) {
        s = malloc(sizeof(struct interned_string));
        if (s == NULL) {
            Pthread_mutex_unlock(&intern_lk);
            return NULL;
        }
        s->str = strdup(str);
        s->ix = node_ix++;
        s->ptr = NULL;
        s->clptr = NULL;
        if (s->str == NULL) {
            free(s);
            Pthread_mutex_unlock(&intern_lk);
            return NULL;
        }
        hash_add(interned_strings, s);
    }
    Pthread_mutex_unlock(&intern_lk);
    return s;
}

char *intern(const char *str)
{
    struct interned_string *s = intern_ptr(str);
    return s->str;
}

char *internn(const char *str, int len)
{
    char *s;
    char *out;
    if (len > 1024)
        s = malloc(len + 1);
    else
        s = alloca(len + 1);
    memcpy(s, str, len);
    s[len] = 0;
    out = intern(s);
    if (len > 1024)
        free(s);
    return out;
}

int isinterned(const char *node)
{
    struct interned_string *s;

    Pthread_mutex_lock(&intern_lk);
    s = hash_find_readonly(interned_strings, &node);
    Pthread_mutex_unlock(&intern_lk);

    if (s && s->str == node) {
        return 1;
    }

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
    Pthread_mutex_destroy(&intern_lk);
}

static int intern_dump(void *ptr, void *unused)
{
    struct interned_string *obj = ptr;
    logmsg(LOGMSG_USER, "%s: str=%s %p (obj %p)\n", __func__, obj->str,
           obj->str, obj);
    return 0;
}

void dump_interned_strings()
{
    hash_for(interned_strings, intern_dump, NULL);
}
