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

#include <clienthost.h>
#include <pthread.h>
#include <locks_wrap.h>

static pthread_mutex_t clienthost_lk = PTHREAD_MUTEX_INITIALIZER;
LISTC_T(struct clienthost) clienthost_list;

void clienthost_lock(void)
{
    Pthread_mutex_lock(&clienthost_lk);
}

void clienthost_unlock(void)
{
    Pthread_mutex_unlock(&clienthost_lk);
}

static struct clienthost *retrieve_clienthost_int(struct interned_string *s, int create)
{
    if (!s->clptr && create) {
        clienthost_lock();
        if (s->clptr == NULL) {
            struct clienthost *r = s->clptr = calloc(sizeof(struct clienthost), 1);
            r->s = s;
            r->mach_class = 0; /* CLASS_UNKNOWN */
            listc_abl(&clienthost_list, r);
        }
        clienthost_unlock();
    }
    return s->clptr;
}

struct clienthost *retrieve_clienthost(struct interned_string *s)
{
    return retrieve_clienthost_int(s, 1);
}

struct clienthost *retrieve_clienthost_nocreate(struct interned_string *s)
{
    return retrieve_clienthost_int(s, 0);
}

void clienthost_init(void)
{
    listc_init(&clienthost_list, offsetof(struct clienthost, lnk));
}
