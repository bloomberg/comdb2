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
#include <logmsg.h>
#include <locks_wrap.h>
#include <schema_lk.h>
#define VERBOSE_SCHEMA_LK 1

static pthread_rwlock_t schema_lk = PTHREAD_RWLOCK_INITIALIZER;

inline void rdlock_schema_int(const char *file, const char *func, int line)
{
    Pthread_rwlock_rdlock(&schema_lk);
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:RDLOCK %s:%d\n", (void *)pthread_self(), func, line);
#endif
}

inline int tryrdlock_schema_int(const char *file, const char *func, int line)
{
    int rc = pthread_rwlock_tryrdlock(&schema_lk);
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:TRYRDLOCK RC:%d %s:%d\n", (void *)pthread_self(), rc, func,
            line);
#endif
    return rc;
}

inline void unlock_schema_int(const char *file, const char *func, int line)
{
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:UNLOCK %s:%d\n", (void *)pthread_self(), func, line);
#endif
    Pthread_rwlock_unlock(&schema_lk);
}

inline void wrlock_schema_int(const char *file, const char *func, int line)
{
    Pthread_rwlock_wrlock(&schema_lk);
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:WRLOCK %s:%d\n", (void *)pthread_self(), func, line);
#endif
}
