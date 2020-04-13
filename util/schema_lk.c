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
#include <assert.h>

static pthread_rwlock_t schema_lk = PTHREAD_RWLOCK_INITIALIZER;
__thread int have_readlock = 0;
__thread int have_writelock = 0;

inline int have_schema_lock(void)
{
    return (have_readlock || have_writelock);
}

/* We actually acquire the readlock recursively: change these asserts to
 * accommodate */
inline void rdlock_schema_int(const char *file, const char *func, int line)
{
    assert(have_writelock == 0);
    Pthread_rwlock_rdlock(&schema_lk);
    have_readlock++;
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:RDLOCK %s:%d\n", (void *)pthread_self(), func,
           line);
#endif
}

inline int tryrdlock_schema_int(const char *file, const char *func, int line)
{
    assert(have_writelock == 0);
    int rc = pthread_rwlock_tryrdlock(&schema_lk);
    if (!rc)
        have_readlock++;
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:TRYRDLOCK RC:%d %s:%d\n", (void *)pthread_self(),
           rc, func, line);
#endif
    return rc;
}

inline void unlock_schema_int(const char *file, const char *func, int line)
{
    assert(have_readlock || have_writelock);
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:UNLOCK %s:%d\n", (void *)pthread_self(), func,
           line);
#endif
    if (have_readlock)
        have_readlock--;
    else if (have_writelock)
        have_writelock = 0;
    Pthread_rwlock_unlock(&schema_lk);
}

inline void wrlock_schema_int(const char *file, const char *func, int line)
{
    assert(have_readlock == 0 && have_writelock == 0);
    Pthread_rwlock_wrlock(&schema_lk);
    have_writelock = 1;
#ifdef VERBOSE_SCHEMA_LK
    logmsg(LOGMSG_USER, "%p:WRLOCK %s:%d\n", (void *)pthread_self(), func,
           line);
#endif
}

inline void assert_wrlock_schema_int(const char *file, const char *func,
                                     int line)
{
    if (have_writelock == 0) {
        logmsg(LOGMSG_FATAL, "%p:ASSERT-WRLOCK %s:%d\n", (void *)pthread_self(),
               func, line);
        abort();
    }
}

inline void assert_rdlock_schema_int(const char *file, const char *func,
                                     int line)
{
    if (have_readlock == 0) {
        logmsg(LOGMSG_FATAL, "%p:ASSERT-RDLOCK %s:%d\n", (void *)pthread_self(),
               func, line);
        abort();
    }
}

inline void assert_lock_schema_int(const char *file, const char *func, int line)
{
    if (have_readlock == 0 && have_writelock == 0) {
        logmsg(LOGMSG_FATAL, "%p:ASSERT-RDLOCK %s:%d\n", (void *)pthread_self(),
               func, line);
        abort();
    }
}

inline void assert_no_schema_lock_int(const char *file, const char *func,
                                      int line)
{
    if (have_readlock != 0 || have_writelock != 0) {
        logmsg(LOGMSG_FATAL, "%p:ASSERT-NOLOCK %s:%d\n", (void *)pthread_self(),
               func, line);
        abort();
    }
}
