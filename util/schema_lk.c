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

#define make_rwlock(lock, assertfunc)                                                                       \
static pthread_rwlock_t lock ## _lk = PTHREAD_RWLOCK_INITIALIZER;                                           \
static pthread_t last_##lock##_wrlock_owner;                                                                \
                                                                                                            \
static __thread int have_##lock##_readlock = 0;                                                             \
static __thread int have_##lock##_writelock = 0;                                                            \
static int debug_##lock##_lock = 1;                                                                         \
static int verbose_##lock##_lock = 1;                                                                       \
                                                                                                            \
int have_##lock##_lock(void)                                                                                \
{                                                                                                           \
    return (have_##lock##_readlock || have_##lock##_writelock);                                             \
}                                                                                                           \
                                                                                                            \
void assertfunc(void);                                                                                      \
                                                                                                            \
void debug_##lock##_enable(void) { debug_##lock##_lock = 1; }                                               \
void debug_##lock##_disable(void) { debug_##lock##_lock = 0; }                                              \
void verbose_##lock##_enable(void) { verbose_##lock##_lock = 1; }                                           \
void verbose_##lock##_disable(void) { verbose_##lock##_lock = 0; }                                          \
                                                                                                            \
void rdlock_##lock##_int(const char *file, const char *func, int line)                                      \
{                                                                                                           \
    if (debug_##lock##_lock)                                                                                \
        assertfunc();                                                                                       \
                                                                                                            \
    assert(have_##lock##_writelock == 0);                                                                   \
    Pthread_rwlock_rdlock(&lock##_lk);                                                                      \
    have_##lock##_readlock++;                                                                               \
    if (verbose_##lock##_lock)                                                                              \
        logmsg(LOGMSG_USER, "%p:RDLOCK %s:%s:%d\n", (void *)pthread_self(), file, func, line);              \
}                                                                                                           \
                                                                                                            \
int tryrdlock_##lock##_int(const char *file, const char *func, int line)                                    \
{                                                                                                           \
    int rc = pthread_rwlock_tryrdlock(&lock##_lk);                                                          \
    if (!rc)                                                                                                \
        have_##lock##_readlock++;                                                                           \
    if (verbose_##lock##_lock)                                                                              \
        logmsg(LOGMSG_USER, "%p:TRYRDLOCK RC:%d %s:%s:%d\n", (void *)pthread_self(), rc, file, func, line); \
    return rc;                                                                                              \
}                                                                                                           \
                                                                                                            \
int trywrlock_##lock##_int(const char *file, const char *func, int line)                                    \
{                                                                                                           \
    if (debug_##lock##_lock)                                                                                \
        assertfunc();                                                                                       \
    assert(have_##lock##_writelock == 0 && have_##lock##_readlock == 0);                                    \
    int rc = pthread_rwlock_trywrlock(&lock##_lk);                                                          \
    if (!rc)                                                                                                \
        have_##lock##_writelock++;                                                                          \
    if (verbose_##lock##_lock)                                                                              \
        logmsg(LOGMSG_USER, "%p:TRYWRLOCK RC:%d %s:%s:%d\n", (void *)pthread_self(), rc, file, func, line); \
    return rc;                                                                                              \
}                                                                                                           \
                                                                                                            \
void unlock_##lock##_int(const char *file, const char *func, int line)                                      \
{                                                                                                           \
    assert(have_##lock##_readlock || have_##lock##_writelock);                                              \
    if (verbose_##lock##_lock)                                                                              \
        logmsg(LOGMSG_USER, "%p:UNLOCK %s:%s:%d\n", (void *)pthread_self(), file, func, line);              \
    if (have_##lock##_readlock)                                                                             \
        have_##lock##_readlock--;                                                                           \
    else if (have_##lock##_writelock)                                                                       \
        have_##lock##_writelock = 0;                                                                        \
    Pthread_rwlock_unlock(&lock##_lk);                                                                      \
}                                                                                                           \
                                                                                                            \
void wrlock_##lock##_int(const char *file, const char *func, int line)                                      \
{                                                                                                           \
    if (debug_##lock##_lock)                                                                                \
        assertfunc();                                                                                       \
    assert(have_##lock##_readlock == 0 && have_##lock##_writelock == 0);                                    \
    Pthread_rwlock_wrlock(&lock##_lk);                                                                      \
    have_##lock##_writelock = 1;                                                                            \
    if (verbose_##lock##_lock)                                                                              \
        logmsg(LOGMSG_USER, "%p:WRLOCK %s:%s:%d\n", (void *)pthread_self(), file, func, line);              \
    last_ ## lock ## _wrlock_owner = pthread_self();                                                        \
}                                                                                                           \
                                                                                                            \
void assert_wrlock_##lock##_int(const char *file, const char *func, int line)                               \
{                                                                                                           \
    if (have_##lock##_writelock == 0) {                                                                     \
        logmsg(LOGMSG_FATAL, "%p:ASSERT-WRLOCK %s:%s:%d\n", (void *)pthread_self(), file, func, line);      \
        abort();                                                                                            \
    }                                                                                                       \
}                                                                                                           \
                                                                                                            \
void assert_rdlock_##lock##_int(const char *file, const char *func, int line)                               \
{                                                                                                           \
    if (have_##lock##_readlock == 0) {                                                                      \
        logmsg(LOGMSG_FATAL, "%p:ASSERT-RDLOCK %s:%s:%d\n", (void *)pthread_self(), file, func, line);      \
        abort();                                                                                            \
    }                                                                                                       \
}                                                                                                           \
                                                                                                            \
void assert_lock_##lock##_int(const char *file, const char *func, int line)                                 \
{                                                                                                           \
    if (have_##lock##_readlock == 0 && have_ ## lock ## _writelock == 0) {                                  \
        logmsg(LOGMSG_FATAL, "%p:ASSERT-LOCK %s:%s:%d\n", (void *)pthread_self(), file, func, line);        \
        abort();                                                                                            \
    }                                                                                                       \
}                                                                                                           \
                                                                                                            \
void assert_no_##lock##_int(const char *file, const char *func, int line)                                   \
{                                                                                                           \
    if (have_##lock##_readlock != 0 || have_##lock##_writelock != 0) {                                      \
        logmsg(LOGMSG_FATAL, "%p:ASSERT-NOLOCK %s:%s:%d\n", (void *)pthread_self(), file, func, line);      \
        abort();                                                                                            \
    }                                                                                                       \
}                                                                                                           \

make_rwlock(schema, thread_assert_nolocks)
make_rwlock(views, thread_assert_nolocks)
