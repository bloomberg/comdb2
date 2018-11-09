/*
   Copyright 2018 Bloomberg Finance L.P.

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

#ifndef _INCLUDED_LOCKS_H
#define _INCLUDED_LOCKS_H

#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include <logmsg.h>

#ifdef LOCK_DEBUG
#  define LKDBG_TRACE(STR, FUNC, OBJ) logmsg(LOGMSG_USER, "%s:%d " #STR " " #FUNC "(0x%"PRIxPTR") thd:%p\n", __func__, __LINE__, (uintptr_t)OBJ, (void *)pthread_self())
#else
#  define LKDBG_TRACE(...)
#endif

#define LKWRAP_FIRST_(a, ...) a
#define LKWRAP_FIRST(...) LKWRAP_FIRST_(__VA_ARGS__, 0)
#define WRAP_PTHREAD(FUNC, ...)                                                \
    do {                                                                       \
        int rc;                                                                \
        LKDBG_TRACE(TRY, FUNC, LKWRAP_FIRST(__VA_ARGS__));                     \
        if ((rc = FUNC(__VA_ARGS__)) != 0) {                                   \
            logmsg(LOGMSG_FATAL,                                               \
                   "%s:%d " #FUNC "(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n",      \
                   __func__, __LINE__, (uintptr_t)LKWRAP_FIRST(__VA_ARGS__),   \
                   rc, strerror(rc), (void *)pthread_self());                  \
            abort();                                                           \
        }                                                                      \
        LKDBG_TRACE(GOT, FUNC, LKWRAP_FIRST(__VA_ARGS__));                     \
    } while (0)

#define Pthread_mutex_init(a, b)    WRAP_PTHREAD(pthread_mutex_init, a, b)
#define Pthread_mutex_destroy(a)    WRAP_PTHREAD(pthread_mutex_destroy, a)
#define Pthread_mutex_lock(a)       WRAP_PTHREAD(pthread_mutex_lock, a)
#define Pthread_mutex_unlock(a)     WRAP_PTHREAD(pthread_mutex_unlock, a)
#define Pthread_rwlock_init(a, b)   WRAP_PTHREAD(pthread_rwlock_init, a, b)
#define Pthread_rwlock_destroy(a)   WRAP_PTHREAD(pthread_rwlock_destroy, a)
#define Pthread_rwlock_rdlock(a)    WRAP_PTHREAD(pthread_rwlock_rdlock, a)
#define Pthread_rwlock_wrlock(a)    WRAP_PTHREAD(pthread_rwlock_wrlock, a)
#define Pthread_rwlock_unlock(a)    WRAP_PTHREAD(pthread_rwlock_unlock, a)
#define Pthread_cond_init(a, b)     WRAP_PTHREAD(pthread_cond_init, a, b)
#define Pthread_cond_destroy(a)     WRAP_PTHREAD(pthread_cond_destroy, a)
#define Pthread_cond_signal(a)      WRAP_PTHREAD(pthread_cond_signal, a)
#define Pthread_cond_broadcast(a)   WRAP_PTHREAD(pthread_cond_broadcast, a)
#define Pthread_cond_wait(a, b)     WRAP_PTHREAD(pthread_cond_wait, a, b)
#define Pthread_attr_init(a)        WRAP_PTHREAD(pthread_attr_init, a)
#define Pthread_attr_destroy(a)     WRAP_PTHREAD(pthread_attr_destroy, a)
#define Pthread_key_create(a, b)    WRAP_PTHREAD(pthread_key_create, a, b)
#define Pthread_key_delete(a)       WRAP_PTHREAD(pthread_key_delete, a)
#define Pthread_setspecific(a, b)   WRAP_PTHREAD(pthread_setspecific, a, b)

#endif
