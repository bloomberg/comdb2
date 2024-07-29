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

#ifndef _INCLUDED_SYSWRAP_H
#define _INCLUDED_SYSWRAP_H

#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include "logmsg.h"

#ifdef SYSWRAP_DEBUG
#define SYSWRAPDBG_TRACE(STR, FUNC, OBJ)                                                                               \
    logmsg(LOGMSG_USER, "%s:%d " #STR " " #FUNC "(0x%" PRIxPTR ") thd:%p\n", __func__, __LINE__, (uintptr_t)OBJ,       \
           (void *)pthread_self())
#else
#define SYSWRAPDBG_TRACE(...)
#endif

#define SYSWRAP_FIRST_(a, ...) a
#define SYSWRAP_FIRST(...) SYSWRAP_FIRST_(__VA_ARGS__, 0)
#define WRAP_SYSFUNC(FUNC, ...)                                                                                        \
    do {                                                                                                               \
        int rc;                                                                                                        \
        SYSWRAPDBG_TRACE(TRY, FUNC, SYSWRAP_FIRST(__VA_ARGS__));                                                       \
        if ((rc = FUNC(__VA_ARGS__)) != 0) {                                                                           \
            logmsg(LOGMSG_FATAL, "%s:%d " #FUNC "(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n", __func__, __LINE__,            \
                   (uintptr_t)SYSWRAP_FIRST(__VA_ARGS__), rc, strerror(rc), (void *)pthread_self());                   \
            abort();                                                                                                   \
        }                                                                                                              \
        SYSWRAPDBG_TRACE(GOT, FUNC, SYSWRAP_FIRST(__VA_ARGS__));                                                       \
    } while (0)

#define WRAP_SYSFUNC_ABORT_ERRNO(FUNC, ABORTERRNO, ...)                                                                \
    ({                                                                                                                 \
        int rc, save_errno;                                                                                            \
        SYSWRAPDBG_TRACE(TRY, FUNC, SYSWRAP_FIRST(__VA_ARGS__));                                                       \
        rc = FUNC(__VA_ARGS__);                                                                                        \
        save_errno = errno;                                                                                            \
        if (rc != 0 && save_errno == ABORTERRNO) {                                                                     \
            logmsg(LOGMSG_FATAL, "%s:%d " #FUNC "(0x%" PRIxPTR ") rc:%d (%s) thd:%p\n", __func__, __LINE__,            \
                   (uintptr_t)SYSWRAP_FIRST(__VA_ARGS__), rc, strerror(rc), (void *)pthread_self());                   \
            abort();                                                                                                   \
        }                                                                                                              \
        SYSWRAPDBG_TRACE(GOT, FUNC, SYSWRAP_FIRST(__VA_ARGS__));                                                       \
        errno = save_errno;                                                                                            \
        rc;                                                                                                            \
    })

#define Pthread_attr_destroy(...) WRAP_SYSFUNC(pthread_attr_destroy, __VA_ARGS__)
#define Pthread_attr_init(...) WRAP_SYSFUNC(pthread_attr_init, __VA_ARGS__)
#define Pthread_attr_setdetachstate(...) WRAP_SYSFUNC(pthread_attr_setdetachstate, __VA_ARGS__)
#define Pthread_attr_setstacksize(...) WRAP_SYSFUNC(pthread_attr_setstacksize, __VA_ARGS__)
#define Pthread_cond_broadcast(...) WRAP_SYSFUNC(pthread_cond_broadcast, __VA_ARGS__)
#define Pthread_cond_destroy(...) WRAP_SYSFUNC(pthread_cond_destroy, __VA_ARGS__)
#define Pthread_cond_init(...) WRAP_SYSFUNC(pthread_cond_init, __VA_ARGS__)
#define Pthread_cond_signal(...) WRAP_SYSFUNC(pthread_cond_signal, __VA_ARGS__)
#define Pthread_cond_wait(...) WRAP_SYSFUNC(pthread_cond_wait, __VA_ARGS__)
#define Pthread_create(...) WRAP_SYSFUNC(pthread_create, __VA_ARGS__)
#define Pthread_detach(...) WRAP_SYSFUNC(pthread_detach, __VA_ARGS__)
#define Pthread_join(...) WRAP_SYSFUNC(pthread_join, __VA_ARGS__)
#define Pthread_key_create(...) WRAP_SYSFUNC(pthread_key_create, __VA_ARGS__)
#define Pthread_key_delete(...) WRAP_SYSFUNC(pthread_key_delete, __VA_ARGS__)
#define Pthread_mutex_destroy(...) WRAP_SYSFUNC(pthread_mutex_destroy, __VA_ARGS__)
#define Pthread_mutex_init(...) WRAP_SYSFUNC(pthread_mutex_init, __VA_ARGS__)
#define Pthread_mutex_lock(...) WRAP_SYSFUNC(pthread_mutex_lock, __VA_ARGS__)
#define Pthread_mutex_unlock(...) WRAP_SYSFUNC(pthread_mutex_unlock, __VA_ARGS__)
#define Pthread_once(...) WRAP_SYSFUNC(pthread_once, __VA_ARGS__)
#define Pthread_rwlock_destroy(...) WRAP_SYSFUNC(pthread_rwlock_destroy, __VA_ARGS__)
#define Pthread_rwlock_init(...) WRAP_SYSFUNC(pthread_rwlock_init, __VA_ARGS__)
#define Pthread_rwlock_rdlock(...) WRAP_SYSFUNC(pthread_rwlock_rdlock, __VA_ARGS__)
#define Pthread_rwlock_unlock(...) WRAP_SYSFUNC(pthread_rwlock_unlock, __VA_ARGS__)
#define Pthread_rwlock_wrlock(...) WRAP_SYSFUNC(pthread_rwlock_wrlock, __VA_ARGS__)
#define Pthread_setspecific(...) WRAP_SYSFUNC(pthread_setspecific, __VA_ARGS__)
#define Close(...) WRAP_SYSFUNC_ABORT_ERRNO(close, EBADF, __VA_ARGS__)

extern void comdb2_name_thread(const char *name);

#endif
