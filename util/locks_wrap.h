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
#include "logmsg.h"
#include <thread_util.h>

struct debug_rw_ref {
    const char *file;
    int line;
    int ref;
    arch_tid tid;
};

struct debug_rwlock {
    pthread_rwlock_t lk;
    pthread_mutex_t lklk;
    struct debug_rw_ref *readrefs;
    struct debug_rw_ref writeref;
    int nreadrefs;
    int warn_deref;
};


#ifndef DEBUG_RW_LOCKS
typedef pthread_rwlock_t Pthread_rwlock_t;
#else
typedef struct debug_rwlock Pthread_rwlock_t;
#endif



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

#define Pthread_attr_destroy(...)           WRAP_PTHREAD(pthread_attr_destroy, __VA_ARGS__)
#define Pthread_attr_init(...)              WRAP_PTHREAD(pthread_attr_init, __VA_ARGS__)
#define Pthread_attr_setdetachstate(...)    WRAP_PTHREAD(pthread_attr_setdetachstate, __VA_ARGS__)
#define Pthread_attr_setstacksize(...)      WRAP_PTHREAD(pthread_attr_setstacksize, __VA_ARGS__)
#define Pthread_cond_broadcast(...)         WRAP_PTHREAD(pthread_cond_broadcast, __VA_ARGS__)
#define Pthread_cond_destroy(...)           WRAP_PTHREAD(pthread_cond_destroy, __VA_ARGS__)
#define Pthread_cond_init(...)              WRAP_PTHREAD(pthread_cond_init, __VA_ARGS__)
#define Pthread_cond_signal(...)            WRAP_PTHREAD(pthread_cond_signal, __VA_ARGS__)
#define Pthread_cond_wait(...)              WRAP_PTHREAD(pthread_cond_wait, __VA_ARGS__)
#define Pthread_create(...)                 WRAP_PTHREAD(pthread_create, __VA_ARGS__)
#define Pthread_detach(...)                 WRAP_PTHREAD(pthread_detach, __VA_ARGS__)
#define Pthread_join(...)                   WRAP_PTHREAD(pthread_join, __VA_ARGS__)
#define Pthread_key_create(...)             WRAP_PTHREAD(pthread_key_create, __VA_ARGS__)
#define Pthread_key_delete(...)             WRAP_PTHREAD(pthread_key_delete, __VA_ARGS__)
#define Pthread_mutex_destroy(...)          WRAP_PTHREAD(pthread_mutex_destroy, __VA_ARGS__)
#define Pthread_mutex_init(...)             WRAP_PTHREAD(pthread_mutex_init, __VA_ARGS__)
#define Pthread_mutex_lock(...)             WRAP_PTHREAD(pthread_mutex_lock, __VA_ARGS__)
#define Pthread_mutex_unlock(...)           WRAP_PTHREAD(pthread_mutex_unlock, __VA_ARGS__)
#define Pthread_once(...)                   WRAP_PTHREAD(pthread_once, __VA_ARGS__)

#ifndef DEBUG_RW_LOCKS

#define Pthread_rwlock_destroy(...)         WRAP_PTHREAD(pthread_rwlock_destroy, __VA_ARGS__)
#define Pthread_rwlock_init(...)            WRAP_PTHREAD(pthread_rwlock_init, __VA_ARGS__)
#define Pthread_rwlock_rdlock(...)          WRAP_PTHREAD(pthread_rwlock_rdlock, __VA_ARGS__)
#define Pthread_rwlock_unlock(...)          WRAP_PTHREAD(pthread_rwlock_unlock, __VA_ARGS__)
#define Pthread_rwlock_wrlock(...)          WRAP_PTHREAD(pthread_rwlock_wrlock, __VA_ARGS__)
#define Pthread_rwlock_trywrlock(...)       pthread_rwlock_trywrlock(__VA_ARGS__)
#define Pthread_rwlock_tryrdlock(...)       pthread_rwlock_tryrdlock(__VA_ARGS__)
#define PPTHREAD_RWLOCK_INITIALIZER PTHREAD_RWLOCK_INITIALIZER

#else

extern int debug_rwlock_destroy(struct debug_rwlock *lk);
extern int debug_rwlock_init(struct debug_rwlock *rwlock, const pthread_rwlockattr_t *attr);
extern int debug_rwlock_rdlock(struct debug_rwlock *rwlock, const char *file, int line);
extern int debug_rwlock_unlock(struct debug_rwlock *rwlock);
extern int debug_rwlock_wrlock(struct debug_rwlock *rwlock, const char *file, int line);
extern int debug_rwlock_tryrdlock(struct debug_rwlock *rwlock, const char *file, int line);
extern int debug_rwlock_trywrlock(struct debug_rwlock *rwlock, const char *file, int line);
#define Pthread_rwlock_destroy(lk) debug_rwlock_destroy(lk)
#define Pthread_rwlock_init(lk, attr) debug_rwlock_init(lk, attr)
#define Pthread_rwlock_rdlock(lk) debug_rwlock_rdlock(lk, __FILE__, __LINE__)
#define Pthread_rwlock_unlock(lk) debug_rwlock_unlock(lk)
#define Pthread_rwlock_wrlock(lk) debug_rwlock_wrlock(lk, __FILE__, __LINE__)
#define Pthread_rwlock_trywrlock(lk) debug_rwlock_wrlock(lk, __FILE__, __LINE__)
#define Pthread_rwlock_tryrdlock(lk) debug_rwlock_tryrdlock(lk, __FILE__, __LINE__)
#define PPTHREAD_RWLOCK_INITIALIZER { PTHREAD_RWLOCK_INITIALIZER, PTHREAD_MUTEX_INITIALIZER, NULL, {NULL, 0, 0, 0}, 0 }

#endif

#define Pthread_setspecific(...)            WRAP_PTHREAD(pthread_setspecific, __VA_ARGS__)

extern void comdb2_name_thread(const char *name);

#endif
