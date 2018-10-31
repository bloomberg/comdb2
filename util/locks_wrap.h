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
#ifndef _INCLUDED_LOCKS_H
#define _INCLUDED_LOCKS_H

#include "logmsg.h"

/* for completeness since pthread_cond_init never returns an error */
#define Pthread_cond_init pthread_cond_init

/* for completeness since pthread_cond_signal never returns an error */
#define Pthread_cond_signal pthread_cond_signal

/* for completeness since pthread_cond_broadcast never returns an error */
#define Pthread_cond_broadcast pthread_cond_broadcast

/* for completeness since pthread_cond_wait never returns an error */
#define Pthread_cond_wait pthread_cond_wait

#ifdef LOCK_DEBUG
#  define TRACE(STR, FUNC, OBJ) logmsg(LOGMSG_USER, "%s:%d " #STR " " #FUNC "(%p) thd:%p\n", __func__, __LINE__, OBJ, (void *)pthread_self())
#else
#  define TRACE(...)
#endif

#define WRAP_PTHREAD(FUNC, OBJ, ...)                                           \
    do {                                                                       \
        int rc;                                                                \
        TRACE(TRY, FUNC, OBJ);                                                 \
        if ((rc = FUNC(__VA_ARGS__)) != 0) {                                   \
            logmsg(LOGMSG_USER, "%s:%d " #FUNC "(%p) rc:%d thd:%p\n",          \
                   __func__, __LINE__, OBJ, rc, (void *)pthread_self());       \
            /*abort();*/                                                       \
        }                                                                      \
        TRACE(GOT, FUNC, OBJ);                                                 \
    } while (0)

#define Pthread_mutex_init(o, a)  WRAP_PTHREAD(pthread_mutex_init, o, o, a)
#define Pthread_mutex_destroy(o)  WRAP_PTHREAD(pthread_mutex_destroy, o, o)
#define Pthread_mutex_lock(o)     WRAP_PTHREAD(pthread_mutex_lock, o, o)
#define Pthread_mutex_unlock(o)   WRAP_PTHREAD(pthread_mutex_unlock, o, o)
#define Pthread_rwlock_init(o, a) WRAP_PTHREAD(pthread_rwlock_init, o, o, a)
#define Pthread_rwlock_destroy(o) WRAP_PTHREAD(pthread_rwlock_destroy, o, o)
#define Pthread_rwlock_rdlock(o)  WRAP_PTHREAD(pthread_rwlock_rdlock, o, o)
#define Pthread_rwlock_wrlock(o)  WRAP_PTHREAD(pthread_rwlock_wrlock, o, o)
#define Pthread_rwlock_unlock(o)  WRAP_PTHREAD(pthread_rwlock_unlock, o, o)

#endif
