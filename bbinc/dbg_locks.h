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

#if defined(DBG_PTHREAD_LOCKS)
#ifndef DBG_LOCKS_H
#define DBG_LOCKS_H

#include "dbg_locks_core.h"
#include "pthread_wrap.h"

#undef Pthread_mutex_lock
#undef Pthread_mutex_trylock
#undef Pthread_mutex_timedlock
#undef Pthread_mutex_unlock
#undef Pthread_rwlock_rdlock
#undef Pthread_rwlock_wrlock
#undef Pthread_rwlock_tryrdlock
#undef Pthread_rwlock_trywrlock
#undef Pthread_rwlock_timedrdlock
#undef Pthread_rwlock_timedwrlock
#undef Pthread_rwlock_unlock

#define Pthread_mutex_lock(a)                                                  \
    WRAP_PTHREAD(dbg_pthread_mutex_lock, a, __FILE__, __func__, __LINE__)

#define Pthread_mutex_unlock(a)                                                \
    WRAP_PTHREAD(dbg_pthread_mutex_unlock, a, __FILE__, __func__, __LINE__)

#define Pthread_rwlock_rdlock(a)                                               \
    WRAP_PTHREAD(dbg_pthread_rwlock_rdlock, a, __FILE__, __func__, __LINE__)

#define Pthread_rwlock_wrlock(a)                                               \
    WRAP_PTHREAD(dbg_pthread_rwlock_wrlock, a, __FILE__, __func__, __LINE__)

#define Pthread_rwlock_unlock(a)                                               \
    WRAP_PTHREAD(dbg_pthread_rwlock_unlock, a, __FILE__, __func__, __LINE__)

#define Pthread_mutex_trylock(a)                                               \
    dbg_pthread_mutex_trylock(a, __FILE__, __func__, __LINE__)

#define Pthread_mutex_timedlock(a, b)                                          \
    dbg_pthread_mutex_timedlock(a, b, __FILE__, __func__, __LINE__)

#define Pthread_rwlock_tryrdlock(a)                                            \
    dbg_pthread_rwlock_tryrdlock(a, __FILE__, __func__, __LINE__)

#define Pthread_rwlock_trywrlock(a)                                            \
    dbg_pthread_rwlock_trywrlock(a, __FILE__, __func__, __LINE__)

#define Pthread_rwlock_timedrdlock(a, b)                                       \
    dbg_pthread_rwlock_timedrdlock(a, b, __FILE__, __func__, __LINE__)

#define Pthread_rwlock_timedwrlock(a, b)                                       \
    dbg_pthread_rwlock_timedwrlock(a, b, __FILE__, __func__, __LINE__)

#endif /* DBG_LOCKS_H */
#endif /* defined(DBG_PTHREAD_LOCKS) */
